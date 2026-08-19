// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Retrying user-initiated splices that fail for recoverable reasons while the node runs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bitcoin::secp256k1::PublicKey;
use bitcoin::{Amount, OutPoint, TxOut};
use lightning::chain::transaction::OutPoint as LdkOutPoint;
use lightning::events::NegotiationFailureReason;
use lightning::ln::channel_state::{SpliceCandidateDetails, SpliceCandidateStatus};
use lightning::ln::channelmanager::PaymentId;
use lightning::ln::funding::FundingContribution;
use lightning::ln::types::ChannelId;
use lightning::util::errors::APIError;

use crate::data_store::StorableObject;
use crate::event::{Event, EventQueue};
use crate::fee_estimator::{
	max_funding_feerate, ConfirmationTarget, FeeEstimator, OnchainFeeEstimator,
};
use crate::logger::{log_error, log_info, LdkLogger, Logger};
use crate::payment::pending_payment_store::{
	PendingPaymentDetails, PendingPaymentDetailsUpdate, SpliceIntent, SpliceKind,
};
use crate::payment::store::PaymentDetails;
use crate::payment::PaymentStatus;
use crate::types::{ChannelManager, PaymentStore, PendingPaymentStore, UserChannelId, Wallet};
use crate::wallet::random_payment_id;
use crate::Error;

/// The number of times a splice contribution is resubmitted to LDK before the splice is given up
/// on and the failure surfaced to the user.
const MAX_SPLICE_ATTEMPTS: u8 = 3;

/// The action to take on a `SpliceNegotiationFailed` for a splice we track, decided purely from
/// the failure `reason`, the attempt count, and whether the originating parameters are available,
/// so the decision matrix can be unit-tested without a live channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryDecision {
	/// Give up: stop tracking the splice and surface the failure to the user.
	Abandon,
	/// Resubmit the stored contribution unchanged (the originating parameters are unavailable, so
	/// a fresh contribution cannot be built).
	ResubmitStored,
	/// Rebuild a fresh contribution from the originating parameters, picking up current fee rates
	/// and wallet state.
	Rebuild,
}

fn decide_retry(
	reason: &NegotiationFailureReason, attempts: u8, can_rebuild: bool,
) -> RetryDecision {
	if !reason.is_retriable() || attempts >= MAX_SPLICE_ATTEMPTS {
		return RetryDecision::Abandon;
	}
	// Rebuilding picks up current fee rates and wallet state, so prefer it whenever the
	// originating parameters are available. Only a splice adopted from a replayed event cannot be
	// rebuilt; its stored contribution is resubmitted unchanged.
	if can_rebuild {
		RetryDecision::Rebuild
	} else {
		RetryDecision::ResubmitStored
	}
}

/// A user-initiated splice tracked for retry: everything needed to hand its contribution back to
/// LDK, or to rebuild the contribution from the originating call's parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingSplice {
	/// The channel being spliced.
	channel_id: ChannelId,
	/// The channel counterparty.
	counterparty_node_id: PublicKey,
	/// The channel's funding outpoint when the splice was initiated. It only changes once a splice
	/// locks, so a newly locked funding at a different outpoint means this splice (or a
	/// replacement) completed and there is nothing left to resubmit.
	pre_splice_funding_txo: OutPoint,
	/// The contribution handed to [`ChannelManager::funding_contributed`], resubmitted verbatim
	/// when a fresh contribution cannot be built.
	///
	/// [`ChannelManager::funding_contributed`]: lightning::ln::channelmanager::ChannelManager::funding_contributed
	contribution: FundingContribution,
	/// The parameters of the originating API call, used to rebuild a fresh contribution at retry
	/// time. `None` for a splice adopted from a failure event LDK replayed after a restart: its
	/// contribution can be resubmitted, but the originating parameters did not survive the
	/// restart, so it cannot be rebuilt.
	kind: Option<SpliceKind>,
	/// The number of times the contribution has been resubmitted to LDK after the originating API
	/// call handed it off.
	attempts: u8,
	/// Whether the synchronous hand-off of the contribution to LDK was rejected. The originating
	/// call already surfaced that error, so a failure event the rejection may have enqueued must
	/// be consumed rather than reported a second time or retried.
	rejected: bool,
	/// The [`PaymentId`] of the persisted [`SpliceIntent`] record backing this splice, or `None`
	/// for a splice adopted from a replayed failure event, which is tracked in memory only: the
	/// event replay that produced it already covers a further restart.
	payment_id: Option<PaymentId>,
}

/// What a `SpliceNegotiationFailed` means for the splice we track for its channel, decided purely
/// from the tracked state and the event's payload so it can be unit-tested without a live channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureClass {
	/// The failure is final for us: surface it to the user.
	Surface,
	/// Nothing is tracked but the failure is recoverable and carries its contribution: start
	/// tracking it (a failure event LDK replayed after a restart, whose registration did not
	/// survive the restart).
	Adopt,
	/// The failure belongs to a contribution whose synchronous hand-off to LDK was rejected: the
	/// originating call already surfaced that error, so the event is dropped.
	Consume,
	/// The failure concerns the tracked splice: the retry matrix decides.
	Tracked(RetryDecision),
}

/// Whether two contributions describe the same splice attempt. LDK may adjust a contribution
/// during negotiation — the quiescence tie-breaker rebuilds the acceptor's copy at a fresh
/// feerate, touching only its fee fields and change value — so fees and feerates do not identify
/// an attempt. Its inputs and outputs do: they are what the user asked to move. Contributions
/// carrying neither (channel-balance-only attempts) fall back to full equality.
fn is_same_splice(a: &FundingContribution, b: &FundingContribution) -> bool {
	if a.inputs().is_empty()
		&& a.outputs().is_empty()
		&& b.inputs().is_empty()
		&& b.outputs().is_empty()
	{
		return a == b;
	}
	a.inputs().iter().map(|i| i.outpoint()).eq(b.inputs().iter().map(|i| i.outpoint()))
		&& a.outputs() == b.outputs()
}

fn classify_failure(
	tracked: Option<&PendingSplice>, reason: &NegotiationFailureReason,
	contribution: Option<&FundingContribution>,
) -> FailureClass {
	match tracked {
		Some(splice) => {
			// Only act on failures of the splice we are tracking. A mismatch means the failure
			// concerns some other attempt (e.g. a stale event replayed after a newer splice was
			// initiated).
			if !contribution.is_some_and(|c| is_same_splice(c, &splice.contribution)) {
				FailureClass::Surface
			} else if splice.rejected {
				FailureClass::Consume
			} else {
				FailureClass::Tracked(decide_retry(reason, splice.attempts, splice.kind.is_some()))
			}
		},
		None => {
			if reason.is_retriable() && contribution.is_some() {
				FailureClass::Adopt
			} else {
				FailureClass::Surface
			}
		},
	}
}

/// The splices being tracked for retry, keyed by the channel's `user_channel_id` — the identifier
/// the splice entry points take and the one LDK echoes back on its channel events. Kept separate
/// from the retrier so the tracking lifecycle is testable without a live [`ChannelManager`].
#[derive(Default)]
struct SpliceRegistry {
	splices: Mutex<HashMap<u128, PendingSplice>>,
}

impl SpliceRegistry {
	/// Tracks `splice`, superseding any tracked one for the channel.
	fn register(&self, user_channel_id: UserChannelId, splice: PendingSplice) {
		self.splices.lock().unwrap().insert(user_channel_id.0, splice);
	}

	/// Removes the tracked splice if it still carries `contribution`; a mismatch means a newer
	/// splice took the channel's slot and must stay tracked.
	fn unregister(&self, user_channel_id: UserChannelId, contribution: &FundingContribution) {
		let mut splices = self.splices.lock().unwrap();
		if splices
			.get(&user_channel_id.0)
			.is_some_and(|s| is_same_splice(&s.contribution, contribution))
		{
			splices.remove(&user_channel_id.0);
		}
	}

	fn get(&self, user_channel_id: UserChannelId) -> Option<PendingSplice> {
		self.splices.lock().unwrap().get(&user_channel_id.0).cloned()
	}

	/// Counts a resubmission: bumps the attempt count and swaps in the contribution being
	/// resubmitted, but only while the tracked splice still carries `prior` — otherwise the splice
	/// moved on (locked, closed, or superseded) and there is nothing to resubmit.
	fn record_attempt(
		&self, user_channel_id: UserChannelId, prior: &FundingContribution,
		contribution: FundingContribution,
	) -> bool {
		let mut splices = self.splices.lock().unwrap();
		match splices.get_mut(&user_channel_id.0) {
			Some(splice) if is_same_splice(&splice.contribution, prior) => {
				splice.attempts += 1;
				splice.contribution = contribution;
				true
			},
			_ => false,
		}
	}

	/// Clears a tracked splice that a newly locked funding transaction made obsolete. A tracked
	/// splice whose pre-splice outpoint matches the locked funding was created after the lock and
	/// stays.
	fn clear_if_obsoleted(&self, user_channel_id: UserChannelId, locked_funding_txo: OutPoint) {
		let mut splices = self.splices.lock().unwrap();
		if splices
			.get(&user_channel_id.0)
			.is_some_and(|s| s.pre_splice_funding_txo != locked_funding_txo)
		{
			splices.remove(&user_channel_id.0);
		}
	}

	/// Clears any tracked splice for the channel.
	fn clear(&self, user_channel_id: UserChannelId) {
		self.splices.lock().unwrap().remove(&user_channel_id.0);
	}
}

/// Retries user-initiated splices that fail for recoverable reasons while the node runs.
///
/// LDK abandons an in-progress splice negotiation whenever the peer disconnects and reports the
/// failure through `SpliceNegotiationFailed`. The splice entry points register each splice here
/// before handing its contribution to LDK; a recoverable failure is then driven back into
/// [`ChannelManager::funding_contributed`] until the splice either locks or fails for a reason
/// retrying cannot address, at which point the failure is surfaced.
///
/// Resubmitting does not require the peer to be connected: LDK holds on to the contribution and
/// initiates quiescence once the peer reconnects.
///
/// Each tracked splice is also persisted as a [`SpliceIntent`]: written before the contribution
/// is handed to LDK, updated on every resubmission, and cleared once the splice locks, its
/// channel closes, or the failure is surfaced. At startup, [`Self::seed`] resumes tracking the
/// persisted intents — so failure events LDK replays find their splice tracked — and
/// [`Self::reconcile`] resubmits any whose splice LDK dropped before durably recording it,
/// including those lost to a crash before LDK persisted anything. LDK likewise persists the
/// failure events themselves, so a failure that was never handled while running — e.g. a
/// disconnect during shutdown — replays after a restart; a failure replayed with nothing
/// persisted behind it (see [`Self::on_negotiation_failed`] on the torn give-up window) is
/// adopted and tracked in memory only, since the event replay that produced it already covers a
/// further restart.
///
/// [`ChannelManager::funding_contributed`]: lightning::ln::channelmanager::ChannelManager::funding_contributed
pub(crate) struct SpliceRetrier {
	channel_manager: Arc<ChannelManager>,
	wallet: Arc<Wallet>,
	fee_estimator: Arc<OnchainFeeEstimator>,
	pending_payment_store: Arc<PendingPaymentStore>,
	payment_store: Arc<PaymentStore>,
	event_queue: Arc<EventQueue<Arc<Logger>>>,
	registry: SpliceRegistry,
	/// Serializes [`Self::submit`]'s persist-register-and-hand-off sequence with
	/// [`Self::on_negotiation_failed`]'s snapshot of the tracked splice. Without it, the failure
	/// event of a synchronously rejected hand-off could be classified while the splice is still
	/// cleanly registered — resubmitting a contribution whose originating call already returned
	/// an error.
	submit_lock: tokio::sync::Mutex<()>,
	logger: Arc<Logger>,
}

impl SpliceRetrier {
	pub(crate) fn new(
		channel_manager: Arc<ChannelManager>, wallet: Arc<Wallet>,
		fee_estimator: Arc<OnchainFeeEstimator>, pending_payment_store: Arc<PendingPaymentStore>,
		payment_store: Arc<PaymentStore>, event_queue: Arc<EventQueue<Arc<Logger>>>,
		logger: Arc<Logger>,
	) -> Self {
		Self {
			channel_manager,
			wallet,
			fee_estimator,
			pending_payment_store,
			payment_store,
			event_queue,
			registry: SpliceRegistry::default(),
			submit_lock: tokio::sync::Mutex::new(()),
			logger,
		}
	}

	/// Resumes tracking the persisted splice intents. Must run before event processing starts, so
	/// a failure event LDK replays finds its splice tracked — classified against the persisted
	/// attempt count and give-up-capable of clearing the intent — rather than adopted as a fresh,
	/// memory-only splice with a reset attempt budget.
	pub(crate) fn seed(&self) {
		let records = self.pending_payment_store.list_filter(|p| p.splice_intent().is_some());
		for record in records {
			let Some(intent) = record.splice_intent() else {
				continue;
			};
			let splice = PendingSplice {
				channel_id: intent.channel_id,
				counterparty_node_id: intent.counterparty_node_id,
				pre_splice_funding_txo: intent.pre_splice_funding_txo.into_bitcoin_outpoint(),
				contribution: intent.contribution.clone(),
				kind: Some(intent.kind.clone()),
				attempts: intent.attempts,
				rejected: false,
				payment_id: Some(record.id()),
			};
			self.registry.register(intent.user_channel_id, splice);
		}
	}

	/// Reconciles the persisted splice intents against live channel state, resubmitting any splice
	/// LDK dropped before durably recording it — including one lost to a crash before LDK
	/// persisted anything, for which no failure event will ever replay. Run once at startup, after
	/// [`Self::seed`].
	pub(crate) async fn reconcile(&self) {
		let records = self.pending_payment_store.list_filter(|p| p.splice_intent().is_some());
		for record in records {
			let payment_id = record.id();
			let Some(intent) = record.splice_intent().cloned() else {
				continue;
			};
			let user_channel_id = intent.user_channel_id;

			let channel = self
				.channel_manager
				.list_channels_with_counterparty(&intent.counterparty_node_id)
				.into_iter()
				.find(|c| c.user_channel_id == user_channel_id.0);
			let channel = match channel {
				Some(channel) => channel,
				None => {
					// The channel is gone; there is nothing to splice anymore.
					self.registry.clear(user_channel_id);
					self.clear_persisted_intent(payment_id).await;
					continue;
				},
			};

			if channel.funding_txo != Some(intent.pre_splice_funding_txo) {
				// The funding moved on, so the splice (or a replacement) locked.
				self.registry.clear(user_channel_id);
				self.clear_persisted_intent(payment_id).await;
				continue;
			}

			let candidates = channel
				.splice_details
				.as_ref()
				.map(|details| details.candidates.as_slice())
				.unwrap_or(&[]);
			match decide_reconcile(&intent, candidates) {
				ReconcileDecision::Keep => continue,
				ReconcileDecision::Abandon => {
					self.abandon(payment_id, &intent).await;
					continue;
				},
				ReconcileDecision::Resubmit => {},
			}

			if intent.attempts >= MAX_SPLICE_ATTEMPTS {
				self.abandon(payment_id, &intent).await;
				continue;
			}

			// The tracked splice may already have moved on — cleared by a `ChannelReady` or
			// superseded by a replayed failure event processed concurrently — in which case
			// whatever moved it owns it now.
			let Some(splice) = self.registry.get(user_channel_id) else {
				continue;
			};
			// The stored contribution is resubmitted verbatim: reconciliation restores the
			// pre-crash hand-off exactly as the originating call made it. Any staleness is then
			// handled the same way it would have been without the restart — a retriable failure
			// comes back and the retry rebuilds, as the seeded splice carries its parameters.
			log_info!(
				self.logger,
				"Resubmitting splice for channel {} with counterparty {}",
				intent.channel_id,
				intent.counterparty_node_id,
			);
			if self.resubmit(user_channel_id, splice, None).await {
				self.abandon(payment_id, &intent).await;
			}
		}
	}

	/// Gives up on a persisted splice intent and surfaces the failure to the user. The splice
	/// stays in the registry so a failure event LDK later replays for it is classified against
	/// the tracked attempt count instead of being adopted afresh with a reset budget.
	async fn abandon(&self, payment_id: PaymentId, intent: &SpliceIntent) {
		log_error!(
			self.logger,
			"Abandoning splice for channel {} with counterparty {}",
			intent.channel_id,
			intent.counterparty_node_id,
		);
		self.clear_persisted_intent(payment_id).await;
		let event = Event::SpliceNegotiationFailed {
			channel_id: intent.channel_id,
			user_channel_id: intent.user_channel_id,
			counterparty_node_id: intent.counterparty_node_id,
		};
		if let Err(e) = self.event_queue.add_event(event).await {
			log_error!(self.logger, "Failed to push to event queue: {}", e);
		}
	}

	/// Tracks a user-initiated splice — persisting its intent — and hands its contribution to
	/// [`ChannelManager::funding_contributed`]. The intent is persisted and tracking starts
	/// before the hand-off, so a crash in between is covered and a failure event cannot arrive
	/// before the splice it concerns is tracked. A newer splice supersedes whatever was tracked
	/// for the channel: at most one splice is ever in flight per channel, and a fee bump replaces
	/// the splice it bumps.
	///
	/// On a synchronous rejection the error is returned for the caller to surface and the
	/// persisted intent is undone, but the splice stays tracked marked rejected: LDK may still
	/// enqueue a `SpliceNegotiationFailed` for the rejected contribution, which must be consumed
	/// rather than reported a second time.
	///
	/// [`ChannelManager::funding_contributed`]: lightning::ln::channelmanager::ChannelManager::funding_contributed
	pub(crate) async fn submit(
		&self, user_channel_id: UserChannelId, counterparty_node_id: PublicKey,
		channel_id: ChannelId, pre_splice_funding_txo: LdkOutPoint,
		contribution: FundingContribution, kind: SpliceKind,
	) -> Result<(), Error> {
		let _guard = self.submit_lock.lock().await;
		let intent = SpliceIntent {
			user_channel_id,
			counterparty_node_id,
			channel_id,
			pre_splice_funding_txo,
			contribution: contribution.clone(),
			kind: kind.clone(),
			attempts: 0,
		};
		// A splice whose intent cannot be persisted is not attempted at all, rather than
		// attempted without restart coverage.
		let (payment_id, restore) = self.persist_intent(intent).await.map_err(|e| {
			log_error!(
				self.logger,
				"Failed to persist the splice intent for channel {} with counterparty {}: {:?}",
				channel_id,
				counterparty_node_id,
				e,
			);
			e
		})?;
		let splice = PendingSplice {
			channel_id,
			counterparty_node_id,
			pre_splice_funding_txo: pre_splice_funding_txo.into_bitcoin_outpoint(),
			contribution: contribution.clone(),
			kind: Some(kind),
			attempts: 0,
			rejected: false,
			payment_id: Some(payment_id),
		};
		self.registry.register(user_channel_id, splice.clone());
		if let Err(e) = self.channel_manager.funding_contributed(
			&channel_id,
			&counterparty_node_id,
			contribution,
			None,
		) {
			log_error!(
				self.logger,
				"LDK rejected the splice contribution for channel {} with counterparty {}: {:?}",
				channel_id,
				counterparty_node_id,
				e,
			);
			self.registry.register(user_channel_id, PendingSplice { rejected: true, ..splice });
			// The caller surfaces the rejection, so the splice must not be resubmitted after a
			// restart: undo the persisted intent.
			self.discard_persisted_intent(&payment_id, restore).await;
			return Err(Error::ChannelSplicingFailed);
		}
		Ok(())
	}

	/// Persists `intent` before its contribution is handed to LDK, so the splice can be
	/// resubmitted if LDK drops it before durably recording it (a restart, or a disconnect
	/// mid-negotiation).
	///
	/// Reuses the channel's existing splice intent record when one is present — so a splice and
	/// its later fee bumps share one [`PaymentId`] and at most one intent ever exists per
	/// channel, which `Wallet::find_splice_payment_id` relies on — otherwise generates a fresh
	/// id. Returns the id and, for restoring on a rejected hand-off, `None` when a fresh record
	/// was created or `Some(prior)` when an existing record's intent was replaced.
	async fn persist_intent(
		&self, intent: SpliceIntent,
	) -> Result<(PaymentId, Option<Option<SpliceIntent>>), Error> {
		let existing = self
			.pending_payment_store
			.list_filter(|p| {
				p.splice_intent().is_some_and(|i| i.user_channel_id == intent.user_channel_id)
			})
			.into_iter()
			.next();
		match existing {
			Some(record) => {
				let payment_id = record.id();
				let prior = record.splice_intent().cloned();
				self.pending_payment_store
					.update(PendingPaymentDetailsUpdate {
						id: payment_id,
						payment_update: None,
						conflicting_txids: None,
						candidates: Vec::new(),
						splice_intent: Some(Some(intent)),
					})
					.await?;
				Ok((payment_id, Some(prior)))
			},
			None => {
				let payment_id = random_payment_id();
				self.pending_payment_store
					.insert(PendingPaymentDetails::pending_splice(payment_id, intent))
					.await?;
				Ok((payment_id, None))
			},
		}
	}

	/// Undoes a splice intent persisted for a hand-off LDK then synchronously rejected: restores
	/// an existing record's prior intent, or removes a freshly created record.
	async fn discard_persisted_intent(
		&self, payment_id: &PaymentId, restore: Option<Option<SpliceIntent>>,
	) {
		let result = match restore {
			Some(prior) => self
				.pending_payment_store
				.update(PendingPaymentDetailsUpdate {
					id: *payment_id,
					payment_update: None,
					conflicting_txids: None,
					candidates: Vec::new(),
					splice_intent: Some(prior),
				})
				.await
				.map(|_| ()),
			None => self.pending_payment_store.remove(payment_id).await,
		};
		if let Err(e) = result {
			log_error!(
				self.logger,
				"Failed to undo the intent of rejected splice payment {}: it may be resubmitted \
				after a restart: {}",
				payment_id,
				e,
			);
		}
	}

	/// Clears the persisted intent behind a splice that locked, was given up on, or whose channel
	/// closed: removes a record with no classified funding payment behind it entirely, or keeps
	/// the record — with the intent cleared — when a payment exists, so the payment keeps
	/// graduating.
	async fn clear_persisted_intent(&self, payment_id: PaymentId) {
		let Some(record) = self.pending_payment_store.get(&payment_id) else {
			return;
		};
		let has_payment = record.details().is_some()
			|| self
				.payment_store
				.get(&payment_id)
				.is_some_and(|details| details.status == PaymentStatus::Pending);
		let result = if has_payment {
			self.pending_payment_store
				.mutate(&payment_id, |existing| {
					record_with_intent_cleared(existing, self.payment_store.get(&payment_id))
				})
				.await
				.map(|_| ())
		} else {
			self.pending_payment_store.remove(&payment_id).await
		};
		if let Err(e) = result {
			log_error!(
				self.logger,
				"Failed to clear the persisted intent of splice payment {}: the splice may be \
				resubmitted after a restart: {}",
				payment_id,
				e,
			);
		}
	}

	/// Applies a `SpliceNegotiationFailed` to any tracked splice, retrying recoverable failures.
	/// Returns whether the failure should be surfaced to the user (i.e. the splice is given up
	/// on).
	pub(crate) async fn on_negotiation_failed(
		&self, channel_id: ChannelId, user_channel_id: UserChannelId,
		counterparty_node_id: PublicKey, reason: NegotiationFailureReason,
		contribution: Option<FundingContribution>,
	) -> bool {
		let tracked = {
			// Serialize with `submit`: a synchronously rejected hand-off must finish marking its
			// entry rejected before the failure event it may have enqueued is classified.
			let _guard = self.submit_lock.lock().await;
			self.registry.get(user_channel_id)
		};
		let (splice, decision) =
			match classify_failure(tracked.as_ref(), &reason, contribution.as_ref()) {
				FailureClass::Surface => return true,
				FailureClass::Consume => return false,
				FailureClass::Adopt => {
					let contribution =
						contribution.expect("a failure without a contribution is never adopted");
					match self.adopt(
						channel_id,
						user_channel_id,
						counterparty_node_id,
						contribution,
					) {
						Some(splice) => {
							let decision =
								decide_retry(&reason, splice.attempts, splice.kind.is_some());
							(splice, decision)
						},
						None => return true,
					}
				},
				FailureClass::Tracked(decision) => {
					(tracked.expect("a tracked failure has a registered splice"), decision)
				},
			};

		let surface = match decision {
			RetryDecision::Abandon => {
				log_error!(
					self.logger,
					"Abandoning splice for channel {} with counterparty {}",
					splice.channel_id,
					splice.counterparty_node_id,
				);
				// The splice deliberately stays tracked on every give-up path: the caller queues
				// the user-facing event only after this returns and has LDK replay the failure
				// event if that write fails, and re-classifying a replay with the entry intact
				// converges on the same give-up — whereas unregistering would make the replay
				// adopt the splice afresh with a reset attempt budget. The entry is cleaned up by
				// a superseding registration, `ChannelReady`, or `ChannelClosed`.
				true
			},
			RetryDecision::ResubmitStored => {
				// Probe whether the channel can take a contribution at all: it cannot when it is
				// gone, mid-shutdown, out of balance for the fees, or the peer no longer supports
				// splicing. Retrying cannot help then, and dropping the failure would leave the
				// user unaware their splice is dead — give up and surface it.
				if let Err(e) = self
					.channel_manager
					.splice_channel(&splice.channel_id, &splice.counterparty_node_id)
				{
					log_error!(
						self.logger,
						"Giving up on splice for channel {} with counterparty {}: {:?}",
						splice.channel_id,
						splice.counterparty_node_id,
						e,
					);
					true
				} else {
					log_info!(
						self.logger,
						"Resubmitting splice for channel {} with counterparty {} after a recoverable failure",
						splice.channel_id,
						splice.counterparty_node_id,
					);
					self.resubmit(user_channel_id, splice.clone(), None).await
				}
			},
			RetryDecision::Rebuild => {
				let kind = splice.kind.clone().expect(
					"a rebuild is only decided when the originating parameters are available",
				);
				let rebuilt = match self
					.rebuild_contribution(&splice.channel_id, &splice.counterparty_node_id, &kind)
					.await
				{
					Ok(contribution) => Some(contribution),
					Err(e) => {
						// The wallet may no longer be able to fund a fresh contribution (or the
						// channel may be gone, which the resubmission below then surfaces). The
						// stored contribution is still worth handing back rather than giving up.
						log_error!(
							self.logger,
							"Failed to rebuild the splice contribution for channel {}, falling \
							back to the stored one: {:?}",
							splice.channel_id,
							e,
						);
						None
					},
				};
				log_info!(
					self.logger,
					"Resubmitting {} splice contribution for channel {} with counterparty {} after \
					a recoverable failure",
					if rebuilt.is_some() { "a rebuilt" } else { "the stored" },
					splice.channel_id,
					splice.counterparty_node_id,
				);
				self.resubmit(user_channel_id, splice.clone(), rebuilt).await
			},
		};
		if surface {
			// A surfaced failure is final: clear the persisted intent so a restart does not
			// resubmit a splice the user watched fail. Clearing before the caller's event-queue
			// write leaves one torn window — a crash in between replays the failure event with
			// nothing persisted, and the replay is adopted for a fresh retry cycle instead of
			// surfacing — which errs on the side of retrying over losing the splice.
			if let Some(payment_id) = splice.payment_id {
				self.clear_persisted_intent(payment_id).await;
			}
		}
		surface
	}

	/// Clears any tracked splice made obsolete by a newly locked funding transaction.
	pub(crate) async fn on_channel_ready(
		&self, user_channel_id: UserChannelId, funding_txo: Option<OutPoint>,
	) {
		let Some(funding_txo) = funding_txo else {
			return;
		};
		self.registry.clear_if_obsoleted(user_channel_id, funding_txo);
		// Mirror the clear onto the persisted intent, which likewise only goes when it predates
		// the locked funding: an intent whose pre-splice outpoint is the newly locked funding
		// was created after this lock and is still pending.
		if let Some(record) = self.record_for_channel(user_channel_id) {
			let obsolete = record.splice_intent().is_some_and(|intent| {
				intent.pre_splice_funding_txo.into_bitcoin_outpoint() != funding_txo
			});
			if obsolete {
				self.clear_persisted_intent(record.id()).await;
			}
		}
	}

	/// Clears any tracked splice for a closed channel, as there is nothing left to splice.
	pub(crate) async fn on_channel_closed(&self, user_channel_id: UserChannelId) {
		self.registry.clear(user_channel_id);
		if let Some(record) = self.record_for_channel(user_channel_id) {
			self.clear_persisted_intent(record.id()).await;
		}
	}

	/// Returns the pending record carrying a splice intent for the given channel, if any. A fee
	/// bump reuses the channel's existing intent record, so at most one record matches.
	fn record_for_channel(&self, user_channel_id: UserChannelId) -> Option<PendingPaymentDetails> {
		self.pending_payment_store
			.list_filter(|p| {
				p.splice_intent().is_some_and(|i| i.user_channel_id == user_channel_id)
			})
			.into_iter()
			.next()
	}

	/// Starts tracking the splice behind a failure event that arrived with nothing tracked: LDK
	/// persists a `SpliceNegotiationFailed` it emitted, so a failure never handled while running —
	/// e.g. a disconnect during shutdown — replays after a restart, and its registration did not
	/// survive that restart. Returns `None` when the channel is gone or not ready, in which case
	/// there is nothing left to retry.
	fn adopt(
		&self, channel_id: ChannelId, user_channel_id: UserChannelId,
		counterparty_node_id: PublicKey, contribution: FundingContribution,
	) -> Option<PendingSplice> {
		let channel = self
			.channel_manager
			.list_channels_with_counterparty(&counterparty_node_id)
			.into_iter()
			.find(|c| c.user_channel_id == user_channel_id.0)?;
		let funding_txo = channel.funding_txo?;
		let splice = PendingSplice {
			channel_id,
			counterparty_node_id,
			pre_splice_funding_txo: funding_txo.into_bitcoin_outpoint(),
			contribution,
			kind: None,
			attempts: 0,
			rejected: false,
			payment_id: None,
		};
		log_info!(
			self.logger,
			"Adopting a replayed splice failure for channel {} with counterparty {}",
			channel_id,
			counterparty_node_id,
		);
		self.registry.register(user_channel_id, splice.clone());
		Some(splice)
	}

	/// Records the attempt and hands the contribution back to LDK, returning whether the failure
	/// being handled should be surfaced after all. The attempt count is bumped before the
	/// hand-off so repeated failures stay bounded. A [`APIError::ChannelUnavailable`] rejection
	/// means the channel or peer is gone, so no further failure event is coming and the splice is
	/// given up on. Any other rejection is only logged: LDK either enqueued a fresh
	/// `SpliceNegotiationFailed` — which re-enters [`Self::on_negotiation_failed`] and gives up
	/// at `MAX_SPLICE_ATTEMPTS` — or is already driving another negotiation for the channel.
	async fn resubmit(
		&self, user_channel_id: UserChannelId, splice: PendingSplice,
		rebuilt: Option<FundingContribution>,
	) -> bool {
		let resubmitting_stored = rebuilt.is_none();
		let contribution = rebuilt.unwrap_or_else(|| splice.contribution.clone());
		// The tracked splice may have moved on while the contribution was being rebuilt (locked,
		// closed, or superseded by a newer splice); resubmit only while it is the one that failed.
		if !self.registry.record_attempt(
			user_channel_id,
			&splice.contribution,
			contribution.clone(),
		) {
			return false;
		}
		if let Some(payment_id) = splice.payment_id {
			// Persist the incremented attempt count — and the contribution being resubmitted —
			// before the hand-off, so a crash mid-submission cannot lead to unbounded retries.
			// If it cannot be persisted, LDK never receives the contribution and nothing further
			// fires for this attempt: give up and surface the failure rather than swallow it.
			if let Err(e) =
				self.persist_attempt(payment_id, splice.attempts + 1, &contribution).await
			{
				log_error!(
					self.logger,
					"Giving up on splice for channel {}: failed to persist the splice attempt: {}",
					splice.channel_id,
					e,
				);
				return true;
			}
		}
		if resubmitting_stored {
			// The failed attempt's `DiscardFunding` released the stored contribution's change
			// output — and any splice-out output paying the wallet — back into the unused address
			// pool; a rebuilt contribution selected fresh outputs instead. Re-reserve those
			// addresses so they are not handed out again. Failing to do so risks only address
			// reuse, so it does not block the resubmission.
			let outputs: Vec<TxOut> = contribution
				.outputs()
				.iter()
				.chain(contribution.change_output())
				.cloned()
				.collect();
			if let Err(e) = self.wallet.reserve_tx_outputs(&outputs).await {
				log_error!(
					self.logger,
					"Failed to re-reserve the resubmitted splice's addresses for channel {}: {:?}",
					splice.channel_id,
					e,
				);
			}
		}
		if let Err(e) = self.channel_manager.funding_contributed(
			&splice.channel_id,
			&splice.counterparty_node_id,
			contribution,
			None,
		) {
			log_error!(
				self.logger,
				"Failed to resubmit splice for channel {} with counterparty {}: {:?}",
				splice.channel_id,
				splice.counterparty_node_id,
				e,
			);
			if let APIError::ChannelUnavailable { .. } = e {
				return true;
			}
		}
		false
	}

	/// Mirrors a resubmission onto the persisted intent: bumps its attempt count and swaps in the
	/// contribution being resubmitted. A record whose intent is already gone (e.g. the splice
	/// locked in the meantime) is left untouched.
	async fn persist_attempt(
		&self, payment_id: PaymentId, attempts: u8, contribution: &FundingContribution,
	) -> Result<(), Error> {
		self.pending_payment_store
			.mutate(&payment_id, |existing| {
				let record = existing?;
				let mut intent = record.splice_intent()?.clone();
				intent.attempts = attempts;
				intent.contribution = contribution.clone();
				let mut updated = record.clone();
				updated
					.update(PendingPaymentDetailsUpdate {
						id: payment_id,
						payment_update: None,
						conflicting_txids: None,
						candidates: Vec::new(),
						splice_intent: Some(Some(intent)),
					})
					.then_some(updated)
			})
			.await
			.map(|_| ())
	}

	/// Builds a fresh contribution from the parameters of the originating API call, mirroring the
	/// corresponding [`Node`] method.
	///
	/// [`Node`]: crate::Node
	async fn rebuild_contribution(
		&self, channel_id: &ChannelId, counterparty_node_id: &PublicKey, kind: &SpliceKind,
	) -> Result<FundingContribution, Error> {
		let template = self
			.channel_manager
			.splice_channel(channel_id, counterparty_node_id)
			.map_err(|_| Error::ChannelSplicingFailed)?;

		let est_feerate = self.fee_estimator.estimate_fee_rate(ConfirmationTarget::ChannelFunding);
		let max_feerate = max_funding_feerate(est_feerate);
		let feerate = match template.min_rbf_feerate() {
			Some(min_rbf_feerate) if min_rbf_feerate <= max_feerate => {
				est_feerate.max(min_rbf_feerate)
			},
			_ => est_feerate,
		};

		match kind {
			SpliceKind::In { amount_sats } => template
				.splice_in(
					Amount::from_sat(*amount_sats),
					feerate,
					max_feerate,
					Arc::clone(&self.wallet),
				)
				.await
				.map_err(|_| Error::ChannelSplicingFailed),
			SpliceKind::Out { outputs } => template
				.splice_out(outputs.clone(), feerate, max_feerate)
				.map_err(|_| Error::ChannelSplicingFailed),
			SpliceKind::Rbf {} => template
				.rbf_prior_contribution(None, max_feerate, Arc::clone(&self.wallet))
				.await
				.map_err(|_| Error::ChannelSplicingFailed),
		}
	}
}

/// The replacement for a pending record whose splice intent is being dropped. A tracked record
/// keeps its payment details with just the intent cleared. A pre-broadcast record whose payment
/// was classified but never mirrored into the pending store — a crash between classification's
/// two store writes — is promoted so the payment keeps graduating and its txids stay mapped; a
/// payment no longer `Pending` graduated already and must not be re-indexed, so its entry is
/// left alone for removal.
fn record_with_intent_cleared(
	existing: Option<&PendingPaymentDetails>, recorded: Option<PaymentDetails>,
) -> Option<PendingPaymentDetails> {
	match existing {
		Some(PendingPaymentDetails::PendingSplice { .. }) => recorded
			.filter(|details| details.status == PaymentStatus::Pending)
			.map(|details| PendingPaymentDetails::tracked(details, Vec::new(), Vec::new(), None)),
		Some(tracked @ PendingPaymentDetails::Tracked { .. }) => {
			let update = PendingPaymentDetailsUpdate {
				id: tracked.id(),
				payment_update: None,
				conflicting_txids: None,
				candidates: Vec::new(),
				splice_intent: Some(None),
			};
			let mut updated = tracked.clone();
			updated.update(update).then_some(updated)
		},
		None => None,
	}
}

/// What [`SpliceRetrier::reconcile`] should do with a persisted intent, decided from the splice
/// rounds LDK reports on the channel.
#[derive(Debug, PartialEq, Eq)]
enum ReconcileDecision {
	/// Leave the intent in place without resubmitting.
	Keep,
	/// Hand the stored contribution back to LDK.
	Resubmit,
	/// Give up: drop the intent and surface the failure to the user.
	Abandon,
}

/// Decides the startup action for a persisted intent from the channel's [`SpliceDetails`]
/// candidates.
///
/// [`SpliceDetails`]: lightning::ln::channel_state::SpliceDetails
fn decide_reconcile(
	intent: &SpliceIntent, candidates: &[SpliceCandidateDetails],
) -> ReconcileDecision {
	// A round short of `Negotiated` is one LDK still drives on its own: only `AwaitingSignatures`
	// survives a restart, and LDK resumes the signature exchange itself on reconnect.
	let in_flight = candidates
		.iter()
		.any(|candidate| !matches!(candidate.status, SpliceCandidateStatus::Negotiated { .. }));
	if in_flight {
		return ReconcileDecision::Keep;
	}

	// LDK persists a splice once negotiated, so a negotiated candidate carrying our contribution
	// means the intent was carried out — unless the intent was a fee bump at a higher feerate
	// than negotiated. Rounds carry our contribution forward, so the newest one holds it.
	let negotiated = candidates.iter().rev().find_map(|candidate| candidate.contribution.as_ref());
	match (&intent.kind, negotiated) {
		(SpliceKind::Rbf {}, Some(prior)) => {
			if prior.feerate() < intent.contribution.feerate() {
				ReconcileDecision::Resubmit
			} else {
				ReconcileDecision::Keep
			}
		},
		// The splice to bump is gone entirely; surface rather than guess.
		(SpliceKind::Rbf {}, None) => ReconcileDecision::Abandon,
		(_, Some(_)) => ReconcileDecision::Keep,
		(_, None) => ReconcileDecision::Resubmit,
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use bitcoin::hashes::Hash;
	use bitcoin::Txid;

	use super::*;
	use crate::payment::pending_payment_store::{
		test_funding_contribution, test_funding_contribution_with_feerate,
	};
	use crate::payment::store::{ConfirmationStatus, PaymentKind};
	use crate::payment::PaymentDirection;

	#[test]
	fn decide_retry_matrix() {
		use NegotiationFailureReason::*;

		// A non-retriable reason gives up regardless of attempts or available parameters.
		assert_eq!(decide_retry(&LocallyCanceled, 0, true), RetryDecision::Abandon);
		// Retriable, but the resubmission budget is exhausted -> give up.
		assert_eq!(
			decide_retry(&PeerDisconnected, MAX_SPLICE_ATTEMPTS, true),
			RetryDecision::Abandon
		);
		// The originating parameters are available: rebuild at current fee rates.
		assert_eq!(decide_retry(&PeerDisconnected, 0, true), RetryDecision::Rebuild);
		assert_eq!(
			decide_retry(&FeeRateTooLow, MAX_SPLICE_ATTEMPTS - 1, true),
			RetryDecision::Rebuild
		);
		// No originating parameters (adopted from a replayed event): resubmitting the stored
		// contribution is all that is possible.
		assert_eq!(decide_retry(&Unknown, 0, false), RetryDecision::ResubmitStored);
		assert_eq!(decide_retry(&ContributionInvalid, 0, false), RetryDecision::ResubmitStored);
	}

	fn test_splice(contribution: FundingContribution) -> PendingSplice {
		PendingSplice {
			channel_id: ChannelId([7u8; 32]),
			counterparty_node_id: PublicKey::from_str(
				"0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
			)
			.unwrap(),
			pre_splice_funding_txo: OutPoint { txid: Txid::from_byte_array([3u8; 32]), vout: 0 },
			contribution,
			kind: Some(SpliceKind::In { amount_sats: 10_000 }),
			attempts: 0,
			rejected: false,
			payment_id: None,
		}
	}

	fn test_intent() -> SpliceIntent {
		SpliceIntent {
			user_channel_id: UserChannelId(42),
			counterparty_node_id: PublicKey::from_str(
				"0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
			)
			.unwrap(),
			channel_id: ChannelId([7u8; 32]),
			pre_splice_funding_txo: LdkOutPoint {
				txid: Txid::from_byte_array([3u8; 32]),
				index: 0,
			},
			contribution: test_funding_contribution(),
			kind: SpliceKind::Rbf {},
			attempts: 0,
		}
	}

	fn payment_details(id: PaymentId, status: PaymentStatus) -> PaymentDetails {
		PaymentDetails::new(
			id,
			PaymentKind::Onchain {
				txid: Txid::from_byte_array([1u8; 32]),
				status: ConfirmationStatus::Unconfirmed,
				tx_type: None,
			},
			Some(1_000_000),
			Some(500),
			PaymentDirection::Outbound,
			status,
		)
	}

	/// A crash between classification's two store writes leaves the pending entry pre-broadcast
	/// while the classified payment record exists. Dropping the intent must promote the entry
	/// rather than remove it, so the payment keeps graduating and its txids stay mapped.
	#[test]
	fn intent_clearing_promotes_a_pre_broadcast_record_over_a_classified_payment() {
		let id = PaymentId([9u8; 32]);
		let existing = PendingPaymentDetails::pending_splice(id, test_intent());
		let recorded = payment_details(id, PaymentStatus::Pending);

		let replacement = record_with_intent_cleared(Some(&existing), Some(recorded.clone()));
		let replacement = replacement.expect("the entry must be promoted, not removed");
		assert_eq!(replacement.details(), Some(&recorded));
		assert!(replacement.splice_intent().is_none());
	}

	/// A payment that already advanced beyond `Pending` graduated and lost its pending entry;
	/// promotion must not re-index it.
	#[test]
	fn intent_clearing_does_not_reindex_an_advanced_payment() {
		let id = PaymentId([9u8; 32]);
		let existing = PendingPaymentDetails::pending_splice(id, test_intent());
		let recorded = payment_details(id, PaymentStatus::Succeeded);
		assert!(record_with_intent_cleared(Some(&existing), Some(recorded)).is_none());
	}

	/// A tracked record keeps its payment details; only the intent is cleared.
	#[test]
	fn intent_clearing_keeps_a_tracked_record() {
		let id = PaymentId([9u8; 32]);
		let details = payment_details(id, PaymentStatus::Pending);
		let existing = PendingPaymentDetails::tracked(
			details.clone(),
			Vec::new(),
			Vec::new(),
			Some(test_intent()),
		);

		let replacement = record_with_intent_cleared(Some(&existing), Some(details.clone()));
		let replacement = replacement.expect("the entry must survive with its intent cleared");
		assert_eq!(replacement.details(), Some(&details));
		assert!(replacement.splice_intent().is_none());
	}

	#[test]
	fn classify_failure_matrix() {
		use NegotiationFailureReason::*;

		let tracked = test_splice(test_funding_contribution());
		let failed = tracked.contribution.clone();

		// The tracked splice failed and its originating parameters are available: rebuild.
		assert_eq!(
			classify_failure(Some(&tracked), &PeerDisconnected, Some(&failed)),
			FailureClass::Tracked(RetryDecision::Rebuild),
		);
		// A splice adopted from a replayed event has no originating parameters: resubmitting the
		// stored contribution is all that is possible, whatever the retriable reason.
		let mut adopted = tracked.clone();
		adopted.kind = None;
		assert_eq!(
			classify_failure(Some(&adopted), &PeerDisconnected, Some(&failed)),
			FailureClass::Tracked(RetryDecision::ResubmitStored),
		);
		assert_eq!(
			classify_failure(Some(&adopted), &FeeRateTooLow, Some(&failed)),
			FailureClass::Tracked(RetryDecision::ResubmitStored),
		);
		// The failure concerns some other attempt (a stale event replayed after a newer splice
		// was initiated): surface it.
		let stale = test_funding_contribution_with_feerate(500);
		assert_eq!(
			classify_failure(Some(&tracked), &PeerDisconnected, Some(&stale)),
			FailureClass::Surface,
		);
		assert_eq!(
			classify_failure(Some(&tracked), &PeerDisconnected, None),
			FailureClass::Surface
		);

		// Nothing tracked, but the failure is recoverable and carries a resubmittable
		// contribution (a failure event replayed after a restart): adopt it.
		assert_eq!(classify_failure(None, &PeerDisconnected, Some(&failed)), FailureClass::Adopt);
		// Nothing tracked and no way to retry: surface.
		assert_eq!(classify_failure(None, &LocallyCanceled, Some(&failed)), FailureClass::Surface);
		assert_eq!(classify_failure(None, &PeerDisconnected, None), FailureClass::Surface);
	}

	#[test]
	fn a_rejected_hand_off_consumes_its_failure_event() {
		use NegotiationFailureReason::*;

		let mut rejected = test_splice(test_funding_contribution());
		rejected.rejected = true;
		let failed = rejected.contribution.clone();

		// The originating call already surfaced the rejection: its failure event is dropped.
		assert_eq!(
			classify_failure(Some(&rejected), &PeerDisconnected, Some(&failed)),
			FailureClass::Consume,
		);
		// A failure for some other attempt still surfaces.
		let other = test_funding_contribution_with_feerate(500);
		assert_eq!(
			classify_failure(Some(&rejected), &PeerDisconnected, Some(&other)),
			FailureClass::Surface,
		);
	}

	#[test]
	fn contributions_match_by_inputs_and_outputs() {
		use bitcoin::{ScriptBuf, TxOut};

		use crate::payment::pending_payment_store::test_funding_contribution_with_outputs;

		let outputs =
			vec![TxOut { value: Amount::from_sat(1_000), script_pubkey: ScriptBuf::new() }];
		// Fee fields differ, inputs and outputs agree: the same attempt.
		let a = test_funding_contribution_with_outputs(253, &outputs);
		let b = test_funding_contribution_with_outputs(500, &outputs);
		assert!(is_same_splice(&a, &b));

		// Different outputs are a different attempt.
		let other = vec![TxOut { value: Amount::from_sat(2_000), script_pubkey: ScriptBuf::new() }];
		assert!(!is_same_splice(&a, &test_funding_contribution_with_outputs(253, &other)));

		// Contributions moving nothing (no inputs, no outputs) only match themselves exactly.
		assert!(is_same_splice(&test_funding_contribution(), &test_funding_contribution()));
		assert!(!is_same_splice(
			&test_funding_contribution(),
			&test_funding_contribution_with_feerate(500)
		));
	}

	#[test]
	fn an_ldk_adjusted_copy_still_matches_the_tracked_splice() {
		use bitcoin::{ScriptBuf, TxOut};
		use NegotiationFailureReason::*;

		use crate::payment::pending_payment_store::test_funding_contribution_with_outputs;

		// LDK may adjust a contribution during negotiation — the quiescence tie-breaker rebuilds
		// the acceptor's copy at a fresh feerate, touching only its fee fields and change value —
		// and the failure event then carries the adjusted copy. It still identifies the tracked
		// splice: the inputs and outputs the user asked to move are untouched.
		let outputs =
			vec![TxOut { value: Amount::from_sat(1_000), script_pubkey: ScriptBuf::new() }];
		let tracked = test_splice(test_funding_contribution_with_outputs(253, &outputs));
		let adjusted = test_funding_contribution_with_outputs(500, &outputs);
		assert_ne!(tracked.contribution, adjusted);
		assert_eq!(
			classify_failure(Some(&tracked), &PeerDisconnected, Some(&adjusted)),
			FailureClass::Tracked(RetryDecision::Rebuild),
		);
	}

	#[test]
	fn a_newer_splice_supersedes_the_tracked_one() {
		let registry = SpliceRegistry::default();
		let user_channel_id = UserChannelId(1);
		registry.register(user_channel_id, test_splice(test_funding_contribution()));
		let newer = test_splice(test_funding_contribution_with_feerate(500));
		registry.register(user_channel_id, newer.clone());
		assert_eq!(registry.get(user_channel_id), Some(newer));
	}

	#[test]
	fn unregister_leaves_a_newer_splice_tracked() {
		let registry = SpliceRegistry::default();
		let user_channel_id = UserChannelId(1);
		let failed = test_splice(test_funding_contribution());
		let newer = test_splice(test_funding_contribution_with_feerate(500));
		registry.register(user_channel_id, newer.clone());

		// The failed splice was already superseded: its undo must not drop the newer one.
		registry.unregister(user_channel_id, &failed.contribution);
		assert_eq!(registry.get(user_channel_id), Some(newer.clone()));

		registry.unregister(user_channel_id, &newer.contribution);
		assert_eq!(registry.get(user_channel_id), None);
	}

	#[test]
	fn record_attempt_bumps_only_the_splice_that_failed() {
		let registry = SpliceRegistry::default();
		let user_channel_id = UserChannelId(1);
		let prior = test_funding_contribution();

		// Nothing tracked: the splice moved on, nothing to resubmit.
		assert!(!registry.record_attempt(user_channel_id, &prior, prior.clone()));

		registry.register(user_channel_id, test_splice(prior.clone()));
		let rebuilt = test_funding_contribution_with_feerate(500);
		assert!(registry.record_attempt(user_channel_id, &prior, rebuilt.clone()));
		let tracked = registry.get(user_channel_id).unwrap();
		assert_eq!(tracked.attempts, 1);
		assert_eq!(tracked.contribution, rebuilt);

		// The tracked contribution no longer matches the failed one: superseded, don't resubmit.
		assert!(!registry.record_attempt(user_channel_id, &prior, prior.clone()));
	}

	#[test]
	fn a_locked_funding_clears_only_a_predating_splice() {
		let registry = SpliceRegistry::default();
		let user_channel_id = UserChannelId(1);
		let splice = test_splice(test_funding_contribution());
		registry.register(user_channel_id, splice.clone());

		// The tracked splice was created after this lock — its pre-splice outpoint is the newly
		// locked funding — so it is still pending.
		registry.clear_if_obsoleted(user_channel_id, splice.pre_splice_funding_txo);
		assert!(registry.get(user_channel_id).is_some());

		// A different outpoint locked: the tracked splice (or a replacement) completed.
		let locked = OutPoint { txid: Txid::from_byte_array([9u8; 32]), vout: 1 };
		registry.clear_if_obsoleted(user_channel_id, locked);
		assert!(registry.get(user_channel_id).is_none());
	}

	#[test]
	fn a_closed_channel_clears_its_splice() {
		let registry = SpliceRegistry::default();
		let user_channel_id = UserChannelId(1);
		registry.register(user_channel_id, test_splice(test_funding_contribution()));
		registry.clear(user_channel_id);
		assert!(registry.get(user_channel_id).is_none());
	}

	fn negotiated_candidate(contribution: Option<FundingContribution>) -> SpliceCandidateDetails {
		SpliceCandidateDetails {
			contribution,
			status: SpliceCandidateStatus::Negotiated {
				txid: Txid::from_byte_array([9u8; 32]),
				new_channel_value_satoshis: 100_000,
			},
		}
	}

	/// While any round is short of `Negotiated`, LDK drives the splice itself; the intent stays
	/// untouched until it settles.
	#[test]
	fn reconcile_keeps_the_intent_while_ldk_drives_a_round() {
		let intent = test_intent();
		let in_flight = SpliceCandidateDetails {
			contribution: Some(test_funding_contribution()),
			status: SpliceCandidateStatus::AwaitingSignatures {
				is_initiator: true,
				funding_feerate_sat_per_1000_weight: 253,
				new_channel_value_satoshis: 100_000,
				txid: Txid::from_byte_array([9u8; 32]),
			},
		};
		assert_eq!(decide_reconcile(&intent, &[in_flight]), ReconcileDecision::Keep);
	}

	/// A negotiated candidate carrying our contribution means the intent was carried out;
	/// resubmitting would duplicate the splice. This holds on zero-conf channels too, where the
	/// pre-splice funding outpoint has not moved on yet.
	#[test]
	fn reconcile_trusts_a_negotiated_contribution() {
		let mut intent = test_intent();
		intent.kind = SpliceKind::In { amount_sats: 10_000 };
		let negotiated = [negotiated_candidate(Some(test_funding_contribution()))];
		assert_eq!(decide_reconcile(&intent, &negotiated), ReconcileDecision::Keep);
	}

	/// With no contribution of ours in LDK — no splice at all, or only a counterparty round — the
	/// original splice is resubmitted, while a fee bump has nothing left to replace and gives up.
	#[test]
	fn reconcile_resubmits_when_ldk_holds_no_contribution() {
		let mut intent = test_intent();
		intent.kind = SpliceKind::In { amount_sats: 10_000 };
		assert_eq!(decide_reconcile(&intent, &[]), ReconcileDecision::Resubmit);
		let counterparty_only = [negotiated_candidate(None)];
		assert_eq!(decide_reconcile(&intent, &counterparty_only), ReconcileDecision::Resubmit);

		intent.kind = SpliceKind::Rbf {};
		assert_eq!(decide_reconcile(&intent, &[]), ReconcileDecision::Abandon);
		let counterparty_only = [negotiated_candidate(None)];
		assert_eq!(decide_reconcile(&intent, &counterparty_only), ReconcileDecision::Abandon);
	}

	/// A fee bump resubmits only when it improves on the negotiated feerate, judged against the
	/// newest round since rounds carry the contribution forward.
	#[test]
	fn reconcile_resubmits_a_bump_only_at_a_higher_feerate() {
		let mut intent = test_intent();
		intent.contribution = test_funding_contribution_with_feerate(500);

		let lower = [negotiated_candidate(Some(test_funding_contribution_with_feerate(253)))];
		assert_eq!(decide_reconcile(&intent, &lower), ReconcileDecision::Resubmit);

		let higher = [negotiated_candidate(Some(test_funding_contribution_with_feerate(1000)))];
		assert_eq!(decide_reconcile(&intent, &higher), ReconcileDecision::Keep);

		let bumped_meanwhile = [
			negotiated_candidate(Some(test_funding_contribution_with_feerate(253))),
			negotiated_candidate(Some(test_funding_contribution_with_feerate(1000))),
		];
		assert_eq!(decide_reconcile(&intent, &bumped_meanwhile), ReconcileDecision::Keep);
	}
}

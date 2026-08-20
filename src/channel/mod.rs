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
use lightning::events::NegotiationFailureReason;
use lightning::ln::funding::FundingContribution;
use lightning::ln::types::ChannelId;
use lightning::util::errors::APIError;

use crate::fee_estimator::{
	max_funding_feerate, ConfirmationTarget, FeeEstimator, OnchainFeeEstimator,
};
use crate::logger::{log_error, log_info, LdkLogger, Logger};
use crate::payment::pending_payment_store::SpliceKind;
use crate::types::{ChannelManager, UserChannelId, Wallet};
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
/// The tracked state is in-memory only, so retries do not survive a restart. LDK does persist the
/// failure events themselves, so a failure that was never handled while running — e.g. a
/// disconnect during shutdown — replays after a restart and its splice is adopted and resubmitted.
/// A splice whose negotiation failed without LDK recording anything is dropped on restart.
///
/// [`ChannelManager::funding_contributed`]: lightning::ln::channelmanager::ChannelManager::funding_contributed
pub(crate) struct SpliceRetrier {
	channel_manager: Arc<ChannelManager>,
	wallet: Arc<Wallet>,
	fee_estimator: Arc<OnchainFeeEstimator>,
	registry: SpliceRegistry,
	/// Serializes [`Self::submit`]'s register-and-hand-off sequence with
	/// [`Self::on_negotiation_failed`]'s snapshot of the tracked splice. Without it, the failure
	/// event of a synchronously rejected hand-off could be classified while the splice is still
	/// cleanly registered — resubmitting a contribution whose originating call already returned
	/// an error.
	submit_lock: Mutex<()>,
	logger: Arc<Logger>,
}

impl SpliceRetrier {
	pub(crate) fn new(
		channel_manager: Arc<ChannelManager>, wallet: Arc<Wallet>,
		fee_estimator: Arc<OnchainFeeEstimator>, logger: Arc<Logger>,
	) -> Self {
		Self {
			channel_manager,
			wallet,
			fee_estimator,
			registry: SpliceRegistry::default(),
			submit_lock: Mutex::new(()),
			logger,
		}
	}

	/// Tracks a user-initiated splice and hands its contribution to
	/// [`ChannelManager::funding_contributed`]. Tracking starts before the hand-off, so a failure
	/// event cannot arrive before the splice it concerns is tracked. A newer splice supersedes
	/// whatever was tracked for the channel: at most one splice is ever in flight per channel,
	/// and a fee bump replaces the splice it bumps.
	///
	/// On a synchronous rejection the error is returned for the caller to surface, and the splice
	/// stays tracked but marked rejected: LDK may still enqueue a `SpliceNegotiationFailed` for
	/// the rejected contribution, which must be consumed rather than reported a second time.
	///
	/// [`ChannelManager::funding_contributed`]: lightning::ln::channelmanager::ChannelManager::funding_contributed
	pub(crate) fn submit(
		&self, user_channel_id: UserChannelId, counterparty_node_id: PublicKey,
		channel_id: ChannelId, pre_splice_funding_txo: OutPoint, contribution: FundingContribution,
		kind: SpliceKind,
	) -> Result<(), APIError> {
		let _guard = self.submit_lock.lock().unwrap();
		let splice = PendingSplice {
			channel_id,
			counterparty_node_id,
			pre_splice_funding_txo,
			contribution: contribution.clone(),
			kind: Some(kind),
			attempts: 0,
			rejected: false,
		};
		self.registry.register(user_channel_id, splice.clone());
		self.channel_manager
			.funding_contributed(&channel_id, &counterparty_node_id, contribution, None)
			.map_err(|e| {
				self.registry.register(user_channel_id, PendingSplice { rejected: true, ..splice });
				e
			})
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
			let _guard = self.submit_lock.lock().unwrap();
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

		match decision {
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
					return true;
				}
				log_info!(
					self.logger,
					"Resubmitting splice for channel {} with counterparty {} after a recoverable failure",
					splice.channel_id,
					splice.counterparty_node_id,
				);
				self.resubmit(user_channel_id, splice, None).await
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
				self.resubmit(user_channel_id, splice, rebuilt).await
			},
		}
	}

	/// Clears any tracked splice made obsolete by a newly locked funding transaction.
	pub(crate) fn on_channel_ready(
		&self, user_channel_id: UserChannelId, funding_txo: Option<OutPoint>,
	) {
		if let Some(funding_txo) = funding_txo {
			self.registry.clear_if_obsoleted(user_channel_id, funding_txo);
		}
	}

	/// Clears any tracked splice for a closed channel, as there is nothing left to splice.
	pub(crate) fn on_channel_closed(&self, user_channel_id: UserChannelId) {
		self.registry.clear(user_channel_id);
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

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use bitcoin::hashes::Hash;
	use bitcoin::Txid;

	use super::*;
	use crate::payment::pending_payment_store::{
		test_funding_contribution, test_funding_contribution_with_feerate,
	};

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
		}
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
}

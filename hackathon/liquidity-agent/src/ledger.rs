// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Forwarding ledger: an in-memory record of every forward and forward failure the node
//! reports, aggregated per channel on demand. LDK Node does not keep this history itself.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ldk_node::{ChannelDetails, Event, ForwardFailureReason, Node};
use schemars::JsonSchema;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::console::Notices;

/// Oldest entries are dropped past this many records per kind.
const CAPACITY: usize = 10_000;
/// A negotiated splice that has not locked in after this long is no longer treated as pending.
pub const SPLICE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct ForwardRecord {
	/// Unix timestamp (seconds) when the forward was reported.
	pub at: u64,
	pub prev_channel_id: String,
	pub next_channel_id: String,
	pub amount_msat: u64,
	/// `None` when the fee is not yet known (e.g. claimed on-chain after a force-close).
	pub fee_msat: Option<u64>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct FailureRecord {
	/// Unix timestamp (seconds) when the failure was reported.
	pub at: u64,
	pub prev_channel_id: String,
	pub next_channel_id: Option<String>,
	pub next_node_id: Option<String>,
	pub reason: String,
}

/// A splice we negotiated whose funding transaction has not locked in yet.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PendingSplice {
	pub channel_id: String,
	/// The splice funding outpoint; the channel's `funding_txo` moves to it once locked in.
	pub new_funding_txo: String,
	/// Unix timestamp (seconds) when the splice was negotiated.
	pub since: u64,
}

/// What [`Ledger::settle_splices`] found.
#[derive(Default)]
pub struct SpliceSettlement {
	/// Locked in since the last check: the added balance is usable now.
	pub ready: Vec<PendingSplice>,
	/// Still unconfirmed after [`SPLICE_TIMEOUT`]; no longer tracked.
	pub timed_out: Vec<PendingSplice>,
	pub pending: Vec<PendingSplice>,
}

#[derive(Clone, Debug, Default, Serialize, JsonSchema)]
pub struct ChannelStats {
	/// Forwards that arrived over this channel.
	pub forwards_in: u64,
	/// Forwards that left over this channel.
	pub forwards_out: u64,
	pub volume_in_msat: u64,
	pub volume_out_msat: u64,
	/// Fees are attributed to the outgoing channel of each forward.
	pub fees_earned_msat: u64,
	/// Failures where this channel was the requested outgoing channel.
	pub failures_out: u64,
	/// Of those, how many were due to insufficient outbound balance on this channel.
	pub liquidity_failures_out: u64,
	/// Failures where the HTLC arrived over this channel.
	pub failures_in: u64,
}

#[derive(Default)]
struct Inner {
	forwards: VecDeque<ForwardRecord>,
	failures: VecDeque<FailureRecord>,
	/// By channel id.
	pending_splices: HashMap<String, PendingSplice>,
}

pub struct Ledger {
	inner: Mutex<Inner>,
	/// Unix timestamp (seconds) when recording started; rates are computed over the time
	/// actually observed, not the nominal window.
	started: u64,
}

fn now_secs() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn push_capped<T>(queue: &mut VecDeque<T>, item: T) {
	if queue.len() >= CAPACITY {
		queue.pop_front();
	}
	queue.push_back(item);
}

impl Ledger {
	pub fn new() -> Self {
		Self { inner: Mutex::default(), started: now_secs() }
	}

	/// How many hours of the last `window_hours` the ledger has actually been recording, at
	/// least one minute so rates stay finite.
	pub fn observed_hours(&self, window_hours: u64) -> f64 {
		let observed = now_secs().saturating_sub(self.started).min(window_hours * 3600).max(60);
		observed as f64 / 3600.0
	}

	/// Records the event if it is forwarding- or splice-related. Returns whether it was recorded.
	pub fn record(&self, event: &Event) -> bool {
		match event {
			Event::PaymentForwarded {
				prev_htlcs,
				next_htlcs,
				total_fee_earned_msat,
				outbound_amount_forwarded_msat,
				..
			} => {
				let (Some(prev), Some(next)) = (prev_htlcs.first(), next_htlcs.first()) else {
					return false;
				};
				let record = ForwardRecord {
					at: now_secs(),
					prev_channel_id: prev.channel_id.to_string(),
					next_channel_id: next.channel_id.to_string(),
					amount_msat: *outbound_amount_forwarded_msat,
					fee_msat: *total_fee_earned_msat,
				};
				push_capped(&mut self.inner.lock().unwrap().forwards, record);
				true
			},
			Event::ForwardFailed { prev_channel_id, next_channel_id, next_node_id, reason } => {
				let record = FailureRecord {
					at: now_secs(),
					prev_channel_id: prev_channel_id.to_string(),
					next_channel_id: next_channel_id.map(|id| id.to_string()),
					next_node_id: next_node_id.map(|id| id.to_string()),
					reason: format!("{reason:?}"),
				};
				push_capped(&mut self.inner.lock().unwrap().failures, record);
				true
			},
			Event::SpliceNegotiated { channel_id, new_funding_txo, .. } => {
				let channel_id = channel_id.to_string();
				let pending = PendingSplice {
					channel_id: channel_id.clone(),
					new_funding_txo: new_funding_txo.to_string(),
					since: now_secs(),
				};
				self.inner.lock().unwrap().pending_splices.insert(channel_id, pending);
				true
			},
			Event::SpliceNegotiationFailed { channel_id, .. } => {
				self.inner.lock().unwrap().pending_splices.remove(&channel_id.to_string());
				true
			},
			_ => false,
		}
	}

	/// Per-channel aggregates over records newer than `since` (unix seconds).
	pub fn stats_since(&self, since: u64) -> HashMap<String, ChannelStats> {
		let inner = self.inner.lock().unwrap();
		let mut stats: HashMap<String, ChannelStats> = HashMap::new();
		for f in inner.forwards.iter().filter(|f| f.at >= since) {
			let prev = stats.entry(f.prev_channel_id.clone()).or_default();
			prev.forwards_in += 1;
			prev.volume_in_msat += f.amount_msat + f.fee_msat.unwrap_or(0);
			let next = stats.entry(f.next_channel_id.clone()).or_default();
			next.forwards_out += 1;
			next.volume_out_msat += f.amount_msat;
			next.fees_earned_msat += f.fee_msat.unwrap_or(0);
		}
		let liquidity = format!("{:?}", ForwardFailureReason::InsufficientOutboundLiquidity);
		for f in inner.failures.iter().filter(|f| f.at >= since) {
			stats.entry(f.prev_channel_id.clone()).or_default().failures_in += 1;
			if let Some(next) = &f.next_channel_id {
				let next = stats.entry(next.clone()).or_default();
				next.failures_out += 1;
				if f.reason == liquidity {
					next.liquidity_failures_out += 1;
				}
			}
		}
		stats
	}

	/// Checks the pending splices against the node's current channels: a splice whose outpoint
	/// is now the channel's funding outpoint has locked in, and one older than
	/// [`SPLICE_TIMEOUT`] is given up on. Both are dropped and reported; the rest stay pending.
	pub fn settle_splices(&self, channels: &[ChannelDetails]) -> SpliceSettlement {
		let mut inner = self.inner.lock().unwrap();
		let now = now_secs();
		let mut settlement = SpliceSettlement::default();
		inner.pending_splices.retain(|channel_id, splice| {
			let locked_in = channels.iter().any(|c| {
				c.channel_id.to_string() == *channel_id
					&& c.funding_txo.is_some_and(|txo| txo.to_string() == splice.new_funding_txo)
			});
			if locked_in {
				settlement.ready.push(splice.clone());
				false
			} else if now.saturating_sub(splice.since) > SPLICE_TIMEOUT.as_secs() {
				settlement.timed_out.push(splice.clone());
				false
			} else {
				settlement.pending.push(splice.clone());
				true
			}
		});
		settlement.pending.sort_by(|a, b| a.since.cmp(&b.since));
		settlement
	}

	pub fn recent_forwards(&self, limit: usize) -> Vec<ForwardRecord> {
		let inner = self.inner.lock().unwrap();
		inner.forwards.iter().rev().take(limit).cloned().collect()
	}

	pub fn recent_failures(&self, limit: usize) -> Vec<FailureRecord> {
		let inner = self.inner.lock().unwrap();
		inner.failures.iter().rev().take(limit).cloned().collect()
	}
}

/// Drains the node's event queue into the ledger until cancelled. Every event is reported
/// through `notices` and acknowledged so the queue never backs up.
pub fn spawn_event_drain(
	node: Arc<Node>, ledger: Arc<Ledger>, notices: Arc<Notices>, cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
	tokio::spawn(async move {
		loop {
			let event = tokio::select! {
				biased;
				_ = cancel.cancelled() => break,
				event = node.next_event_async() => event,
			};
			if ledger.record(&event) {
				match &event {
					Event::PaymentForwarded { total_fee_earned_msat, .. } => notices.note(format!(
						"[node] forwarded, fee {} msat",
						total_fee_earned_msat.unwrap_or(0)
					)),
					Event::ForwardFailed { reason, .. } => {
						notices.note(format!("[node] forward failed: {reason:?}"))
					},
					Event::SpliceNegotiated { channel_id, .. } => notices.note(format!(
						"[node] splice negotiated on channel {channel_id}; waiting for it to confirm"
					)),
					Event::SpliceNegotiationFailed { channel_id, .. } => notices
						.note(format!("[node] splice negotiation failed on channel {channel_id}")),
					_ => {},
				}
			} else {
				notices.note(format!("[node] {event:?}"));
			}
			if let Err(e) = node.event_handled() {
				notices.note(format!("[node] failed to mark event handled: {e}"));
			}
		}
	})
}

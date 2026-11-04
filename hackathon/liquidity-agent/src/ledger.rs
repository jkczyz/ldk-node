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
use std::time::{SystemTime, UNIX_EPOCH};

use ldk_node::{Event, ForwardFailureReason, Node};
use schemars::JsonSchema;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

/// Oldest entries are dropped past this many records per kind.
const CAPACITY: usize = 10_000;

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
}

#[derive(Default)]
pub struct Ledger {
	inner: Mutex<Inner>,
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
		Self::default()
	}

	/// Records the event if it is forwarding-related. Returns whether it was recorded.
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

	pub fn recent_forwards(&self, limit: usize) -> Vec<ForwardRecord> {
		let inner = self.inner.lock().unwrap();
		inner.forwards.iter().rev().take(limit).cloned().collect()
	}

	pub fn recent_failures(&self, limit: usize) -> Vec<FailureRecord> {
		let inner = self.inner.lock().unwrap();
		inner.failures.iter().rev().take(limit).cloned().collect()
	}
}

/// Drains the node's event queue into the ledger until cancelled. Non-forwarding events are
/// logged to stderr and acknowledged so the queue never backs up.
pub fn spawn_event_drain(
	node: Arc<Node>, ledger: Arc<Ledger>, cancel: CancellationToken,
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
					Event::PaymentForwarded { total_fee_earned_msat, .. } => eprintln!(
						"[node] forwarded, fee {} msat",
						total_fee_earned_msat.unwrap_or(0)
					),
					Event::ForwardFailed { reason, .. } => {
						eprintln!("[node] forward failed: {reason:?}")
					},
					_ => {},
				}
			} else {
				eprintln!("[node] {event:?}");
			}
			if let Err(e) = node.event_handled() {
				eprintln!("[node] failed to mark event handled: {e}");
			}
		}
	})
}

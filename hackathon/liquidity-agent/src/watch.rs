// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! When `watch` mode reviews the node on its own: on a timer, and as soon as one channel
//! fails a burst of forwards for lack of outbound liquidity. A channel with a splice pending
//! is left alone until the splice locks in, since more liquidity is already on the way.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::ledger::ChannelStats;

/// Liquidity failures on one outgoing channel within [`BURST_WINDOW`] that trigger a review.
pub const BURST_THRESHOLD: u64 = 3;
pub const BURST_WINDOW: Duration = Duration::from_secs(120);
/// How often the ledger is checked for a burst.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);
/// A burst does not trigger a review this soon after the previous one: a fee change has no
/// completion signal, and this gives it time to propagate through gossip before the same
/// failures are reviewed again.
pub const SETTLE_AFTER_REVIEW: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct WatchOptions {
	/// A review runs this long after the previous one even if nothing happened.
	pub interval: Duration,
}

impl WatchOptions {
	/// `WATCH_INTERVAL_SECS` (600).
	pub fn from_env() -> Result<Self> {
		let secs = |name: &str, default: u64| -> Result<Duration> {
			let value = match std::env::var(name) {
				Ok(value) if !value.trim().is_empty() => {
					value.trim().parse().with_context(|| format!("{name} must be seconds"))?
				},
				_ => default,
			};
			Ok(Duration::from_secs(value))
		};
		Ok(Self { interval: secs("WATCH_INTERVAL_SECS", 600)? })
	}
}

/// A usable channel whose local balance falls below this share of its capacity is about to
/// stop forwarding; crossing it triggers a review before forwards start failing.
pub const LOW_BALANCE_PCT: u64 = 15;
/// A channel holding at least this share of its capacity is named in the trigger text as a
/// place capital could come from.
pub const IDLE_BALANCE_PCT: u64 = 70;

/// Watches each channel's local balance share for a crossing below [`LOW_BALANCE_PCT`]. Only
/// a crossing counts: a channel that has been low since it was first seen (an inbound-only
/// channel, say) does not trigger, and neither does one that stays low.
#[derive(Default)]
pub struct BalanceWatch {
	/// Whether each channel was low at the previous observation.
	was_low: HashMap<String, bool>,
	/// Channels that crossed below the threshold and have not been reviewed for it yet.
	due: HashSet<String>,
}

impl BalanceWatch {
	/// Feeds the current local-balance percentage of every usable channel.
	pub fn observe(&mut self, local_pct: impl IntoIterator<Item = (String, u64)>) {
		let mut now_low = HashMap::new();
		for (channel_id, pct) in local_pct {
			let low = pct < LOW_BALANCE_PCT;
			if low && self.was_low.get(&channel_id) == Some(&false) {
				self.due.insert(channel_id.clone());
			} else if !low {
				self.due.remove(&channel_id);
			}
			now_low.insert(channel_id, low);
		}
		self.due.retain(|id| now_low.contains_key(id));
		self.was_low = now_low;
	}

	/// The channels whose crossing should be reviewed now, dropping them from the queue.
	/// Channels in `excluded` stay queued until they are no longer excluded.
	pub fn take_due(&mut self, excluded: &HashSet<String>) -> Vec<String> {
		let mut due: Vec<String> =
			self.due.iter().filter(|id| !excluded.contains(*id)).cloned().collect();
		due.sort();
		for id in &due {
			self.due.remove(id);
		}
		due
	}
}

/// The channels whose failures must not trigger a review right now: those with a splice
/// pending, and those whose splice locked in less than a burst window ago, since failures
/// from before the lock-in would otherwise count against the new balance. Entries older than
/// the window are dropped from `recently_locked`.
pub fn excluded_channels(
	splice_pending: impl IntoIterator<Item = String>,
	recently_locked: &mut HashMap<String, Instant>, now: Instant,
) -> HashSet<String> {
	recently_locked.retain(|_, at| now.duration_since(*at) < BURST_WINDOW);
	splice_pending.into_iter().chain(recently_locked.keys().cloned()).collect()
}

/// The channel with the most liquidity failures in `stats`, if any channel reached
/// [`BURST_THRESHOLD`]. `stats` should cover the burst window only. Channels in
/// `splice_pending` are skipped: their failures are expected until the splice locks in.
pub fn burst(
	stats: &HashMap<String, ChannelStats>, splice_pending: &HashSet<String>,
) -> Option<(String, u64)> {
	stats
		.iter()
		.filter(|(id, s)| {
			s.liquidity_failures_out >= BURST_THRESHOLD && !splice_pending.contains(*id)
		})
		.max_by_key(|(id, s)| (s.liquidity_failures_out, (*id).clone()))
		.map(|(id, s)| (id.clone(), s.liquidity_failures_out))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn stats(failures: &[(&str, u64)]) -> HashMap<String, ChannelStats> {
		failures
			.iter()
			.map(|(id, n)| {
				let stats = ChannelStats { liquidity_failures_out: *n, ..Default::default() };
				(id.to_string(), stats)
			})
			.collect()
	}

	#[test]
	fn burst_needs_the_threshold_on_one_channel() {
		let none = HashSet::new();
		assert_eq!(burst(&stats(&[("a", 2), ("b", 2)]), &none), None);
		assert_eq!(burst(&stats(&[("a", 2), ("b", 3)]), &none), Some(("b".into(), 3)));
	}

	#[test]
	fn burst_picks_the_worst_channel() {
		assert_eq!(burst(&stats(&[("a", 5), ("b", 3)]), &HashSet::new()), Some(("a".into(), 5)));
	}

	fn observe(watch: &mut BalanceWatch, pcts: &[(&str, u64)]) {
		watch.observe(pcts.iter().map(|(id, pct)| (id.to_string(), *pct)));
	}

	#[test]
	fn balance_crossing_fires_once_and_again_after_recovery() {
		let none = HashSet::new();
		let mut watch = BalanceWatch::default();
		observe(&mut watch, &[("a", 50), ("inbound", 0)]);
		assert!(watch.take_due(&none).is_empty(), "a channel low from the start is not a crossing");
		observe(&mut watch, &[("a", 10), ("inbound", 0)]);
		assert_eq!(watch.take_due(&none), vec!["a".to_string()]);
		observe(&mut watch, &[("a", 5), ("inbound", 0)]);
		assert!(watch.take_due(&none).is_empty(), "staying low is not a new crossing");
		observe(&mut watch, &[("a", 40), ("inbound", 0)]);
		observe(&mut watch, &[("a", 12), ("inbound", 0)]);
		assert_eq!(watch.take_due(&none), vec!["a".to_string()]);
	}

	#[test]
	fn excluded_crossings_stay_queued() {
		let mut watch = BalanceWatch::default();
		observe(&mut watch, &[("a", 50)]);
		observe(&mut watch, &[("a", 10)]);
		let excluded = HashSet::from(["a".to_string()]);
		assert!(watch.take_due(&excluded).is_empty());
		assert_eq!(watch.take_due(&HashSet::new()), vec!["a".to_string()]);
		observe(&mut watch, &[("a", 50)]);
		observe(&mut watch, &[("a", 10)]);
		observe(&mut watch, &[("a", 50)]);
		assert!(watch.take_due(&HashSet::new()).is_empty(), "recovering clears the queue");
	}

	#[test]
	fn recently_locked_splices_are_excluded_for_one_window() {
		let locked = Instant::now();
		let mut recently = HashMap::from([("a".to_string(), locked)]);
		let excluded =
			excluded_channels(["p".to_string()], &mut recently, locked + Duration::from_secs(1));
		assert_eq!(excluded, HashSet::from(["a".to_string(), "p".to_string()]));
		let excluded = excluded_channels([], &mut recently, locked + BURST_WINDOW);
		assert!(excluded.is_empty());
		assert!(recently.is_empty());
	}

	#[test]
	fn burst_skips_channels_with_a_splice_pending() {
		let pending = HashSet::from(["a".to_string()]);
		assert_eq!(burst(&stats(&[("a", 5), ("b", 3)]), &pending), Some(("b".into(), 3)));
		assert_eq!(burst(&stats(&[("a", 5), ("b", 2)]), &pending), None);
	}
}

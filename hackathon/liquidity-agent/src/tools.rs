// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Read-only tools over the node. Each tool is a typed [`AsyncTool`] registered on the GDK
//! tool operation. Fund-moving tools live behind the policy step and are added separately.

use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};

use goose_agent::tool::ToolOperation;
use rmcp::handler::server::router::tool::{AsyncTool, ToolBase};
use rmcp::model::ErrorData;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ledger::{ChannelStats, FailureRecord, ForwardRecord};
use crate::session::Session;

pub fn tool_operation() -> ToolOperation<Session> {
	ToolOperation::<Session>::new()
		.with_async_tool::<ChannelOverview>()
		.with_async_tool::<RecentForwards>()
		.with_async_tool::<ForwardFailures>()
		.with_async_tool::<NodeInfo>()
}

fn now_secs() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

// ---- channel_overview --------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct ChannelOverviewInput {
	/// Window, in hours, for the per-channel forwarding statistics. Defaults to 24.
	pub window_hours: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
pub struct ChannelRow {
	pub channel_id: String,
	pub user_channel_id: String,
	pub counterparty_node_id: String,
	pub capacity_sats: u64,
	/// Balance on our side; what we can forward out over this channel.
	pub local_balance_sats: u64,
	/// Balance on the peer's side; what can be forwarded in to us over this channel.
	pub remote_balance_sats: u64,
	/// `local_balance_sats` as a percentage of capacity.
	pub local_balance_pct: u8,
	/// Largest single HTLC we can currently send over this channel.
	pub max_outbound_htlc_sats: u64,
	pub is_usable: bool,
	pub is_announced: bool,
	/// Whether we opened (and funded) this channel.
	pub we_opened: bool,
	/// Our forwarding fee policy for HTLCs leaving over this channel.
	pub fee_base_msat: u32,
	pub fee_ppm: u32,
	pub cltv_expiry_delta: u16,
	pub stats: ChannelStats,
}

#[derive(Serialize, JsonSchema)]
pub struct ChannelOverviewOutput {
	pub window_hours: u64,
	pub spendable_onchain_sats: u64,
	pub total_lightning_sats: u64,
	pub channels: Vec<ChannelRow>,
}

pub fn channel_overview(session: &Session, window_hours: u64) -> ChannelOverviewOutput {
	let since = now_secs().saturating_sub(window_hours * 3600);
	let mut stats = session.ledger.stats_since(since);
	let balances = session.node.list_balances();
	let channels = session
		.node
		.list_channels()
		.into_iter()
		.map(|c| {
			let channel_id = c.channel_id.to_string();
			let local = c.outbound_capacity_msat / 1000;
			let remote = c.inbound_capacity_msat / 1000;
			let pct = if c.channel_value_sats == 0 {
				0
			} else {
				(local * 100 / c.channel_value_sats).min(100) as u8
			};
			ChannelRow {
				stats: stats.remove(&channel_id).unwrap_or_default(),
				channel_id,
				user_channel_id: c.user_channel_id.0.to_string(),
				counterparty_node_id: c.counterparty.node_id.to_string(),
				capacity_sats: c.channel_value_sats,
				local_balance_sats: local,
				remote_balance_sats: remote,
				local_balance_pct: pct,
				max_outbound_htlc_sats: c.next_outbound_htlc_limit_msat / 1000,
				is_usable: c.is_usable,
				is_announced: c.is_announced,
				we_opened: c.is_outbound,
				fee_base_msat: c.config.forwarding_fee_base_msat,
				fee_ppm: c.config.forwarding_fee_proportional_millionths,
				cltv_expiry_delta: c.config.cltv_expiry_delta,
			}
		})
		.collect();
	ChannelOverviewOutput {
		window_hours,
		spendable_onchain_sats: balances.spendable_onchain_balance_sats,
		total_lightning_sats: balances.total_lightning_balance_sats,
		channels,
	}
}

/// Compact text rendering for the review prompt. Cheaper in tokens than the JSON the tool
/// returns, and easier for the model to scan.
pub fn render_overview(o: &ChannelOverviewOutput) -> String {
	let mut out = String::new();
	out.push_str(&format!(
		"on-chain spendable: {} sats; lightning total: {} sats; stats window: {}h\n\n",
		o.spendable_onchain_sats, o.total_lightning_sats, o.window_hours
	));
	out.push_str("channel | peer | cap sats | local% | local sats | remote sats | fee base/ppm | fwd in/out | fees msat | fail out (liq) | usable\n");
	for c in &o.channels {
		out.push_str(&format!(
			"{} | {} | {} | {}% | {} | {} | {}/{} | {}/{} | {} | {} ({}) | {}\n",
			&c.channel_id[..8.min(c.channel_id.len())],
			&c.counterparty_node_id[..8.min(c.counterparty_node_id.len())],
			c.capacity_sats,
			c.local_balance_pct,
			c.local_balance_sats,
			c.remote_balance_sats,
			c.fee_base_msat,
			c.fee_ppm,
			c.stats.forwards_in,
			c.stats.forwards_out,
			c.stats.fees_earned_msat,
			c.stats.failures_out,
			c.stats.liquidity_failures_out,
			c.is_usable,
		));
	}
	out
}

pub struct ChannelOverview;

impl ToolBase for ChannelOverview {
	type Parameter = ChannelOverviewInput;
	type Output = ChannelOverviewOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"channel_overview".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("List every channel with its capacity, local/remote balance, our fee policy, and forwarding statistics (forwards, fees earned, failures) over a time window. Amounts are in satoshis unless the field name says msat. Start here for any liquidity question.".into())
	}
}

impl AsyncTool<Session> for ChannelOverview {
	async fn invoke(
		session: &Session, input: ChannelOverviewInput,
	) -> Result<ChannelOverviewOutput, ErrorData> {
		Ok(channel_overview(session, input.window_hours.unwrap_or(24)))
	}
}

// ---- recent_forwards ---------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct LimitInput {
	/// Maximum number of records to return, newest first. Defaults to 50.
	pub limit: Option<usize>,
}

#[derive(Serialize, JsonSchema)]
pub struct RecentForwardsOutput {
	pub forwards: Vec<ForwardRecord>,
}

pub struct RecentForwards;

impl ToolBase for RecentForwards {
	type Parameter = LimitInput;
	type Output = RecentForwardsOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"recent_forwards".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Return the most recent successful forwards through this node: which channel each came in on, which it left on, the amount, and the fee earned in msat.".into())
	}
}

impl AsyncTool<Session> for RecentForwards {
	async fn invoke(
		session: &Session, input: LimitInput,
	) -> Result<RecentForwardsOutput, ErrorData> {
		Ok(RecentForwardsOutput {
			forwards: session.ledger.recent_forwards(input.limit.unwrap_or(50)),
		})
	}
}

// ---- forward_failures --------------------------------------------------------------------

#[derive(Serialize, JsonSchema)]
pub struct ForwardFailuresOutput {
	pub failures: Vec<FailureRecord>,
}

pub struct ForwardFailures;

impl ToolBase for ForwardFailures {
	type Parameter = LimitInput;
	type Output = ForwardFailuresOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"forward_failures".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Return the most recent forwards this node could not complete, with the incoming channel, the requested outgoing channel if it was one of ours, and the reason. `InsufficientOutboundLiquidity` on a channel means it is depleted on our side.".into())
	}
}

impl AsyncTool<Session> for ForwardFailures {
	async fn invoke(
		session: &Session, input: LimitInput,
	) -> Result<ForwardFailuresOutput, ErrorData> {
		Ok(ForwardFailuresOutput {
			failures: session.ledger.recent_failures(input.limit.unwrap_or(50)),
		})
	}
}

// ---- node_info ---------------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct NoInput {}

#[derive(Serialize, JsonSchema)]
pub struct PeerRow {
	pub node_id: String,
	pub address: String,
	pub is_connected: bool,
}

#[derive(Serialize, JsonSchema)]
pub struct NodeInfoOutput {
	pub node_id: String,
	pub listening_addresses: Vec<String>,
	pub is_running: bool,
	pub block_height: u32,
	pub latest_lightning_wallet_sync_timestamp: Option<u64>,
	pub latest_onchain_wallet_sync_timestamp: Option<u64>,
	pub spendable_onchain_sats: u64,
	pub total_onchain_sats: u64,
	pub total_lightning_sats: u64,
	pub peers: Vec<PeerRow>,
}

pub struct NodeInfo;

impl ToolBase for NodeInfo {
	type Parameter = NoInput;
	type Output = NodeInfoOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"node_info".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Return this node's identity, sync status, on-chain and lightning balances, and connected peers.".into())
	}
}

impl AsyncTool<Session> for NodeInfo {
	async fn invoke(session: &Session, _: NoInput) -> Result<NodeInfoOutput, ErrorData> {
		let node = &session.node;
		let status = node.status();
		let balances = node.list_balances();
		Ok(NodeInfoOutput {
			node_id: node.node_id().to_string(),
			listening_addresses: node
				.listening_addresses()
				.unwrap_or_default()
				.into_iter()
				.map(|a| a.to_string())
				.collect(),
			is_running: status.is_running,
			block_height: status.current_best_block.height,
			latest_lightning_wallet_sync_timestamp: status.latest_lightning_wallet_sync_timestamp,
			latest_onchain_wallet_sync_timestamp: status.latest_onchain_wallet_sync_timestamp,
			spendable_onchain_sats: balances.spendable_onchain_balance_sats,
			total_onchain_sats: balances.total_onchain_balance_sats,
			total_lightning_sats: balances.total_lightning_balance_sats,
			peers: node
				.list_peers()
				.into_iter()
				.map(|p| PeerRow {
					node_id: p.node_id.to_string(),
					address: p.address.to_string(),
					is_connected: p.is_connected,
				})
				.collect(),
		})
	}
}

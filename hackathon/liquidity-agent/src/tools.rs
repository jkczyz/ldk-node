// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Tools over the node. Each tool is a typed [`AsyncTool`] registered on the GDK tool
//! operation. The read tools are always available; the write tools listed in [`WRITE_TOOLS`]
//! only run once the policy step has let the request through.

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use goose_agent::tool::ToolOperation;
use ldk_node::ChannelDetails;
use rmcp::handler::server::router::tool::{AsyncTool, ToolBase};
use rmcp::model::ErrorData;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ledger::{ChannelStats, FailureRecord, ForwardRecord, PendingSplice};
use crate::session::Session;

/// Tools that change the node. The policy step gates these by name.
pub const WRITE_TOOLS: &[&str] = &[SET_CHANNEL_FEES, SPLICE_IN, SPLICE_OUT];
pub const TOOL_NAMES: &[&str] = &[
	"channel_overview",
	"recent_forwards",
	"forward_failures",
	"node_info",
	SET_CHANNEL_FEES,
	SPLICE_IN,
	SPLICE_OUT,
];
pub const SET_CHANNEL_FEES: &str = "set_channel_fees";
pub const SPLICE_IN: &str = "splice_in";
pub const SPLICE_OUT: &str = "splice_out";

pub fn tool_operation() -> ToolOperation<Session> {
	ToolOperation::<Session>::new()
		.with_async_tool::<ChannelOverview>()
		.with_async_tool::<RecentForwards>()
		.with_async_tool::<ForwardFailures>()
		.with_async_tool::<NodeInfo>()
		.with_async_tool::<SetChannelFees>()
		.with_async_tool::<SpliceIn>()
		.with_async_tool::<SpliceOut>()
}

/// Runs a tool by name outside the machine, for the operator's own `/tool` command. The policy
/// step is not involved: the operator typing the command is the approval.
pub async fn run_by_name(
	session: &Session, name: &str, arguments: serde_json::Value,
) -> Result<serde_json::Value, ErrorData> {
	fn parse<T: for<'de> Deserialize<'de>>(arguments: serde_json::Value) -> Result<T, ErrorData> {
		serde_json::from_value(arguments)
			.map_err(|e| ErrorData::invalid_params(format!("bad arguments: {e}"), None))
	}
	fn json<T: Serialize>(output: T) -> Result<serde_json::Value, ErrorData> {
		serde_json::to_value(output)
			.map_err(|e| ErrorData::internal_error(format!("serialize output: {e}"), None))
	}
	match name {
		"channel_overview" => json(ChannelOverview::invoke(session, parse(arguments)?).await?),
		"recent_forwards" => json(RecentForwards::invoke(session, parse(arguments)?).await?),
		"forward_failures" => json(ForwardFailures::invoke(session, parse(arguments)?).await?),
		"node_info" => json(NodeInfo::invoke(session, parse(arguments)?).await?),
		SET_CHANNEL_FEES => json(SetChannelFees::invoke(session, parse(arguments)?).await?),
		SPLICE_IN => json(SpliceIn::invoke(session, parse(arguments)?).await?),
		SPLICE_OUT => json(SpliceOut::invoke(session, parse(arguments)?).await?),
		other => Err(ErrorData::invalid_params(format!("unknown tool {other}"), None)),
	}
}

/// Looks a channel up by its full id or a unique prefix of at least six hex characters.
fn find_channel(session: &Session, channel_id: &str) -> Result<ChannelDetails, ErrorData> {
	let wanted = channel_id.trim().to_ascii_lowercase();
	if wanted.len() < 6 {
		return Err(ErrorData::invalid_params(
			"channel_id must be the full channel id or a prefix of at least 6 characters",
			None,
		));
	}
	let mut matches: Vec<ChannelDetails> = session
		.node
		.list_channels()
		.into_iter()
		.filter(|c| c.channel_id.to_string().starts_with(&wanted))
		.collect();
	match matches.len() {
		1 => Ok(matches.remove(0)),
		0 => Err(ErrorData::invalid_params(format!("no channel matches {channel_id}"), None)),
		n => Err(ErrorData::invalid_params(
			format!("{n} channels match {channel_id}; give more of the id"),
			None,
		)),
	}
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
	/// Forwards that left over this channel per hour, over the hours actually observed
	/// (`observed_hours` in the overview).
	pub forwards_out_per_hour: f64,
	/// Fees earned on this channel per hour, same basis.
	pub fees_earned_msat_per_hour: f64,
	/// A splice we negotiated on this channel that has not locked in yet. Do not propose
	/// another splice while one is pending.
	pub pending_splice: Option<PendingSplice>,
	pub stats: ChannelStats,
}

#[derive(Serialize, JsonSchema)]
pub struct ChannelOverviewOutput {
	pub window_hours: u64,
	/// How many hours of the window the forwarding statistics actually cover; the per-hour
	/// rates divide by this.
	pub observed_hours: f64,
	pub spendable_onchain_sats: u64,
	pub total_lightning_sats: u64,
	pub channels: Vec<ChannelRow>,
}

pub fn channel_overview(session: &Session, window_hours: u64) -> ChannelOverviewOutput {
	let since = now_secs().saturating_sub(window_hours * 3600);
	let mut stats = session.ledger.stats_since(since);
	let observed_hours = session.ledger.observed_hours(window_hours);
	let balances = session.node.list_balances();
	let details = session.node.list_channels();
	let mut pending_splices: HashMap<String, PendingSplice> = session
		.ledger
		.settle_splices(&details)
		.pending
		.into_iter()
		.map(|s| (s.channel_id.clone(), s))
		.collect();
	let channels = details
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
			let stats = stats.remove(&channel_id).unwrap_or_default();
			ChannelRow {
				forwards_out_per_hour: stats.forwards_out as f64 / observed_hours,
				fees_earned_msat_per_hour: stats.fees_earned_msat as f64 / observed_hours,
				stats,
				pending_splice: pending_splices.remove(&channel_id),
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
		observed_hours,
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
		"on-chain spendable: {} sats; lightning total: {} sats; stats window: {}h ({:.2}h observed)\n\n",
		o.spendable_onchain_sats, o.total_lightning_sats, o.window_hours, o.observed_hours
	));
	out.push_str("channel | peer | cap sats | local% | local sats | remote sats | fee base/ppm | fwd in/out | fees msat | fwd out/h | fees msat/h | fail out (liq) | usable | splice\n");
	for c in &o.channels {
		out.push_str(&format!(
			"{} | {} | {} | {}% | {} | {} | {}/{} | {}/{} | {} | {:.1} | {:.0} | {} ({}) | {} | {}\n",
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
			c.forwards_out_per_hour,
			c.fees_earned_msat_per_hour,
			c.stats.failures_out,
			c.stats.liquidity_failures_out,
			c.is_usable,
			if c.pending_splice.is_some() { "pending" } else { "-" },
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

// ---- set_channel_fees --------------------------------------------------------------------

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct SetChannelFeesInput {
	/// Channel id (or unique prefix) from channel_overview.
	pub channel_id: String,
	/// New proportional fee in parts per million of the forwarded amount. Unchanged if omitted.
	pub fee_ppm: Option<u32>,
	/// New base fee in millisatoshis per forward. Unchanged if omitted.
	pub fee_base_msat: Option<u32>,
}

#[derive(Serialize, JsonSchema)]
pub struct FeePolicy {
	pub fee_base_msat: u32,
	pub fee_ppm: u32,
}

#[derive(Serialize, JsonSchema)]
pub struct SetChannelFeesOutput {
	pub channel_id: String,
	pub counterparty_node_id: String,
	pub previous: FeePolicy,
	pub current: FeePolicy,
}

pub struct SetChannelFees;

impl ToolBase for SetChannelFees {
	type Parameter = SetChannelFeesInput;
	type Output = SetChannelFeesOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		SET_CHANNEL_FEES.into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Change the forwarding fee we charge for HTLCs leaving over one channel: the proportional fee in ppm and/or the base fee in msat. Raising the fee on a draining channel slows the drain; lowering it on an idle channel attracts flow. Changes inside the operator policy apply immediately; others wait for the operator's approval.".into())
	}
}

impl AsyncTool<Session> for SetChannelFees {
	async fn invoke(
		session: &Session, input: SetChannelFeesInput,
	) -> Result<SetChannelFeesOutput, ErrorData> {
		let channel = find_channel(session, &input.channel_id)?;
		let previous = FeePolicy {
			fee_base_msat: channel.config.forwarding_fee_base_msat,
			fee_ppm: channel.config.forwarding_fee_proportional_millionths,
		};
		let mut config = channel.config;
		if let Some(ppm) = input.fee_ppm {
			config.forwarding_fee_proportional_millionths = ppm;
		}
		if let Some(base) = input.fee_base_msat {
			config.forwarding_fee_base_msat = base;
		}
		session
			.node
			.update_channel_config(&channel.user_channel_id, channel.counterparty.node_id, config)
			.map_err(|e| ErrorData::internal_error(format!("fee update failed: {e}"), None))?;
		Ok(SetChannelFeesOutput {
			channel_id: channel.channel_id.to_string(),
			counterparty_node_id: channel.counterparty.node_id.to_string(),
			previous,
			current: FeePolicy {
				fee_base_msat: config.forwarding_fee_base_msat,
				fee_ppm: config.forwarding_fee_proportional_millionths,
			},
		})
	}
}

// ---- splice_in ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct SpliceInInput {
	/// Channel id (or unique prefix) from channel_overview.
	pub channel_id: String,
	/// On-chain satoshis to add to our side of the channel.
	pub amount_sats: u64,
}

#[derive(Serialize, JsonSchema)]
pub struct SpliceInOutput {
	pub channel_id: String,
	pub amount_sats: u64,
	pub status: String,
}

pub struct SpliceIn;

impl ToolBase for SpliceIn {
	type Parameter = SpliceInInput;
	type Output = SpliceInOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		SPLICE_IN.into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Add on-chain funds to our side of an existing channel without closing it, increasing its outbound liquidity. Spends from the node's on-chain wallet (see spendable_onchain_sats) and pays a transaction fee. The new balance becomes usable once the splice transaction confirms. Waits for the operator's approval unless inside the policy's auto-splice limit.".into())
	}
}

impl AsyncTool<Session> for SpliceIn {
	async fn invoke(session: &Session, input: SpliceInInput) -> Result<SpliceInOutput, ErrorData> {
		if input.amount_sats == 0 {
			return Err(ErrorData::invalid_params("amount_sats must be positive", None));
		}
		let channel = find_channel(session, &input.channel_id)?;
		session
			.node
			.splice_in(&channel.user_channel_id, channel.counterparty.node_id, input.amount_sats)
			.map_err(|e| ErrorData::internal_error(format!("splice failed: {e}"), None))?;
		Ok(SpliceInOutput {
			channel_id: channel.channel_id.to_string(),
			amount_sats: input.amount_sats,
			status: "splice negotiated; the added balance is usable once the splice transaction confirms".into(),
		})
	}
}

// ---- splice_out --------------------------------------------------------------------------

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct SpliceOutInput {
	/// Channel id (or unique prefix) from channel_overview.
	pub channel_id: String,
	/// Satoshis to move from our side of the channel to the node's on-chain wallet.
	pub amount_sats: u64,
}

#[derive(Serialize, JsonSchema)]
pub struct SpliceOutOutput {
	pub channel_id: String,
	pub amount_sats: u64,
	/// The on-chain wallet address the funds go to.
	pub address: String,
	pub status: String,
}

pub struct SpliceOut;

impl ToolBase for SpliceOut {
	type Parameter = SpliceOutInput;
	type Output = SpliceOutOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		SPLICE_OUT.into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Move funds from our side of an existing channel to the node's on-chain wallet without closing it, reducing its outbound liquidity. Use it to take capital out of a channel that holds most of its capacity but earns little, so it can be spliced into a channel that needs it once this splice has locked in. Pays a transaction fee. Always waits for the operator's approval.".into())
	}
}

impl AsyncTool<Session> for SpliceOut {
	async fn invoke(
		session: &Session, input: SpliceOutInput,
	) -> Result<SpliceOutOutput, ErrorData> {
		if input.amount_sats == 0 {
			return Err(ErrorData::invalid_params("amount_sats must be positive", None));
		}
		let channel = find_channel(session, &input.channel_id)?;
		let local_sats = channel.outbound_capacity_msat / 1000;
		if input.amount_sats > local_sats {
			return Err(ErrorData::invalid_params(
				format!("amount_sats exceeds our balance of {local_sats} sats on this channel"),
				None,
			));
		}
		let address = session
			.node
			.onchain_payment()
			.new_address()
			.map_err(|e| ErrorData::internal_error(format!("new address: {e}"), None))?;
		session
			.node
			.splice_out(
				&channel.user_channel_id,
				channel.counterparty.node_id,
				&address,
				input.amount_sats,
			)
			.map_err(|e| ErrorData::internal_error(format!("splice failed: {e}"), None))?;
		Ok(SpliceOutOutput {
			channel_id: channel.channel_id.to_string(),
			amount_sats: input.amount_sats,
			address: address.to_string(),
			status: "splice negotiated; the funds reach the on-chain wallet once the splice transaction confirms".into(),
		})
	}
}

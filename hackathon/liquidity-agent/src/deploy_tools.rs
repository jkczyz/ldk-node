// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Tools for deciding where capital goes: the gossip graph, what our own traffic wanted,
//! live probes, and the channel opens and closes that act on the decision. Registered on
//! the GDK tool operation next to the liquidity tools in [`crate::tools`].

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::lightning::ln::msgs::SocketAddress;
use rmcp::handler::server::router::tool::{AsyncTool, ToolBase};
use rmcp::model::ErrorData;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::graph::{FindPeersQuery, GraphView, PathCost, PeerCandidate};
use crate::ledger::ProbeRecord;
use crate::session::Session;
use crate::tools::NoInput;

pub const OPEN_CHANNEL: &str = "open_channel";
pub const CLOSE_CHANNEL: &str = "close_channel";
pub const TOOL_NAMES: &[&str] = &[
	"graph_summary",
	"find_peers",
	"node_profile",
	"route_cost",
	"demand",
	"probe",
	"probe_results",
	"receive_address",
	"connect_peer",
	OPEN_CHANNEL,
	CLOSE_CHANNEL,
];

/// Runs one of this module's tools by name; `None` when the name is not one of them.
pub async fn run_by_name(
	session: &Session, name: &str, arguments: serde_json::Value,
) -> Option<Result<serde_json::Value, ErrorData>> {
	fn parse<T: for<'de> Deserialize<'de>>(arguments: serde_json::Value) -> Result<T, ErrorData> {
		serde_json::from_value(arguments)
			.map_err(|e| ErrorData::invalid_params(format!("bad arguments: {e}"), None))
	}
	fn json<T: Serialize>(output: T) -> Result<serde_json::Value, ErrorData> {
		serde_json::to_value(output)
			.map_err(|e| ErrorData::internal_error(format!("serialize output: {e}"), None))
	}
	async fn run<T: AsyncTool<Session, Error = ErrorData>>(
		session: &Session, arguments: serde_json::Value,
	) -> Result<serde_json::Value, ErrorData>
	where
		T::Parameter: for<'de> Deserialize<'de>,
		T::Output: Serialize,
	{
		json(T::invoke(session, parse(arguments)?).await?)
	}
	Some(match name {
		"graph_summary" => run::<GraphSummary>(session, arguments).await,
		"find_peers" => run::<FindPeers>(session, arguments).await,
		"node_profile" => run::<NodeProfile>(session, arguments).await,
		"route_cost" => run::<RouteCost>(session, arguments).await,
		"demand" => run::<Demand>(session, arguments).await,
		"probe" => run::<Probe>(session, arguments).await,
		"probe_results" => run::<ProbeResults>(session, arguments).await,
		"receive_address" => run::<ReceiveAddress>(session, arguments).await,
		"connect_peer" => run::<ConnectPeer>(session, arguments).await,
		OPEN_CHANNEL => run::<OpenChannel>(session, arguments).await,
		CLOSE_CHANNEL => run::<CloseChannel>(session, arguments).await,
		_ => return None,
	})
}

fn now_secs() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Node ids of the counterparties of our channels.
fn channel_peers(session: &Session) -> HashSet<String> {
	session.node.list_channels().iter().map(|c| c.counterparty.node_id.to_string()).collect()
}

/// Resolves a full node id or a unique prefix of at least six hex characters against the
/// graph and our own peers.
fn resolve_node_id(session: &Session, graph: &GraphView, id: &str) -> Result<String, ErrorData> {
	let wanted = id.trim().to_ascii_lowercase();
	if wanted.len() == 66 {
		return Ok(wanted);
	}
	if wanted.len() < 6 {
		return Err(ErrorData::invalid_params(
			"node_id must be the full 66-character id or a prefix of at least 6 characters",
			None,
		));
	}
	let mut matches: HashSet<String> = graph.nodes_with_prefix(&wanted).into_iter().collect();
	matches.extend(channel_peers(session).into_iter().filter(|p| p.starts_with(&wanted)));
	matches.extend(
		session
			.node
			.list_peers()
			.into_iter()
			.map(|p| p.node_id.to_string())
			.filter(|p| p.starts_with(&wanted)),
	);
	let mut matches: Vec<String> = matches.into_iter().collect();
	match matches.len() {
		1 => Ok(matches.remove(0)),
		0 => Err(ErrorData::invalid_params(format!("no known node matches {id}"), None)),
		n => Err(ErrorData::invalid_params(
			format!("{n} nodes match {id}; give more of the id"),
			None,
		)),
	}
}

fn pubkey(node_id: &str) -> Result<PublicKey, ErrorData> {
	PublicKey::from_str(node_id)
		.map_err(|e| ErrorData::invalid_params(format!("{node_id}: not a node id: {e}"), None))
}

fn alias_of(graph: &GraphView, node_id: &str) -> Option<String> {
	graph.node(node_id).and_then(|n| n.alias.clone())
}

/// The address to connect to a node at: the given one, or the first clearnet address it
/// announced.
fn address_for(
	graph: &GraphView, node_id: &str, given: Option<&str>,
) -> Result<SocketAddress, ErrorData> {
	let address = match given {
		Some(address) => address.to_string(),
		None => graph
			.node(node_id)
			.and_then(|n| n.addresses.iter().find(|a| !a.contains(".onion")).cloned())
			.ok_or_else(|| {
				ErrorData::invalid_params(
					format!("{node_id} announced no clearnet address; pass one in `address`"),
					None,
				)
			})?,
	};
	SocketAddress::from_str(&address)
		.map_err(|e| ErrorData::invalid_params(format!("{address}: bad address: {e:?}"), None))
}

// ---- graph_summary -----------------------------------------------------------------------

#[derive(Serialize, JsonSchema)]
pub struct NodeBrief {
	pub node_id: String,
	pub alias: Option<String>,
	pub channels: usize,
	pub capacity_sats: u64,
}

#[derive(Serialize, JsonSchema)]
pub struct GraphSummaryOutput {
	pub node_count: usize,
	pub channel_count: usize,
	pub our_node_id: String,
	/// Nodes we already have a channel with.
	pub our_channel_peers: Vec<NodeBrief>,
	/// The ten nodes with the most public channels.
	pub best_connected: Vec<NodeBrief>,
}

pub struct GraphSummary;

impl ToolBase for GraphSummary {
	type Parameter = NoInput;
	type Output = GraphSummaryOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"graph_summary".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Size of the public network graph this node knows, which nodes we already have channels with, and the ten best-connected nodes. Start here before choosing where to open channels. Capacities come from gossip: where the node has not verified a channel on-chain, its capacity is the largest HTLC it accepts.".into())
	}
}

impl AsyncTool<Session> for GraphSummary {
	async fn invoke(session: &Session, _: NoInput) -> Result<GraphSummaryOutput, ErrorData> {
		let graph = GraphView::from_node(&session.node);
		let brief = |node_id: &str| NodeBrief {
			node_id: node_id.to_string(),
			alias: alias_of(&graph, node_id),
			channels: graph.degree(node_id),
			capacity_sats: graph.capacity_sats(node_id),
		};
		let mut peers: Vec<String> = channel_peers(session).into_iter().collect();
		peers.sort();
		Ok(GraphSummaryOutput {
			node_count: graph.node_count(),
			channel_count: graph.channel_count(),
			our_node_id: session.node.node_id().to_string(),
			our_channel_peers: peers.iter().map(|p| brief(p)).collect(),
			best_connected: graph.best_connected(10).iter().map(|(id, _, _)| brief(id)).collect(),
		})
	}
}

// ---- find_peers --------------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct FindPeersInput {
	/// Node ids (or unique prefixes) of destinations the operator wants to reach cheaply.
	/// Empty ranks by connectivity alone.
	pub toward: Option<Vec<String>>,
	/// Payment size the route costs are computed for. Defaults to 100000.
	pub amount_sats: Option<u64>,
	/// Ignore nodes with fewer public channels than this. Defaults to 3.
	pub min_channels: Option<usize>,
	/// How many candidates to return. Defaults to 10.
	pub limit: Option<usize>,
}

#[derive(Serialize, JsonSchema)]
pub struct FindPeersOutput {
	pub amount_sats: u64,
	pub toward: Vec<String>,
	/// Nodes left out because we already have a channel with them.
	pub excluded_existing_peers: Vec<String>,
	/// Best first: lowest total fee to the destinations, then most channels.
	pub candidates: Vec<PeerCandidate>,
}

pub struct FindPeers;

impl ToolBase for FindPeers {
	type Parameter = FindPeersInput;
	type Output = FindPeersOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"find_peers".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Shortlist nodes to open a channel with. For each candidate: its public channel count and capacity, its typical fee, its addresses, and the cheapest known fee and hop count from it to each destination you name. Costs come from gossip, so they say what a route would charge, not whether it has liquidity right now; use probe for that.".into())
	}
}

impl AsyncTool<Session> for FindPeers {
	async fn invoke(
		session: &Session, input: FindPeersInput,
	) -> Result<FindPeersOutput, ErrorData> {
		let graph = GraphView::from_node(&session.node);
		let toward = input
			.toward
			.unwrap_or_default()
			.iter()
			.map(|id| resolve_node_id(session, &graph, id))
			.collect::<Result<Vec<_>, _>>()?;
		let amount_sats = input.amount_sats.unwrap_or(100_000);
		let mut exclude = channel_peers(session);
		let mut excluded_existing_peers: Vec<String> = exclude.iter().cloned().collect();
		excluded_existing_peers.sort();
		exclude.insert(session.node.node_id().to_string());
		let candidates = graph.find_peers(&FindPeersQuery {
			toward: &toward,
			amount_msat: amount_sats * 1000,
			min_channels: input.min_channels.unwrap_or(3),
			exclude: &exclude,
			limit: input.limit.unwrap_or(10).clamp(1, 50),
		});
		Ok(FindPeersOutput { amount_sats, toward, excluded_existing_peers, candidates })
	}
}

// ---- node_profile ------------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct NodeIdInput {
	/// Full node id or a unique prefix of at least 6 characters.
	pub node_id: String,
}

#[derive(Serialize, JsonSchema)]
pub struct PublicChannel {
	pub counterparty: String,
	pub counterparty_alias: Option<String>,
	pub short_channel_id: u64,
	pub capacity_sats: Option<u64>,
	/// The fee this node charges to forward out over the channel, in ppm.
	pub fee_ppm: u64,
	pub enabled: bool,
}

#[derive(Serialize, JsonSchema)]
pub struct OurChannelBrief {
	pub channel_id: String,
	pub capacity_sats: u64,
	pub local_balance_sats: u64,
	pub remote_balance_sats: u64,
	pub is_usable: bool,
}

#[derive(Serialize, JsonSchema)]
pub struct ProbeSummary {
	pub succeeded: u64,
	pub failed: u64,
	pub last: Option<ProbeRecord>,
}

#[derive(Serialize, JsonSchema)]
pub struct NodeProfileOutput {
	pub node_id: String,
	pub alias: Option<String>,
	pub addresses: Vec<String>,
	pub channels: usize,
	pub capacity_sats: u64,
	pub median_fee_ppm: Option<u64>,
	/// Up to 30 of its public channels, largest first.
	pub public_channels: Vec<PublicChannel>,
	pub is_connected_to_us: bool,
	pub our_channel: Option<OurChannelBrief>,
	/// Forwards we failed in the last 24 hours whose requested next hop was this node.
	pub failed_forwards_toward_in_24h: u64,
	pub probes: ProbeSummary,
}

pub struct NodeProfile;

impl ToolBase for NodeProfile {
	type Parameter = NodeIdInput;
	type Output = NodeProfileOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"node_profile".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Everything we know about one node: its alias and addresses, its public channels with capacities and fees, whether we are connected or have a channel with it, how many of our failed forwards wanted to go to it, and what probes toward it found.".into())
	}
}

impl AsyncTool<Session> for NodeProfile {
	async fn invoke(session: &Session, input: NodeIdInput) -> Result<NodeProfileOutput, ErrorData> {
		let graph = GraphView::from_node(&session.node);
		let node_id = resolve_node_id(session, &graph, &input.node_id)?;
		let row = graph.node(&node_id).cloned().unwrap_or_default();
		let mut public_channels: Vec<PublicChannel> = graph
			.channels_of(&node_id)
			.into_iter()
			.map(|(counterparty, scid, capacity_sats, fee_ppm, enabled)| PublicChannel {
				counterparty_alias: alias_of(&graph, &counterparty),
				counterparty,
				short_channel_id: scid,
				capacity_sats,
				fee_ppm,
				enabled,
			})
			.collect();
		public_channels.sort_by(|a, b| b.capacity_sats.cmp(&a.capacity_sats));
		public_channels.truncate(30);
		let our_channel = session
			.node
			.list_channels()
			.into_iter()
			.find(|c| c.counterparty.node_id.to_string() == node_id)
			.map(|c| OurChannelBrief {
				channel_id: c.channel_id.to_string(),
				capacity_sats: c.channel_value_sats,
				local_balance_sats: c.outbound_capacity_msat / 1000,
				remote_balance_sats: c.inbound_capacity_msat / 1000,
				is_usable: c.is_usable,
			});
		let since = now_secs().saturating_sub(24 * 3600);
		let failed_forwards_toward_in_24h = session
			.ledger
			.recent_failures(10_000)
			.iter()
			.filter(|f| f.at >= since && f.next_node_id.as_deref() == Some(node_id.as_str()))
			.count() as u64;
		let probes = session.ledger.probes_since(0, Some(&node_id));
		Ok(NodeProfileOutput {
			alias: row.alias,
			addresses: row.addresses,
			channels: graph.degree(&node_id),
			capacity_sats: graph.capacity_sats(&node_id),
			median_fee_ppm: graph.median_fee_ppm(&node_id),
			public_channels,
			is_connected_to_us: session
				.node
				.list_peers()
				.iter()
				.any(|p| p.is_connected && p.node_id.to_string() == node_id),
			our_channel,
			failed_forwards_toward_in_24h,
			probes: ProbeSummary {
				succeeded: probes.iter().filter(|p| p.success).count() as u64,
				failed: probes.iter().filter(|p| !p.success).count() as u64,
				last: probes.first().cloned(),
			},
			node_id,
		})
	}
}

// ---- route_cost --------------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct RouteCostInput {
	/// Destination node id (or unique prefix).
	pub to: String,
	/// Starting node id (or unique prefix). Defaults to this node.
	pub from: Option<String>,
	/// Payment size the fees are computed for. Defaults to 100000.
	pub amount_sats: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
pub struct RouteCostOutput {
	pub from: String,
	pub to: String,
	pub amount_sats: u64,
	/// The cheapest route gossip knows, or `None` when no public route carries the amount.
	pub path: Option<PathCost>,
	pub hop_aliases: Vec<Option<String>>,
}

pub struct RouteCost;

impl ToolBase for RouteCost {
	type Parameter = RouteCostInput;
	type Output = RouteCostOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"route_cost".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("The cheapest public route between two nodes by advertised fees, hop by hop. From this node by default; give `from` to see what a route through a prospective peer would cost. Advertised fees say nothing about live liquidity; probe to check that.".into())
	}
}

impl AsyncTool<Session> for RouteCost {
	async fn invoke(
		session: &Session, input: RouteCostInput,
	) -> Result<RouteCostOutput, ErrorData> {
		let graph = GraphView::from_node(&session.node);
		let to = resolve_node_id(session, &graph, &input.to)?;
		let from = match &input.from {
			Some(from) => resolve_node_id(session, &graph, from)?,
			None => session.node.node_id().to_string(),
		};
		let amount_sats = input.amount_sats.unwrap_or(100_000);
		let path = graph.cheapest_path(&from, &to, amount_sats * 1000);
		let hop_aliases = path
			.as_ref()
			.map(|p| p.hops.iter().map(|h| alias_of(&graph, &h.node_id)).collect())
			.unwrap_or_default();
		Ok(RouteCostOutput { from, to, amount_sats, path, hop_aliases })
	}
}

// ---- demand ------------------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct DemandInput {
	/// Window, in hours. Defaults to 24.
	pub window_hours: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
pub struct FailedDestination {
	pub node_id: String,
	pub alias: Option<String>,
	pub failures: u64,
	/// Failure count by reason; `UnknownNextHop` means we have no channel toward it.
	pub reasons: BTreeMap<String, u64>,
	pub we_have_channel: bool,
}

#[derive(Serialize, JsonSchema)]
pub struct PeerFlow {
	pub node_id: String,
	pub alias: Option<String>,
	pub forwards_out: u64,
	pub volume_out_msat: u64,
	pub fees_earned_msat: u64,
}

#[derive(Serialize, JsonSchema)]
pub struct DemandOutput {
	pub window_hours: u64,
	/// Where forwards we could not complete wanted to go, most first.
	pub failed_forwards_by_next_node: Vec<FailedDestination>,
	/// Which peers our successful forwards left toward, most first.
	pub forwards_out_by_peer: Vec<PeerFlow>,
}

pub struct Demand;

impl ToolBase for Demand {
	type Parameter = DemandInput;
	type Output = DemandOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"demand".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("What traffic through this node wanted: the next hops of forwards we could not complete (with the reason, and whether we even have a channel there) and the peers our successful forwards left toward. Demand we could not serve is the strongest signal for where a new channel would earn.".into())
	}
}

impl AsyncTool<Session> for Demand {
	async fn invoke(session: &Session, input: DemandInput) -> Result<DemandOutput, ErrorData> {
		let graph = GraphView::from_node(&session.node);
		let window_hours = input.window_hours.unwrap_or(24);
		let since = now_secs().saturating_sub(window_hours * 3600);
		let peers = channel_peers(session);
		let counterparty_by_channel: HashMap<String, String> = session
			.node
			.list_channels()
			.into_iter()
			.map(|c| (c.channel_id.to_string(), c.counterparty.node_id.to_string()))
			.collect();

		let mut failed: HashMap<String, FailedDestination> = HashMap::new();
		for f in session.ledger.recent_failures(10_000).into_iter().filter(|f| f.at >= since) {
			let node_id = match f.next_node_id {
				Some(id) => id,
				None => {
					match f.next_channel_id.as_ref().and_then(|c| counterparty_by_channel.get(c)) {
						Some(id) => id.clone(),
						None => continue,
					}
				},
			};
			let entry = failed.entry(node_id.clone()).or_insert_with(|| FailedDestination {
				alias: alias_of(&graph, &node_id),
				we_have_channel: peers.contains(&node_id),
				node_id,
				failures: 0,
				reasons: BTreeMap::new(),
			});
			entry.failures += 1;
			*entry.reasons.entry(f.reason).or_default() += 1;
		}
		let mut failed_forwards_by_next_node: Vec<FailedDestination> =
			failed.into_values().collect();
		failed_forwards_by_next_node.sort_by(|a, b| b.failures.cmp(&a.failures));

		let mut flows: HashMap<String, PeerFlow> = HashMap::new();
		for f in session.ledger.recent_forwards(10_000).into_iter().filter(|f| f.at >= since) {
			let Some(node_id) = counterparty_by_channel.get(&f.next_channel_id) else { continue };
			let entry = flows.entry(node_id.clone()).or_insert_with(|| PeerFlow {
				alias: alias_of(&graph, node_id),
				node_id: node_id.clone(),
				forwards_out: 0,
				volume_out_msat: 0,
				fees_earned_msat: 0,
			});
			entry.forwards_out += 1;
			entry.volume_out_msat += f.amount_msat;
			entry.fees_earned_msat += f.fee_msat.unwrap_or(0);
		}
		let mut forwards_out_by_peer: Vec<PeerFlow> = flows.into_values().collect();
		forwards_out_by_peer.sort_by(|a, b| b.fees_earned_msat.cmp(&a.fees_earned_msat));

		Ok(DemandOutput { window_hours, failed_forwards_by_next_node, forwards_out_by_peer })
	}
}

// ---- probe -------------------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct ProbeInput {
	/// Destination node id (or unique prefix).
	pub node_id: String,
	/// Amount the probe should try to carry, in satoshis.
	pub amount_sats: u64,
	/// How long to wait for results before returning. Defaults to 20, at most 60.
	pub wait_secs: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
pub struct ProbeOutput {
	pub node_id: String,
	pub amount_sats: u64,
	/// Results that came back within the wait; more may follow in probe_results.
	pub results: Vec<ProbeRecord>,
	pub note: String,
}

pub struct Probe;

impl ToolBase for Probe {
	type Parameter = ProbeInput;
	type Output = ProbeOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"probe".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Send payment probes from this node to a destination for an amount and report whether the paths could carry it right now, and what they would charge. A probe is a payment that cannot be claimed, so no funds move. Needs an outbound channel to leave from: before we have one, routes cannot be probed, only costed with route_cost.".into())
	}
}

impl AsyncTool<Session> for Probe {
	async fn invoke(session: &Session, input: ProbeInput) -> Result<ProbeOutput, ErrorData> {
		if input.amount_sats == 0 {
			return Err(ErrorData::invalid_params("amount_sats must be positive", None));
		}
		let graph = GraphView::from_node(&session.node);
		let node_id = resolve_node_id(session, &graph, &input.node_id)?;
		let sent_at = now_secs();
		session
			.node
			.spontaneous_payment()
			.send_probes(input.amount_sats * 1000, pubkey(&node_id)?)
			.map_err(|e| ErrorData::internal_error(format!("could not send probes: {e}"), None))?;
		let wait = Duration::from_secs(input.wait_secs.unwrap_or(20).clamp(1, 60));
		let started = tokio::time::Instant::now();
		let mut results = Vec::new();
		while started.elapsed() < wait {
			tokio::time::sleep(Duration::from_secs(1)).await;
			results = session.ledger.probes_since(sent_at, Some(&node_id));
			if !results.is_empty() {
				// Give the other paths of the same probe a moment to report.
				tokio::time::sleep(Duration::from_secs(2)).await;
				results = session.ledger.probes_since(sent_at, Some(&node_id));
				break;
			}
		}
		let note = if results.is_empty() {
			format!(
				"no result within {}s; the probe may still be in flight, check probe_results later",
				wait.as_secs()
			)
		} else {
			format!(
				"{} of {} paths carried {} sats",
				results.iter().filter(|r| r.success).count(),
				results.len(),
				input.amount_sats
			)
		};
		Ok(ProbeOutput { node_id, amount_sats: input.amount_sats, results, note })
	}
}

// ---- probe_results -----------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct ProbeResultsInput {
	/// Only probes toward this node id (or unique prefix).
	pub node_id: Option<String>,
	/// Maximum number of results, newest first. Defaults to 50.
	pub limit: Option<usize>,
}

#[derive(Serialize, JsonSchema)]
pub struct ProbeResultsOutput {
	pub probes: Vec<ProbeRecord>,
}

pub struct ProbeResults;

impl ToolBase for ProbeResults {
	type Parameter = ProbeResultsInput;
	type Output = ProbeResultsOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"probe_results".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Results of probes sent so far, newest first: the ones you asked for and the ones the node's background prober sends on its own toward well-connected nodes. Each says the path, the amount, whether it got through, and the channel that failed it.".into())
	}
}

impl AsyncTool<Session> for ProbeResults {
	async fn invoke(
		session: &Session, input: ProbeResultsInput,
	) -> Result<ProbeResultsOutput, ErrorData> {
		let limit = input.limit.unwrap_or(50);
		let probes = match input.node_id {
			Some(id) => {
				let graph = GraphView::from_node(&session.node);
				let node_id = resolve_node_id(session, &graph, &id)?;
				let mut probes = session.ledger.probes_since(0, Some(&node_id));
				probes.truncate(limit);
				probes
			},
			None => session.ledger.recent_probes(limit),
		};
		Ok(ProbeResultsOutput { probes })
	}
}

// ---- receive_address ---------------------------------------------------------------------

#[derive(Serialize, JsonSchema)]
pub struct ReceiveAddressOutput {
	pub address: String,
	pub note: String,
}

pub struct ReceiveAddress;

impl ToolBase for ReceiveAddress {
	type Parameter = NoInput;
	type Output = ReceiveAddressOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"receive_address".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("A fresh on-chain address of this node's wallet, for the operator to fund it. Funds count as spendable once confirmed.".into())
	}
}

impl AsyncTool<Session> for ReceiveAddress {
	async fn invoke(session: &Session, _: NoInput) -> Result<ReceiveAddressOutput, ErrorData> {
		let address = session
			.node
			.onchain_payment()
			.new_address()
			.map_err(|e| ErrorData::internal_error(format!("new address: {e}"), None))?;
		Ok(ReceiveAddressOutput {
			address: address.to_string(),
			note: "funds sent here are spendable after one confirmation".into(),
		})
	}
}

// ---- connect_peer ------------------------------------------------------------------------

#[derive(Default, Deserialize, JsonSchema)]
pub struct ConnectPeerInput {
	/// Node id (or unique prefix).
	pub node_id: String,
	/// `host:port`. Defaults to the first clearnet address the node announced.
	pub address: Option<String>,
}

#[derive(Serialize, JsonSchema)]
pub struct ConnectPeerOutput {
	pub node_id: String,
	pub address: String,
	pub status: String,
}

pub struct ConnectPeer;

impl ToolBase for ConnectPeer {
	type Parameter = ConnectPeerInput;
	type Output = ConnectPeerOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		"connect_peer".into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Connect to a node (and reconnect to it after restarts). Moves no funds. Use it to restore a dropped connection to a channel peer, or before opening a channel to a node that announced no address.".into())
	}
}

impl AsyncTool<Session> for ConnectPeer {
	async fn invoke(
		session: &Session, input: ConnectPeerInput,
	) -> Result<ConnectPeerOutput, ErrorData> {
		let graph = GraphView::from_node(&session.node);
		let node_id = resolve_node_id(session, &graph, &input.node_id)?;
		let address = address_for(&graph, &node_id, input.address.as_deref())?;
		session
			.node
			.connect(pubkey(&node_id)?, address.clone(), true)
			.map_err(|e| ErrorData::internal_error(format!("connect failed: {e}"), None))?;
		Ok(ConnectPeerOutput {
			node_id,
			address: address.to_string(),
			status: "connected; the node will reconnect on its own from now on".into(),
		})
	}
}

// ---- open_channel ------------------------------------------------------------------------

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct OpenChannelInput {
	/// Node id (or unique prefix) of the peer.
	pub node_id: String,
	/// Channel capacity in satoshis, funded from this node's on-chain wallet.
	pub amount_sats: u64,
	/// `host:port`. Defaults to the first clearnet address the node announced.
	pub address: Option<String>,
	/// Satoshis to give the peer at open, so the channel starts with inbound capacity.
	/// Defaults to 0.
	pub push_sats: Option<u64>,
}

#[derive(Serialize, JsonSchema)]
pub struct OpenChannelOutput {
	pub node_id: String,
	pub address: String,
	pub amount_sats: u64,
	pub push_sats: u64,
	pub user_channel_id: String,
	pub status: String,
}

pub struct OpenChannel;

impl ToolBase for OpenChannel {
	type Parameter = OpenChannelInput;
	type Output = OpenChannelOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		OPEN_CHANNEL.into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Open a public channel to a node, funded from this node's on-chain wallet (see spendable_onchain_sats). The channel becomes usable after the funding transaction confirms. Locks the amount until the channel closes, so it always waits for the operator's approval; propose it with the reason and the expected use.".into())
	}
}

impl AsyncTool<Session> for OpenChannel {
	async fn invoke(
		session: &Session, input: OpenChannelInput,
	) -> Result<OpenChannelOutput, ErrorData> {
		if input.amount_sats == 0 {
			return Err(ErrorData::invalid_params("amount_sats must be positive", None));
		}
		let spendable = session.node.list_balances().spendable_onchain_balance_sats;
		if input.amount_sats > spendable {
			return Err(ErrorData::invalid_params(
				format!(
					"amount_sats exceeds the spendable on-chain balance of {spendable} sats \
					 (the funding transaction fee comes on top)"
				),
				None,
			));
		}
		let graph = GraphView::from_node(&session.node);
		let node_id = resolve_node_id(session, &graph, &input.node_id)?;
		let address = address_for(&graph, &node_id, input.address.as_deref())?;
		let push_sats = input.push_sats.unwrap_or(0);
		let user_channel_id = session
			.node
			.open_announced_channel(
				pubkey(&node_id)?,
				address.clone(),
				input.amount_sats,
				(push_sats > 0).then_some(push_sats * 1000),
				None,
			)
			.map_err(|e| ErrorData::internal_error(format!("open failed: {e}"), None))?;
		Ok(OpenChannelOutput {
			node_id,
			address: address.to_string(),
			amount_sats: input.amount_sats,
			push_sats,
			user_channel_id: user_channel_id.0.to_string(),
			status: "channel negotiated and funding transaction broadcast; usable once it confirms"
				.into(),
		})
	}
}

// ---- close_channel -----------------------------------------------------------------------

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct CloseChannelInput {
	/// Channel id (or unique prefix) from channel_overview.
	pub channel_id: String,
}

#[derive(Serialize, JsonSchema)]
pub struct CloseChannelOutput {
	pub channel_id: String,
	pub counterparty_node_id: String,
	pub local_balance_sats: u64,
	pub status: String,
}

pub struct CloseChannel;

impl ToolBase for CloseChannel {
	type Parameter = CloseChannelInput;
	type Output = CloseChannelOutput;
	type Error = ErrorData;

	fn name() -> Cow<'static, str> {
		CLOSE_CHANNEL.into()
	}

	fn description() -> Option<Cow<'static, str>> {
		Some("Cooperatively close one of our channels. Our balance returns to the on-chain wallet once the closing transaction confirms, ready to be deployed elsewhere. Always waits for the operator's approval.".into())
	}
}

impl AsyncTool<Session> for CloseChannel {
	async fn invoke(
		session: &Session, input: CloseChannelInput,
	) -> Result<CloseChannelOutput, ErrorData> {
		let channel = crate::tools::find_channel(session, &input.channel_id)?;
		session
			.node
			.close_channel(&channel.user_channel_id, channel.counterparty.node_id)
			.map_err(|e| ErrorData::internal_error(format!("close failed: {e}"), None))?;
		Ok(CloseChannelOutput {
			channel_id: channel.channel_id.to_string(),
			counterparty_node_id: channel.counterparty.node_id.to_string(),
			local_balance_sats: channel.outbound_capacity_msat / 1000,
			status: "close negotiated; the balance is spendable on-chain once the closing transaction confirms".into(),
		})
	}
}

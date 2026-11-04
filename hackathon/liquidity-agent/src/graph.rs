// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Read-only analysis over the node's gossip graph: who is well connected, what a route to
//! a destination costs, and which peers would put us close to the destinations we care
//! about. The numeric work happens here so the model reasons over a shortlist rather than
//! the whole graph.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

use ldk_node::lightning::routing::gossip::ChannelUpdateInfo;
use ldk_node::Node;
use schemars::JsonSchema;
use serde::Serialize;

/// Fee charged for routes that cannot reach a destination at all, so that one unreachable
/// destination ranks a candidate below any candidate that reaches them all.
const UNREACHABLE_FEE_MSAT: u64 = 10_000_000;

/// One direction of a public channel, as gossip describes it.
#[derive(Clone, Debug)]
pub struct Edge {
	pub from: String,
	pub to: String,
	pub short_channel_id: u64,
	pub base_fee_msat: u64,
	pub fee_ppm: u64,
	pub htlc_maximum_msat: u64,
	pub enabled: bool,
	/// The funding output's value when the node verified it against the chain; otherwise the
	/// largest HTLC either direction accepts, which is the best gossip alone can say.
	pub capacity_sats: Option<u64>,
}

impl Edge {
	fn fee_for(&self, amount_msat: u64) -> u64 {
		self.base_fee_msat + amount_msat * self.fee_ppm / 1_000_000
	}

	fn carries(&self, amount_msat: u64) -> bool {
		self.enabled && self.htlc_maximum_msat >= amount_msat
	}
}

#[derive(Clone, Debug, Default)]
pub struct NodeRow {
	pub node_id: String,
	pub alias: Option<String>,
	pub addresses: Vec<String>,
}

pub struct GraphView {
	nodes: HashMap<String, NodeRow>,
	edges_from: HashMap<String, Vec<Edge>>,
	edges_to: HashMap<String, Vec<Edge>>,
	channel_count: usize,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Hop {
	pub node_id: String,
	pub short_channel_id: u64,
	/// What this hop's channel charges for the amount, in msat.
	pub fee_msat: u64,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PathCost {
	pub hops: Vec<Hop>,
	pub total_fee_msat: u64,
}

/// How a candidate peer reaches one destination the operator cares about.
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Reach {
	pub destination: String,
	/// Cheapest known fee from the candidate to the destination, in msat; `None` when gossip
	/// shows no route that carries the amount.
	pub fee_msat: Option<u64>,
	pub hops: Option<usize>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct PeerCandidate {
	pub node_id: String,
	pub alias: Option<String>,
	/// Clearnet addresses first; empty when the node announced none.
	pub addresses: Vec<String>,
	/// Public channels.
	pub channels: usize,
	/// Sum of the capacities of its public channels, where known.
	pub capacity_sats: u64,
	/// Median proportional fee it charges across its channels, in ppm.
	pub median_fee_ppm: Option<u64>,
	pub reach: Vec<Reach>,
	/// Sum of the fees to every destination (an unreachable one counts as a large fee); the
	/// list is sorted by this, lowest first.
	pub score_msat: u64,
}

pub struct FindPeersQuery<'a> {
	/// Destinations the operator wants to reach; empty ranks by connectivity alone.
	pub toward: &'a [String],
	pub amount_msat: u64,
	pub min_channels: usize,
	pub exclude: &'a HashSet<String>,
	pub limit: usize,
}

/// Keeps printable ASCII and trims; a gossip alias may contain anything.
fn clean_alias(alias: &str) -> Option<String> {
	let cleaned: String =
		alias.chars().filter(|c| c.is_ascii_graphic() || *c == ' ').take(32).collect();
	let cleaned = cleaned.trim().to_string();
	(!cleaned.is_empty()).then_some(cleaned)
}

impl GraphView {
	pub fn new(nodes: Vec<NodeRow>, edges: Vec<Edge>) -> Self {
		let mut channels = HashSet::new();
		let mut edges_from: HashMap<String, Vec<Edge>> = HashMap::new();
		let mut edges_to: HashMap<String, Vec<Edge>> = HashMap::new();
		for edge in edges {
			channels.insert(edge.short_channel_id);
			edges_to.entry(edge.to.clone()).or_default().push(edge.clone());
			edges_from.entry(edge.from.clone()).or_default().push(edge);
		}
		let nodes = nodes.into_iter().map(|n| (n.node_id.clone(), n)).collect();
		Self { nodes, edges_from, edges_to, channel_count: channels.len() }
	}

	/// Snapshots the node's current gossip graph.
	pub fn from_node(node: &Node) -> Self {
		let graph = node.network_graph();
		let nodes = graph
			.list_nodes()
			.into_iter()
			.filter_map(|id| {
				let info = graph.node(&id)?;
				let (alias, addresses) = match info.announcement_info {
					Some(announcement) => {
						let mut addresses: Vec<String> =
							announcement.addresses().iter().map(|a| a.to_string()).collect();
						// Clearnet first: that is what we can connect to.
						addresses.sort_by_key(|a| a.contains(".onion"));
						(clean_alias(&announcement.alias().to_string()), addresses)
					},
					None => (None, vec![]),
				};
				Some(NodeRow { node_id: id.to_string(), alias, addresses })
			})
			.collect();
		let edges = graph
			.list_channels()
			.into_iter()
			.filter_map(|scid| {
				let info = graph.channel(scid)?;
				let one = info.node_one.to_string();
				let two = info.node_two.to_string();
				let capacity_sats = info.capacity_sats.or_else(|| {
					[&info.one_to_two, &info.two_to_one]
						.into_iter()
						.filter_map(|update| update.as_ref().map(|u| u.htlc_maximum_msat / 1000))
						.max()
						.filter(|sats| *sats > 0)
				});
				let direction = |from: &str, to: &str, update: Option<&ChannelUpdateInfo>| Edge {
					from: from.to_string(),
					to: to.to_string(),
					short_channel_id: scid,
					base_fee_msat: update.map(|u| u.fees.base_msat as u64).unwrap_or(0),
					fee_ppm: update.map(|u| u.fees.proportional_millionths as u64).unwrap_or(0),
					htlc_maximum_msat: update.map(|u| u.htlc_maximum_msat).unwrap_or(0),
					// No update yet means the direction cannot be used.
					enabled: update.is_some_and(|u| u.enabled),
					capacity_sats,
				};
				Some([
					direction(&one, &two, info.one_to_two.as_ref()),
					direction(&two, &one, info.two_to_one.as_ref()),
				])
			})
			.flatten()
			.collect();
		Self::new(nodes, edges)
	}

	pub fn node_count(&self) -> usize {
		self.nodes.len()
	}

	pub fn channel_count(&self) -> usize {
		self.channel_count
	}

	pub fn node(&self, node_id: &str) -> Option<&NodeRow> {
		self.nodes.get(node_id)
	}

	/// Node ids that start with `prefix`, for looking a node up by a partial id.
	pub fn nodes_with_prefix(&self, prefix: &str) -> Vec<String> {
		self.nodes.keys().filter(|id| id.starts_with(prefix)).cloned().collect()
	}

	/// Number of public channels the node is a party to.
	pub fn degree(&self, node_id: &str) -> usize {
		self.edges_from.get(node_id).map(|e| e.len()).unwrap_or(0)
	}

	pub fn capacity_sats(&self, node_id: &str) -> u64 {
		self.edges_from
			.get(node_id)
			.map(|edges| edges.iter().filter_map(|e| e.capacity_sats).sum())
			.unwrap_or(0)
	}

	pub fn median_fee_ppm(&self, node_id: &str) -> Option<u64> {
		let mut fees: Vec<u64> =
			self.edges_from.get(node_id)?.iter().filter(|e| e.enabled).map(|e| e.fee_ppm).collect();
		if fees.is_empty() {
			return None;
		}
		fees.sort_unstable();
		Some(fees[fees.len() / 2])
	}

	/// Our peer's channels with their counterparties, for a profile: (counterparty, scid,
	/// capacity, fee ppm charged by `node_id` on that channel).
	pub fn channels_of(&self, node_id: &str) -> Vec<(String, u64, Option<u64>, u64, bool)> {
		self.edges_from
			.get(node_id)
			.map(|edges| {
				edges
					.iter()
					.map(|e| {
						(e.to.clone(), e.short_channel_id, e.capacity_sats, e.fee_ppm, e.enabled)
					})
					.collect()
			})
			.unwrap_or_default()
	}

	/// The nodes with the most public channels, best first.
	pub fn best_connected(&self, limit: usize) -> Vec<(String, usize, u64)> {
		let mut rows: Vec<(String, usize, u64)> = self
			.nodes
			.keys()
			.map(|id| (id.clone(), self.degree(id), self.capacity_sats(id)))
			.collect();
		rows.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)).then(a.0.cmp(&b.0)));
		rows.truncate(limit);
		rows
	}

	/// Cheapest known path from `from` to `to` for `amount_msat`, by gossip fees alone. The
	/// fee at every hop is computed on the final amount, which is close enough for ranking.
	pub fn cheapest_path(&self, from: &str, to: &str, amount_msat: u64) -> Option<PathCost> {
		if from == to {
			return Some(PathCost { hops: vec![], total_fee_msat: 0 });
		}
		let mut best: HashMap<&str, u64> = HashMap::new();
		let mut previous: HashMap<&str, &Edge> = HashMap::new();
		let mut heap = BinaryHeap::new();
		best.insert(from, 0);
		heap.push(Reverse((0u64, from)));
		while let Some(Reverse((cost, node))) = heap.pop() {
			if node == to {
				break;
			}
			if best.get(node).is_some_and(|b| *b < cost) {
				continue;
			}
			for edge in self.edges_from.get(node).into_iter().flatten() {
				if !edge.carries(amount_msat) {
					continue;
				}
				let next_cost = cost + edge.fee_for(amount_msat);
				let next = edge.to.as_str();
				if best.get(next).is_none_or(|b| next_cost < *b) {
					best.insert(next, next_cost);
					previous.insert(next, edge);
					heap.push(Reverse((next_cost, next)));
				}
			}
		}
		let total_fee_msat = *best.get(to)?;
		let mut hops = Vec::new();
		let mut node = to;
		while node != from {
			let edge = previous.get(node)?;
			hops.push(Hop {
				node_id: edge.to.clone(),
				short_channel_id: edge.short_channel_id,
				fee_msat: edge.fee_for(amount_msat),
			});
			node = edge.from.as_str();
		}
		hops.reverse();
		Some(PathCost { hops, total_fee_msat })
	}

	/// Cheapest known fee from every node to `to`, by gossip fees alone; nodes with no route
	/// are absent.
	fn costs_to<'a>(&'a self, to: &'a str, amount_msat: u64) -> HashMap<&'a str, (u64, usize)> {
		let mut best: HashMap<&'a str, (u64, usize)> = HashMap::new();
		let mut heap = BinaryHeap::new();
		best.insert(to, (0, 0));
		heap.push(Reverse((0u64, 0usize, to)));
		while let Some(Reverse((cost, hops, node))) = heap.pop() {
			if best.get(node).is_some_and(|b| b.0 < cost) {
				continue;
			}
			for edge in self.edges_to.get(node).into_iter().flatten() {
				if !edge.carries(amount_msat) {
					continue;
				}
				let next_cost = cost + edge.fee_for(amount_msat);
				let from = edge.from.as_str();
				if best.get(from).is_none_or(|b| next_cost < b.0) {
					best.insert(from, (next_cost, hops + 1));
					heap.push(Reverse((next_cost, hops + 1, from)));
				}
			}
		}
		best
	}

	/// Candidate peers ranked by how cheaply they reach the destinations in `toward`, then
	/// by connectivity. With no destinations, ranked by connectivity alone.
	pub fn find_peers(&self, query: &FindPeersQuery<'_>) -> Vec<PeerCandidate> {
		let costs: Vec<(&String, HashMap<&str, (u64, usize)>)> =
			query.toward.iter().map(|to| (to, self.costs_to(to, query.amount_msat))).collect();
		let mut candidates: Vec<PeerCandidate> = self
			.nodes
			.values()
			.filter(|n| !query.exclude.contains(&n.node_id))
			.filter(|n| self.degree(&n.node_id) >= query.min_channels)
			.map(|n| {
				let reach: Vec<Reach> = costs
					.iter()
					.map(|(to, costs)| {
						let cost = costs.get(n.node_id.as_str());
						Reach {
							destination: (*to).clone(),
							fee_msat: cost.map(|c| c.0),
							hops: cost.map(|c| c.1),
						}
					})
					.collect();
				let score_msat =
					reach.iter().map(|r| r.fee_msat.unwrap_or(UNREACHABLE_FEE_MSAT)).sum();
				PeerCandidate {
					node_id: n.node_id.clone(),
					alias: n.alias.clone(),
					addresses: n.addresses.clone(),
					channels: self.degree(&n.node_id),
					capacity_sats: self.capacity_sats(&n.node_id),
					median_fee_ppm: self.median_fee_ppm(&n.node_id),
					reach,
					score_msat,
				}
			})
			.collect();
		candidates.sort_by(|a, b| {
			a.score_msat
				.cmp(&b.score_msat)
				.then(b.channels.cmp(&a.channels))
				.then(b.capacity_sats.cmp(&a.capacity_sats))
				.then(a.node_id.cmp(&b.node_id))
		});
		candidates.truncate(query.limit);
		candidates
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn edge(from: &str, to: &str, scid: u64, ppm: u64) -> Edge {
		Edge {
			from: from.into(),
			to: to.into(),
			short_channel_id: scid,
			base_fee_msat: 0,
			fee_ppm: ppm,
			htlc_maximum_msat: 1_000_000_000,
			enabled: true,
			capacity_sats: Some(1_000_000),
		}
	}

	fn node(id: &str) -> NodeRow {
		NodeRow { node_id: id.into(), ..Default::default() }
	}

	/// a -> b -> d is cheaper than a -> c -> d; c reaches d directly, b does not reach e.
	fn graph() -> GraphView {
		GraphView::new(
			["a", "b", "c", "d", "e"].into_iter().map(node).collect(),
			vec![
				edge("a", "b", 1, 100),
				edge("b", "a", 1, 100),
				edge("b", "d", 2, 100),
				edge("d", "b", 2, 100),
				edge("a", "c", 3, 1000),
				edge("c", "a", 3, 1000),
				edge("c", "d", 4, 1000),
				edge("d", "c", 4, 1000),
				edge("c", "e", 5, 50),
				edge("e", "c", 5, 50),
			],
		)
	}

	#[test]
	fn cheapest_path_follows_fees_not_hops() {
		let path = graph().cheapest_path("a", "d", 1_000_000).unwrap();
		let hops: Vec<&str> = path.hops.iter().map(|h| h.node_id.as_str()).collect();
		assert_eq!(hops, vec!["b", "d"]);
		assert_eq!(path.total_fee_msat, 200);
		assert!(graph().cheapest_path("b", "e", 1_000_000).is_some(), "via a and c");
	}

	#[test]
	fn disabled_or_small_channels_are_skipped() {
		let mut edges = vec![edge("a", "b", 1, 10), edge("b", "c", 2, 10)];
		edges[1].htlc_maximum_msat = 10;
		let graph = GraphView::new(["a", "b", "c"].into_iter().map(node).collect(), edges);
		assert!(graph.cheapest_path("a", "c", 1_000).is_none());
		assert_eq!(graph.cheapest_path("a", "c", 10).unwrap().hops.len(), 2);
	}

	#[test]
	fn peers_rank_by_cost_toward_destinations() {
		let exclude = HashSet::from(["a".to_string()]);
		let toward = ["d".to_string(), "e".to_string()];
		let graph = graph();
		let ranked = graph.find_peers(&FindPeersQuery {
			toward: &toward,
			amount_msat: 1_000_000,
			min_channels: 1,
			exclude: &exclude,
			limit: 10,
		});
		let ids: Vec<&str> = ranked.iter().map(|c| c.node_id.as_str()).collect();
		// c reaches both directly (1000 + 50), d reaches e through c (0 + 1050), e reaches d
		// through c (1050 + 0): a three-way tie broken by channel count. b pays 100 to d but
		// 1150 to e.
		assert_eq!(ids, vec!["c", "d", "e", "b"]);
		assert!(ranked.iter().all(|c| c.reach.len() == 2));
		assert_eq!(ranked[3].reach[1].fee_msat, Some(1150));
	}

	#[test]
	fn peers_without_destinations_rank_by_connectivity() {
		let graph = graph();
		let ranked = graph.find_peers(&FindPeersQuery {
			toward: &[],
			amount_msat: 1,
			min_channels: 2,
			exclude: &HashSet::new(),
			limit: 2,
		});
		let ids: Vec<&str> = ranked.iter().map(|c| c.node_id.as_str()).collect();
		assert_eq!(ids, vec!["c", "a"], "c has three channels, a and b and d have two");
	}
}

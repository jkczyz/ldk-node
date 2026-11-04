// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Builds and starts the LDK Node instance the agent manages, from environment variables.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use ldk_node::bitcoin::secp256k1::PublicKey;
use ldk_node::bitcoin::Network;
use ldk_node::config::{BackgroundSyncConfig, EsploraSyncConfig};
use ldk_node::entropy::NodeEntropy;
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::probing::ProbingConfigBuilder;
use ldk_node::{Builder, Node};

/// How often the background prober sends a probe, when probing is on.
const PROBING_INTERVAL: Duration = Duration::from_secs(20);
/// At most this much can be locked in in-flight probes; a probe stuck behind an offline hop
/// holds its amount until the HTLC times out.
const PROBING_MAX_LOCKED_MSAT: u64 = 50_000_000;

#[derive(Clone, Debug)]
pub enum GossipSource {
	P2p,
	Rgs(String),
}

#[derive(Clone)]
pub struct NodeOptions {
	pub network: Network,
	pub storage_dir: PathBuf,
	pub esplora_url: String,
	pub listen: SocketAddress,
	pub alias: Option<String>,
	/// Lightning wallet sync interval; the on-chain interval is twice this. `None` keeps LDK
	/// Node's defaults.
	pub sync_interval_secs: Option<u64>,
	pub gossip: GossipSource,
	/// Peers to connect to (and remember) once the node is running: gossip comes from them on
	/// a network where the node has no channels yet.
	pub peers: Vec<(PublicKey, SocketAddress)>,
	/// Background probing toward this many of the best-connected nodes; `None` turns it off.
	pub probing_top_nodes: Option<usize>,
}

fn env_or(name: &str, default: &str) -> String {
	std::env::var(name).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| default.to_string())
}

impl NodeOptions {
	/// `LDK_NETWORK` (regtest), `LDK_STORAGE_DIR` (./ldk-data), `LDK_ESPLORA_URL`
	/// (http://127.0.0.1:3002), `LDK_LISTEN` (127.0.0.1:9735), `LDK_ALIAS` (unset),
	/// `LDK_GOSSIP` (p2p, or a Rapid Gossip Sync server URL), `LDK_PEERS` (unset; comma-separated
	/// `node_id@host:port`), `LDK_PROBING_TOP_NODES` (unset: no background probing).
	pub fn from_env() -> Result<Self> {
		let network = Network::from_str(&env_or("LDK_NETWORK", "regtest"))
			.map_err(|e| anyhow::anyhow!("LDK_NETWORK: {e}"))?;
		let storage_dir = PathBuf::from(env_or("LDK_STORAGE_DIR", "./ldk-data"));
		let storage_dir = if storage_dir.is_absolute() {
			storage_dir
		} else {
			std::env::current_dir()?.join(storage_dir)
		};
		let listen = SocketAddress::from_str(&env_or("LDK_LISTEN", "127.0.0.1:9735"))
			.map_err(|e| anyhow::anyhow!("LDK_LISTEN: {e:?}"))?;
		let gossip = match env_or("LDK_GOSSIP", "p2p").as_str() {
			"p2p" => GossipSource::P2p,
			url if url.starts_with("http://") || url.starts_with("https://") => {
				GossipSource::Rgs(url.to_string())
			},
			other => bail!("LDK_GOSSIP must be p2p or a Rapid Gossip Sync URL, not {other}"),
		};
		let peers = env_or("LDK_PEERS", "")
			.split(',')
			.map(str::trim)
			.filter(|p| !p.is_empty())
			.map(parse_peer)
			.collect::<Result<Vec<_>>>()?;
		let probing_top_nodes = match env_or("LDK_PROBING_TOP_NODES", "").trim() {
			"" | "0" => None,
			n => Some(n.parse().context("LDK_PROBING_TOP_NODES must be a count")?),
		};
		Ok(Self {
			network,
			storage_dir,
			esplora_url: env_or("LDK_ESPLORA_URL", "http://127.0.0.1:3002"),
			listen,
			alias: std::env::var("LDK_ALIAS").ok().filter(|v| !v.trim().is_empty()),
			sync_interval_secs: None,
			gossip,
			peers,
			probing_top_nodes,
		})
	}
}

/// Parses `node_id@host:port`.
pub fn parse_peer(peer: &str) -> Result<(PublicKey, SocketAddress)> {
	let (node_id, address) =
		peer.split_once('@').with_context(|| format!("{peer}: expected node_id@host:port"))?;
	let node_id = PublicKey::from_str(node_id).with_context(|| format!("{peer}: node id"))?;
	let address =
		SocketAddress::from_str(address).map_err(|e| anyhow::anyhow!("{peer}: address: {e:?}"))?;
	Ok((node_id, address))
}

pub fn build_and_start(options: &NodeOptions) -> Result<Arc<Node>> {
	std::fs::create_dir_all(&options.storage_dir)
		.with_context(|| format!("create {}", options.storage_dir.display()))?;
	let storage_dir = options.storage_dir.to_string_lossy().into_owned();

	let mut builder = Builder::new();
	builder.set_network(options.network);
	builder.set_storage_dir_path(storage_dir.clone());
	let sync_config = options.sync_interval_secs.map(|secs| EsploraSyncConfig {
		background_sync_config: Some(BackgroundSyncConfig {
			onchain_wallet_sync_interval_secs: secs * 2,
			lightning_wallet_sync_interval_secs: secs,
			fee_rate_cache_update_interval_secs: 60,
		}),
		..Default::default()
	});
	builder.set_chain_source_esplora(options.esplora_url.clone(), sync_config);
	match &options.gossip {
		GossipSource::P2p => builder.set_gossip_source_p2p(),
		GossipSource::Rgs(url) => builder.set_gossip_source_rgs(url.clone()),
	};
	if let Some(top_nodes) = options.probing_top_nodes {
		builder.set_probing_config(
			ProbingConfigBuilder::high_degree(top_nodes)
				.interval(PROBING_INTERVAL)
				.max_locked_msat(PROBING_MAX_LOCKED_MSAT)
				.build(),
		);
	}
	builder.set_filesystem_logger(None, None);
	builder
		.set_listening_addresses(vec![options.listen.clone()])
		.map_err(|e| anyhow::anyhow!("listening address: {e}"))?;
	if let Some(alias) = &options.alias {
		builder.set_node_alias(alias.clone()).map_err(|e| anyhow::anyhow!("node alias: {e}"))?;
	}

	let seed_path = format!("{storage_dir}/keys_seed");
	let entropy = NodeEntropy::from_seed_path(seed_path)
		.map_err(|e| anyhow::anyhow!("node entropy: {e:?}"))?;
	let node = builder.build(entropy).map_err(|e| anyhow::anyhow!("build node: {e}"))?;
	node.start().map_err(|e| anyhow::anyhow!("start node: {e}"))?;
	for (node_id, address) in &options.peers {
		if let Err(e) = node.connect(*node_id, address.clone(), true) {
			eprintln!("Could not connect to peer {node_id} at {address}: {e}");
		}
	}
	Ok(Arc::new(node))
}

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

use anyhow::{Context, Result};
use ldk_node::bitcoin::Network;
use ldk_node::config::{BackgroundSyncConfig, EsploraSyncConfig};
use ldk_node::entropy::NodeEntropy;
use ldk_node::lightning::ln::msgs::SocketAddress;
use ldk_node::{Builder, Node};

pub struct NodeOptions {
	pub network: Network,
	pub storage_dir: PathBuf,
	pub esplora_url: String,
	pub listen: SocketAddress,
	pub alias: Option<String>,
	/// Lightning wallet sync interval; the on-chain interval is twice this. `None` keeps LDK
	/// Node's defaults.
	pub sync_interval_secs: Option<u64>,
}

fn env_or(name: &str, default: &str) -> String {
	std::env::var(name).ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| default.to_string())
}

impl NodeOptions {
	/// `LDK_NETWORK` (regtest), `LDK_STORAGE_DIR` (./ldk-data), `LDK_ESPLORA_URL`
	/// (http://127.0.0.1:3002), `LDK_LISTEN` (127.0.0.1:9735), `LDK_ALIAS` (unset).
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
		Ok(Self {
			network,
			storage_dir,
			esplora_url: env_or("LDK_ESPLORA_URL", "http://127.0.0.1:3002"),
			listen,
			alias: std::env::var("LDK_ALIAS").ok().filter(|v| !v.trim().is_empty()),
			sync_interval_secs: None,
		})
	}
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
	Ok(Arc::new(node))
}

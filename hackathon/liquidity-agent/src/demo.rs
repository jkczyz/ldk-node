// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Regtest demo: four peer nodes around the agent's routing node R, funded and connected
//! through an existing regtest bitcoind, plus a traffic generator that drains one of R's
//! outbound channels so the agent has something to diagnose.
//!
//! ```text
//!   S1 ──(2M, push 0)──▶ R ──(1M, push 0)──▶ D1
//!   S2 ──(2M, push 0)──▶ R ──(1M, push 0)──▶ D2
//! ```
//!
//! All channels are announced: LDK Node does not forward into unannounced channels by
//! default, and announced channels let the sources route through R from gossip alone.
//!
//! R's wallet is funded with only what its two channel opens need, so once R -> D1 is drained
//! the agent cannot simply splice in new funds: the liquidity has to come from the idle
//! R -> D2 channel (or fees have to steer the traffic).

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use ldk_node::lightning_invoice::{Bolt11InvoiceDescription, Description};
use ldk_node::payment::PaymentStatus;
use ldk_node::{Event, Node};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::console::Notices;
use crate::node::{build_and_start, GossipSource, NodeOptions};

const FUNDING_BTC: &str = "0.05";
const FUNDING_SATS: u64 = 5_000_000;
/// What R gets on top of its two channel amounts: the anchor reserve LDK Node keeps back for
/// each of its four channels (25,000 sats each) plus funding fees. Little is left to splice in.
const ROUTER_FUNDING_MARGIN_SATS: u64 = 120_000;
const PEER_NAMES: [&str; 4] = ["S1", "S2", "D1", "D2"];
const SYNC_INTERVAL_SECS: u64 = 5;
/// How often the background miner produces a block once the demo is running.
const BLOCK_INTERVAL_SECS: u64 = 20;

pub struct DemoOptions {
	pub dir: PathBuf,
	pub port_base: u16,
	/// The `bitcoin-cli` command prefix, already pointed at the regtest node.
	pub bitcoin_cli: Vec<String>,
	pub channel_sats: u64,
	pub payment_sats: u64,
}

impl DemoOptions {
	/// `DEMO_DIR` (./demo-data), `DEMO_PORT_BASE` (9741), `DEMO_BITCOIN_CLI` (required; e.g.
	/// `bitcoin-cli -regtest -rpcuser=u -rpcpassword=p`), `DEMO_CHANNEL_SATS` (1000000),
	/// `DEMO_PAYMENT_SATS` (40000).
	pub fn from_env() -> Result<Self> {
		let env = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
		let dir = PathBuf::from(env("DEMO_DIR").unwrap_or_else(|| "./demo-data".into()));
		let dir = if dir.is_absolute() { dir } else { std::env::current_dir()?.join(dir) };
		let port_base = env("DEMO_PORT_BASE").map(|v| v.parse()).transpose()?.unwrap_or(9741);
		let bitcoin_cli: Vec<String> = env("DEMO_BITCOIN_CLI")
			.context("set DEMO_BITCOIN_CLI to a bitcoin-cli command for the regtest node")?
			.split_whitespace()
			.map(str::to_owned)
			.collect();
		let channel_sats =
			env("DEMO_CHANNEL_SATS").map(|v| v.parse()).transpose()?.unwrap_or(1_000_000);
		let payment_sats =
			env("DEMO_PAYMENT_SATS").map(|v| v.parse()).transpose()?.unwrap_or(40_000);
		Ok(Self { dir, port_base, bitcoin_cli, channel_sats, payment_sats })
	}

	/// Empties the demo directory. Refuses to touch a directory this demo did not create.
	pub fn prepare_dir(&self) -> Result<()> {
		let marker = self.dir.join(".ldk-demo");
		if self.dir.exists() {
			if !marker.exists() {
				bail!(
					"{} exists but was not created by this demo; refusing to wipe it",
					self.dir.display()
				);
			}
			fs::remove_dir_all(&self.dir)
				.with_context(|| format!("wipe {}", self.dir.display()))?;
		}
		fs::create_dir_all(&self.dir)?;
		fs::write(marker, b"")?;
		Ok(())
	}

	fn node_options(
		&self, base: &NodeOptions, name: &str, port_offset: u16,
	) -> Result<NodeOptions> {
		let listen = format!("127.0.0.1:{}", self.port_base + port_offset)
			.parse()
			.map_err(|e| anyhow!("listen address: {e:?}"))?;
		Ok(NodeOptions {
			network: base.network,
			storage_dir: self.dir.join(name.to_lowercase()),
			esplora_url: base.esplora_url.clone(),
			listen,
			alias: Some(format!("demo-{name}")),
			sync_interval_secs: Some(SYNC_INTERVAL_SECS),
			gossip: GossipSource::P2p,
			peers: vec![],
			probing_top_nodes: None,
		})
	}

	/// Options for R, the routing node the agent manages.
	pub fn router_options(&self, base: &NodeOptions) -> Result<NodeOptions> {
		self.node_options(base, "R", 0)
	}

	fn cli(&self, args: &[&str]) -> Result<String> {
		let (program, base) =
			self.bitcoin_cli.split_first().context("DEMO_BITCOIN_CLI is empty")?;
		let output = Command::new(program)
			.args(base)
			.args(args)
			.output()
			.with_context(|| format!("run {program}"))?;
		if !output.status.success() {
			bail!(
				"bitcoin-cli {} failed: {}",
				args.join(" "),
				String::from_utf8_lossy(&output.stderr).trim()
			);
		}
		Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
	}

	fn mine(&self, blocks: u32) -> Result<()> {
		self.cli(&["-generate", &blocks.to_string()])?;
		Ok(())
	}

	fn mempool(&self) -> Result<Vec<String>> {
		Ok(serde_json::from_str(&self.cli(&["getrawmempool"])?)?)
	}

	fn in_mempool(&self, txid: &str) -> bool {
		self.mempool().map(|txids| txids.iter().any(|t| t == txid)).unwrap_or(false)
	}
}

pub struct Peer {
	pub name: &'static str,
	pub node: Arc<Node>,
	pub paid: AtomicU64,
	pub failed: AtomicU64,
}

pub struct Topology {
	/// `S1`, `S2`, `D1`, `D2`, in that order.
	pub peers: Vec<Arc<Peer>>,
	drains: Vec<JoinHandle<()>>,
}

impl Topology {
	pub fn sources(&self) -> (&Peer, &Peer) {
		(&self.peers[0], &self.peers[1])
	}

	pub fn destinations(&self) -> (&Peer, &Peer) {
		(&self.peers[2], &self.peers[3])
	}

	pub fn shutdown(&self) {
		for drain in &self.drains {
			drain.abort();
		}
		for peer in &self.peers {
			if let Err(e) = peer.node.stop() {
				eprintln!("[demo] failed to stop {}: {e}", peer.name);
			}
		}
	}
}

/// Polls `cond` once a second until it holds or `timeout` passes.
async fn wait_for(timeout: Duration, what: &str, mut cond: impl FnMut() -> bool) -> Result<()> {
	let start = Instant::now();
	loop {
		if cond() {
			return Ok(());
		}
		if start.elapsed() > timeout {
			bail!("timed out after {}s waiting for {what}", timeout.as_secs());
		}
		tokio::time::sleep(Duration::from_secs(1)).await;
	}
}

fn drain_events(peer: Arc<Peer>, cancel: CancellationToken) -> JoinHandle<()> {
	tokio::spawn(async move {
		loop {
			let event = tokio::select! {
				biased;
				_ = cancel.cancelled() => break,
				event = peer.node.next_event_async() => event,
			};
			match &event {
				Event::ChannelReady { .. } => eprintln!("[demo] {}: channel ready", peer.name),
				Event::ChannelClosed { reason, .. } => {
					eprintln!("[demo] {}: channel closed: {reason:?}", peer.name)
				},
				_ => {},
			}
			if let Err(e) = peer.node.event_handled() {
				eprintln!("[demo] {}: failed to mark event handled: {e}", peer.name);
			}
		}
	})
}

/// The funding txid of `node`'s channel with `peer`, once the channel has one.
fn funding_txid(node: &Node, peer: &Node) -> Option<String> {
	node.list_channels()
		.into_iter()
		.find(|c| c.counterparty.node_id == peer.node_id())
		.and_then(|c| c.funding_txo)
		.map(|txo| txo.txid.to_string())
}

fn open_announced(from: &Node, to: &Node, sats: u64) -> Result<()> {
	let address = to
		.listening_addresses()
		.and_then(|addresses| addresses.into_iter().next())
		.context("peer has no listening address")?;
	from.open_announced_channel(to.node_id(), address, sats, None, None)
		.map_err(|e| anyhow!("open channel: {e}"))?;
	Ok(())
}

/// Starts the four peers, funds all five nodes, opens the channels, and waits until R sees
/// them all usable. `router` must already be running.
pub async fn build(
	router: &Arc<Node>, base: &NodeOptions, options: &DemoOptions, cancel: CancellationToken,
) -> Result<Topology> {
	let mut peers = Vec::new();
	let mut drains = Vec::new();
	for (i, name) in PEER_NAMES.iter().enumerate() {
		let node = build_and_start(&options.node_options(base, name, i as u16 + 1)?)
			.with_context(|| format!("start {name}"))?;
		eprintln!("[demo] {name} is {}", node.node_id());
		let peer =
			Arc::new(Peer { name, node, paid: AtomicU64::new(0), failed: AtomicU64::new(0) });
		drains.push(drain_events(Arc::clone(&peer), cancel.clone()));
		peers.push(peer);
	}
	let topology = Topology { peers, drains };

	let router_funding_sats = options.channel_sats * 2 + ROUTER_FUNDING_MARGIN_SATS;
	let mut all: Vec<&Arc<Node>> = vec![router];
	all.extend(topology.peers.iter().map(|p| &p.node));
	for (i, node) in all.iter().enumerate() {
		let address =
			node.onchain_payment().new_address().map_err(|e| anyhow!("new address: {e}"))?;
		let amount = if i == 0 { btc(router_funding_sats) } else { FUNDING_BTC.to_string() };
		options.cli(&["sendtoaddress", &address.to_string(), &amount])?;
	}
	options.mine(1)?;
	eprintln!(
		"[demo] funded R with {router_funding_sats} sats (just enough for its two channels) and \
		 the peers with {FUNDING_BTC} BTC each, waiting for wallets to sync"
	);
	wait_for(Duration::from_secs(120), "wallets to see their funds", || {
		all.iter().enumerate().all(|(i, node)| {
			let _ = node.sync_wallets();
			let expected = if i == 0 { router_funding_sats } else { FUNDING_SATS };
			node.list_balances().spendable_onchain_balance_sats >= expected
		})
	})
	.await?;

	let (s1, s2) = topology.sources();
	let (d1, d2) = topology.destinations();
	let sats = options.channel_sats;
	// The sources get twice R's outbound capacity so R -> D1 runs dry before S1 does; that is
	// what makes R fail forwards instead of S1 failing to send.
	open_announced(&s1.node, router, sats * 2).context("S1 -> R")?;
	open_announced(&s2.node, router, sats * 2).context("S2 -> R")?;
	open_announced(router, &d1.node, sats).context("R -> D1")?;
	// Back-to-back opens from one wallet: the second must not be funded until the wallet has
	// seen the first funding tx, or it spends the same inputs again.
	wait_for(Duration::from_secs(60), "the R -> D1 funding tx to reach the mempool", || {
		funding_txid(router, &d1.node).is_some_and(|txid| options.in_mempool(&txid))
	})
	.await?;
	wait_for(Duration::from_secs(60), "R's wallet to see the R -> D1 funding spend", || {
		router.sync_wallets().is_ok()
			&& router.list_balances().total_onchain_balance_sats <= router_funding_sats - sats
	})
	.await?;
	open_announced(router, &d2.node, sats).context("R -> D2")?;
	wait_for(Duration::from_secs(60), "all four funding txs to reach the mempool", || {
		[(&s1.node, router), (&s2.node, router), (router, &d1.node), (router, &d2.node)]
			.iter()
			.all(|(from, to)| funding_txid(from, to).is_some_and(|txid| options.in_mempool(&txid)))
	})
	.await?;
	options.mine(6)?;
	eprintln!("[demo] four funding txs mined, waiting for channels to become usable");
	wait_for(Duration::from_secs(180), "R's four channels to be usable", || {
		for node in &all {
			let _ = node.sync_wallets();
		}
		router.list_channels().iter().filter(|c| c.is_usable).count() >= 4
	})
	.await?;
	Ok(topology)
}

/// Formats satoshis as the decimal BTC string `bitcoin-cli` expects.
fn btc(sats: u64) -> String {
	format!("{}.{:08}", sats / 100_000_000, sats % 100_000_000)
}

/// Pays `sats` from `source` to `destination` and waits for the outcome. `Ok(true)` on
/// success, `Ok(false)` on a failed payment, `Err` if the payment could not be started.
async fn pay(source: &Peer, destination: &Peer, sats: u64) -> Result<bool> {
	let description = Bolt11InvoiceDescription::Direct(
		Description::new(format!("demo {}", destination.name)).map_err(|e| anyhow!("{e:?}"))?,
	);
	let invoice = destination
		.node
		.bolt11_payment()
		.receive(sats * 1000, &description, 3600)
		.map_err(|e| anyhow!("{}: invoice: {e}", destination.name))?;
	let payment_id = source
		.node
		.bolt11_payment()
		.send(&invoice, None)
		.map_err(|e| anyhow!("{} -> {}: send: {e}", source.name, destination.name))?;
	let start = Instant::now();
	loop {
		match source.node.payment(&payment_id).ok().flatten().map(|p| p.status) {
			Some(PaymentStatus::Succeeded) => {
				source.paid.fetch_add(1, Ordering::Relaxed);
				return Ok(true);
			},
			Some(PaymentStatus::Failed) => {
				source.failed.fetch_add(1, Ordering::Relaxed);
				return Ok(false);
			},
			_ => {},
		}
		if start.elapsed() > Duration::from_secs(60) {
			bail!("{} -> {}: still pending after 60s", source.name, destination.name);
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	}
}

/// S1 pays D1 steadily, which drains R -> D1; every fourth payment is a small S2 -> D2 one
/// so there is a healthy channel to compare against. Runs until cancelled.
pub fn spawn_traffic(
	topology: Arc<Topology>, options: Arc<DemoOptions>, notices: Arc<Notices>,
	cancel: CancellationToken,
) -> JoinHandle<()> {
	tokio::spawn(async move {
		let mut tick: u64 = 0;
		let mut consecutive_failures: u32 = 0;
		loop {
			if cancel.is_cancelled() {
				break;
			}
			let (source, destination, sats) = if tick % 4 == 3 {
				(&topology.peers[1], &topology.peers[3], options.payment_sats / 8)
			} else {
				(&topology.peers[0], &topology.peers[2], options.payment_sats)
			};
			let delay = match pay(source, destination, sats).await {
				Ok(true) => {
					consecutive_failures = 0;
					Duration::from_millis(1500)
				},
				Ok(false) => {
					consecutive_failures += 1;
					// A few quick failures make the drain visible; after that, slow down so
					// the operator's chat is not flooded with the same failure.
					if consecutive_failures <= 3 {
						Duration::from_secs(5)
					} else {
						Duration::from_secs(45)
					}
				},
				Err(e) => {
					notices.note(format!("[traffic] {e}"));
					Duration::from_secs(3)
				},
			};
			tick += 1;
			tokio::select! {
				biased;
				_ = cancel.cancelled() => break,
				_ = tokio::time::sleep(delay) => {},
			}
		}
	})
}

/// Mines one block every [`BLOCK_INTERVAL_SECS`] until cancelled, so that anything the agent
/// puts on chain (a splice, a new channel) confirms without the operator running
/// `bitcoin-cli` by hand. Started only after [`build`], whose funding steps watch the mempool.
pub fn spawn_miner(
	options: Arc<DemoOptions>, notices: Arc<Notices>, cancel: CancellationToken,
) -> JoinHandle<()> {
	tokio::spawn(async move {
		eprintln!("[demo] mining a block every {BLOCK_INTERVAL_SECS}s");
		loop {
			tokio::select! {
				biased;
				_ = cancel.cancelled() => break,
				_ = tokio::time::sleep(Duration::from_secs(BLOCK_INTERVAL_SECS)) => {},
			}
			let mining = Arc::clone(&options);
			match tokio::task::spawn_blocking(move || mining.mine(1)).await {
				Ok(Ok(())) => {},
				Ok(Err(e)) => notices.note(format!("[demo] mining failed: {e:#}")),
				Err(e) => notices.note(format!("[demo] miner panicked: {e}")),
			}
		}
	})
}

// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! A GDK agent that manages channel liquidity for a fee-earning LDK Node.
//!
//! Modes:
//! - `review`: snapshot the node, ask the agent for a diagnosis and proposals, exit.
//! - `chat`: interactive operator questions over the same persisted session.
//! - `demo`: build a regtest topology around the node, drain a channel, then review and chat.

mod auth;
mod console;
mod demo;
mod ledger;
mod node;
mod policy;
mod prompt;
mod runtime;
mod session;
mod tools;
mod watch;

use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use goose_agent::events::AgentEvent;
use goose_agent::inference::InferenceRunner;
use goose_agent::machine::{StateMachine, Step};
use goose_agent::operation::Emitter;
use goose_provider_types::base::Provider;
use goose_provider_types::conversation::message::{Message, MessageContent};
use goose_provider_types::model::ModelConfig;
use goose_providers::anthropic::{AnthropicProviderBuilder, ANTHROPIC_API_VERSION};
use goose_providers::api_client::{ApiClient, AuthMethod};
use goose_providers::databricks::DatabricksProvider;
use goose_providers::databricks_auth::DatabricksAuth;
use ldk_node::Node;
use tokio_util::sync::CancellationToken;

use console::Notices;
use ledger::Ledger;
use policy::{Policy, PolicyBounds, Proposal};
use prompt::{Prompt, REVIEW_INSTRUCTION};
use runtime::{Effect, Runtime};
use session::{SessionMetadata, SessionStore};
use tools::{channel_overview, render_overview};
use watch::WatchOptions;

const HELP: &str =
	"Usage: ldk-liquidity-agent [--session-dir DIR | --resume FILE.jsonl] (review | chat [prompt] | demo)

  review   Snapshot the node, ask the agent for a liquidity diagnosis and proposals, exit.
  chat     Interactive questions and instructions; EOF or Ctrl-C exits.
           '/tool NAME [JSON]' runs a tool yourself, bypassing the model and the policy.
  watch    Chat, plus a review on its own every WATCH_INTERVAL_SECS and as soon as one
           channel fails three forwards for lack of outbound liquidity within two
           minutes. A channel with a splice pending is left alone until it locks in.
  demo     Start four regtest peers around this node, open channels, drain one with
           traffic, then run watch; mines a block every 20s from then on so splices
           confirm. Needs a regtest bitcoind.

Node (env):   LDK_NETWORK LDK_STORAGE_DIR LDK_ESPLORA_URL LDK_LISTEN LDK_ALIAS
Agent (env):  AGENT_PROVIDER (anthropic | databricks, default anthropic),
              AGENT_MODEL (default claude-opus-5-5, or goose for databricks),
              AGENT_SESSION_DIR (default ./sessions)
Watch (env):  WATCH_INTERVAL_SECS (600)
Policy (env): POLICY_MIN_FEE_PPM (0) and POLICY_MAX_FEE_PPM (5000): fee changes in this range
              apply without asking; POLICY_MAX_AUTO_SPLICE_SATS (0): larger splices ask.
              Every other fund-moving action asks. Answers are recorded in the session log.
  anthropic:  ANTHROPIC_API_KEY
  databricks: DATABRICKS_HOST; signs in through the browser (OAuth) and keeps the tokens in
              DATABRICKS_TOKEN_FILE (default ~/.ldk-liquidity-agent/databricks_oauth.json);
              set DATABRICKS_TOKEN to use a personal access token instead
Demo (env):   DEMO_BITCOIN_CLI (required, e.g. \"bitcoin-cli -regtest -rpcuser=u -rpcpassword=p\"),
              DEMO_DIR (./demo-data, wiped on start), DEMO_PORT_BASE (9741),
              DEMO_CHANNEL_SATS (1000000), DEMO_PAYMENT_SATS (40000)";

enum Mode {
	Review,
	Watch,
	Chat { initial: Option<String> },
	Demo,
}

struct Options {
	session_dir: Option<PathBuf>,
	resume: Option<PathBuf>,
	mode: Mode,
}

impl Options {
	fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
		let mut session_dir = None;
		let mut resume = None;
		let mut args = args.into_iter().peekable();
		while let Some(arg) = args.peek() {
			match arg.as_str() {
				"--session-dir" | "--resume" => {
					let flag = args.next().unwrap();
					let value = args.next().with_context(|| format!("{flag} requires a path"))?;
					if flag == "--resume" {
						resume = Some(PathBuf::from(value));
					} else {
						session_dir = Some(PathBuf::from(value));
					}
				},
				"-h" | "--help" => return Ok(None),
				_ => break,
			}
		}
		let mode = match args.next().as_deref() {
			Some("review") => Mode::Review,
			Some("watch") => Mode::Watch,
			Some("demo") => Mode::Demo,
			Some("chat") | None => {
				let rest: Vec<_> = args.collect();
				Mode::Chat { initial: (!rest.is_empty()).then(|| rest.join(" ")) }
			},
			Some(other) => bail!("unknown mode {other}; see --help"),
		};
		if session_dir.is_some() && resume.is_some() {
			bail!("--session-dir and --resume cannot be combined");
		}
		Ok(Some(Self { session_dir, resume, mode }))
	}
}

fn agent(
	policy: Arc<Policy>, provider: Arc<dyn Provider>, model: ModelConfig, cancel: CancellationToken,
) -> StateMachine<'static, session::Session, Effect> {
	StateMachine::new(
		vec![
			Step::Operation(policy),
			Step::Operation(Arc::new(tools::tool_operation())),
			Step::Inference(Arc::new(
				InferenceRunner::<session::Session, Effect>::new(provider, model)
					.with_request_preparer(Arc::new(Prompt)),
			)),
		],
		cancel,
	)
}

/// Runs the machine over the stored session until it yields, streaming assistant text.
async fn run_turn(
	runtime: &Runtime, policy: Arc<Policy>, provider: Arc<dyn Provider>, model: ModelConfig,
	cancel: CancellationToken,
) -> Result<session::Session> {
	let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
	let emitter = Emitter::new(tx, cancel.clone());
	let display = tokio::spawn(async move {
		while let Some(event) = rx.recv().await {
			if let AgentEvent::Message(message) = event {
				for content in message.content {
					match content {
						MessageContent::Text(text) => {
							print!("{}", text.text);
							io::stdout().flush()?;
						},
						MessageContent::ToolRequest(request) => {
							if let Ok(call) = &request.tool_call {
								eprintln!("\n[tool] {}", call.name);
							}
						},
						MessageContent::Error(error) => eprintln!("\n{}", error.message),
						MessageContent::SystemNotification(note) => eprintln!("\n{}", note.msg),
						_ => {},
					}
				}
			}
		}
		println!();
		Ok::<_, io::Error>(())
	});
	let machine = agent(policy, provider, model, cancel);
	let id = runtime.snapshot().id;
	// Await the run even on cancellation so the GDK can finalize tool responses.
	let result = machine.run(runtime, &id, &emitter).await;
	drop(emitter);
	display.await??;
	result
}

/// What every turn needs: the session, the policy, the model, and one reader over stdin (a
/// second reader would race the first for lines).
struct Agent<'a> {
	runtime: &'a Runtime,
	policy: Arc<Policy>,
	provider: Arc<dyn Provider>,
	model: ModelConfig,
	lines: tokio::sync::mpsc::Receiver<io::Result<String>>,
	notices: Arc<Notices>,
	ledger: Arc<Ledger>,
	node: Arc<Node>,
	cancel: CancellationToken,
}

impl Agent<'_> {
	/// Reads one line from the operator, or `None` on EOF or cancellation.
	async fn read_line(&mut self) -> Result<Option<String>> {
		let line = tokio::select! {
			biased;
			_ = self.cancel.cancelled() => return Ok(None),
			line = self.lines.recv() => line,
		};
		Ok(line.transpose()?)
	}

	/// Runs the machine, and whenever it stops for the operator's decision on a proposed
	/// action, asks on the terminal, records the answer, and runs again. EOF declines.
	/// Node notices are held for the whole turn and printed once it is over.
	async fn turn(&mut self) -> Result<session::Session> {
		self.notices.hold();
		let result = self.turn_inner().await;
		self.notices.release();
		result
	}

	async fn turn_inner(&mut self) -> Result<session::Session> {
		loop {
			let session = run_turn(
				self.runtime,
				Arc::clone(&self.policy),
				Arc::clone(&self.provider),
				self.model.clone(),
				self.cancel.clone(),
			)
			.await?;
			if self.cancel.is_cancelled() {
				return Ok(session);
			}
			let proposals = self.policy.pending_decisions(&session)?;
			if proposals.is_empty() {
				return Ok(session);
			}
			for proposal in proposals {
				let approved = self.ask(&proposal).await?;
				eprintln!("[policy] {}", if approved { "approved" } else { "declined" });
				self.runtime.record_decision(
					proposal.request_id,
					proposal.tool,
					proposal.summary,
					approved,
				)?;
			}
		}
	}

	async fn ask(&mut self, proposal: &Proposal) -> Result<bool> {
		loop {
			eprint!("\n{}\nApprove? [y/N] ", proposal.render());
			io::stderr().flush()?;
			let Some(line) = self.read_line().await? else { return Ok(false) };
			match line.trim().to_ascii_lowercase().as_str() {
				"y" | "yes" => return Ok(true),
				"" | "n" | "no" => return Ok(false),
				_ => eprintln!("Answer y or n."),
			}
		}
	}
}

fn stdin_lines() -> tokio::sync::mpsc::Receiver<io::Result<String>> {
	let (tx, rx) = tokio::sync::mpsc::channel(1);
	std::thread::spawn(move || {
		for line in io::stdin().lock().lines() {
			let failed = line.is_err();
			if tx.blocking_send(line).is_err() || failed {
				break;
			}
		}
	});
	rx
}

async fn chat(agent: &mut Agent<'_>, initial: Option<String>) -> Result<()> {
	let mut pending = initial;
	loop {
		if agent.cancel.is_cancelled() {
			break;
		}
		let prompt = if let Some(prompt) = pending.take() {
			prompt
		} else {
			show_prompt()?;
			let Some(line) = agent.read_line().await? else { break };
			line
		};
		handle_line(agent, prompt).await?;
	}
	Ok(())
}

fn show_prompt() -> io::Result<()> {
	eprint!("\n> ");
	io::stderr().flush()
}

/// One line from the operator: a `/tool` command, or a message for the model.
async fn handle_line(agent: &mut Agent<'_>, line: String) -> Result<()> {
	if line.trim().is_empty() {
		return Ok(());
	}
	if let Some(rest) = line.trim().strip_prefix("/tool") {
		return run_tool_by_hand(agent, rest).await;
	}
	agent.runtime.append_message(Message::user().with_text(line))?;
	let session = agent.turn().await?;
	if session.conversation.last().is_some_and(|message| message.error_kind().is_some()) {
		eprintln!("Request failed. Send another message to retry.");
	}
	Ok(())
}

/// Settles the pending splices against the node's channels, reporting the ones that locked
/// in or timed out. Called from the watch loop between turns, so the lines print live.
fn settle_splices(agent: &Agent<'_>) -> ledger::SpliceSettlement {
	let settlement = agent.ledger.settle_splices(&agent.node.list_channels());
	for splice in &settlement.ready {
		agent.notices.note(format!(
			"[watch] splice on channel {} has locked in; the added balance is usable",
			splice.channel_id
		));
	}
	for splice in &settlement.timed_out {
		agent.notices.note(format!(
			"[watch] splice on channel {} has not confirmed after {} min; no longer waiting on it",
			splice.channel_id,
			ledger::SPLICE_TIMEOUT.as_secs() / 60
		));
	}
	settlement
}

/// Remembers when each splice in `settlement.ready` locked in.
fn note_locked(
	settlement: &ledger::SpliceSettlement, recently_locked: &mut HashMap<String, Instant>,
) {
	for splice in &settlement.ready {
		recently_locked.insert(splice.channel_id.clone(), Instant::now());
	}
}

/// A sentence for the review prompt naming the channels with a splice pending, or nothing.
fn splice_note(pending: &[ledger::PendingSplice]) -> String {
	if pending.is_empty() {
		return String::new();
	}
	let now = unix_now();
	let list: Vec<String> = pending
		.iter()
		.map(|s| format!("{} (negotiated {}s ago)", s.channel_id, now.saturating_sub(s.since)))
		.collect();
	format!(
		" A splice is pending on channel {}; its added balance becomes usable once it locks in, \
		 so do not propose another splice there.",
		list.join(", ")
	)
}

fn unix_now() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Chat, plus reviews the agent starts on its own: one every `interval`, and one as soon as
/// the ledger shows a burst of liquidity failures on a channel that has no splice pending.
/// Runs until EOF or cancellation.
async fn watch(agent: &mut Agent<'_>, options: WatchOptions) -> Result<()> {
	eprintln!(
		"Watching: a review every {}s, or sooner when a channel fails {} forwards for lack of \
		 outbound liquidity within {}s. Type to chat; Ctrl-C or EOF exits.",
		options.interval.as_secs(),
		watch::BURST_THRESHOLD,
		watch::BURST_WINDOW.as_secs()
	);
	// The settle time only applies once a review has happened; before that a burst is
	// reviewed at once.
	let started = Instant::now();
	let mut last_review: Option<Instant> = None;
	let mut last_review_unix = unix_now();
	// Channels whose splice locked in recently, with when; see `watch::excluded_channels`.
	let mut recently_locked: HashMap<String, Instant> = HashMap::new();
	show_prompt()?;
	loop {
		if agent.cancel.is_cancelled() {
			break;
		}
		let scheduled =
			tokio::time::sleep_until((last_review.unwrap_or(started) + options.interval).into());
		let trigger = tokio::select! {
			biased;
			line = agent.read_line() => match line? {
				Some(line) => {
					handle_line(agent, line).await?;
					show_prompt()?;
					continue;
				},
				None => break,
			},
			_ = scheduled => {
				let settlement = settle_splices(agent);
				note_locked(&settlement, &mut recently_locked);
				format!(
					"Scheduled review, {}s after the previous one.{}",
					options.interval.as_secs(),
					splice_note(&settlement.pending)
				)
			},
			_ = tokio::time::sleep(watch::POLL_INTERVAL) => {
				let settlement = settle_splices(agent);
				note_locked(&settlement, &mut recently_locked);
				let pending = settlement.pending;
				if last_review.is_some_and(|at| at.elapsed() < watch::SETTLE_AFTER_REVIEW) {
					continue;
				}
				let since = last_review_unix.max(unix_now().saturating_sub(watch::BURST_WINDOW.as_secs()));
				let excluded = watch::excluded_channels(
					pending.iter().map(|s| s.channel_id.clone()),
					&mut recently_locked,
					Instant::now(),
				);
				let Some((channel_id, failures)) =
					watch::burst(&agent.ledger.stats_since(since), &excluded)
				else {
					continue;
				};
				format!(
					"Triggered by {failures} forwards that failed for insufficient outbound \
					 liquidity on channel {channel_id} in the last {}s.{}",
					watch::BURST_WINDOW.as_secs(),
					splice_note(&pending)
				)
			},
		};
		eprintln!("\n[watch] {trigger}");
		review(agent, Some(&trigger)).await?;
		last_review = Some(Instant::now());
		last_review_unix = unix_now();
		show_prompt()?;
	}
	Ok(())
}

/// `/tool NAME [JSON]`: runs a tool directly, prints the result, and tells the model what the
/// operator did so the next turn can build on it.
async fn run_tool_by_hand(agent: &mut Agent<'_>, rest: &str) -> Result<()> {
	let rest = rest.trim();
	let (name, arguments) = match rest.split_once(char::is_whitespace) {
		Some((name, arguments)) => (name, arguments.trim()),
		None => (rest, ""),
	};
	if name.is_empty() {
		eprintln!("usage: /tool NAME [JSON]; tools: {}", tools::TOOL_NAMES.join(", "));
		return Ok(());
	}
	let arguments: serde_json::Value = if arguments.is_empty() {
		serde_json::json!({})
	} else {
		match serde_json::from_str(arguments) {
			Ok(value) => value,
			Err(e) => {
				eprintln!("arguments must be JSON: {e}");
				return Ok(());
			},
		}
	};
	let session = agent.runtime.snapshot();
	let note = match tools::run_by_name(&session, name, arguments.clone()).await {
		Ok(output) => {
			let pretty = serde_json::to_string_pretty(&output)?;
			println!("{pretty}");
			format!("Operator note: I ran {name} {arguments} myself. Result: {output}")
		},
		Err(e) => {
			eprintln!("{name} failed: {}", e.message);
			format!("Operator note: I ran {name} {arguments} myself and it failed: {}", e.message)
		},
	};
	agent.runtime.append_message(Message::user().with_text(note))?;
	Ok(())
}

/// One review turn. `trigger` tells the model why the review is happening, when it did not
/// come from the operator.
async fn review(agent: &mut Agent<'_>, trigger: Option<&str>) -> Result<()> {
	let snapshot = render_overview(&channel_overview(&agent.runtime.snapshot(), 24));
	eprintln!("{snapshot}");
	let trigger = trigger.map(|t| format!("{t}\n\n")).unwrap_or_default();
	let text = format!("Current node state:\n\n{snapshot}\n{trigger}{REVIEW_INSTRUCTION}");
	agent.runtime.append_message(Message::user().with_text(text))?;
	agent.turn().await?;
	Ok(())
}

fn required_env(name: &str) -> Result<String> {
	let value = std::env::var(name).with_context(|| format!("set {name}"))?;
	if value.trim().is_empty() {
		bail!("{name} must not be empty");
	}
	Ok(value)
}

/// Returns the provider name, the provider, and its default model.
async fn provider_from_env() -> Result<(&'static str, Arc<dyn Provider>, &'static str)> {
	let name = std::env::var("AGENT_PROVIDER").unwrap_or_default();
	match name.trim() {
		"" | "anthropic" => Ok(("anthropic", anthropic_provider()?, "claude-opus-5-5")),
		"databricks" => Ok(("databricks", databricks_provider().await?, "goose")),
		other => bail!("unknown AGENT_PROVIDER {other}; use anthropic or databricks"),
	}
}

/// Databricks model serving, as used by goose at Block. Signs in through the browser like the
/// goose app does, unless a personal access token is given.
async fn databricks_provider() -> Result<Arc<dyn Provider>> {
	let host = required_env("DATABRICKS_HOST")?;
	let token = std::env::var("DATABRICKS_TOKEN").ok().filter(|t| !t.trim().is_empty());
	let (auth, oauth_token_provider) = match token {
		Some(token) => (DatabricksAuth::token(token), None),
		None => {
			let login = Arc::new(auth::Login::new(auth::default_token_path()?));
			login.ensure_signed_in(&host).await?;
			(DatabricksAuth::oauth(host.clone()), Some(auth::token_provider(login)))
		},
	};
	let retry = DatabricksProvider::load_retry_config(|key| std::env::var(key).ok());
	let provider = DatabricksProvider::new(
		host,
		auth,
		retry,
		Some(Default::default()),
		oauth_token_provider,
		None,
		None,
		None,
		None,
		None,
	)?;
	Ok(Arc::new(provider))
}

fn anthropic_provider() -> Result<Arc<dyn Provider>> {
	let key = required_env("ANTHROPIC_API_KEY")?;
	let client = ApiClient::new_with_tls(
		"https://api.anthropic.com".into(),
		AuthMethod::ApiKey { header_name: "x-api-key".into(), key },
		Some(Default::default()),
	)?
	.with_header("anthropic-version", ANTHROPIC_API_VERSION)?;
	Ok(Arc::new(AnthropicProviderBuilder::new(client).build()))
}

#[tokio::main]
async fn main() -> Result<()> {
	let Some(options) = Options::parse(std::env::args().skip(1))? else {
		println!("{HELP}");
		return Ok(());
	};
	let (provider_name, provider, default_model) = provider_from_env().await?;
	let model_name = std::env::var("AGENT_MODEL")
		.ok()
		.filter(|m| !m.trim().is_empty())
		.unwrap_or_else(|| default_model.to_string());

	let mut node_options = node::NodeOptions::from_env()?;
	let demo_options = if matches!(options.mode, Mode::Demo) {
		let demo_options = demo::DemoOptions::from_env()?;
		demo_options.prepare_dir()?;
		node_options = demo_options.router_options(&node_options)?;
		Some(Arc::new(demo_options))
	} else {
		None
	};
	eprintln!(
		"Starting node on {} (storage {}, esplora {})",
		node_options.network,
		node_options.storage_dir.display(),
		node_options.esplora_url
	);
	let node = node::build_and_start(&node_options)?;
	eprintln!("Node {} listening on {}", node.node_id(), node_options.listen);

	let cancel = CancellationToken::new();
	let ledger = Arc::new(Ledger::new());
	let notices = Arc::new(Notices::new());
	let drain = ledger::spawn_event_drain(
		Arc::clone(&node),
		Arc::clone(&ledger),
		Arc::clone(&notices),
		cancel.clone(),
	);

	let metadata = SessionMetadata {
		provider: provider_name.into(),
		model: model_name.clone(),
		node_id: node.node_id().to_string(),
	};
	let store = if let Some(path) = options.resume {
		let mut store = SessionStore::open(&path, Arc::clone(&node), Arc::clone(&ledger))?;
		if store.session.metadata.node_id != metadata.node_id {
			bail!("session {} belongs to node {}", path.display(), store.session.metadata.node_id);
		}
		store.update_metadata(metadata)?;
		store
	} else {
		let directory = options.session_dir.unwrap_or_else(|| {
			PathBuf::from(
				std::env::var("AGENT_SESSION_DIR")
					.ok()
					.filter(|d| !d.trim().is_empty())
					.unwrap_or_else(|| "./sessions".to_string()),
			)
		});
		SessionStore::create(&directory, metadata, Arc::clone(&node), Arc::clone(&ledger))?
	};
	eprintln!("Session: {}", store.path.display());
	let runtime = Runtime(Mutex::new(store));

	let stop = cancel.clone();
	let ctrl_c = tokio::spawn(async move {
		if tokio::signal::ctrl_c().await.is_ok() {
			stop.cancel();
		}
	});

	let policy = Arc::new(Policy::new(PolicyBounds::from_env()?));
	let mut agent = Agent {
		runtime: &runtime,
		policy,
		provider,
		model: ModelConfig::new(model_name),
		lines: stdin_lines(),
		notices: Arc::clone(&notices),
		ledger: Arc::clone(&ledger),
		node: Arc::clone(&node),
		cancel: cancel.clone(),
	};
	let mut topology = None;
	let result = match options.mode {
		Mode::Review => review(&mut agent, None).await,
		Mode::Watch => watch(&mut agent, WatchOptions::from_env()?).await,
		Mode::Chat { initial } => chat(&mut agent, initial).await,
		Mode::Demo => {
			let demo_options = demo_options.expect("set for demo mode");
			match demo::build(&node, &node_options, &demo_options, cancel.clone()).await {
				Ok(built) => {
					let built = Arc::new(built);
					topology = Some(Arc::clone(&built));
					let _traffic = demo::spawn_traffic(
						Arc::clone(&built),
						Arc::clone(&demo_options),
						Arc::clone(&notices),
						cancel.clone(),
					);
					let _miner =
						demo::spawn_miner(demo_options, Arc::clone(&notices), cancel.clone());
					// The drained channel triggers the first review; a shorter interval than
					// the default lets the next scheduled review report the recovery.
					let mut watch_options = WatchOptions::from_env()?;
					if std::env::var_os("WATCH_INTERVAL_SECS").is_none() {
						watch_options.interval = Duration::from_secs(300);
					}
					watch(&mut agent, watch_options).await
				},
				Err(e) => Err(e),
			}
		},
	};

	ctrl_c.abort();
	cancel.cancel();
	let _ = drain.await;
	if let Some(topology) = topology {
		topology.shutdown();
	}
	if let Err(e) = node.stop() {
		eprintln!("Failed to stop node cleanly: {e}");
	}
	result
}

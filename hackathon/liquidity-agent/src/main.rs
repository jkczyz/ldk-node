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

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
use tokio_util::sync::CancellationToken;

use console::Notices;
use ledger::Ledger;
use policy::{Policy, PolicyBounds, Proposal};
use prompt::{Prompt, REVIEW_INSTRUCTION};
use runtime::{Effect, Runtime};
use session::{SessionMetadata, SessionStore};
use tools::{channel_overview, render_overview};

const HELP: &str =
	"Usage: ldk-liquidity-agent [--session-dir DIR | --resume FILE.jsonl] (review | chat [prompt] | demo)

  review   Snapshot the node, ask the agent for a liquidity diagnosis and proposals, exit.
  chat     Interactive questions and instructions; EOF or Ctrl-C exits.
           '/tool NAME [JSON]' runs a tool yourself, bypassing the model and the policy.
  demo     Start four regtest peers around this node, open channels, drain one with
           traffic, then run review followed by chat; mines a block every 20s from then
           on so splices confirm. Needs a regtest bitcoind.

Node (env):   LDK_NETWORK LDK_STORAGE_DIR LDK_ESPLORA_URL LDK_LISTEN LDK_ALIAS
Agent (env):  AGENT_PROVIDER (anthropic | databricks, default anthropic),
              AGENT_MODEL (default claude-opus-5-5, or goose for databricks),
              AGENT_SESSION_DIR (default ./sessions)
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
			eprint!("\n> ");
			io::stderr().flush()?;
			let Some(line) = agent.read_line().await? else { break };
			line
		};
		if prompt.trim().is_empty() {
			continue;
		}
		if let Some(rest) = prompt.trim().strip_prefix("/tool") {
			run_tool_by_hand(agent, rest).await?;
			continue;
		}
		agent.runtime.append_message(Message::user().with_text(prompt))?;
		let session = agent.turn().await?;
		if session.conversation.last().is_some_and(|message| message.error_kind().is_some()) {
			eprintln!("Request failed. Send another message to retry.");
		}
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

async fn review(agent: &mut Agent<'_>) -> Result<()> {
	let snapshot = render_overview(&channel_overview(&agent.runtime.snapshot(), 24));
	eprintln!("{snapshot}");
	let text = format!("Current node state:\n\n{snapshot}\n{REVIEW_INSTRUCTION}");
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
		cancel: cancel.clone(),
	};
	let mut topology = None;
	let result = match options.mode {
		Mode::Review => review(&mut agent).await,
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
					demo::wait_for_first_failure(
						&ledger,
						&built,
						Duration::from_secs(180),
						&cancel,
					)
					.await;
					match review(&mut agent).await {
						Ok(()) => chat(&mut agent, None).await,
						Err(e) => Err(e),
					}
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

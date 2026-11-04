// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Append-only session storage. The JSONL log is authoritative; memory is a replayed view.
//! The log doubles as the audit trail of what the agent saw, proposed, and was allowed to do.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use goose_agent::machine::MachineSession;
use goose_provider_types::conversation::message::Message;
use goose_provider_types::conversation::token_usage::ProviderUsage;
use goose_provider_types::conversation::Conversation;
use ldk_node::Node;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ledger::Ledger;

const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionMetadata {
	pub provider: String,
	pub model: String,
	/// The node this session was created against. Resuming against a different node is refused.
	pub node_id: String,
}

impl SessionMetadata {
	fn validate(&self) -> Result<()> {
		if self.provider.trim().is_empty() || self.model.trim().is_empty() {
			bail!("session provider and model must not be empty");
		}
		if self.node_id.trim().is_empty() {
			bail!("session node id must not be empty");
		}
		Ok(())
	}
}

/// The state a machine pass runs against: the persisted conversation plus live handles to the
/// node and its forwarding ledger, which tools read.
#[derive(Clone)]
pub struct Session {
	pub id: String,
	pub created_at: u64,
	pub metadata: SessionMetadata,
	pub conversation: Conversation,
	pub usage: Vec<ProviderUsage>,
	/// Operator decisions on proposed actions, by tool request id. Read by the policy step.
	pub decisions: HashMap<String, Decision>,
	pub node: Arc<Node>,
	pub ledger: Arc<Ledger>,
}

/// What the operator said to a proposed action, and what was proposed, so the log reads on
/// its own.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Decision {
	pub tool: String,
	pub summary: String,
	pub approved: bool,
	pub at: u64,
}

impl MachineSession for Session {
	fn id(&self) -> &str {
		&self.id
	}

	fn conversation(&self) -> Option<&Conversation> {
		Some(&self.conversation)
	}
}

/// Metadata changes and message deltas are events, never edits to earlier lines.
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Record {
	Created { version: u32, id: String, created_at: u64, metadata: SessionMetadata },
	Metadata { metadata: SessionMetadata },
	Message { message: Message },
	Usage { usage: ProviderUsage },
	Decision { request_id: String, decision: Decision },
}

impl Session {
	fn apply(&mut self, record: Record) {
		match record {
			Record::Metadata { metadata } => self.metadata = metadata,
			Record::Message { message } => self.conversation.push(message),
			Record::Usage { usage } => self.usage.push(usage),
			Record::Decision { request_id, decision } => {
				self.decisions.insert(request_id, decision);
			},
			Record::Created { .. } => unreachable!("creation is only valid as the first record"),
		}
	}
}

pub struct SessionStore {
	// Holding the file also holds its exclusive advisory lock until drop.
	file: File,
	pub path: PathBuf,
	pub session: Session,
	failed: bool,
}

impl Drop for SessionStore {
	fn drop(&mut self) {
		let _ = self.file.unlock();
	}
}

impl SessionStore {
	pub fn create(
		directory: &Path, metadata: SessionMetadata, node: Arc<Node>, ledger: Arc<Ledger>,
	) -> Result<Self> {
		metadata.validate()?;
		fs::create_dir_all(directory)
			.with_context(|| format!("create session directory {}", directory.display()))?;
		let id = Uuid::new_v4().to_string();
		let path = directory.join(format!("{id}.jsonl"));
		let mut options = OpenOptions::new();
		options.read(true).append(true).create_new(true);
		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			options.mode(0o600);
		}
		let file =
			options.open(&path).with_context(|| format!("create session {}", path.display()))?;
		file.try_lock().with_context(|| format!("lock session {}", path.display()))?;
		let created_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
		let record = Record::Created {
			version: FORMAT_VERSION,
			id: id.clone(),
			created_at,
			metadata: metadata.clone(),
		};
		let mut store = Self {
			file,
			path,
			session: Session {
				id,
				created_at,
				metadata,
				conversation: Conversation::empty(),
				usage: Vec::new(),
				decisions: HashMap::new(),
				node,
				ledger,
			},
			failed: false,
		};
		store.write_records(&[record])?;
		Ok(store)
	}

	/// Reads every record of a session log without opening it for writing or needing a node;
	/// for rendering a transcript.
	pub fn read_records(path: &Path) -> Result<Vec<Record>> {
		let file = File::open(path).with_context(|| format!("open session {}", path.display()))?;
		let mut records = Vec::new();
		for (i, line) in BufReader::new(file).lines().enumerate() {
			let line = line?;
			if line.trim().is_empty() {
				continue;
			}
			records.push(
				serde_json::from_str(&line)
					.with_context(|| format!("{}:{}: malformed record", path.display(), i + 1))?,
			);
		}
		Ok(records)
	}

	pub fn open(path: &Path, node: Arc<Node>, ledger: Arc<Ledger>) -> Result<Self> {
		if path.extension().is_none_or(|extension| extension != "jsonl") {
			bail!("session file must have a .jsonl extension");
		}
		let file = OpenOptions::new()
			.read(true)
			.append(true)
			.open(path)
			.with_context(|| format!("open session {}", path.display()))?;
		file.try_lock()
			.with_context(|| format!("lock session {} (is it already open?)", path.display()))?;
		let session = replay(&file, node, ledger)
			.with_context(|| format!("read session {}", path.display()))?;
		Ok(Self { file, path: path.to_owned(), session, failed: false })
	}

	pub fn update_metadata(&mut self, metadata: SessionMetadata) -> Result<()> {
		if metadata != self.session.metadata {
			self.append(vec![Record::Metadata { metadata }])?;
		}
		Ok(())
	}

	pub fn append(&mut self, records: Vec<Record>) -> Result<()> {
		for record in &records {
			validate_update(record)?;
		}
		self.write_records(&records)?;
		for record in records {
			self.session.apply(record);
		}
		Ok(())
	}

	fn write_records(&mut self, records: &[Record]) -> Result<()> {
		if self.failed {
			bail!("session log previously failed to write; reopen it before continuing");
		}
		if records.is_empty() {
			return Ok(());
		}
		let mut bytes = Vec::new();
		for record in records {
			serde_json::to_writer(&mut bytes, record)?;
			bytes.push(b'\n');
		}
		if let Err(error) = self.file.write_all(&bytes).and_then(|()| self.file.sync_data()) {
			self.failed = true;
			return Err(error).with_context(|| format!("append session {}", self.path.display()));
		}
		Ok(())
	}
}

fn validate_update(record: &Record) -> Result<()> {
	match record {
		Record::Created { .. } => {
			bail!("session creation must be the first and only creation record")
		},
		Record::Metadata { metadata } => metadata.validate(),
		_ => Ok(()),
	}
}

fn replay(file: &File, node: Arc<Node>, ledger: Arc<Ledger>) -> Result<Session> {
	let mut reader = BufReader::new(file);
	let mut session: Option<Session> = None;
	let mut bytes = Vec::new();
	let mut line = 0;
	loop {
		bytes.clear();
		if reader.read_until(b'\n', &mut bytes)? == 0 {
			break;
		}
		line += 1;
		if bytes.last() != Some(&b'\n') {
			bail!("incomplete record at line {line}; session log was not modified");
		}
		let record: Record = serde_json::from_slice(&bytes)
			.with_context(|| format!("invalid session record at line {line}"))?;
		if let Some(session) = session.as_mut() {
			validate_update(&record).with_context(|| format!("invalid record at line {line}"))?;
			session.apply(record);
		} else {
			let Record::Created { version, id, created_at, metadata } = record else {
				bail!("first session record must be creation metadata");
			};
			if version != FORMAT_VERSION {
				bail!("unsupported session format version {version}");
			}
			if id.is_empty() {
				bail!("session ID must not be empty");
			}
			metadata.validate()?;
			session = Some(Session {
				id,
				created_at,
				metadata,
				conversation: Conversation::empty(),
				usage: Vec::new(),
				decisions: HashMap::new(),
				node: Arc::clone(&node),
				ledger: Arc::clone(&ledger),
			});
		}
	}
	session.context("empty session log")
}

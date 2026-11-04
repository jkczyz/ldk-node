// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Glue between the GDK state machine and the session store: the effect type the machine
//! produces and the runtime that loads sessions and persists effects.

use std::sync::Mutex;

use anyhow::{bail, Result};
use async_trait::async_trait;
use goose_agent::inference::InferenceEffect;
use goose_agent::machine::{EffectHandler, SessionLoader};
use goose_agent::operation::{Emitter, MachineEffect};
use goose_provider_types::conversation::message::Message;
use goose_provider_types::conversation::token_usage::ProviderUsage;

use crate::session::{Record, Session, SessionStore};

/// Everything a machine pass can produce. Each variant maps to one persisted record.
pub enum Effect {
	Message(Message),
	Usage(ProviderUsage),
}

impl From<Message> for Effect {
	fn from(message: Message) -> Self {
		Self::Message(message)
	}
}

impl InferenceEffect for Effect {
	fn record_usage(usage: ProviderUsage) -> Self {
		Self::Usage(usage)
	}
}

impl MachineEffect for Effect {
	fn ensure_message_ids(&mut self) {
		if let Self::Message(message) = self {
			*message = message.clone().with_generated_id_if_missing();
		}
	}
}

pub struct Runtime(pub Mutex<SessionStore>);

impl Runtime {
	pub fn snapshot(&self) -> Session {
		self.0.lock().unwrap().session.clone()
	}

	pub fn append_message(&self, message: Message) -> Result<()> {
		self.0
			.lock()
			.unwrap()
			.append(vec![Record::Message { message: message.with_generated_id_if_missing() }])
	}
}

#[async_trait]
impl SessionLoader<Session> for Runtime {
	async fn load(&self, id: &str) -> Result<Session> {
		let session = self.snapshot();
		if session.id != id {
			bail!("unknown session {id}");
		}
		Ok(session)
	}
}

#[async_trait]
impl EffectHandler<Session, Effect> for Runtime {
	async fn apply_effects(
		&self, session: &Session, effects: &mut [Effect], _: &Emitter,
	) -> Result<()> {
		let mut store = self.0.lock().unwrap();
		if store.session.id != session.id {
			bail!("effects belong to a different session");
		}
		let records = effects
			.iter()
			.map(|effect| match effect {
				Effect::Message(message) => Record::Message { message: message.clone() },
				Effect::Usage(usage) => Record::Usage { usage: usage.clone() },
			})
			.collect();
		store.append(records)
	}
}

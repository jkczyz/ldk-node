// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! The approval gate. This operation sits first in the step list, so every pass sees pending
//! tool requests before the tool step can execute them.
//!
//! Scaffold status: only read tools exist, so `run` declines everything and lets the tool step
//! proceed. Once fund-moving tools are registered, `run` classifies each pending request and
//! yields for operator approval when it is outside the configured bounds.

use anyhow::Result;
use async_trait::async_trait;
use goose_agent::operation::Operation;
use goose_provider_types::conversation::Conversation;

use crate::runtime::Effect;
use crate::session::Session;

/// Operator-set limits. Read tools are never gated; write tools inside these bounds are
/// auto-approved; anything else yields for a decision.
#[derive(Clone, Debug)]
pub struct PolicyBounds {
	pub min_fee_ppm: u32,
	pub max_fee_ppm: u32,
	pub max_auto_splice_sats: u64,
}

impl Default for PolicyBounds {
	fn default() -> Self {
		Self { min_fee_ppm: 0, max_fee_ppm: 5_000, max_auto_splice_sats: 0 }
	}
}

pub struct Policy {
	pub bounds: PolicyBounds,
}

impl Policy {
	pub fn new(bounds: PolicyBounds) -> Self {
		Self { bounds }
	}
}

#[async_trait]
impl Operation<Session, Effect> for Policy {
	fn name(&self) -> &'static str {
		"policy"
	}

	/// Tells the model what it can do on its own and what needs the operator, so proposals
	/// land inside the bounds instead of bouncing off them.
	async fn prompt_parts(
		&self, _session: &Session, _conversation: &Conversation,
	) -> Result<Vec<(String, String)>> {
		let text = format!(
			"Operator policy: forwarding fee changes between {} and {} ppm are applied without \
			 asking. Splices up to {} sats are applied without asking. Every other fund-moving \
			 action (larger splices, opening or closing channels, buying inbound liquidity) is \
			 proposed to the operator and only executed after approval. Read-only tools are \
			 always available.",
			self.bounds.min_fee_ppm, self.bounds.max_fee_ppm, self.bounds.max_auto_splice_sats
		);
		Ok(vec![("policy".to_string(), text)])
	}

	// `run` keeps the trait default (`not_applicable`) until write tools exist.
}

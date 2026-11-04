// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! The approval gate. This operation sits first in the step list, so every pass sees pending
//! tool requests before the tool step can execute them.
//!
//! Read tools pass straight through. A write tool inside the operator's bounds passes too. Any
//! other write tool yields the machine until the operator's decision is in the session; an
//! approved request then runs normally, a declined one is answered with a tool error so the
//! model can plan differently.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use async_trait::async_trait;
use goose_agent::operation::{
	applied, messages_since_kickoff, not_applicable, yielded, Emitter, Operation, OperationResult,
};
use goose_provider_types::conversation::message::{Message, MessageContent, ToolRequest};
use goose_provider_types::conversation::Conversation;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::Value;

use crate::runtime::Effect;
use crate::session::{Decision, Session};
use crate::tools::{
	SetChannelFeesInput, SpliceInInput, SpliceOutInput, SET_CHANNEL_FEES, SPLICE_IN, SPLICE_OUT,
	WRITE_TOOLS,
};

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

impl PolicyBounds {
	/// `POLICY_MIN_FEE_PPM`, `POLICY_MAX_FEE_PPM`, `POLICY_MAX_AUTO_SPLICE_SATS`; the given
	/// defaults otherwise.
	pub fn from_env_or(defaults: Self) -> Result<Self> {
		Ok(Self {
			min_fee_ppm: env_number("POLICY_MIN_FEE_PPM", defaults.min_fee_ppm)?,
			max_fee_ppm: env_number("POLICY_MAX_FEE_PPM", defaults.max_fee_ppm)?,
			max_auto_splice_sats: env_number(
				"POLICY_MAX_AUTO_SPLICE_SATS",
				defaults.max_auto_splice_sats,
			)?,
		})
	}
}

fn env_number<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
	T::Err: std::fmt::Display,
{
	match std::env::var(name) {
		Ok(value) if !value.trim().is_empty() => {
			value.trim().parse().map_err(|e| anyhow::anyhow!("{name}: {e}"))
		},
		_ => Ok(default),
	}
}

/// A write action the model asked for that the operator has to decide on.
#[derive(Clone, Debug)]
pub struct Proposal {
	pub request_id: String,
	pub tool: String,
	/// The call as the model made it, for the operator to read.
	pub summary: String,
	/// Why the policy did not apply it on its own.
	pub reason: String,
}

impl Proposal {
	pub fn render(&self) -> String {
		format!("Proposed action needs your approval:\n  {}\n  {}", self.summary, self.reason)
	}
}

enum Verdict {
	/// A read tool, a malformed call (the tool step reports those), or a write inside bounds.
	PassThrough,
	NeedsApproval(Proposal),
}

pub struct Policy {
	pub bounds: PolicyBounds,
}

impl Policy {
	pub fn new(bounds: PolicyBounds) -> Self {
		Self { bounds }
	}

	fn classify(&self, request: &ToolRequest) -> Verdict {
		let Ok(call) = &request.tool_call else { return Verdict::PassThrough };
		let name = call.name.as_ref();
		if !WRITE_TOOLS.contains(&name) {
			return Verdict::PassThrough;
		}
		let arguments = Value::Object(call.arguments.clone().unwrap_or_default());
		let summary = format!("{name} {arguments}");
		let proposal = |reason: String| {
			Verdict::NeedsApproval(Proposal {
				request_id: request.id.clone(),
				tool: name.to_string(),
				summary: summary.clone(),
				reason,
			})
		};
		match name {
			SET_CHANNEL_FEES => {
				let Ok(input) = serde_json::from_value::<SetChannelFeesInput>(arguments.clone())
				else {
					return Verdict::PassThrough;
				};
				match input.fee_ppm {
					Some(ppm) if ppm < self.bounds.min_fee_ppm || ppm > self.bounds.max_fee_ppm => {
						proposal(format!(
							"Outside policy: {ppm} ppm is not within the {}-{} ppm range applied \
							 without asking.",
							self.bounds.min_fee_ppm, self.bounds.max_fee_ppm
						))
					},
					_ => Verdict::PassThrough,
				}
			},
			SPLICE_IN => {
				let Ok(input) = serde_json::from_value::<SpliceInInput>(arguments.clone()) else {
					return Verdict::PassThrough;
				};
				if input.amount_sats <= self.bounds.max_auto_splice_sats {
					Verdict::PassThrough
				} else {
					proposal(format!(
						"Outside policy: splices above {} sats need approval; this one moves {} \
						 sats of on-chain funds into the channel.",
						self.bounds.max_auto_splice_sats, input.amount_sats
					))
				}
			},
			SPLICE_OUT => {
				let Ok(input) = serde_json::from_value::<SpliceOutInput>(arguments.clone()) else {
					return Verdict::PassThrough;
				};
				if input.amount_sats <= self.bounds.max_auto_splice_sats {
					Verdict::PassThrough
				} else {
					proposal(format!(
						"Outside policy: splices above {} sats need approval; this one moves {} \
						 sats out of the channel to the on-chain wallet.",
						self.bounds.max_auto_splice_sats, input.amount_sats
					))
				}
			},
			_ => proposal("Outside policy: this action always needs approval.".to_string()),
		}
	}

	/// Write requests in the current turn that are waiting on the operator (no decision yet),
	/// and those the operator declined (to be answered with an error).
	fn sort_pending(
		&self, decisions: &HashMap<String, Decision>, conversation: &Conversation,
	) -> Result<(Vec<Proposal>, Vec<Proposal>)> {
		let mut waiting = Vec::new();
		let mut declined = Vec::new();
		for request in pending_requests(conversation)? {
			if let Verdict::NeedsApproval(proposal) = self.classify(&request) {
				match decisions.get(&proposal.request_id).map(|d| d.approved) {
					Some(true) => {},
					Some(false) => declined.push(proposal),
					None => waiting.push(proposal),
				}
			}
		}
		Ok((waiting, declined))
	}

	/// Proposals the operator still has to answer before the machine can continue.
	pub fn pending_decisions(&self, session: &Session) -> Result<Vec<Proposal>> {
		Ok(self.sort_pending(&session.decisions, &session.conversation)?.0)
	}
}

/// Tool requests in the current turn that have no response yet. Mirrors the tool step's own
/// detection so both agree on what is pending.
fn pending_requests(conversation: &Conversation) -> Result<Vec<ToolRequest>> {
	let turn = messages_since_kickoff(conversation)?;
	let answered: HashSet<&str> = turn.iter().flat_map(Message::get_tool_response_ids).collect();
	Ok(turn
		.iter()
		.flat_map(|message| message.content.iter())
		.filter_map(MessageContent::as_tool_request)
		.filter(|request| {
			!request.was_executed_externally() && !answered.contains(request.id.as_str())
		})
		.cloned()
		.collect())
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
			 asking. Splices of up to {} sats, in or out, are applied without asking. Every other \
			 fund-moving \
			 action is proposed to the operator and only executed after approval; a declined \
			 action comes back as a tool error, and you should not retry it unchanged. Read-only \
			 tools are always available.",
			self.bounds.min_fee_ppm, self.bounds.max_fee_ppm, self.bounds.max_auto_splice_sats
		);
		Ok(vec![("policy".to_string(), text)])
	}

	async fn run(
		&self, session: &Session, conversation: &Conversation, emit: &Emitter,
	) -> Result<OperationResult<Effect>> {
		if messages_since_kickoff(conversation).is_err() {
			return not_applicable();
		}
		let (waiting, declined) = self.sort_pending(&session.decisions, conversation)?;
		if !waiting.is_empty() {
			let text = waiting.iter().map(Proposal::render).collect::<Vec<_>>().join("\n");
			// Shown to the operator; not persisted, the decision record is the audit trail.
			emit.message(Message::assistant().with_text(text).user_only());
			return yielded();
		}
		if declined.is_empty() {
			return not_applicable();
		}
		let mut response = Message::user();
		for proposal in declined {
			let text = format!(
				"The operator declined this action ({}). Do not retry it unchanged; explain the \
				 trade-off or propose an alternative.",
				proposal.summary
			);
			response = response.with_tool_response(
				proposal.request_id,
				Ok(CallToolResult::error(vec![ContentBlock::text(text)])),
			);
		}
		applied([Effect::from(emit.message(response))])
	}
}

#[cfg(test)]
mod tests {
	use rmcp::model::CallToolRequestParams;
	use serde_json::json;

	use super::*;

	fn request(id: &str, tool: &str, arguments: serde_json::Value) -> Message {
		let arguments = arguments.as_object().cloned().unwrap_or_default();
		let call = CallToolRequestParams::new(tool.to_string()).with_arguments(arguments);
		Message::assistant().with_tool_request(id, Ok(call))
	}

	fn conversation(messages: Vec<Message>) -> Conversation {
		let mut conversation = Conversation::empty();
		conversation.push(Message::user().with_text("review"));
		for message in messages {
			conversation.push(message);
		}
		conversation
	}

	fn decision(approved: bool) -> Decision {
		Decision { tool: String::new(), summary: String::new(), approved, at: 0 }
	}

	#[test]
	fn reads_and_in_bounds_writes_pass() {
		let policy = Policy::new(PolicyBounds::default());
		let conversation = conversation(vec![
			request("r1", "channel_overview", json!({})),
			request("r2", SET_CHANNEL_FEES, json!({"channel_id": "abcdef01", "fee_ppm": 2500})),
			request("r3", SET_CHANNEL_FEES, json!({"channel_id": "abcdef01", "fee_base_msat": 0})),
		]);
		let (waiting, declined) = policy.sort_pending(&HashMap::new(), &conversation).unwrap();
		assert!(waiting.is_empty());
		assert!(declined.is_empty());
	}

	#[test]
	fn out_of_bounds_writes_wait_until_decided() {
		let policy = Policy::new(PolicyBounds::default());
		let conversation = conversation(vec![
			request("r1", SET_CHANNEL_FEES, json!({"channel_id": "abcdef01", "fee_ppm": 9000})),
			request("r2", SPLICE_IN, json!({"channel_id": "abcdef01", "amount_sats": 50_000})),
			request("r3", SPLICE_OUT, json!({"channel_id": "abcdef01", "amount_sats": 40_000})),
		]);
		let (waiting, declined) = policy.sort_pending(&HashMap::new(), &conversation).unwrap();
		assert_eq!(
			waiting.iter().map(|p| p.request_id.as_str()).collect::<Vec<_>>(),
			["r1", "r2", "r3"]
		);
		assert!(declined.is_empty());
		assert!(waiting[0].reason.contains("9000 ppm"));
		assert!(waiting[1].reason.contains("50000 sats"));
		assert!(waiting[2].reason.contains("40000 sats out of the channel"));

		let decisions = HashMap::from([
			("r1".to_string(), decision(true)),
			("r2".to_string(), decision(false)),
			("r3".to_string(), decision(true)),
		]);
		let (waiting, declined) = policy.sort_pending(&decisions, &conversation).unwrap();
		assert!(waiting.is_empty());
		assert_eq!(declined.iter().map(|p| p.request_id.as_str()).collect::<Vec<_>>(), ["r2"]);
	}

	#[test]
	fn answered_requests_are_not_pending() {
		let policy = Policy::new(PolicyBounds::default());
		let mut messages = vec![request(
			"r1",
			SET_CHANNEL_FEES,
			json!({"channel_id": "abcdef01", "fee_ppm": 9000}),
		)];
		messages.push(Message::user().with_tool_response(
			"r1",
			Ok(CallToolResult::error(vec![ContentBlock::text("declined")])),
		));
		let (waiting, declined) =
			policy.sort_pending(&HashMap::new(), &conversation(messages)).unwrap();
		assert!(waiting.is_empty());
		assert!(declined.is_empty());
	}

	#[test]
	fn splices_inside_auto_limit_pass() {
		let bounds = PolicyBounds { max_auto_splice_sats: 100_000, ..PolicyBounds::default() };
		let policy = Policy::new(bounds);
		let conversation = conversation(vec![
			request("r1", SPLICE_IN, json!({"channel_id": "abcdef01", "amount_sats": 100_000})),
			request("r2", SPLICE_OUT, json!({"channel_id": "abcdef01", "amount_sats": 100_000})),
			request("r3", SPLICE_OUT, json!({"channel_id": "abcdef01", "amount_sats": 100_001})),
		]);
		let (waiting, _) = policy.sort_pending(&HashMap::new(), &conversation).unwrap();
		assert_eq!(waiting.iter().map(|p| p.request_id.as_str()).collect::<Vec<_>>(), ["r3"]);
	}
}

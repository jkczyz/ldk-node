// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Renders a session log as a readable transcript: what the operator and the agent said, the
//! tools the agent called and what they returned, and the operator's decisions on proposed
//! actions. Markdown, so it can go straight into notes or slides.

use std::fmt::Write;

use goose_provider_types::conversation::message::{Message, MessageContent};
use rmcp::model::Role;

use crate::session::Record;

/// Tool results longer than this are cut in the transcript; the full text is in the log.
const RESULT_LIMIT: usize = 600;

pub fn render(records: &[Record]) -> String {
	let mut out = String::new();
	let mut created_at = 0u64;
	for record in records {
		match record {
			Record::Created { id, created_at: at, metadata, .. } => {
				created_at = *at;
				let _ = writeln!(out, "# Session {id}\n");
				let _ = writeln!(
					out,
					"Node {}, model {} via {}, started at unix time {at}.\n",
					metadata.node_id, metadata.model, metadata.provider
				);
			},
			Record::Metadata { .. } | Record::Usage { .. } => {},
			Record::Message { message } => render_message(&mut out, message, created_at),
			Record::Decision { decision, .. } => {
				let verdict = if decision.approved { "approved" } else { "declined" };
				let _ = writeln!(
					out,
					"**Operator {verdict}** (+{}s): `{}`\n",
					decision.at.saturating_sub(created_at),
					decision.summary
				);
			},
		}
	}
	out
}

fn render_message(out: &mut String, message: &Message, created_at: u64) {
	let offset = (message.created.max(0) as u64).saturating_sub(created_at);
	for content in &message.content {
		match content {
			MessageContent::Text(text) => {
				let speaker = match message.role {
					Role::User => "Operator",
					Role::Assistant => "Agent",
				};
				let _ = writeln!(out, "## {speaker} (+{offset}s)\n\n{}\n", text.text.trim());
			},
			MessageContent::ToolRequest(request) => match &request.tool_call {
				Ok(call) => {
					let arguments = call
						.arguments
						.as_ref()
						.map(|a| serde_json::to_string(a).unwrap_or_default())
						.unwrap_or_default();
					let _ = writeln!(out, "- **Tool call** `{} {arguments}`", call.name);
				},
				Err(e) => {
					let _ = writeln!(out, "- **Tool call failed to parse:** {e}");
				},
			},
			MessageContent::ToolResponse(response) => match &response.tool_result {
				Ok(result) => {
					let text: Vec<&str> = result
						.content
						.iter()
						.filter_map(|c| c.as_text())
						.map(|t| t.text.as_str())
						.collect();
					let label =
						if result.is_error == Some(true) { "Tool error" } else { "Tool result" };
					let _ = writeln!(out, "- **{label}:** {}\n", truncate(&text.join(" ")));
				},
				Err(e) => {
					let _ = writeln!(out, "- **Tool error:** {}\n", e.message);
				},
			},
			MessageContent::Error(error) => {
				let _ = writeln!(out, "- **Error:** {}\n", error.message);
			},
			_ => {},
		}
	}
}

fn truncate(text: &str) -> String {
	let text = text.trim();
	if text.chars().count() <= RESULT_LIMIT {
		return text.to_string();
	}
	let cut: String = text.chars().take(RESULT_LIMIT).collect();
	format!("{cut}… ({} more characters)", text.chars().count() - RESULT_LIMIT)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::session::{Decision, SessionMetadata};

	#[test]
	fn renders_messages_tool_calls_and_decisions() {
		let metadata =
			SessionMetadata { provider: "p".into(), model: "m".into(), node_id: "n".into() };
		let records = vec![
			Record::Created { version: 1, id: "s1".into(), created_at: 100, metadata },
			Record::Message { message: Message::user().with_text("Current node state") },
			Record::Message { message: Message::assistant().with_text("Raising the fee.") },
			Record::Decision {
				request_id: "r".into(),
				decision: Decision {
					tool: "splice_in".into(),
					summary: "splice_in {}".into(),
					approved: true,
					at: 160,
				},
			},
		];
		let text = render(&records);
		assert!(text.contains("# Session s1"));
		assert!(text.contains("## Operator"));
		assert!(text.contains("## Agent"));
		assert!(text.contains("Raising the fee."));
		assert!(text.contains("**Operator approved** (+60s): `splice_in {}`"));
	}

	#[test]
	fn long_results_are_cut() {
		let long = "x".repeat(RESULT_LIMIT + 5);
		assert!(truncate(&long).ends_with("(5 more characters)"));
		assert_eq!(truncate("short"), "short");
	}
}

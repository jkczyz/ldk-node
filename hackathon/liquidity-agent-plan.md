# Routing-node liquidity agent on LDK Node + GDK: build plan

Goal: an LDK Node instance that earns forwarding fees, with a GDK agent that watches channel balances and forward outcomes, proposes liquidity actions (fee changes, splices, new channels, LSP purchases), and executes them behind an operator approval gate.

Design rule: Rust owns telemetry, guardrails, and execution. The model plans and explains. Nothing moves funds without passing the policy step.

## 0. Prerequisites (do first, ~30 min)

- **Toolchain.** `goose-agent` 0.1.0-alpha.13 declares `rust-version = 1.94.1`, and its dependency `parking_lot 0.13` needs 1.95. Local stable is 1.90.0; `rustup check` shows 1.99.1 available. Run `rustup update stable`. LDK Node's own `rust-version = "1.85"` ([Cargo.toml:8](Cargo.toml:8)) is unaffected because the agent lives in a separate crate that depends on LDK Node by path.
- **Pins.** The GDK hello-world pins exact versions: `goose-agent`, `goose-provider-types`, `goose-providers` (feature `rustls-tls`) at `=0.1.0-alpha.13`, plus `rmcp = "=3.4.1"` with features `schemars`, `server`, and `schemars = "1"`. Run `cargo tree -i rmcp` after the first build to confirm alpha.13 still wants rmcp 3.4.1; adjust the pin if not.
- **Provider.** `goose-providers` ships `AnthropicProvider::new(ApiClient)` (anthropic.rs:46, :75 in the crate source) alongside OpenAI and others. Its built-in default model constant is stale (`claude-sonnet-4-5`, anthropic.rs:34), so always pass the model explicitly via `ModelConfig::new("claude-opus-5-5")`. Key from `ANTHROPIC_API_KEY`. Smoke-test one inference before building anything else; if the adapter misbehaves with current Anthropic models, fall back to `OpenAiProvider` exactly as the hello-world does.
- **Regtest binaries.** The harness takes `BITCOIND_EXE` / `ELECTRS_EXE` or falls back to downloaded binaries ([tests/common/mod.rs:343](tests/common/mod.rs:343), [tests/common/mod.rs:351](tests/common/mod.rs:351)). Export both to known-good builds before the demo.

## 1. Crate layout

```
hackathon/liquidity-agent/
  Cargo.toml              ldk-node = { path = "../.." }, GDK pins, tokio, serde, schemars, rmcp
  src/main.rs             CLI: `review` (one cycle), `watch` (loop), `chat` (operator Q&A)
  src/session.rs          Session struct + append-only JSONL store (copy the hello-world pattern)
  src/telemetry.rs        event drain → forward ledger + periodic channel snapshot
  src/tools/mod.rs        ToolOperation registration
  src/tools/read.rs       read-only tools
  src/tools/write.rs      fund-moving tools
  src/policy.rs           approval Operation (the gate)
  src/prompt.rs           InferenceRequestPreparer: system prompt + rendered snapshot
  tests/regtest_demo.rs   topology + traffic generator for the demo
```

Keep LDK Node changes (section 2) as separate commits on this branch so they can be upstreamed independently of the agent crate.

## 2. LDK Node change: surface forward failures (~2 h)

Why: a depleted or unreliable channel shows up first as failed forwards, and LDK Node currently hands `HTLCHandlingFailed` to the LSPS2 service and drops it ([src/event.rs:1502](src/event.rs:1502)). Without this the agent only sees successes.

- Add `Event::ForwardFailed { prev_channel_id: ChannelId, next_channel_id: Option<ChannelId>, next_node_id: Option<PublicKey>, reason: ForwardFailureReason }` to the `Event` enum ([src/event.rs:110](src/event.rs:110)). Next free TLV id in the enum serialization is 10 (ids 0–9 are taken, [src/event.rs:312](src/event.rs:312) to [src/event.rs:387](src/event.rs:387)).
- `ForwardFailureReason` is a small enum: `InsufficientOutboundLiquidity`, `PeerOffline`, `ChannelNotReady`, `FeeInsufficient`, `Other(String)`. Map from LDK's `HTLCHandlingFailureReason::Local(LocalHTLCFailureReason)`; the liquidity case is `ChannelBalanceOverdrawn` (`/Users/jkczyz/.cargo/git/checkouts/rust-lightning-e47a539aa5104a91/9174965/lightning/src/ln/onion_utils.rs:1674`). `PeerOffline`, `ChannelNotReady`, `FeeInsufficient` exist by those names in the same enum.
- LDK's event carries `prev_channel_ids`, `failure_type`, and `failure_reason: Option<HTLCHandlingFailureReason>` (`.../lightning/src/events/mod.rs:1816`); `HTLCHandlingFailureType::Forward` has `node_id: Option<PublicKey>` and `channel_id` (`.../lightning/src/events/mod.rs:664`). Only emit our event for the `Forward` and `UnknownNextHop` failure types; receive-side failures are not routing signal.
- In the handler arm, keep the existing LSPS2 call and then push the new event the same way the `PaymentForwarded` arm does (the arm starts at [src/event.rs:1662](src/event.rs:1662), constructs the event at [src/event.rs:1754](src/event.rs:1754), and adds it to the event queue).
- `Event` derives `uniffi::Enum` behind the `uniffi` feature, so the bindings pick the variant up without UDL edits. `ChannelId`/`PublicKey` are already used by other variants.
- Test: extend an existing forwarding integration test (e.g. [tests/integration_tests_rust.rs:637](tests/integration_tests_rust.rs:637) `multi_hop_sending`) with a payment larger than the intermediate channel's outbound capacity and `expect_event!(node, ForwardFailed)`.

Cut line: if this slips, the demo still works on `PaymentForwarded` plus balance ratios; failures would just be inferred from volume dropping to zero.

## 3. Telemetry (~2 h)

- **Event drain task.** A tokio task calls `Node::wait_next_event` ([src/lib.rs:978](src/lib.rs:978)) then `event_handled` ([src/lib.rs:988](src/lib.rs:988)). Record `PaymentForwarded` (fields: `prev_htlcs`, `next_htlcs` with channel id / counterparty / amount, `total_fee_earned_msat`, `outbound_amount_forwarded_msat`, [src/event.rs:165](src/event.rs:165)) and `ForwardFailed`. Everything else is logged and dropped.
- **Ledger.** Per channel id: inbound/outbound forward count, volume msat, fees earned msat, failure count by reason, timestamps for windowing. Persist as JSONL next to the session so a restart keeps history.
- **Snapshot.** On demand: `list_channels` ([src/lib.rs:1241](src/lib.rs:1241)) for `channel_value_sats`, `outbound_capacity_msat`, `inbound_capacity_msat`, `next_outbound_htlc_limit_msat`, `is_usable`, `counterparty` ([src/types.rs:558](src/types.rs:558) onward); `list_balances` ([src/lib.rs:2155](src/lib.rs:2155)); `list_peers` ([src/lib.rs:2262](src/lib.rs:2262)); `status()` timestamps ([src/lib.rs:908](src/lib.rs:908)).
- **Rendering.** One compact text table per channel: alias/peer, capacity, local balance %, our fee policy, 1h/24h volume in/out, fees, failures by reason. This is what the model reads; keep it under ~2k tokens for a dozen channels.

## 4. Tools (~3 h)

All tools are `ToolBase` + `AsyncTool<Session>` with `schemars` input/output types, registered on `ToolOperation::<Session>::new().with_async_tool::<T>()` ([tool.rs:247](tool.rs:247) in the goose-agent source). `Node` methods are synchronous and block on the node's runtime wrapper (e.g. [src/lib.rs:2146](src/lib.rs:2146)), which reuses the caller's tokio context through `block_in_place` ([src/runtime.rs:184](src/runtime.rs:184), [src/runtime.rs:188](src/runtime.rs:188)). `block_in_place` works on a multi-thread runtime and panics on a `current_thread` one, so the agent binary must use the default multi-thread `#[tokio::main]`. Wrapping long calls (splice, open, close) in `tokio::task::spawn_blocking` is still good practice so a slow node call doesn't stall event streaming.

Read tools (never gated):

| Tool | Backing API |
|---|---|
| `channel_overview` | snapshot + ledger join from section 3 |
| `recent_forwards` / `forward_failures` | ledger, windowed |
| `peer_info` | `network_graph().node()` and `.channel()` ([src/graph.rs:42](src/graph.rs:42), [src/graph.rs:52](src/graph.rs:52)) for a peer's fee policy and channel set |
| `probe_liquidity` | `spontaneous_payment().send_probes(amount_msat, node_id)` ([src/payment/spontaneous.rs:210](src/payment/spontaneous.rs:210)); result arrives as a probe event, so the tool returns "probe sent" and the next snapshot reports the outcome |
| `onchain_status` | `list_balances` spendable on-chain sats, `status()` fee-rate timestamp |

Write tools (gated by section 5):

| Tool | Backing API | Default gate |
|---|---|---|
| `set_channel_fees` | `update_channel_config(user_channel_id, counterparty, ChannelConfig)` ([src/lib.rs:2120](src/lib.rs:2120)); fields `forwarding_fee_base_msat`, `forwarding_fee_proportional_millionths` ([src/config.rs:706](src/config.rs:706)) | auto-approve inside operator bounds, else ask |
| `splice_in` | [src/lib.rs:1812](src/lib.rs:1812) | ask |
| `splice_out` | [src/lib.rs:1853](src/lib.rs:1853) | ask |
| `open_channel` | [src/lib.rs:1486](src/lib.rs:1486) with `push_to_counterparty_msat` to pre-balance | ask |
| `close_channel` | [src/lib.rs:2050](src/lib.rs:2050) | ask |
| `buy_inbound` | `liquidity().lsps1().request_channel` / `check_order_status` ([src/liquidity/client/lsps1.rs:474](src/liquidity/client/lsps1.rs:474), [:514](src/liquidity/client/lsps1.rs:514)); needs an LSP configured via `add_liquidity_source` ([src/liquidity/mod.rs:158](src/liquidity/mod.rs:158)) | ask |
| `connect_peer` | [src/lib.rs:1252](src/lib.rs:1252) | auto |

Every write tool returns a structured result the model can quote (new fee values, splice txid, order id). Tool descriptions state units (msat vs sat) explicitly; that is the most likely model error.

## 5. Policy operation: the approval gate (~3 h)

GDK has no interception hook; the documented pattern is an operation placed before the tool step that yields while a decision is pending. Implement `Operation<Session>` ([operation.rs:91](operation.rs:91) in the goose-agent source) named `"policy"`:

- `run(session, conversation, emit)`: find the trailing assistant message's pending tool requests (ones without a tool response yet; mirror how `ToolOperation::run` in tool.rs detects them). Classify each:
  - read tool → return `not_applicable()` so the tool step runs it.
  - write tool within bounds (fee change inside `[min_ppm, max_ppm]`, splice below `max_auto_splice_sats`) → `not_applicable()`, and note the auto-approval on the message via `set_message_meta` so a replay reaches the same conclusion.
  - otherwise, look up the decision recorded in the session for that request id. None → emit a human-readable proposal and return `yielded()` ([operation.rs:208](operation.rs:208)). Approved → `not_applicable()`. Denied → `applied([tool error response])` so the model learns it was refused and can plan differently.
- `prompt_parts` ([operation.rs:129](operation.rs:129)): contribute the current bounds and budget so the model proposes actions it can actually get approved.
- Step order is `[policy, tools, inference]`. Because each pass restarts at the top of the list, the policy step sees every pending request before the tool step can execute it.
- Decisions live in the session store, never in local variables; the CLI persists `approve`/`deny` for a request id and re-runs `StateMachine::run`.

Bounds come from a TOML file the operator edits: fee range, max auto splice, max on-chain spend per day, peers that are never closed.

## 6. Driver and CLI (~2 h)

- `review`: append a user message containing the rendered snapshot and a fixed instruction ("review liquidity, propose at most three actions, call tools"), run the machine until it yields. If it yielded on approval, print the proposal, read y/n, persist, re-run. Exit when the assistant turn ends.
- `watch`: `review` every N minutes, and immediately when the ledger sees a burst of `ForwardFailed` with `InsufficientOutboundLiquidity`.
- `chat`: free-form operator questions over the same session ("why did fees drop since this morning?").
- Persistence: one JSONL session file as in the hello-world (`SessionLoader` + `EffectHandler` over an append-only log). The log doubles as the audit trail of every decision and the reasoning behind it, which is worth showing in the demo.
- Streaming: print `AgentEvent::Message` text as it arrives; Ctrl-C cancels via the shared `CancellationToken` and awaits the machine so interrupted tool calls are finalized.

## 7. Regtest demo (~4 h)

Topology, built with the harness helpers (`setup_bitcoind_and_electrsd` [tests/common/mod.rs:341](tests/common/mod.rs:341), `premine_and_distribute_funds` [:956](tests/common/mod.rs:956), `open_channel` [:1056](tests/common/mod.rs:1056), `open_channel_push_amt` [:1102](tests/common/mod.rs:1102), `generate_blocks_and_wait` [:823](tests/common/mod.rs:823)); copy what the demo needs into the agent crate rather than depending on the test module, and add `electrsd`/`corepc-node` as dev-dependencies there with the same versions as the root `Cargo.toml`:

```
  S1 ──(1M, push 0)──▶ R ──(1M, push 0)──▶ D1
  S2 ──(1M, push 0)──▶ R ──(1M, push 0)──▶ D2
```

R is our routing node with the agent attached; S1/S2 are payment sources; D1/D2 are destinations. R's outbound channels start fully on R's side, inbound channels fully on the sources' side.

Script:

1. Start the five nodes, open channels, mine, wait for `ChannelReady`.
2. Traffic generator: D1 issues BOLT11 invoices, S1 pays them through R in a loop. Fees accrue; R→D1 drains.
3. Forwards start failing with `InsufficientOutboundLiquidity`. `watch` triggers a review.
4. Agent reads the overview, explains the drain, proposes: raise the proportional fee on R→D1 (auto-approved, inside bounds) and `splice_in` 500k sats on R→D1 (asks). Operator approves. Mine blocks; splice locks.
5. Traffic resumes; the ledger shows fees recovering. Run `chat` with "summarize what you changed and why".

Fallback if splicing misbehaves on the day: approve `open_channel` to D1 with a push amount instead; same gate, same story.

## 8. Timeline (two-day hackathon)

| Slot | Work | Done when |
|---|---|---|
| Day 1 AM | §0, crate skeleton, hello-world agent with `channel_overview` and `recent_forwards` | agent answers "how are my channels?" against a live regtest node |
| Day 1 PM | §2 `ForwardFailed`, §3 ledger + snapshot, remaining read tools | review cycle produces a correct diagnosis of a drained channel |
| Day 2 AM | §4 write tools, §5 policy operation, decision persistence | fee change auto-applies; splice yields for approval and resumes after y |
| Day 2 PM | §7 demo topology + traffic generator, dry-run twice, write the pitch | end-to-end run under 10 minutes |

Cut lines, in order: `buy_inbound` and `splice_out` (keep fee + `splice_in` + `open_channel`), `watch` mode (keep `review`), `probe_liquidity`.

## 9. Risks and things to verify early

- **GDK alpha churn.** alpha.13 is newer than the documented alpha.12; trait names above were read from the alpha.13 source, but `ToolOperation`'s pending-request detection and the message content types for tool requests still need a look before writing `policy.rs`.
- **Anthropic adapter in goose-providers.** Unverified against current model ids; the smoke test in §0 decides provider choice.
- **Runtime flavor.** `Node` calls from inside tokio rely on `block_in_place` (see §4), which panics on a `current_thread` runtime. Keep the agent on the multi-thread runtime and never call `Node` from inside a `LocalSet` or a single-threaded test runtime.
- **Persisted `Event` compatibility.** Adding a variant changes the on-disk event queue format. Fine for a hackathon branch; for upstreaming, check `tests/upgrade_downgrade_tests.rs` expectations.
- **Splice preconditions on regtest.** Both sides are LDK Node so negotiation is supported; splices need confirmations before the new capacity is usable, so the demo mines blocks and waits for `SpliceNegotiated` and channel-ready signals.
- **Token budget.** Snapshots grow with channel count; render tables, not JSON, and cap history windows. Opus 5.5 defaults to medium effort; set `high` for the review prompt if diagnoses look shallow.
- **No circular rebalancing.** LDK's router refuses routes to ourselves (`/Users/jkczyz/.cargo/git/checkouts/rust-lightning-e47a539aa5104a91/9174965/lightning/src/routing/router.rs:2666`). The tool set above deliberately excludes it; a custom `Router` that allows self-routes is a post-hackathon idea.

## 10. Stretch goals

- Inbound channel acceptance policy: LDK Node auto-handles `OpenChannelRequest` ([src/event.rs:1521](src/event.rs:1521)); exposing a hook would let the agent decide which inbound channels to accept.
- Run the LSPS2 service ([src/liquidity/mod.rs:281](src/liquidity/mod.rs:281)) to sell JIT channels as a second fee stream the agent can price.
- Agent-supplied `ProbingStrategy` ([src/probing.rs:345](src/probing.rs:345), `custom` at [src/probing.rs:215](src/probing.rs:215)) that probes toward the peers the agent is considering opening to.
- Swift/Kotlin front end: both LDK Node and GDK ship UniFFI bindings, so the same agent could run inside a mobile node app.

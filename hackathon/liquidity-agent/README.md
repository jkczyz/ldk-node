# LDK liquidity agent (hackathon prototype)

An agent built on the GDK (`goose-agent`) that watches an LDK Node routing node and manages its channel liquidity for the operator. See `../liquidity-agent-plan.md` for the design.

## Build

```bash
cargo build
```

The crate pins its own toolchain (`rust-toolchain.toml`) and is its own workspace, so LDK Node's MSRV is unaffected.

## Run against a node with Databricks (browser sign-in)

```bash
AGENT_PROVIDER=databricks \
DATABRICKS_HOST=https://block-lakehouse-production.cloud.databricks.com \
LDK_LISTEN=127.0.0.1:9736 \
LDK_ESPLORA_URL=http://127.0.0.1:3002 \
./target/debug/ldk-liquidity-agent chat "How are my channels?"
```

Replace `DATABRICKS_HOST` with your workspace URL. The first run opens the browser for the OAuth sign-in and caches the tokens (mode 0600) in `~/.ldk-liquidity-agent/databricks_oauth.json`, or in `$DATABRICKS_TOKEN_FILE`. Set `DATABRICKS_TOKEN` to use a personal access token instead. `AGENT_MODEL` picks the serving endpoint (default `goose`).

With an Anthropic key instead: `ANTHROPIC_API_KEY=... ./target/debug/ldk-liquidity-agent chat "How are my channels?"`.

## Modes

- `review`: snapshot the node, ask for a diagnosis and up to three actions, exit.
- `chat [prompt]`: interactive operator session over the same persisted conversation.
- `watch`: `chat`, plus reviews the agent starts on its own: one every `WATCH_INTERVAL_SECS` (600), one when a channel's local balance drops below 15 % of its capacity (before forwards start failing), one as soon as a channel fails three forwards for lack of outbound liquidity within two minutes, and one when a splice locks in (so a splice-in can follow a splice-out). The trigger names the channels that hold most of their capacity and what they earned, so the agent can move capital from an idle channel (`splice_out`, then `splice_in`) or steer with fees instead of adding funds. A channel with a splice pending is left alone until the splice locks in (or 30 minutes pass), and the review prompt says so, so the agent does not propose a second splice. Fee changes inside the policy bounds apply on their own, and so do splices (in or out) up to `POLICY_MAX_AUTO_SPLICE_SATS`; anything larger waits for your approval at the prompt. That bound defaults to 0, so a node outside the demo asks before every splice unless you raise it.
- `transcript FILE`: print a session log as a Markdown transcript (operator and agent messages, tool calls and results, approvals) and exit.
- `demo`: build a five-node regtest topology around the node, drive traffic that drains one channel, then run `watch` (with a 300 s interval unless set). The routing node's wallet is funded with only what its two channel opens need, so the agent has to move capital from the idle channel rather than splice in new funds. Once the topology is up it mines a block every 20 seconds, so a splice the agent makes confirms on its own. The splice bound defaults to the channel size here, so the whole move runs unattended; set `POLICY_MAX_AUTO_SPLICE_SATS=0` to be asked instead. Needs `DEMO_BITCOIN_CLI`.

## Deciding where capital goes

Besides keeping existing channels balanced, the agent can decide where channels should exist. Tell it what you want in `chat` ("reach Olympus and cumulo cheaply with 200k sats", "where would a channel earn?") and it works through:

- `demand`: the next hops of forwards this node could not complete (with the reason, and whether we have a channel there) and where successful forwards went. Unserved demand is the strongest sign a channel would earn.
- `graph_summary`, `find_peers`, `node_profile`, `route_cost`: the public gossip graph. `find_peers` shortlists candidates by the advertised fee and hop count from each to the destinations named, then by connectivity; the model reasons over the shortlist rather than the whole graph.
- `probe`, `probe_results`: live checks. `probe` sends unclaimable payments toward a destination for an amount and reports which paths carried it and at what fee; `probe_results` also holds what LDK Node's background prober found on its own (`LDK_PROBING_TOP_NODES` turns that on). Gossip says what a route advertises; a probe says whether it works right now. Probes need an outbound channel to leave from.
- `open_channel`, `close_channel`: always proposals, approved at the prompt. `connect_peer` and `receive_address` are free.

On a public test network, point the node at peers for gossip (`LDK_GOSSIP=p2p`, `LDK_PEERS=node_id@host:port,...`); Mutinynet's Rapid Gossip Sync snapshot is too thin to rank peers from. Where the node has not verified a channel on-chain, gossip gives no capacity, so the tools report the largest HTLC the channel accepts instead.

Run `./target/debug/ldk-liquidity-agent --help` for every environment variable, including the policy bounds that decide which actions apply without asking.

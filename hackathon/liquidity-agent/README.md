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
- `demo`: build a five-node regtest topology around the node, drive traffic that drains one channel, then run `review` and `chat`. Needs `DEMO_BITCOIN_CLI`.

Run `./target/debug/ldk-liquidity-agent --help` for every environment variable, including the policy bounds that decide which actions apply without asking.

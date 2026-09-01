# monitord

**The runtime for monitors that cannot afford to miss.**

Catch the seat that opens for thirty seconds. The filing that lands overnight. The product page that quietly changes. The stream event that arrives while your process is restarting.

monitord turns focused Go programs into durable, observable monitors. Your code decides what changed. One daemon runs the system around that decision.

## Small monitors, serious infrastructure

A useful monitor may be only a request, a comparison, and an event. Operating it is the hard part.

Networks fail. Processes restart. Deploys overlap. Streams disconnect. Notifications rate-limit. Sources repeat data, rewrite history, or disappear for days. State must survive all of it without turning every monitor into its own service.

monitord provides that missing runtime:

| Capability | What it gives you |
| --- | --- |
| Typed durable state | Memory that survives restarts and remains inspectable and editable |
| Atomic commits | State, source checkpoints, and events advance together—or not at all |
| Worker-generation fencing | Old processes cannot write after a deploy, pause, edit, or restart |
| Immutable event identity | Replays coalesce while conflicting reuse fails loudly |
| Durable delivery outbox | Every destination retries independently without rerunning monitor logic |
| Health and lifecycle supervision | Readiness, failures, recovery, timeouts, backoff, and graceful shutdown |
| Exact secret grants | Each monitor receives only the credentials it declares |
| Reusable network resources | Long-lived clients, connections, rate limiters, and rotating proxy pools |

The result is a monitor that stays conceptually small even when its reliability requirements are not.

## One decision, committed durably

```text
 source observation
        │
        ▼
  monitor logic
        │
        ▼
 ┌─────────────────────────────────────┐
 │ atomic commit                       │
 │                                     │
 │ typed state + checkpoints + events │
 └─────────────────────────────────────┘
        │
        ▼
 durable per-destination outbox
        │
        ├── Discord bot
        └── Discord webhook
```

If a worker loses its acknowledgement after committing, it can replay the same transaction and receive the stored result. If a retired worker tries to commit late, generation fencing rejects it. If delivery fails, the outbox retries it without touching monitor state.

Those guarantees are shared by every monitor instead of being reimplemented imperfectly in each one.

## Poll anything. Follow anything.

monitord supports two deliberately small execution models:

- Polling monitors run immediately and then on a non-overlapping interval.
- Continuous monitors own a long-lived stream or subscription under supervised lifecycle control.

Both use the same state, checkpoint, event, health, secret, and delivery machinery. A five-line watcher and a durable streaming source get the same operational foundation.

The included catalogs cover browser-compatible HTTP, direct and rotating-proxy clients, QuickNode transport, and managed EVM and Solana sources. On-chain monitoring is one supported workload among many, not a special runtime.

## Designed for change

Deployments are immutable artifacts with stable identities. Redeploying activates a fresh fenced generation while preserving compatible state. Operators can pause, resume, inspect, edit state, recover checkpoints, retry deliveries, archive, and purge without reaching into SQLite or inventing monitor-specific tooling.

```bash
monitord list
monitord inspect <name>
monitord state get <name>
monitord events list <name>
monitord pause <name>
```

Configuration owns deployment policy and destinations. Go owns source behavior. The daemon owns runtime correctness. Each concern has one obvious home.

## Built to be operated

- Health tracks starting, healthy, failing, unhealthy, and stopped workers.
- Bounded, redacted errors remain available through inspection and daemon logs.
- Delivery retries respect destination-level rate limits and end in inspectable dead letters.
- Committed deliveries continue draining while a deployment is paused or archived.
- Checkpoints make offline recovery and HTTP backfill explicit for durable sources.
- Checkpoint recovery resets source progress without discarding deployment identity or typed state.

monitord is intentionally local-first and self-contained: a Go toolchain, one SQLite database, immutable worker artifacts, and a launchd or systemd user service.

## Explore

- [Monitor authoring](docs/monitors.md) — the Go contract, state, checkpoints, events, lifecycle resources, and secrets.
- [Operations](docs/operations.md) — installation, deployment lifecycle, health, logs, delivery recovery, and backups.
- [HTTP watch](examples/http-watch) — a complete package-structured monitor.
- [`catalog/httpx`](catalog/httpx) — browser-compatible clients and rotating proxy pools.
- [`catalog/quicknode`](catalog/quicknode) — provider transport and managed chain sources.

```bash
git clone https://github.com/saucesteals/monitord.git
cd monitord
./infra/install.sh
```

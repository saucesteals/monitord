---
name: monitord
description: Author, validate, deploy, and operate monitord monitors with typed state, checkpoints, lifecycle-owned resources, exact secrets, and durable event delivery. Use for work inside a monitord installation or monitor source tree.
---

# monitord

Use monitord for recurring or streaming watches that need durable progress and reliable notification handoff. Inspect the existing monitor and `monitor.yaml` before changing behavior; preserve its source semantics, destinations, and deployment name unless the user asks otherwise.

## Authoring model

A monitor is a small Go package with one `Monitor[S]`, one polling or continuous plan, and a `monitor.yaml`. Keep entrypoint, metadata, plan, and state together. Split source-specific fetching or parsing only where it forms a useful boundary; do not impose a file-count rule.

```go
package main

import (
	"context"
	"time"

	"github.com/saucesteals/monitord"
)

var ordersURL = monitord.RequiredSecret("orders", "websocket-url")

type State struct {
	Cursor uint64 `json:"cursor"`
}

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{Name: "orders", Description: "Watches new orders"},
		monitord.Continuous(watch, monitord.WithSecrets(ordersURL)),
	))
}

func watch(ctx context.Context, session *monitord.Session[State]) error {
	endpoint, err := session.Secrets().Require(ordersURL)
	if err != nil {
		return err
	}
	return consumeOrders(ctx, endpoint, func(order Order) error {
		return session.Commit(ctx, func(tx *monitord.Tx[State]) error {
			if order.Cursor <= tx.State.Cursor {
				return nil
			}
			tx.State.Cursor = order.Cursor
			return tx.Emit(monitord.Event{ID: "order:" + order.ID, Title: "New order"})
		})
	})
}
```

Use lower-case kebab-case for monitor names and secret group/key components. `SecretSet.Require` and `Get` accept the declared `SecretRef`, not raw strings.

## Choose the right durable primitive

- State is user-meaningful monitor memory and configuration that operators may inspect or edit with `monitord state`.
- Checkpoints are daemon-owned source progress or observations used for replay, cursors, baselines, and transition counters.
- Events are immutable source occurrences. Use a source ID or a state/checkpoint-backed sequence; never use the current time merely to manufacture uniqueness.
- State, checkpoint updates, and events written in one `Commit` are atomic.

Perform network I/O, sleeping, and expensive parsing outside `Commit`. Recheck ordering and repeated-condition predicates inside the closure. Keep it bounded and deterministic; do not retain `tx.State` references or launch goroutines from it. The SDK retains and resends an unacknowledged transaction, so never implement ACK retry by rerunning a closure.

## Lifecycle-owned resources

When no managed catalog source owns a reusable client, proxy pool, connection, or subscription, implement `monitord.Starter` and optionally `monitord.Stopper` on the monitor. Construct the resource in `Start(context.Context, monitord.Environment)`, after exact secrets are available and before the worker becomes ready. Stop closes resources after callbacks have ended. A failing `Start` must clean up its partial work.

For browser-compatible HTTP and proxy rotation, use `catalog/httpx`. A proxy secret is a JSON array of `http`, `https`, or `socks5` URLs. Create one `httpx.ProxyClient` in `Start`; do not recreate clients inside each check.

## Delivery

Declare each destination inline under `deliveries` in `monitor.yaml`. A delivery
contains exactly one backend plus its own optional `rate_limit`. Discord accepts
either a named bot account with a channel or a direct webhook. OpenClaw requires
an account token and task prompt; `agent_id` and `url` are optional. Leave
session, model, thinking, timeout, and outbound-channel policy to OpenClaw.

```yaml
deliveries:
  - openclaw:
      account: local
      agent_id: analyst
      prompt: Investigate this event and explain why it matters.
    rate_limit:
      per_second: 0.2
      burst: 1
```

## Chain sources and QuickNode

Prefer a managed chain source over wiring raw subscriptions in a monitor. Use `catalog/quicknode` only for provider transport and `catalog/quicknode/evm` or `catalog/quicknode/solana` for chain identity, finality, replay, and managed monitors. Copy exact QuickNode URLs into chain-named secret refs; never derive or rewrite endpoint hosts or paths.

`evm.Events` sends an exact address/topic filter to `eth_subscribe` and handles
matching logs immediately. Ranged `eth_getLogs` replay advances a confirmed
durable cursor and repairs startup or reconnect gaps without scanning blocks one
at a time. `Confirmations` controls replay finality, not notification latency.
Matching live logs remain in a durable journal until replay confirms them;
orphaned entries return to the handler with `Removed`, even when the WebSocket
was disconnected during the reorganization. Use the log block hash with
EIP-1898 for state reads and stable `log.ID()` identities for emitted events.
Use `client.ERC20Metadata(ctx, token, log.BlockHash)` for exact-fork ERC-20
name, symbol, decimals, and total supply; optional ERC-20 fields may be empty.

`solana.AddressEvents` uses QuickNode `transactionSubscribe` with the monitored
account applied at the provider. The live notification contains the full
transaction; `MatchLogs` narrows it locally before the handler runs. Finalized
QuickNode `getTransactionsForAddress` history repairs gaps with complete,
ascending pages from the saved slot and transaction index. It does not perform a
serial signature lookup followed by one transaction request per record.
Use `client.TokenMetadata(ctx, mint, commitment)` for Metaplex name, symbol,
URI, and metadata-account identity instead of implementing PDA or Borsh parsing
inside a monitor.

Keep the live handler deterministic and short. Use stable event IDs and content
so inclusive replay coalesces in the durable outbox. Do not put third-party
market-data or indexing APIs in the detection path; enrich later or only when
the alert contract truly requires it.

Chain monitors require a durable cursor and authoritative replay by default. Use raw subscriptions only when missed events are intentionally disposable or another source guarantees replay, and document that exception in the monitor.

## Workflow

```bash
monitord new <name>
$EDITOR ~/.monitord/monitors/<name>/
monitord test <name>
monitord deploy <name> [--name <deployment>]
monitord inspect <deployment>
```

`test` runs one polling callback or a continuous monitor for `--duration` without persisting state or sending notifications. Use `--stored-state` when behavior depends on deployed state. Redeploy preserves deployment identity and state; use `--reset-state` only for an intentional incompatible state reset.

Before handing off a change, build the affected monitor, run `monitord test` when its external dependencies are available, and inspect the deployed generation after rollout. Do not edit the SQLite database directly. Use `state get/set/clear`, lifecycle commands, `checkpoints clear NAME --all` after pausing for source-cursor recovery, and `events retry` for operator changes.

When working in the monitord checkout, `docs/monitors.md` and `docs/operations.md` provide the full configuration and operational reference. The skill remains usable when installed by itself through `monitord skill`.

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

## Chain sources and QuickNode

Prefer a managed chain source over wiring raw subscriptions in a monitor. Use `catalog/quicknode` only for provider transport and `catalog/quicknode/evm` or `catalog/quicknode/solana` for chain identity, finality, replay, and managed monitors. Copy exact QuickNode URLs into chain-named secret refs; never derive or rewrite endpoint hosts or paths.

`evm.Events` and `evm.Wallet` use confirmed HTTP history. `solana.AddressEvents` uses finalized HTTP signature history as its durable source and WSS as a low-latency wake-up. Its handler performs enrichment before returning a deterministic `AddressEventUpdate`; that update and the source checkpoint commit atomically. `MatchLogs` can skip live transactions that are conclusively irrelevant, while signatures without a WSS hint are still fetched during replay. History gaps fail closed unless the monitor explicitly chooses availability with `ResumeFromLatestOnGap`.

Chain monitors require a durable cursor and authoritative replay by default. Use raw subscriptions only when missed events are intentionally disposable or another source guarantees replay, and document that exception in the monitor.

## Workflow

```bash
monitord new <name>
$EDITOR ~/.monitord/monitors/<name>/
monitord test <name>
monitord deploy <name> [--name <deployment>]
monitord inspect <deployment>
```

`test` runs one callback without persisting state or sending notifications. Use `--stored-state` when behavior depends on deployed state. Redeploy preserves deployment identity and state; use `--reset-state` only for an intentional incompatible state reset.

Before handing off a change, build the affected monitor, run `monitord test` when its external dependencies are available, and inspect the deployed generation after rollout. Do not edit the SQLite database directly. Use `state get/set/clear`, lifecycle commands, and `events retry` for operator changes.

When working in the monitord checkout, `docs/monitors.md` and `docs/operations.md` provide the full configuration and operational reference. The skill remains usable when installed by itself through `monitord skill`.

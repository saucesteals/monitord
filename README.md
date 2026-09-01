# monitord

**Run tiny Go monitors with durable state, safe redeploys, and reliable notifications—without rebuilding the runtime around every watch.**

monitord is a daemon and authoring SDK for focused, stateful monitors. Your code talks to the source and decides what changed. monitord handles scheduling, persistence, worker lifecycle, health, and delivery.

Use it to poll an API, watch inventory, consume a stream, or follow on-chain activity. A monitor is a normal Go package—not a configuration language or a long-running service you have to operate on its own.

## Your logic, its runtime

| You write | monitord owns |
| --- | --- |
| One polling or continuous Go callback | Scheduling, non-overlap, timeouts, and restarts |
| A typed state transition | Strict decoding and durable state |
| Checkpoints and events inside `Commit` | One atomic transaction for state, progress, and events |
| A stable `Event.ID` | Occurrence deduplication and a durable per-destination outbox |
| Lifetime and destinations in `monitor.yaml` | TTL expiry, health, retries, rate limits, and dead letters |

Deployments are built as immutable artifacts. Redeploying preserves compatible state, while worker-generation fencing prevents an old process from writing after a deploy, pause, state edit, or restart.

## A monitor is just Go

This monitor remembers the last inventory observation and emits only when the state changes. Source-specific network I/O happens before the small atomic decision:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/saucesteals/monitord"
)

type State struct {
	InStock  bool   `json:"in_stock"`
	Observed bool   `json:"observed"`
	Changes  uint64 `json:"changes"`
}

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{Name: "restock", Description: "Watches product availability"},
		monitord.Every(5*time.Minute, check),
	))
}

func check(ctx context.Context, session *monitord.Session[State]) error {
	inStock, err := fetchProduct(ctx) // ordinary, source-specific Go
	if err != nil {
		return err
	}

	return session.Commit(ctx, func(tx *monitord.Tx[State]) error {
		previous := tx.State.InStock
		if !tx.State.Observed {
			tx.State.Observed, tx.State.InStock = true, inStock
			return nil
		}
		if previous == inStock {
			return nil
		}

		tx.State.InStock = inStock
		tx.State.Changes++
		title, severity := "Out of stock", monitord.SeverityWarn
		if inStock {
			title, severity = "Back in stock", monitord.SeverityInfo
		}
		return tx.Emit(monitord.Event{
			ID:       fmt.Sprintf("availability:%d", tx.State.Changes),
			Severity: severity,
			Title:    title,
		})
	})
}
```

The corresponding deployment policy stays small:

```yaml
ttl: 30d
deliveries:
  - discord:
      account: personal
      channel_id: "123456789012345678"
    rate_limit:
      per_second: 1
      burst: 5
```

`Commit` publishes the new state and event together. Identical replays of an event coalesce; conflicting reuse of the same ID fails loudly. Delivery is at least once, with independent retry state for each destination.

## From clone to first deployment

The installer builds a self-contained installation under `~/.monitord` and installs a launchd user service on macOS or a systemd user service on Linux.

```bash
git clone https://github.com/saucesteals/monitord.git
cd monitord
./infra/install.sh

monitord new restock
```

Edit `~/.monitord/monitors/restock/`, add a destination to its `monitor.yaml`, then test one callback locally before deploying:

```bash
# macOS: store a Discord bot token in Keychain
monitord account set discord personal --token "$DISCORD_TOKEN"

monitord test restock
monitord deploy restock
monitord inspect restock
```

`monitord test` builds the real monitor and shows emitted events and state changes without persisting state or sending notifications. On Linux, direct Discord webhooks are available without the macOS Keychain account backend. See [operations](docs/operations.md#accounts-and-destinations) for destination setup and installer options.

Once deployed, the everyday surface stays compact:

```bash
monitord list
monitord inspect restock
monitord state get restock
monitord events list restock
monitord pause restock
```

## Built for monitors that need memory

- Poll immediately and then on a non-overlapping interval with `Every`, or own one long-lived stream with `Continuous`.
- Keep operator-editable data in typed state and source progress in durable checkpoints.
- Grant each worker generation only the exact secrets declared by its plan.
- Reuse lifecycle-owned clients and connections, including browser-compatible HTTP and proxy pools.
- Start EVM and Solana monitors from the QuickNode catalogs with chain identity, finality, and replay behavior already modeled.
- Let committed deliveries drain even while a deployment is paused, expired, or archived.

## Go deeper

- [Monitor authoring](docs/monitors.md) — state, checkpoints, event identity, secrets, lifecycle resources, and the complete `monitor.yaml` reference.
- [Operations](docs/operations.md) — installation layout, deployments, health, logs, delivery recovery, and backups.
- [HTTP watch example](examples/http-watch) — a package-structured monitor over multiple targets.
- [`catalog/httpx`](catalog/httpx) — browser-compatible direct and rotating-proxy clients.
- [`catalog/quicknode`](catalog/quicknode) — provider transport plus managed [EVM](catalog/quicknode/evm) and [Solana](catalog/quicknode/solana) sources.

Use `monitord <command> --help` for the exact CLI surface.

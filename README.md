# monitord

`monitord` runs small, stateful Go monitors while one daemon owns scheduling, durable state, worker generations, checkpoints, health, and at-least-once event delivery.

monitord favors explicit monitor code over framework magic. A monitor declares one polling or continuous plan; optional lifecycle interfaces own reusable clients and connections; exact secret references grant only the credentials that generation needs.

## Install

The installer creates a self-contained root, builds the CLI, and installs a user service with launchd on macOS or systemd on Linux:

```bash
git clone https://github.com/saucesteals/monitord.git
cd monitord
./infra/install.sh
monitord version
```

By default, the root is `~/.monitord` and the CLI symlink is `~/.local/bin/monitord`. Use `./infra/install.sh --help` for a different root, ref, repository, or service mode. A manual development daemon is also valid:

```bash
go run ./cmd/monitord --root /tmp/monitord-dev init
go run ./cmd/monitord --root /tmp/monitord-dev daemon
```

## Create a monitor

```bash
monitord new restock
```

Treat the generated directory as a small Go package. Keep its contract together and split fetching or parsing only when the boundary is useful:

```text
restock/
├── monitor.go
└── monitor.yaml
```

For a monitor this small, `monitor.go` can contain the whole implementation. Split fetching or parsing into another file only when it becomes a useful domain boundary.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/saucesteals/monitord"
)

type State struct {
	InStock bool   `json:"in_stock"`
	Changes uint64 `json:"changes"`
}

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{Name: "restock", Description: "Watches one product"},
		monitord.Every(5*time.Minute, check),
	))
}

func check(ctx context.Context, session *monitord.Session[State]) error {
	inStock, err := fetchProduct(ctx) // network I/O stays outside Commit
	if err != nil {
		return err
	}
	return session.Commit(ctx, func(tx *monitord.Tx[State]) error {
		if tx.State.InStock == inStock {
			return nil
		}
		tx.State.InStock = inStock
		tx.State.Changes++
		if !inStock {
			return nil
		}
		return tx.Emit(monitord.Event{
			ID:    fmt.Sprintf("restock:%d", tx.State.Changes),
			Title: "Back in stock",
		})
	})
}
```

`monitor.yaml` owns deployment policy and destinations. Choose exactly one lifetime policy: `ttl` or `persistent`.

```yaml
ttl: 30d
health:
  failure_threshold: 3
events:
  max_per_transaction: 20
  retention: 30d
deliveries:
  - discord:
      account: personal
      channel_id: "123456789012345678"
    rate_limit:
      per_second: 1
      burst: 5
```

Store the Discord token in Keychain, then test and deploy:

```bash
monitord account set discord personal --token "$DISCORD_TOKEN"
monitord test restock
monitord deploy restock
monitord inspect restock
```

`test` runs one callback locally without saving state or sending deliveries. Deploying an existing name keeps its immutable deployment ID and compatible state while activating a newly built artifact and worker generation.

## Core semantics

- `Every` runs immediately and then at its interval without overlap. `Continuous` runs one long-lived callback.
- `Session.State()` returns a fresh typed copy. Strict decoding rejects unknown fields.
- `Session.Commit` atomically persists state, checkpoints, and events.
- `Event.ID` identifies one source occurrence. Duplicate IDs with identical payloads coalesce; conflicting reuse fails.
- Events enter a durable per-destination outbox. Delivery is at least once, so an external destination can rarely receive a duplicate.
- Worker generations fence stale writes after deploys, state edits, pauses, or restarts.
- Ordinary callback failures update health and continue polling. Fatal callbacks and continuous-plan exits restart with backoff.

## Documentation

- [Monitor authoring](docs/monitors.md): state, checkpoints, events, lifecycle clients, proxy pools, secrets, and the complete `monitor.yaml` shape.
- [Operations](docs/operations.md): installation layout, deployments, health, logs, outbox recovery, state editing, and clean-root recovery.
- [`catalog/httpx`](catalog/httpx): browser-compatible direct and rotating proxy clients.
- [`catalog/quicknode`](catalog/quicknode): chain-neutral QuickNode JSON-RPC transport and subscriptions.
- [`catalog/quicknode/evm`](catalog/quicknode/evm): EVM identity, subscriptions, confirmed logs, and wallet monitoring.
- [`catalog/quicknode/solana`](catalog/quicknode/solana): Solana RPC, subscriptions, and finalized address-event processing.

## Common CLI workflow

```bash
monitord list
monitord inspect restock
monitord state get restock
monitord events list restock
monitord pause restock
monitord resume restock --persistent
monitord pause restock
monitord archive restock
monitord purge restock
```

Use `monitord <command> --help` for flags and destructive-operation requirements.

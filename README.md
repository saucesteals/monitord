# monitord V5

`monitord` runs small, stateful Go monitors while the daemon owns deployment identity, scheduling, durable state, worker fencing, checkpoints, event delivery, and recovery.

V5 has two authoring layers: a monitor declares its identity and opaque plan; `monitord.Run` compiles that plan into a daemon-owned session. State belongs to an immutable deployment ID, not a Go implementation name or source directory.

V5 is an intentional clean break. Its migration removes the V4 name-keyed monitor, run, and event tables; export or back up a V4 root before opening it with V5. Route, account, and proxy configuration remains reusable, but V4 deployments must be created again as V5 deployments.

## A monitor

```go
package main

import (
	"context"
	"time"

	"github.com/saucesteals/monitord"
)

type State struct { InStock bool `json:"in_stock"` }

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{Name: "restock", Description: "Watches one product"},
		monitord.Every(5*time.Minute, check),
	))
}

func check(ctx context.Context, session *monitord.Session[State]) error {
	inStock, err := fetchProduct(ctx) // network I/O stays outside Commit
	if err != nil { return err }
	return session.Commit(ctx, func(tx *monitord.Tx[State]) error {
		wasInStock := tx.State.InStock
		tx.State.InStock = inStock
		if inStock && !wasInStock {
			return tx.Emit(monitord.Event{ID: "restock:2026-08-31", Title: "Back in stock"})
		}
		return nil
	})
}
```

`Every` runs immediately and then without overlap. `Continuous` supervises a long-lived source. Use `Combined(Named(...), Named(...))` when polling and streaming children share state; `Optional()` makes a terminal child degrade health without cancelling required siblings.

## Durable sessions

- `Session.State()` decodes a fresh state value. Maps, slices, pointers, and `big.Int` values never alias canonical state.
- `Session.Commit` admits one bounded deterministic closure at a time. Fetch and decode externally before entering it, then recheck ordering against `tx.State`.
- `Tx.Emit`, `Tx.Checkpoint`, and `Tx.Progress` become durable atomically with state.
- `Session.Progress` records useful source progress without changing state.
- `Session.Checkpoint(source, &value)` reads a fresh daemon-owned checkpoint snapshot.
- A closure error, panic, encoding failure, or cancellation before admission publishes nothing.

The worker retains the exact unacknowledged transaction bytes. If an ACK is lost it resends those bytes; the daemon returns the ledgered ACK for the same `(deployment, generation, sequence, payload hash)`. Cancellation after admission does not reconstruct or abandon the frame.

## Events and delivery

`Event.ID` is an immutable source occurrence ID. Replaying the same ID and payload coalesces; reusing it for different content conflicts. `DedupeKey`/`DedupeFor` optionally suppress repeated state edges without rolling back state or checkpoints.

Events enter a durable per-destination outbox in the same SQLite commit. Delivery is at least once: a destination may receive a duplicate if it accepted a request immediately before the daemon lost the success marker. Retries, partial destination success, leases, and dead letters are independent of worker lifetime.

## Exact secrets

Plans request exact group/key references:

```go
monitord.Continuous(watch, monitord.WithSecrets(
	monitord.RequiredSecret("orders", "ORDERS_WSS_URL"),
))
```

Only requested keys cross the generation-bound handshake. Read them with `session.Secrets().Require("orders", "ORDERS_WSS_URL")`. Resolution precedence is deployment credential override, monitor-local `.env`, `~/.monitord/secrets/<group>.env`, global `~/.monitord/.env`, then a declared non-secret default. Workers and compiler subprocesses receive scrubbed environments.

## QuickNode catalog

The turnkey case is intentionally small:

```go
func main() {
	monitord.Run(quicknode.Wallet{Address: quicknode.Address("0x7130...7777")})
}
```

The catalog also exposes managed confirmed `quicknode.Events[S]` and a raw JSON-RPC client. Wallet monitoring covers ERC-20/721/1155 Transfer logs and non-zero top-level native transactions; it does not imply internal traces or every balance change.

## CLI workflow

```bash
monitord new restock
monitord describe ~/.monitord/monitors/restock
monitord test restock
monitord deploy restock --name shop-restock
monitord list
monitord inspect shop-restock
monitord state get shop-restock
monitord runs shop-restock
monitord events shop-restock
monitord expire shop-restock
monitord resume shop-restock
monitord rm shop-restock            # archive
monitord rm shop-restock --purge    # destructive, confirmation required
```

Deploying an existing name preserves its immutable deployment ID, state, and history while activating a new artifact and worker generation. Manual state changes fence the old generation. Expired and archived deployments continue draining already committed outbox rows.

State schema version and concurrency revision are separate. Implement `StateVersion() int` and `MigrateState(from int, raw json.RawMessage) error` when changing the stored shape; deploy validates and migrates before activation.

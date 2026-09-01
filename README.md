<div align="center">

# monitord

**Stateful monitors. Durable delivery. One small daemon.**

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev)
[![SQLite](https://img.shields.io/badge/State-SQLite-3f7f5f?style=flat)](https://sqlite.org/)
[![Platform](https://img.shields.io/badge/Platform-macOS%20%7C%20Linux-lightgrey)]()

**Polling** · **Streams** · **Typed State** · **Checkpoints** · **Discord** · **OpenClaw** · **CLI Ops**

</div>

---

`monitord` runs focused Go monitors while one daemon owns scheduling, persistence, workers, health, and delivery.

Use it when cron is too stateless and building another service is too much.

## Highlights

- Run immediate, non-overlapping polls or long-lived streams.
- Persist typed state across ticks, restarts, and redeploys.
- Commit state, source checkpoints, and events atomically.
- Reuse HTTP clients, authenticated sessions, connections, and proxy pools for a worker generation.
- Fence old workers after deploys, pauses, state edits, and daemon restarts.
- Retry each destination independently through a durable outbox.
- Inspect readiness, failures, recovery, checkpoint progress, and outbox state from one CLI.
- Send rich Discord alerts or turn an event into an OpenClaw agent task.

## Code

Monitors are ordinary Go programs. Source I/O stays ordinary; the durable decision stays small.

```go
type State struct {
	Observed bool   `json:"observed"`
	InStock  bool   `json:"in_stock"`
	Changes  uint64 `json:"changes"`
}

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{Name: "restock", Description: "Watches one product"},
		monitord.Every(30*time.Second, check),
	))
}

func check(ctx context.Context, session *monitord.Session[State]) error {
	product, err := fetchProduct(ctx)
	if err != nil {
		return err
	}

	return session.Commit(ctx, func(tx *monitord.Tx[State]) error {
		if !tx.State.Observed {
			tx.State.Observed, tx.State.InStock = true, product.InStock
			return nil
		}
		if tx.State.InStock == product.InStock {
			return nil
		}

		tx.State.InStock = product.InStock
		tx.State.Changes++
		return tx.Emit(monitord.Event{
			ID:      fmt.Sprintf("availability:%d", tx.State.Changes),
			Title:   product.Name + " availability changed",
			Body:    product.Summary(),
			URL:     product.URL,
		})
	})
}
```

The state change and event either commit together or do not happen. Replaying the same event is safe; reusing its ID for different content fails loudly.

## Delivery

Deployment policy and destinations live beside the monitor:

```yaml
ttl: 30d
health:
  failure_threshold: 3

deliveries:
  - discord:
      account: personal
      channel_id: "123456789012345678"
      mentions: "role:123456789012345679"
    rate_limit:
      per_second: 1
      burst: 5

routes:
  - route: openclaw:analyst
    options:
      prompt: Investigate this event and explain why it matters.
    rate_limit:
      per_second: 0.2
      burst: 1
```

- Discord receives constrained rich embeds through a bot account or direct webhook.
- OpenClaw receives the event plus a monitor-specific prompt as an agent task.
- Every destination has independent retry, leasing, rate limiting, and terminal state.
- Committed deliveries keep draining while a monitor is paused or archived.

## Runtime guarantees

- Immutable deployment artifacts and stable deployment identities.
- Generation capabilities that reject stale or unauthorized writes.
- Transaction ledger replay when a commit succeeds but its acknowledgement is lost.
- Strict state and checkpoint decoding instead of silent schema drift.
- Stable event identity, occurrence deduplication, and conflict detection.
- Bounded retries, restart backoff, graceful shutdown, and fatal-error handling.
- Exact secret grants; undeclared credentials never cross into a worker.
- Explicit checkpoint recovery without discarding typed state or deployment identity.

## CLI

```bash
monitord new restock
monitord test restock
monitord deploy restock

monitord list
monitord inspect restock
monitord state get restock
monitord events list restock
monitord pause restock
```

## Catalogs

- [`catalog/httpx`](catalog/httpx): browser-compatible direct and rotating-proxy HTTP clients.
- [`catalog/quicknode`](catalog/quicknode): provider transport with managed EVM and Solana sources.

## Install

```bash
git clone https://github.com/saucesteals/monitord.git
cd monitord
./infra/install.sh
```

## Documentation

- [Monitor authoring](docs/monitors.md)
- [Operations](docs/operations.md)
- [HTTP watch example](examples/http-watch)
- [Agent skill](SKILL.md)

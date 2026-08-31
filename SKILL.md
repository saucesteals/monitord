---
name: monitord
description: Build, test, deploy, and operate durable monitord monitors with typed state, polling or continuous plans, exact scoped secrets, and at-least-once notification delivery.
---

# monitord

Use monitord for a recurring or streaming watch that needs durable state and reliable event handoff.

## Workflow

```bash
monitord new <name>
$EDITOR ~/.monitord/monitors/<name>/monitor.go
monitord describe ~/.monitord/monitors/<name>
monitord test <name>
monitord deploy <name> [--name <deployment>]
monitord inspect <deployment>
```

Always describe and test before deploy. Deployment names select instances; `Info.Name` identifies Go behavior and is not a persistence key.

## Authoring contract

```go
type State struct { Cursor uint64 `json:"cursor"` }

func main() {
	monitord.Run(monitord.Define(
		monitord.Info{Name: "orders"},
		monitord.Continuous(watch,
			monitord.WithSecrets(monitord.RequiredSecret("orders", "ORDERS_WSS_URL"))),
	))
}

func watch(ctx context.Context, session *monitord.Session[State]) error {
	endpoint, err := session.Secrets().Require("orders", "ORDERS_WSS_URL")
	if err != nil { return err }
	stream, err := connect(ctx, endpoint)
	if err != nil { return err }
	if err := session.Progress(ctx); err != nil { return err }
	for stream.Next(ctx) {
		record := stream.Record()
		if err := session.Commit(ctx, func(tx *monitord.Tx[State]) error {
			if record.Cursor <= tx.State.Cursor { return nil }
			tx.State.Cursor = record.Cursor
			return tx.Emit(monitord.Event{ID: "order:"+record.ID, Title: "New order"})
		}); err != nil { return err }
	}
	return stream.Err()
}
```

Choose `Every(interval, fn)` for polling and `Continuous(fn)` for streams. A `Combined` plan requires unique `Named` children sharing one state coordinator. Mark non-critical children with `Optional()`. Return `monitord.Permanent(err)` only for a terminal configuration/source error.

## Transaction rules

- Perform network I/O, sleeping, and expensive parsing before `Commit`.
- Recheck cursors, ordering, and dedupe predicates against `tx.State`.
- Keep the closure bounded and deterministic; do not launch goroutines or retain `tx.State` references.
- Use `Tx.Checkpoint` for source resume boundaries and `Session.Checkpoint` to read them.
- Handler state, checkpoints, progress, events, dedupe decisions, and outbox rows commit together.
- Do not implement retries by rerunning a closure. The SDK retains and resends the exact frame until its durable ACK is known.

## Event identity

Use an immutable occurrence identifier for `Event.ID`, such as a chain/log identity or source record ID. A recurring condition needs a new occurrence ID. Use `DedupeKey` plus `DedupeFor` for temporary state-edge suppression. Delivery is durable and at least once per destination; do not promise external exactly-once behavior.

## Secrets

Declare exact refs with `WithSecrets`; never read arbitrary environment variables, put values in source/config, or log secret-bearing URLs. Only requested values enter the worker handshake. `describe` reports references, never values.

## Operations

```bash
monitord list
monitord inspect <name-or-full-id>
monitord state get|set|clear <name-or-full-id>
monitord runs <name-or-full-id>
monitord events <name-or-full-id>
monitord expire <name-or-full-id>
monitord resume <name-or-full-id>
monitord rm <name-or-full-id>
```

State changes and resume/redeploy create a new fenced worker generation. Archive/expiry do not discard queued deliveries. Purge is destructive and must report pending deliveries before confirmation.

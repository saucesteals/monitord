---
name: monitord
description: Build, test, deploy, and operate durable monitord monitors with typed state, polling or continuous plans, exact scoped secrets, and at-least-once notification delivery.
---

# monitord

Use monitord for a recurring or streaming watch that needs durable state and reliable event handoff.

## Workflow

```bash
monitord new <name>
$EDITOR ~/.monitord/monitors/<name>/
monitord test <name>
monitord deploy <name> [--name <deployment>]
monitord inspect <deployment>
```

Run the local test command before deploy. Deployment names select instances; `Info.Name` identifies Go behavior and is not a persistence key. Keep the entrypoint, metadata, plan, and state contract in `monitor.go`; split source or domain logic into additional files only when the boundary is useful.

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

Choose `Every(interval, fn)` for polling and `Continuous(fn)` for streams. A monitor has one plan; use separate deployments for independent work. Callback errors are returned to the daemon, which owns restart and backoff.

## Transaction rules

- Perform network I/O, sleeping, and expensive parsing before `Commit`.
- Recheck cursors, ordering, and repeated-condition predicates against `tx.State`.
- Keep the closure bounded and deterministic; do not launch goroutines or retain `tx.State` references.
- Use `Tx.Checkpoint` for source resume boundaries and `Session.Checkpoint` to read them.
- Handler state, checkpoints, events, and outbox rows commit together.
- Do not implement retries by rerunning a closure. The SDK retains and resends the exact frame until its durable ACK is known.

## Event identity

Use an immutable occurrence identifier for `Event.ID`, such as a chain/log identity or source record ID. A recurring condition needs a new occurrence ID. Suppress repeated conditions using durable monitor state. monitord assigns the durable outbox timestamp; do not put wall-clock presentation metadata into occurrence identity. Delivery is durable and at least once per destination; do not promise external exactly-once behavior.

## Secrets

Declare exact refs with `WithSecrets`; never read arbitrary environment variables, put values in source/config, or log secret-bearing URLs. Only requested values enter the worker handshake. `describe` reports references, never values.

## Operations

```bash
monitord list
monitord inspect <name-or-full-id>
monitord state get|set|clear <name-or-full-id>
monitord events list <name-or-full-id>
monitord pause <name-or-full-id>
monitord resume <name-or-full-id> --persistent
monitord archive <name-or-full-id>
monitord purge <name-or-full-id>
```

State changes and resume/redeploy create a new fenced worker generation. Pausing and archiving do not discard queued deliveries. Purge is destructive and must report pending deliveries.

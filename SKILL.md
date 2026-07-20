---
name: monitord
description: Build, deploy, operate, and debug persistent monitors that run on a schedule, keep typed state, and notify Discord when something changes, fails, or recovers. Use for requests like "monitor X", "watch X", "alert me when X changes", "check X every N minutes", "tell me when X is back", or to inspect, repair, redeploy, expire, or remove an existing monitor.
---

# monitord

`monitord` runs small Go programs that check something repeatedly and report only the moments that matter. Use it instead of cron when a watch needs durable state, a TTL, edge-triggered failure/recovery notifications, structured Discord alerts, or proxy-backed HTTP clients.

Default install layout:

- Binary: `monitord`, usually symlinked onto `PATH`
- Root: `~/.monitord` unless `--root` or `MONITORD_ROOT` is set
- Monitor sources: `~/.monitord/monitors/<name>/`
- Pinned SDK clone: `~/.monitord/lib`
- Full reference: `~/.monitord/lib/README.md`

The install is self-contained: the daemon binary and every monitor compile against the SDK clone under the same root. Updating the root updates the daemon and monitor SDK together.

## Before Creating A Monitor

Clarify these points before writing code. Ask the requester only when the answer changes behavior.

- What is a hit: status code, JSON field, text selector, price threshold, inventory state, feed item, or some other condition?
- How often should it run: set `--every`.
- How long should it live: set `--ttl`, or use `--persistent` only for long-lived infrastructure checks.
- What state must it remember: previous status, last seen ID, last price, known hashes, auth/session data?
- Does it need proxies: use them only for targets that rate-limit or block direct traffic.
- Who should be notified: choose a route and optional mention override.

Default to a TTL. Temporary watches should expire by themselves.

## Standard Workflow

```bash
monitord new <name>
$EDITOR ~/.monitord/monitors/<name>/monitor.go
monitord test <name>
monitord deploy <name> --every 5m --ttl 24h --route discord:alerts
monitord list
```

Always run `monitord test` before deploying. It builds the monitor, runs one real tick, prints logs, events, result, and state, and does not deploy or send Discord messages.

After editing a deployed monitor, `monitord deploy <name>` rebuilds it and keeps the existing schedule, timeout, route, proxy pool, TTL, and mention override unless new flags are provided.

## Writing A Monitor

Scaffolding creates a working monitor. The usual shape is:

```go
package main

import (
	"context"

	"github.com/saucesteals/monitord"
	http "github.com/saucesteals/fhttp"
)

type State struct {
	LastSeen string `json:"last_seen"`
}

func main() {
	monitord.Main(monitord.Definition{
		Name:    "<name>",
		Clients: 1,
	}, run)
}

func run(ctx context.Context, r *monitord.Run[State]) monitord.Result {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/", nil)
	if err != nil {
		return monitord.Failuref("build request: %v", err)
	}

	resp, err := r.Client().Do(req)
	if err != nil {
		return monitord.Failuref("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	current := resp.Status
	if current != r.State.LastSeen {
		r.State.LastSeen = current
		r.Save()

		return monitord.Alert("changed to " + current)
	}

	return monitord.Success("unchanged")
}
```

Rules that matter:

- Import `http "github.com/saucesteals/fhttp"` and send requests with `r.Client()`.
- `Definition.Name` must match the monitor directory name.
- Call `r.Save()` after mutating `r.State`, or the state change is discarded.
- Put data files beside the monitor source and read them with `r.Path("targets.json")`.
- Do not hardcode secrets. Store monitor-owned credentials in state and update them with `monitord state set`.
- Workers receive a minimal environment, so ordinary shell environment variables are not a reliable monitor configuration channel.

## Return Semantics

- `monitord.Success(...)`: the check ran and there is nothing to report. Silent.
- `monitord.Alert(...)`: the check ran and found something noteworthy. Sends every time it is returned.
- `monitord.Failure(...)`: the check itself broke. Sends on the failure edge and again on recovery, not every tick.

Use `Alert` for interesting healthy outcomes such as restock, threshold crossed, new item, or content change. Use `Failure` for broken checks such as unreachable target, bad response, auth failure, or unparseable data.

## Useful Discord Messages

Notifications render as Discord embeds. Add a URL and fields when the message should be actionable without opening logs.

```go
return monitord.Alert("latency crossed 500ms").
	WithURL("https://status.example.com/").
	WithField("Latency", "742ms", true).
	WithField("Region", "us-east", true).
	WithField("Target", "`https://api.example.com/health`", false)
```

Use inline fields for short comparable values and non-inline fields for longer values such as URLs, IDs, snippets, or explanations. Field values accept Discord markdown and are truncated to Discord limits.

For monitors that check multiple targets, emit one event per target and set `DedupeKey` so one noisy target does not bury the rest:

```go
r.Event(monitord.Event{
	Severity:  monitord.SeverityWarn,
	Title:     target.Name + " is unhealthy",
	Summary:   "HTTP 503 from health endpoint",
	DedupeKey: "down:" + target.Name,
	URL:       target.URL,
	Fields:    []monitord.Field{{Name: "Status", Value: "503", Inline: true}},
})
```

Event severities map to embed colors. Events with the same dedupe key are suppressed for one hour. Events without a key always send.

## State

State is a typed Go struct owned by the monitor and stored by the daemon. It survives ticks, daemon restarts, and redeploys.

Commands:

```bash
monitord state get <name>
monitord state set <name> ./state.json
echo '{"last_seen":"ok"}' | monitord state set <name> -
monitord state clear <name>
```

Deploy validates stored state against the new binary before accepting it. If the state shape changes, declare a version and migration:

```go
func (State) StateVersion() int { return 2 }

func (s *State) MigrateState(from int, raw json.RawMessage) error {
	// Decode the old shape and populate s.
	return nil
}
```

If the shape changed and there is no migration, deploy refuses instead of silently dropping data. Either add a migration or clear state intentionally.

## Routes And Mentions

```bash
monitord route list
monitord route create discord alerts --webhook-url "$DISCORD_WEBHOOK_URL"
monitord route test discord:alerts
```

Routes are named `<kind>:<name>`, for example `discord:alerts`. The route name is a local label; the webhook URL decides where the message lands. Webhook URLs are redacted from output and logs.

Mentions can live on the route or be overridden per monitor:

```bash
monitord route create discord ops --webhook-url "$DISCORD_WEBHOOK_URL" --mention role:ROLE_ID
monitord deploy api-health --route discord:ops --mention user:USER_ID
monitord deploy quiet-watch --route discord:ops --mention none
```

`--mention` accepts `user:ID`, `role:ID`, `here`, `everyone`, comma-separated combinations, or `none`. Mentions are also an allowlist: scraped content containing `@everyone` is rendered inert unless that mention was explicitly allowed.

Prefer one route per destination webhook and per-monitor mention overrides for who gets pinged.

## Proxies

Skip proxies unless the target needs them.

```bash
monitord proxy import residential ./proxies.txt
monitord proxy list
monitord proxy show residential
monitord deploy <name> --proxies residential --route discord:alerts
```

Import accepts common proxy formats:

```text
host:port:user:pass
host:port
user:pass@host:port
scheme://user:pass@host:port
scheme://host:port
```

Set `Clients: N` in the monitor definition to request N clients. Each client gets its own proxy assignment and connection pool. `r.Client()` rotates through them; `r.Clients().At(i)` gives a stable client when one target should stick to one exit.

Proxy credentials live in monitord's database, not monitor source, environment, or process arguments.

## Operating And Debugging

```bash
monitord list
monitord inspect <name>
monitord runs <name>
monitord runs <name> --failed
monitord runs <name> --run <RUN_ID>
monitord stats <name>
monitord expire <name>
monitord rm <name> [--purge]
```

Use `monitord runs <name> --failed` to find the pattern, then `monitord runs <name> --run <RUN_ID>` for the full logs, events, result, and error stream from one run.

Use `monitord test <name> --stored-state` to rerun the current source against the state the deployed monitor actually has.

Use `monitord stats <name>` after deploying interval-sensitive monitors. A monitor can be healthy while quietly missing its desired cadence; stats show observed interval and tick latency.

## Notification Behavior

- Failure notifications are edge-triggered: one message when a monitor starts failing and one when it recovers.
- `Alert` sends every time it is returned.
- Events dedupe by key for one hour.
- Redeploy rolls the worker to the new artifact on the next tick; in-flight ticks finish on the old artifact.

This means a monitor can run frequently without adding manual "only alert once" logic. Let the daemon handle failure and event suppression.

## Install And Update

```bash
infra/install.sh
infra/install.sh --ref v1.2.3
infra/install.sh --no-restart
```

Useful overrides:

```bash
MONITORD_ROOT="$HOME/.monitord" infra/install.sh
MONITORD_REPO="https://github.com/OWNER/monitord.git" infra/install.sh
MONITORD_SERVICE=none infra/install.sh
```

The installer clones or fetches the pinned SDK under the root, builds the daemon, refreshes the monitor module, installs a background service when supported, and restarts it unless `--no-restart` is set.

When run from a clone, the installer infers the repository from that clone's `origin`. Set `MONITORD_REPO` or pass `--repo` when running the script from a mirror, archive, or standalone copy.

Run the daemon in the foreground when a service manager is not desired:

```bash
monitord daemon --interval 5s --concurrency 8
```

## Gotchas

- `--ttl` is required unless `--persistent` is set.
- Slow checks need `--timeout` above the default 30s.
- `monitord test` runs direct and sends no notifications. Deployed behavior can differ when proxies are attached.
- Deploy fails when a route or proxy pool does not exist. Create routes and import proxy pools first.
- State schema changes require a version bump and migration, or an intentional `monitord state clear`.

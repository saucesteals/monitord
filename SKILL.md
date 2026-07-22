---
name: monitord
description: Build, deploy, operate, and debug persistent monitors that run on a schedule, keep typed state, and notify Discord or trigger OpenClaw agent tasks when something changes, fails, or recovers. Use for requests like "monitor X", "watch X", "alert me when X changes", "check X every N minutes", "tell me when X is back", or to inspect, repair, redeploy, expire, or remove an existing monitor.
---

# monitord

`monitord` runs small Go programs that check something repeatedly and report only the moments that matter. Use it instead of cron when a watch needs durable state, a TTL, edge-triggered failure/recovery notifications, structured Discord alerts, OpenClaw agent tasks, or proxy-backed HTTP clients.

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
- How often should it run: set `every` in `monitor.yaml`.
- How long should it live: set `ttl`, or use `persistent: true` only for long-lived infrastructure checks.
- What state must it remember: previous status, last seen ID, last price, known hashes, auth/session data?
- Does it need proxies: use them only for targets that rate-limit or block direct traffic.
- Who should be notified or acted for: choose a route and its per-monitor route options.

Default to a TTL. Temporary watches should expire by themselves.

## Standard Workflow

```bash
monitord new <name>
$EDITOR ~/.monitord/monitors/<name>/{monitor.go,monitor.yaml}
monitord test <name>
monitord deploy <name>
monitord list
```

Always run `monitord test` before deploying. It builds the monitor, runs one real tick, prints logs, events, result, and state, and does not deploy or deliver notifications.

After editing either source or YAML, `monitord deploy <name>` rebuilds the monitor and applies the complete `monitor.yaml` configuration.

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
	monitord.Main(run)
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

		r.Emit(monitord.Event{ID: "status:" + current, Title: "status changed to " + current})
	}

	return monitord.Success("unchanged")
}
```

The matching `monitor.yaml` owns all runtime configuration:

```yaml
description: What this monitor watches
clients: 1
every: 5m
ttl: 24h
timeout: 30s
max_events: 20 # optional; per-tick event cap, default 20
routes:
  - route: discord:alerts
```

Rules that matter:

- Import `http "github.com/saucesteals/fhttp"` and send requests with `r.Client()`.
- Keep `main` to `monitord.Main(run)`; configuration belongs in `monitor.yaml`.
- Call `r.Save()` after mutating `r.State`, or the state change is discarded.
- Put data files beside the monitor source and read them with `r.Path("targets.json")`.
- Do not hardcode secrets. Store monitor-owned credentials in state and update them with `monitord state set`.
- Workers receive a minimal environment, so ordinary shell environment variables are not a reliable monitor configuration channel.

## Result And Events

A tick does two separate jobs: it returns a health `Result`, and along the way it emits zero or more `Event`s.

- `monitord.Success(...)`: the check ran and the watched thing is healthy. Silent.
- `monitord.Failure(...)`: the check itself broke. Pages on the failure edge and again on recovery, not every tick.
- `r.Emit(event)`: something worth reporting happened. Each event is its own Discord embed, delivered the moment it is emitted.

Return `Failure` for broken checks (unreachable target, bad response, auth failure, unparseable data). Emit an `Event` for every noteworthy finding (restock, threshold crossed, new listing, content change) and return `Success`. A tick can emit many events — one per finding — and each is sent live and independently of the result. Deliveries run concurrently, so events are not guaranteed to arrive in emission order.

## Building Events

An event is one Discord embed, written as a plain struct literal.

```go
r.Emit(monitord.Event{
	ID:       "down:" + target.Name,
	Title:    target.Name + " is unhealthy",
	Summary:  "HTTP 503 from health endpoint",
	Severity: monitord.SeverityWarn,
	URL:      target.URL,
	Image:    "https://example.com/chart.png",
	Fields: []monitord.Field{
		{Name: "Status", Value: "503", Inline: true},
	},
})
```

Field reference:

- `ID` is the event's identity: repeats of the same id are suppressed for one hour, so a target that stays down pings once, not every tick. An empty id always sends.
- `Fields` are labelled values — `Inline: true` for short comparable values, false for longer values such as URLs, IDs, snippets, or explanations. Field values accept Discord markdown and are truncated to Discord limits.
- `Image` renders a large image below the body; `Thumbnail` a small corner image.
- `Color` is an explicit accent as `0xRRGGBB`; leave it zero to derive the colour from `Severity` (info/warn/critical).
- `Author` adds an attribution line; `Footer`/`FooterIcon` override the default monitor-name footer.

Emit as many events as a tick finds — each is delivered immediately, concurrently, and on its own. A tick is capped so a runaway monitor can't flood a route: past the cap, the rest are dropped and logged. The default is 20; raise or lower it per monitor with `max_events` in `monitor.yaml`.

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

## Routes

```bash
monitord route list
monitord route create discord alerts --option url="$DISCORD_WEBHOOK_URL"
monitord route test discord:alerts
```

Routes are named `<kind>:<name>`, for example `discord:alerts`. The route name is a local label; the webhook URL decides where the message lands. Webhook URLs are redacted from output and logs.

Mentions can live on the route or be overridden per monitor in YAML:

```bash
monitord route create discord ops \
  --option url="$DISCORD_WEBHOOK_URL" \
  --option mentions=role:ROLE_ID
```

```yaml
routes:
  - route: discord:ops
    options:
      mentions: user:USER_ID
```

The Discord `mentions` option accepts `user:ID`, `role:ID`, `here`, `everyone`, comma-separated combinations, or `none`. Mentions are also an allowlist: scraped content containing `@everyone` is rendered inert unless that mention was explicitly allowed.

Prefer one route per destination webhook and per-monitor mention overrides for who gets pinged.

## OpenClaw Routes

Use an OpenClaw route when a monitor hit should become an agent task, such as reserving a table, drafting a response, or checking out a matching item.

```bash
monitord route create openclaw concierge \
  --option token="$OPENCLAW_HOOK_TOKEN" \
  --option agent-id=main
```

```yaml
every: 30s
ttl: 2h
routes:
  - route: discord:alerts
  - route: openclaw:concierge
    options:
      prompt: Reserve the table if the available slot matches the alert.
```

OpenClaw routes call `POST /hooks/agent` using `Authorization: Bearer <token>`. The monitor's `prompt` option is sent first, then monitord appends the notification title, summary, URL, details, fields, level, and monitor name as context.

Set route settings with repeatable `--option key=value` flags. OpenClaw supports `url`, `token`, `agent-id`, `session-key`, `wake-mode`, `deliver`, `channel`, `to`, `model`, `thinking`, and `timeout-seconds`. Only set `session-key` when OpenClaw hooks are configured to allow request session keys.

## Proxies

Skip proxies unless the target needs them.

```bash
monitord proxy import residential ./proxies.txt
monitord proxy list
monitord proxy show residential
```

Set `proxies: residential` in `monitor.yaml`.

Import accepts common proxy formats:

```text
host:port:user:pass
host:port
user:pass@host:port
scheme://user:pass@host:port
scheme://host:port
```

Set `clients: N` in `monitor.yaml` to request N clients. Each client gets its own proxy assignment and connection pool. `r.Client()` rotates through them; `r.Clients().At(i)` gives a stable client when one target should stick to one exit.

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

- Failure notifications are edge-triggered: one message when a monitor starts failing and one when it recovers. This comes from the result status alone.
- Events are delivered immediately and concurrently, so they may arrive out of emission order. An event with a dedupe key is suppressed for one hour after it sends; without a key it always sends.
- Redeploy rolls the worker to the new artifact on the next tick; in-flight ticks finish on the old artifact.

This means a monitor can run frequently without adding manual "only alert once" logic: give repeating events a stable dedupe key and let the daemon suppress them, and let failure edges handle themselves.

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

- `ttl` is required unless `persistent: true` is set.
- Slow checks need a larger `timeout` than the default 30s.
- `monitord test` runs direct and sends no notifications. Deployed behavior can differ when proxies are attached.
- Deploy fails when a route or proxy pool does not exist. Create routes and import proxy pools first.
- State schema changes require a version bump and migration, or an intentional `monitord state clear`.

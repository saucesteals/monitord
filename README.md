<div align="center">

<img src="infra/banner.png" alt="monitord" width="100%">

# monitord

**Stateful monitors. Clean alerts. One small daemon.**

[![Go](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev)
[![SQLite](https://img.shields.io/badge/State-SQLite-3f7f5f?style=flat)](https://sqlite.org/)
[![Platform](https://img.shields.io/badge/Platform-macOS%20%7C%20Linux-lightgrey)]()

**Monitoring** | **Typed State** | **Long-Lived Workers** | **Discord Alerts** | **OpenClaw Tasks** | **CLI Ops**

</div>

---

`monitord` is a local Go daemon for recurring checks that need memory. It handles scheduling, durable state, worker lifecycle, run history, proxy-backed clients, and notification delivery, while each monitor stays a small Go program focused on one question: did something meaningful happen?

Use it when cron is too stateless and a full job platform is too much.

## Highlights

- Monitoring: run site checks, API health checks, restock alerts, feed watchers, and other recurring "tell me when this changes" jobs.
- State: each monitor owns a typed Go state struct persisted in SQLite across ticks, restarts, and redeploys.
- Lifecycle: `test`, `deploy`, schedule, expire, inspect, and remove monitors through one CLI.
- Workers: deployed monitors run as long-lived worker processes, so HTTP clients, sessions, and caches can survive between ticks.
- Discord: send rich embeds for alerts, failure edges, recoveries, and deduped per-target events.
- OpenClaw: turn monitor hits into agent tasks with a per-monitor prompt and notification context.
- Proxies: import proxy pools once, then assign managed proxy clients at deploy time without putting credentials in source.

## Code

Monitors are ordinary Go programs. The daemon provides typed state and a managed HTTP client. A tick returns a health `Result` (`Success` or `Failure`) and emits notification `Event`s along the way.

```go
type State struct {
	InStock bool `json:"in_stock"`
}

func main() {
	monitord.Main(run)
}

func run(ctx context.Context, r *monitord.Run[State]) monitord.Result {
	res, _ := r.Client().Get("https://example.com/product")
	inStock := isInStock(res.Body)

	wasInStock := r.State.InStock
	r.State.InStock = inStock
	r.Save()

	if inStock && !wasInStock {
		r.Emit(monitord.Event{
			ID:      "instock",
			Title:   "back in stock",
			Message: "Everyday Hoodie, medium — $68",
			URL:     "https://example.com/product",
		})

		return monitord.Success("back in stock")
	}

	return monitord.Success("unchanged")
}
```

Everything except the runner lives beside it in `monitor.yaml`:

```yaml
description: Watches a product for restocks
clients: 1
every: 5m
ttl: 24h
timeout: 30s
failure_threshold: 3 # optional; alert after three consecutive failed ticks
deliveries:
  - discord:
      account: jarvis
      channel_id: "CHANNEL_ID"
      thread_id: "THREAD_ID" # optional
      mentions: user:USER_ID
```

Events render as Discord embeds. Add fields and an image when the notification should be useful without opening the listing:

```go
r.Emit(monitord.Event{
	ID:    "instock",
	Title: "back in stock",
	URL:   "https://example.com/product",
	Image: "https://example.com/product.jpg",
	Fields: []monitord.Field{
		{Name: "Product", Value: "Everyday Hoodie", Inline: true},
		{Name: "Size", Value: "Medium", Inline: true},
		{Name: "Price", Value: "$68", Inline: true},
		{Name: "Signal", Value: "page contains `In Stock`"},
	},
})
```

Use an event's optional `Message` for the compact notification preview. Direct
Discord deliveries put it in the top-level message content, with any configured
mention on the next line; the embed still carries the full event details. Keep
it brief—the combined message content and mention are limited to Discord's
2,000 characters.

## CLI

```bash
infra/install.sh

monitord init
monitord account set discord jarvis --token "$JARVIS_BOT_TOKEN"

monitord new restock-alert
$EDITOR ~/.monitord/monitors/restock-alert/{monitor.go,monitor.yaml}
monitord test restock-alert

monitord deploy restock-alert
monitord list
```

Useful commands:

```bash
monitord inspect restock-alert
monitord runs restock-alert
monitord runs restock-alert --failed
monitord runs restock-alert --run RUN_ID
monitord stats restock-alert
monitord state get restock-alert
monitord expire restock-alert
monitord rm restock-alert --purge
```

`--root PATH` may appear anywhere in CLI arguments and defaults to `$MONITORD_ROOT` or `~/.monitord`.

Run history is retained for seven days. Pruning happens incrementally as new
runs are recorded, while lifetime run and failure counts remain on the monitor.
Captured stdout and stderr are limited to 64 KiB per run; durable monitor state
is stored separately and is not duplicated in captured output.

## State

State is decoded by the monitor binary itself. Deploy validates stored state against the new struct before accepting a new artifact, so schema drift fails early instead of silently dropping data.

When the shape changes, version and migrate it:

```go
func (State) StateVersion() int { return 2 }

func (s *State) MigrateState(from int, raw json.RawMessage) error {
	// Decode the old shape and populate s.
	return nil
}
```

State commands:

```bash
monitord state get restock-alert
monitord state set restock-alert ./state.json
echo '{"in_stock":false}' | monitord state set restock-alert -
monitord state clear restock-alert
```

### Controlled simulation

To stop a monitor, edit its state, and restart it from that exact state, expire
it first. Expiry stops scheduling but retains both the source and state;
deploying the monitor reactivates it.

```bash
monitord expire restock-alert
monitord state get restock-alert > /tmp/restock-alert-state.json
$EDITOR /tmp/restock-alert-state.json
monitord state set restock-alert /tmp/restock-alert-state.json
monitord test restock-alert --stored-state # optional: preview without delivery
monitord deploy restock-alert
```

This is useful for a controlled simulation, such as removing a known item from
state so the next live tick naturally rediscovers it. Normal state edits do not
need expiry: every tick reloads state from the database.

## Notifications

`Failure` notifications require three consecutive failed ticks by default, then remain edge-triggered: one message when a monitor starts failing and one when it recovers. Set `failure_threshold: 1` for immediate alerting. Everything else is an `Event` — emitted with `r.Emit`, delivered immediately, one embed each. Every event needs a stable ID; repeats of the same ID are suppressed for one hour and kept in event history. `Emit` returns an error for invalid output, and an ignored error still fails the tick.

The delivery's YAML `mentions` are the default. An event can override them with
a typed allowlist: nil inherits the default, an empty `Mentions{}` suppresses
all pings, and a populated value replaces the default.

```go
r.Emit(monitord.Event{
	ID:       "listing:" + pair.ID,
	Title:    "new listing",
	Message:  "M3 MacBook Pro — $1,200",
	Mentions: &monitord.Mentions{Everyone: true},
})
```

Each monitor owns its destinations directly in `monitor.yaml`; there is no shared route registry. A Discord delivery takes exactly one of these forms:

```yaml
deliveries:
  - discord:
      account: jarvis
      channel_id: "CHANNEL_ID"
      thread_id: "THREAD_ID" # optional
      mentions: user:USER_ID

  - discord:
      webhook_url: "https://discord.com/api/webhooks/..."
      thread_id: "THREAD_ID" # optional
```

`account` plus `channel_id` is mutually exclusive with `webhook_url`. `thread_id` and `mentions` work with either form; monitord appends a webhook's `thread_id` query parameter at send time. Discord mentions accept `user:ID`, `role:ID`, `here`, `everyone`, comma-separated combinations, or `none`; they are sent through `allowed_mentions`, so scraped content cannot create surprise mass pings.

Account tokens are stored in macOS Keychain, never YAML or SQLite:

```bash
monitord account set discord jarvis --token "$DISCORD_BOT_TOKEN"
monitord account list
monitord account remove discord jarvis
```

Account-backed Discord and OpenClaw delivery currently require macOS; direct Discord webhook deliveries work on Linux too.

OpenClaw stays agentic and continues to use named agent routes, because it carries reusable hook credentials and delivery policy rather than a Discord destination:

```bash
monitord route create openclaw concierge \
  --option account=main \
  --option agent-id=main
```

Store that account once with `monitord account set openclaw main --token "$OPENCLAW_HOOK_TOKEN"`.

```yaml
routes:
  - route: openclaw:concierge
    options:
      prompt: Reserve the table if the monitor event matches the request.
```

`prompt` is required per monitor. The route defaults to `http://127.0.0.1:18789/hooks/agent`; configure a different gateway with `--option url=...`. It also accepts `agent-id`, `session-key`, `wake-mode`, `deliver`, `channel`, `to`, `model`, `thinking`, and `timeout-seconds`. `channel` and `to` require `deliver=true`.

## Install And Update

```bash
infra/install.sh
infra/install.sh --ref v1.2.3
infra/install.sh --no-restart
```

The installer is idempotent. It keeps a pinned clone under `~/.monitord/lib`, builds `bin/monitord`, symlinks it to `~/.local/bin/monitord`, initializes the root layout, retidies the monitors module, and installs a user background service when supported.

Supported service modes:

- macOS: launchd user LaunchAgent
- Linux: systemd user service
- Other environments: `MONITORD_SERVICE=none`

Useful overrides:

```bash
MONITORD_ROOT="$HOME/.monitord" infra/install.sh
MONITORD_REPO="https://github.com/OWNER/monitord.git" infra/install.sh
MONITORD_SERVICE=none infra/install.sh
```

Foreground mode:

```bash
monitord daemon --interval 5s --concurrency 8
```

## Proxies

Import a pool once, then name it in `monitor.yaml`:

```bash
monitord proxy import residential ./proxies.txt
monitord proxy list
```

Set `proxies: residential` and `clients: N`; monitord assigns that many proxy-backed clients to the worker. Proxy credentials live in monitord state, not monitor source or process arguments.

## AI Skill

`monitord skill` prints the embedded `SKILL.md` for agents that can operate monitors on your behalf:

```bash
mkdir -p /path/to/agent/skills/monitord
monitord skill > /path/to/agent/skills/monitord/SKILL.md
```

## Development

```bash
go test ./...
bash -n infra/install.sh
bash -n infra/run.sh
```

Main directories:

- `cmd/monitord`: CLI entrypoint
- `infra`: installer, service runner, and README assets
- `internal/daemon`: scheduler and worker supervision
- `internal/storage`: SQLite persistence
- `internal/routes`: notification delivery
- `internal/monitor`: scaffold, build, describe, and artifact lifecycle
- `internal/model`: shared names, statuses, and validation
- `examples/http-watch`: example monitor

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
			ID:    "instock",
			Title: "back in stock",
			URL:   "https://example.com/product",
		})
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
routes:
  - route: discord:alerts
    options:
      mentions: user:USER_ID
  - route: openclaw:concierge
    options:
      prompt: Reserve the item when it comes back in stock.
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

## CLI

```bash
infra/install.sh

monitord init
monitord route create discord alerts --option url="$DISCORD_WEBHOOK_URL"

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

## Notifications

`Failure` notifications are edge-triggered from the result status: one message when a monitor starts failing and one when it recovers. Everything else is an `Event` — emitted with `r.Emit`, delivered immediately, one embed each. Give a repeating event a dedupe key to suppress it for an hour; without a key it always sends.

Routes are local labels for notification backends:

```bash
monitord route create discord alerts --option url="$DISCORD_WEBHOOK_URL"
monitord route test discord:alerts
```

Assign routes and per-monitor options in `monitor.yaml`. A monitor may list any number of routes, including multiple routes of the same kind. Discord mentions accept `user:ID`, `role:ID`, `here`, `everyone`, comma-separated combinations, or `none`; they are sent through `allowed_mentions`, so scraped content cannot create surprise mass pings.

OpenClaw routes call the Gateway's `/hooks/agent` endpoint. The route stores hook settings; the monitor stores the prompt OpenClaw should follow when a notification fires:

```bash
monitord route create openclaw concierge \
  --option token="$OPENCLAW_HOOK_TOKEN" \
  --option agent-id=main \
  --option deliver=true \
  --option channel=discord \
  --option to=user:USER_ID
```

Then configure the monitor:

```yaml
every: 30s
ttl: 2h
routes:
  - route: discord:alerts
  - route: openclaw:concierge
    options:
      prompt: >-
        Reserve the table if the available slot matches the monitor alert.
```

OpenClaw defaults to `http://127.0.0.1:18789/hooks/agent`; override it with `--option url=...`. Other supported route keys are `session-key`, `wake-mode`, `model`, `thinking`, and `timeout-seconds`.

Route drivers own their accepted route and monitor option keys. Both sets are stored as JSON, so adding another delivery backend does not require another database column or CLI flag.

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
- `internal/routes`: notification routes
- `internal/monitor`: scaffold, build, describe, and artifact lifecycle
- `internal/model`: shared names, statuses, and validation
- `examples/http-watch`: example monitor

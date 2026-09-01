# Operations

## Installation layout

`infra/install.sh` creates a self-contained root at `~/.monitord` unless `MONITORD_ROOT` or `--root` overrides it:

```text
~/.monitord/
├── artifacts/   content-addressed monitor binaries
├── bin/         installed monitord executable
├── lib/         exact SDK/daemon checkout used by monitor builds
├── logs/        service stdout and stderr
├── monitors/    authored monitor source module
├── secrets/     group-scoped dotenv files
└── state/       SQLite database and daemon lock
```

The installer pins `lib`, builds the daemon, points the shared monitor module at that checkout, and installs a launchd or systemd user service. Useful forms:

```bash
./infra/install.sh --ref origin/main
./infra/install.sh --root ~/monitord-alt --service systemd
./infra/install.sh --no-restart
MONITORD_SERVICE=none ./infra/install.sh
```

The daemon normally reconciles every five seconds. Scheduling deadlines remain exact; `--interval` only bounds idle sleep.

## Clean V5 installation

V5 has one current database shape and no up/down migrations. To replace an older installation:

1. Stop or unload the existing service.
2. Move the entire old root to a timestamped backup.
3. Run the V5 installer into a clean root.
4. Port or recreate only the selected monitor source against V5, then restore its group secret files.
5. Recreate named routes and deploy each monitor.
6. Restore intentional state through `state set`, then inspect health.

Do not copy the old SQLite database, artifacts, generation data, or checkpoints. A format sentinel rejects nonempty incompatible databases rather than guessing how to transform them. On macOS, Keychain account tokens are outside the root and remain available to the same OS user.

## Accounts and destinations

On macOS, Discord and OpenClaw tokens are stored in Keychain, not the database:

```bash
monitord account set discord personal --token "$DISCORD_TOKEN"
monitord account set openclaw local --token "$OPENCLAW_TOKEN"
monitord account list
monitord account remove discord personal
```

The current named-account backend invokes the macOS `security` command. On Linux, direct Discord `webhook_url` destinations work without it; named Discord bot and OpenClaw accounts require a compatible credential backend before use.

Direct Discord destinations are declared in each `monitor.yaml`. Named agent routes are database-owned configuration:

```bash
monitord route create openclaw analyst \
  --option account=local \
  --option agent_id=main \
  --option timeout_seconds=60s
monitord route list
monitord route test openclaw:analyst
```

OpenClaw defaults to `http://127.0.0.1:18789/hooks/agent`. Supported route options are `url`, `account`, `agent_id`, `session_key`, `wake_mode`, `deliver`, `channel`, `to`, `model`, `thinking`, and `timeout_seconds`. A monitor referencing the route must provide its `prompt` option in `monitor.yaml`.

## Deployment lifecycle

```bash
monitord deploy inventory
monitord deploy ./path/to/source --name inventory
monitord pause inventory
monitord resume inventory --ttl 720h
monitord pause inventory
monitord archive inventory
monitord purge inventory
```

Deployment names identify durable instances. `Info.Name` identifies implementation behavior and may differ. Redeploying an existing name preserves its deployment ID and compatible state while activating a new artifact and generation.

Use `deploy --reset-state` only when the new Go state type intentionally cannot decode the existing JSON. It replaces state with the new monitor's defaults and fences the old worker. Ordinary manual edits use `state set` or `state clear`.

Pause makes a deployment inactive and stops its worker. Resume requires exactly one of a fresh `--ttl` or `--persistent`; use `--persistent` instead of `--ttl` when the deployment should not expire. Archive requires an inactive deployment. Purge requires an archived deployment and refuses while queued deliveries remain; `--force` explicitly discards those deliveries.

Committed outbox rows keep draining while a deployment is inactive or archived. Purge is the only lifecycle command that deletes deployment data.

## State operations

```bash
monitord state get inventory
monitord state set inventory state.json
generate-state | monitord state set inventory -
monitord state clear inventory
```

`set` validates JSON by running it through the deployed monitor's exact state type. `clear` asks that artifact for its default state; it does not write arbitrary `null`. Every successful edit fences the old generation so an in-flight callback cannot overwrite the operator change.

State has no public version or migration chain. Keep state transformations explicit and reviewable: export JSON, transform it deliberately, and feed it back through the CLI.

## Health and generations

`monitord list` gives the deployment state, health, consecutive failures, and expiration. `monitord inspect NAME` adds generation readiness, last run timing, redacted operational errors, exact secret availability, checkpoints, latest transaction, destinations, and outbox counts.

Health states:

- `starting`: active but no successful callback for the current operation yet.
- `healthy`: the latest callback succeeded, or a continuous worker remained ready for the stability window.
- `failing`: one or more consecutive failures below `health.failure_threshold`.
- `unhealthy`: the configured consecutive-failure threshold was reached.
- `stopped`: the deployment is inactive, archived, expired, or otherwise not running.

Polling callback errors are recorded and the same worker continues on its schedule. A panic, callback that remains alive after cancellation, failed lifecycle hook, continuous callback exit, or worker/protocol failure retires the generation and is restarted with bounded backoff. A continuous worker becomes healthy after remaining ready for one minute. Successful work resets the consecutive-failure count and emits a recovery notification after an unhealthy transition.

Unhealthy and recovery notifications use the deployment's durable destination bindings and mute mentions. Persisted notification payloads do not include raw operational error text; inspect the deployment or daemon log for the bounded, redacted error.

Generation fencing ensures that retired workers cannot commit state, checkpoints, or events. Daemon startup retires orphaned active generations before launching replacements.

## Events and delivery recovery

```bash
monitord events list inventory
monitord events list inventory --dead --limit 50
monitord events retry OUTBOX_ID DESTINATION_ID
```

Each event has independent delivery rows for its destinations. A successful destination is never retried because another destination failed. Transient failures use backoff; supported `Retry-After` responses override it within the configured ceiling. Permanent client errors become dead letters.

Delivery is at least once. If a destination accepts a request just before the daemon loses the success marker, it can receive a duplicate. Monitor event identity prevents duplicate outbox occurrences; it cannot provide external exactly-once delivery.

Per-destination rate limiting delays rows without charging a failed attempt. Pending and leased deliveries are retained regardless of age. Terminal events are pruned only after `events.retention` has elapsed and every destination is delivered or dead.

## Logs and service checks

The daemon writes structured operational logs to stdout and reserves stderr for process-level failures. The default macOS LaunchAgent sends them to:

```text
~/.monitord/logs/monitord.log
~/.monitord/logs/monitord.err.log
```

A nonempty macOS stderr log indicates a process-level failure rather than a copy of routine INFO output. The default Linux systemd service sends both streams to the user journal. Persisted health and delivery errors are bounded and secret values are redacted; avoid returning scraped credentials or complete authenticated URLs from monitor code.

Useful checks:

```bash
monitord version
monitord list
monitord inspect inventory
tail -f ~/.monitord/logs/monitord.log
```

On macOS, inspect the default service with `launchctl print gui/$(id -u)/dev.monitord.daemon`. On Linux, use `systemctl --user status dev.monitord.daemon.service` and `journalctl --user -u dev.monitord.daemon.service`.

## Backup and recovery

Stop the service before taking a whole-root backup. The simplest recoverable operation is moving the complete root aside and installing into a new directory. Keep the backup until the new daemon, all expected workers, secret availability, and first successful callbacks are verified.

For a V5-to-V5 recovery at the exact same code revision, preserve the whole root together so database rows, artifacts, source, and generation metadata agree. For an architectural upgrade or incompatible state redesign, prefer a clean root and explicit state restoration rather than copying selected database tables.

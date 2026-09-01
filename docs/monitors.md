# Monitor authoring

A monitor is a Go `main` package containing one `monitord.Monitor[S]` and a `monitor.yaml`. `S` is the deployment's current durable state type. The daemon, not the monitor process, owns persistence and delivery.

## Package shape

Keep `main`, metadata, plan, and the state contract in `monitor.go`. A simple monitor may need only that file. Split network adapters, parsers, or domain models when those boundaries make the package easier to understand; file count is not a design goal.

Deployments contain the compiled monitor, not loose files from its source directory. Embed static runtime assets with Go's `//go:embed` directive, as the [HTTP watch example](../examples/http-watch) does for `targets.json`. Keep operator-editable configuration in typed state or secrets instead of a sidecar file.

`monitord.Define` is the concise form:

```go
monitord.Run(monitord.Define(
	monitord.Info{Name: "inventory", Description: "Watches inventory"},
	monitord.Every(time.Minute, check, monitord.WithTimeout(20*time.Second)),
))
```

Use `Every` for non-overlapping polls and `Continuous` for one long-lived stream. A monitor has exactly one plan. Separate independent work into separate monitors rather than hiding multiple schedulers inside one process.

## State and checkpoints

State is user-meaningful monitor memory or configuration. Operators can inspect and replace it with `monitord state`. Prefer the simplest JSON shape that expresses that data; a mapping can remain a mapping instead of becoming an administrative wrapper struct.

```go
type State map[string][]string // selector -> subscribers
```

Checkpoints are daemon-owned source progress and observations: cursors, fetched baselines, canonical block journals, or transition sequences. They are visible in `inspect`, but operators do not edit them as monitor configuration.

```go
var previous Snapshot
found, err := session.Checkpoint("inventory", &previous)
if err != nil {
	return err
}

return session.Commit(ctx, func(tx *monitord.Tx[State]) error {
	if !found {
		return tx.Checkpoint("inventory", current)
	}
	// Update state, emit events, and advance the checkpoint atomically.
	return tx.Checkpoint("inventory", current)
})
```

State and checkpoint values are strictly decoded. Unknown fields, trailing JSON values, and incompatible Go types fail loudly. Change compatible state structs normally; for an intentional incompatible state reset, deploy with `--reset-state`, or prepare exact JSON and use `state set`. To discard incompatible source progress, pause the deployment and run `monitord checkpoints clear NAME --all`; review the source's initialization policy first because its next run may snapshot the current edge rather than replay history.

## Commit rules

Perform network I/O, sleeping, and expensive parsing before `Session.Commit`. The closure is the small deterministic decision that applies an already-observed result:

- Recheck cursors, ordering, and suppression predicates against `tx.State`.
- Do not perform external effects, start goroutines, or retain state references in the closure.
- Return an error to publish nothing.
- `Tx.Emit` and `Tx.Checkpoint` publish atomically with the next state.

Only one transaction is admitted at a time. If its ACK is lost, the SDK resends the exact serialized transaction and the daemon returns its ledgered ACK. Do not implement persistence retry by rerunning the closure.

External effects that cannot be expressed as monitord deliveries must happen after a successful commit. They are not made atomic by the framework.

## Event identity

`Event.ID` names one immutable source occurrence. Good IDs come from a source record, chain/log identity, document revision, or a state/checkpoint-backed transition sequence:

```go
tx.State.Changes++
return tx.Emit(monitord.Event{
	ID:       fmt.Sprintf("availability:%d", tx.State.Changes),
	Severity: monitord.SeverityInfo,
	Title:    "Availability changed",
})
```

Do not use the current time merely to manufacture uniqueness. Replaying the same ID and payload coalesces; reusing an ID for different content conflicts while that occurrence is retained. monitord assigns delivery timestamps when the event enters the outbox.

A correction is a new immutable event whose `CorrectionOf` names the original
event ID. Give the correction its own stable ID. Delivery adapters show the
relationship as a `corrects` field; they never rewrite or delete a notification
that may already have been seen.

The valid severities are `info`, `warn`, and `critical`. Keep scraped or untrusted text in event fields; delivery adapters constrain mentions so source text cannot create arbitrary pings.

## Exact secrets

Declare every required value on the plan and keep the `SecretRef` for access:

```go
var endpoint = monitord.RequiredSecret("orders", "websocket-url")

func (m *Monitor) Plan() monitord.Plan[State] {
	return monitord.Continuous(m.watch, monitord.WithSecrets(endpoint))
}

func (m *Monitor) watch(ctx context.Context, session *monitord.Session[State]) error {
	url, err := session.Secrets().Require(endpoint)
	// ...
}
```

Group and key components must be lower-case kebab-case. `OptionalSecret` allows absence; `Get(ref)` reports whether it resolved. Only declared values cross the generation-bound handshake.

Resolution order is monitor-local `.env`, `~/.monitord/secrets/<group>.env`, then global `~/.monitord/.env`. Normal installations use group files:

```dotenv
# ~/.monitord/secrets/orders.env
websocket-url="wss://example.invalid/private"
```

Secret files must be regular files owned by the daemon user and inaccessible to group or world users:

```bash
chmod 600 ~/.monitord/secrets/orders.env
```

The dotenv reader supports blank lines, comments, optional `export`, unquoted values, and single- or double-quoted values. It does not execute shell substitutions or expand variables. Do not place secrets in `monitor.yaml`, source, event bodies, or errors.

## Lifecycle-owned clients

When no managed catalog source owns a resource that needs secrets or should live for the worker generation, implement `Starter`. `Start` completes before readiness and before callbacks begin. `Stop` runs after active callbacks end. If `Start` fails, it must clean up anything it initialized.

```go
var proxies = monitord.RequiredSecret("proxies", "isps")

type Monitor struct {
	client *httpx.ProxyClient
}

func (m *Monitor) Info() monitord.Info {
	return monitord.Info{Name: "inventory", Description: "Watches inventory"}
}

func (m *Monitor) Plan() monitord.Plan[State] {
	return monitord.Every(time.Minute, m.check, monitord.WithSecrets(proxies))
}

func (m *Monitor) Start(ctx context.Context, env monitord.Environment) error {
	client, err := httpx.NewProxyClient(env.Secrets(), proxies)
	if err != nil {
		return err
	}
	m.client = client
	return nil
}

func (m *Monitor) Stop(context.Context) error {
	if m.client != nil {
		m.client.CloseIdleConnections()
	}
	return nil
}

func main() { monitord.Run(&Monitor{}) }
```

The proxy secret is a JSON array of URLs:

```dotenv
# ~/.monitord/secrets/proxies.env
isps='["http://user:pass@proxy-1.invalid:8080","socks5://proxy-2.invalid:1080"]'
```

`httpx.ProxyClient` creates one reusable browser-compatible client per proxy and selects the next client for each request. Redirects stay on the selected proxy. Use `httpx.NewClient` for the same browser-compatible transport without a proxy.

## Catalog monitors

QuickNode support has one provider layer and separate chain layers:

- `catalog/quicknode` owns exact provider endpoints, JSON-RPC transport, limiting, retrying HTTP reads, credential-safe errors, and reconnecting subscriptions. It has no chain types or finality model.
- `catalog/quicknode/evm` owns EVM identity, log filters, live subscriptions, and confirmed replay.
- `catalog/quicknode/solana` owns Solana cluster identity, commitments, RPC methods, filtered transaction subscriptions, and finalized replay.

Use an indexed EVM event filter narrow enough for the node to apply before it
sends anything. This example receives only standard ERC-20 transfers to one
wallet:

```go
var robinhoodHTTP = monitord.RequiredSecret("quicknode", "robinhood-mainnet-http-url")
var robinhoodWSS = monitord.RequiredSecret("quicknode", "robinhood-mainnet-websocket-url")

func main() {
	monitord.Run(evm.Events[State]{
		Filter:          evm.ERC20TransfersTo(wallet),
		ExpectedChainID: evm.ChainID("0x1237"),
		Confirmations:   3,
		HTTPSecret:      robinhoodHTTP,
		WSSSecret:       robinhoodWSS,
		Handle:          handleTransfer,
	})
}
```

`evm.Events` opens its filtered `eth_subscribe` log stream before loading the
cursor, so matching logs enter the handler as soon as the node publishes them.
It independently reconciles confirmed history with ranged `eth_getLogs` calls;
it never scans empty blocks one at a time. A reconnect can lose subscription
messages, but cannot create a permanent gap because HTTP replay advances the
durable cursor. `Confirmations` controls the replay boundary, not live latency.
Ethereum mainnet is the default endpoint and chain ID when no endpoints or
secret refs are supplied. Other EVM networks must name their exact HTTP and WSS
secrets and set `ExpectedChainID` and `Confirmations`. A fresh deployment starts
at the current confirmed edge unless `BackfillFrom` is set.

Filters should include every address and indexed topic known in advance. A
handler may perform a small deterministic read using the event's block hash,
then return an `EventUpdate`. EIP-1898 calls should set `requireCanonical` so a
fork change cannot silently substitute state from another block. Event IDs and
content must be stable: confirmed replay is inclusive and the durable outbox
coalesces an identical event ID, while different content for the same ID is an
error.

`Log.Confirmed` distinguishes canonical HTTP replay from the immediate WSS
observation. `Events` journals matching live logs until the confirmation
boundary. It passes an orphan back with `Log.Removed`, either from the live
subscription or after comparing the durable journal with canonical replay.
That journal makes corrections survive a disconnected WebSocket. A monitor
that alerts immediately should remember which live logs actually emitted and
return a correction only for those occurrences.

`solana.AddressEvents` uses QuickNode `transactionSubscribe` with vote and failed
transactions excluded and the monitored address applied at the provider. Each
notification already contains the complete transaction, so the live path does
not wait for `getTransaction`. `MatchLogs` narrows that pushed transaction by
program log before `Handle` runs. The default live commitment is `confirmed`.

Finalized QuickNode `getTransactionsForAddress` history remains the durable
source. It returns complete address-filtered transactions in ascending pages,
so startup and reconnect recovery move forward from the saved slot and
transaction index without one `getTransaction` call per signature. A fresh
deployment snapshots the newest finalized address transaction; `BackfillFrom`
may instead name an inclusive slot. Both `quicknode/solana-mainnet-http-url` and
`quicknode/solana-mainnet-websocket-url` are required by default; exact
`HTTPSecret` and `WSSSecret` refs select another cluster.

```go
func main() {
	monitord.Run(solana.AddressEvents[State]{
		Name:      "program-events",
		Address:   solana.PublicKey("..."),
		MatchLogs: relevantLogs,
		Handle:    prepareEvent,
	})
}

func prepareEvent(ctx context.Context, client *solana.Client, event solana.AddressEvent) (solana.AddressEventUpdate[State], error) {
	observed, err := readDetails(ctx, client, event)
	if err != nil {
		return nil, err
	}
	return func(tx *monitord.Tx[State]) error {
		return tx.Emit(observed.Event())
	}, nil
}
```

Use exact URLs copied from the QuickNode dashboard:

```dotenv
# ~/.monitord/secrets/quicknode.env
ethereum-mainnet-http-url="<exact Ethereum mainnet HTTP URL>"
ethereum-mainnet-websocket-url="<exact Ethereum mainnet WSS URL>"
robinhood-mainnet-http-url="<exact Robinhood Chain mainnet HTTP URL>"
robinhood-mainnet-websocket-url="<exact Robinhood Chain mainnet WSS URL>"
solana-mainnet-http-url="<exact Solana mainnet HTTP URL>"
solana-mainnet-websocket-url="<exact Solana mainnet WSS URL>"
```

The catalog never derives or rewrites QuickNode hosts or paths. Managed live
sources require both HTTP and WSS endpoints: WSS carries immediate matching
records and HTTP repairs gaps. Raw-monitor authors declare whichever exact refs
their client consumes. Open the appropriate chain client in `Start`:

```go
client, err := solana.Open(ctx, solana.Config{
	Endpoint:            quicknode.Endpoint{HTTPURL: httpURL, WSSURL: websocketURL},
	Commitment:          solana.Finalized,
	ExpectedGenesisHash: solana.MainnetGenesisHash,
})
```

Solana defaults to `finalized`; `GetBlock` and `GetTransaction` reject `processed`, explicitly support transaction version zero by default, and retain variable block and transaction bodies as `json.RawMessage`. The context passed to `Subscribe` bounds its opening handshake; close subscriptions before their client in `Stop`. Subscriptions reconnect with bounded attempts but do not replay missed notifications, so durable monitors need HTTP backfill, checkpoints, and stable event IDs.

Treat a durable cursor and authoritative replay as the default for chain monitors. Raw WebSocket-only monitoring is reserved for explicitly disposable signals or protocols with another guaranteed replay source; the monitor should document why losing a notification is acceptable.

## monitor.yaml

Choose exactly one lifetime mode:

```yaml
ttl: 30d
# or: persistent: true
```

`ttl` accepts positive Go durations and whole-day values such as `30d`. `persistent: true` has no expiry and cannot be combined with `ttl`.

The complete policy and direct Discord shape is:

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
      thread_id: "123456789012345679" # optional
      mentions: "role:123456789012345680" # user:ID, role:ID, here, everyone, or none
    rate_limit:
      per_second: 1
      burst: 5
```

A Discord destination uses either `account` plus `channel_id`, or `webhook_url`; those modes are mutually exclusive. `thread_id` is optional. Mention specs can be comma-separated. Both rate-limit fields are required together; omitting the block means unthrottled delivery.

Defaults are failure threshold `3`, maximum `256` events per transaction, and `30d` event retention. Set a lower event cap when a monitor should bound notification bursts. `max_per_transaction` must be between 1 and 256. Retention controls pruning only after every destination is terminal; pending and leased deliveries are not deleted because of age.

OpenClaw is another inline delivery. `account` selects its hook token, `prompt`
defines the task, and `agent_id` optionally selects a configured OpenClaw agent:

```yaml
deliveries:
  - openclaw:
      account: local
      agent_id: analyst # optional
      prompt: Summarize this occurrence and explain why it matters.
      # url: https://openclaw.example/hooks/agent # optional; defaults to the local gateway
    rate_limit:
      per_second: 0.2
      burst: 1
```

OpenClaw owns its session, model, thinking level, timeout, and outbound-channel
policy. Those are intentionally not monitor options. The monitor event is treated
as untrusted context beneath the authored prompt.

Unknown YAML fields and multiple YAML documents are rejected.

## Authoring loop

```bash
monitord test inventory
monitord test inventory --stored-state --duration 45s
monitord deploy inventory
monitord inspect inventory
```

Local tests do not persist state, checkpoints, or deliveries. They print state changes and emitted events. Polling monitors run one callback; continuous monitors run until `--duration` and then receive the normal graceful shutdown lifecycle. After deployment, use `inspect` to confirm the generation is ready, required secrets are available, and the first callback succeeds.

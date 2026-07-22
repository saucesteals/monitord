-- +goose Up
-- +goose StatementBegin

-- Timestamps are INTEGER unix-milliseconds throughout. Booleans are INTEGER 0/1.
-- JSON payloads are TEXT.

-- routes: a named notification backend (discord webhook, openclaw hook, ...).
-- Keyed by name; the id autoincrement is gone.
CREATE TABLE routes (
    name        TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    config      TEXT NOT NULL DEFAULT '{}',   -- driver options (json)
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
) STRICT;

-- proxy_pools: a named set of proxies plus its round-robin cursor
-- (proxy_offsets folded in as `offset`).
CREATE TABLE proxy_pools (
    name        TEXT PRIMARY KEY,
    strategy    TEXT NOT NULL,
    proxies     TEXT NOT NULL,                -- newline-joined proxy urls
    offset      INTEGER NOT NULL DEFAULT 0,   -- round-robin cursor
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
) STRICT;

-- monitors: one row per deployed monitor. Grouped by concern, one entity.
CREATE TABLE monitors (
    name            TEXT PRIMARY KEY,

    -- artifact
    source_dir      TEXT NOT NULL,
    artifact_dir    TEXT NOT NULL,
    binary_path     TEXT NOT NULL,
    definition      TEXT NOT NULL,            -- monitor Definition (json)
    status          TEXT NOT NULL,            -- active | expired

    -- config (from monitor.yaml)
    interval_ms     INTEGER NOT NULL,
    ttl_ms          INTEGER NOT NULL,
    timeout_ms      INTEGER NOT NULL,
    max_events      INTEGER NOT NULL DEFAULT 0,
    deliveries      TEXT NOT NULL DEFAULT '[]', -- route bindings (json)
    proxy_pool      TEXT REFERENCES proxy_pools(name) ON DELETE SET NULL, -- null = no pool

    -- durable state
    state           TEXT NOT NULL DEFAULT '{}',
    state_version   INTEGER NOT NULL DEFAULT 1,
    state_revision  INTEGER NOT NULL DEFAULT 0,

    -- schedule + lease
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    expires_at      INTEGER,
    next_due_at     INTEGER,
    last_run_at     INTEGER,
    expired_at      INTEGER,
    running_run_id  TEXT NOT NULL DEFAULT '',
    running_started_at INTEGER,
    running_expires_at INTEGER,

    -- health
    last_status          TEXT NOT NULL DEFAULT '',
    notified_status      TEXT NOT NULL DEFAULT '',
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    total_runs           INTEGER NOT NULL DEFAULT 0,
    total_failures       INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE INDEX monitors_due ON monitors(next_due_at) WHERE status = 'active';

-- runs: one row per tick, newest-first history.
CREATE TABLE runs (
    id            TEXT PRIMARY KEY,
    monitor_name  TEXT NOT NULL REFERENCES monitors(name) ON DELETE CASCADE,
    started_at    INTEGER NOT NULL,
    finished_at   INTEGER NOT NULL,
    status        TEXT NOT NULL,
    exit_code     INTEGER NOT NULL,
    stdout        TEXT NOT NULL DEFAULT '',
    stderr        TEXT NOT NULL DEFAULT '',
    error         TEXT NOT NULL DEFAULT '',
    notified      INTEGER NOT NULL DEFAULT 0,  -- bool
    notify_error  TEXT NOT NULL DEFAULT ''
) STRICT;

CREATE INDEX runs_monitor_started ON runs(monitor_name, started_at DESC);

-- events: the alert history AND the dedupe source. One row per identified event
-- (monitor_name, event_id), upserted on every send — so it's the set of distinct
-- events a monitor has emitted, each carrying its latest send. Dedupe is a
-- windowed check on that row; history is kept until a periodic prune reclaims it.
-- Only monitor-emitted events with an id land here (health pages have none).
CREATE TABLE events (
    monitor_name  TEXT NOT NULL REFERENCES monitors(name) ON DELETE CASCADE,
    event_id      TEXT NOT NULL,
    title         TEXT NOT NULL,
    summary       TEXT NOT NULL DEFAULT '',
    url           TEXT NOT NULL DEFAULT '',
    severity      TEXT NOT NULL DEFAULT 'info',
    sent_at       INTEGER NOT NULL,
    delivered     INTEGER NOT NULL DEFAULT 0,  -- bool
    error         TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (monitor_name, event_id)
) STRICT;

-- history/prune: by monitor over time, and global prune by age. The primary key
-- already serves dedupe lookups on (monitor_name, event_id).
CREATE INDEX events_monitor_sent ON events(monitor_name, sent_at DESC);
CREATE INDEX events_sent ON events(sent_at);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE events;
DROP TABLE runs;
DROP TABLE monitors;
DROP TABLE proxy_pools;
DROP TABLE routes;
-- +goose StatementEnd

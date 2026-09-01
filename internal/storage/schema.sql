-- Operator-owned delivery routes live independently of deployment runtime
-- state.
CREATE TABLE routes (
    name        TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    config      TEXT NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
) STRICT;

CREATE TABLE artifacts (
    id              TEXT PRIMARY KEY,
    content_hash    TEXT NOT NULL UNIQUE,
    path            TEXT NOT NULL,
    describe_json   BLOB NOT NULL CHECK(json_valid(describe_json)),
    created_at      INTEGER NOT NULL
) STRICT;

-- Deployment identity is deliberately independent of the implementation name.
CREATE TABLE deployments (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    info_name           TEXT NOT NULL,
    source_dir          TEXT NOT NULL,
    status              TEXT NOT NULL CHECK(status IN ('active', 'inactive', 'archived')),
    artifact_id         TEXT REFERENCES artifacts(id),
    config_revision     INTEGER NOT NULL DEFAULT 1 CHECK(config_revision > 0),
    config_hash         TEXT NOT NULL,
    failure_threshold   INTEGER NOT NULL CHECK(failure_threshold > 0),
    max_events_per_transaction INTEGER NOT NULL CHECK(max_events_per_transaction > 0 AND max_events_per_transaction <= 256),
    event_retention_ms  INTEGER NOT NULL CHECK(event_retention_ms > 0),
    active_generation   INTEGER NOT NULL DEFAULT 0 CHECK(active_generation >= 0),
    state               BLOB NOT NULL CHECK(json_valid(state)),
    state_revision      INTEGER NOT NULL DEFAULT 0 CHECK(state_revision >= 0),
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    expires_at          INTEGER,
    archived_at         INTEGER
) STRICT;

CREATE TABLE deployment_generations (
    deployment_id       TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    generation          INTEGER NOT NULL CHECK(generation > 0),
    worker_token_hash   BLOB NOT NULL,
    artifact_id         TEXT NOT NULL REFERENCES artifacts(id),
    config_revision     INTEGER NOT NULL CHECK(config_revision > 0),
    secret_fingerprint  BLOB NOT NULL,
    status              TEXT NOT NULL CHECK(status IN ('active', 'retired')),
    last_transaction_seq INTEGER NOT NULL DEFAULT 0 CHECK(last_transaction_seq >= 0),
    started_at          INTEGER NOT NULL,
    ready_at            INTEGER,
    stopped_at          INTEGER,
    stop_reason         TEXT NOT NULL DEFAULT '',
    stop_error          TEXT NOT NULL DEFAULT '',
    retired_at          INTEGER,
    PRIMARY KEY (deployment_id, generation)
) STRICT;

-- The current operational state is kept separately from immutable deployment
-- configuration and generation history.
CREATE TABLE deployment_health (
    deployment_id       TEXT PRIMARY KEY REFERENCES deployments(id) ON DELETE CASCADE,
    generation          INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
    status              TEXT NOT NULL CHECK(status IN ('starting', 'healthy', 'failing', 'unhealthy', 'stopped')),
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_failures >= 0),
    last_run_status     TEXT CHECK(last_run_status IS NULL OR last_run_status IN ('success', 'failure')),
    last_run_at         INTEGER,
    last_duration_ms    INTEGER CHECK(last_duration_ms IS NULL OR last_duration_ms >= 0),
    last_success_at     INTEGER,
    last_failure_at     INTEGER,
    last_error          TEXT NOT NULL DEFAULT '',
    unhealthy_notified INTEGER NOT NULL DEFAULT 0 CHECK(unhealthy_notified IN (0, 1)),
    updated_at          INTEGER NOT NULL
) STRICT;

CREATE TABLE checkpoints (
    deployment_id       TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    source              TEXT NOT NULL,
    value               BLOB NOT NULL,
    value_hash          BLOB NOT NULL,
    updated_generation  INTEGER NOT NULL,
    updated_seq         INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    PRIMARY KEY (deployment_id, source)
) STRICT;

CREATE TABLE transactions (
    deployment_id   TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    generation      INTEGER NOT NULL,
    seq             INTEGER NOT NULL,
    payload_hash    BLOB NOT NULL,
    base_revision   INTEGER NOT NULL,
    result_revision INTEGER NOT NULL,
    ack_payload     BLOB NOT NULL,
    committed_at    INTEGER NOT NULL,
    PRIMARY KEY (deployment_id, generation, seq),
    FOREIGN KEY (deployment_id, generation)
        REFERENCES deployment_generations(deployment_id, generation) ON DELETE CASCADE
) STRICT;

CREATE TABLE destination_bindings (
    id              TEXT NOT NULL,
    revision        INTEGER NOT NULL CHECK(revision > 0),
    deployment_id   TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    config          BLOB NOT NULL CHECK(json_valid(config)),
    config_hash     BLOB NOT NULL,
    created_at      INTEGER NOT NULL,
    retired_at      INTEGER,
    PRIMARY KEY (deployment_id, id, revision)
) STRICT;

CREATE TABLE outbox_events (
    outbox_id       TEXT PRIMARY KEY,
    deployment_id   TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    kind            TEXT NOT NULL CHECK(kind IN ('monitor', 'health')),
    generation      INTEGER NOT NULL,
    transaction_seq INTEGER,
    event_id        TEXT NOT NULL,
    payload         BLOB NOT NULL CHECK(json_valid(payload)),
    payload_hash    BLOB NOT NULL,
    created_at      INTEGER NOT NULL,
    CHECK ((kind='monitor' AND transaction_seq IS NOT NULL) OR (kind='health' AND transaction_seq IS NULL)),
    UNIQUE (deployment_id, kind, event_id),
    UNIQUE (outbox_id, deployment_id),
    FOREIGN KEY (deployment_id, generation, transaction_seq)
        REFERENCES transactions(deployment_id, generation, seq) DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE outbox_deliveries (
    outbox_id              TEXT NOT NULL,
    deployment_id          TEXT NOT NULL,
    destination_id         TEXT NOT NULL,
    destination_revision   INTEGER NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'sending', 'delivered', 'dead')),
    attempt_count          INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    next_attempt_at        INTEGER NOT NULL,
    lease_owner            TEXT,
    lease_expires_at       INTEGER,
    delivered_at           INTEGER,
    last_error             TEXT NOT NULL DEFAULT '',
    dead_at                INTEGER,
    PRIMARY KEY (outbox_id, destination_id),
    FOREIGN KEY (outbox_id, deployment_id)
        REFERENCES outbox_events(outbox_id, deployment_id) ON DELETE CASCADE,
    FOREIGN KEY (deployment_id, destination_id, destination_revision)
        REFERENCES destination_bindings(deployment_id, id, revision)
) STRICT;

CREATE INDEX outbox_deliveries_ready
    ON outbox_deliveries(status, next_attempt_at)
    WHERE status = 'pending';
CREATE INDEX transactions_committed
    ON transactions(deployment_id, generation, committed_at);
CREATE INDEX outbox_deliveries_lease
    ON outbox_deliveries(status, lease_expires_at)
    WHERE status = 'sending';
CREATE INDEX outbox_events_retention
    ON outbox_events(deployment_id, created_at);

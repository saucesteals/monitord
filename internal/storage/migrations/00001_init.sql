-- +goose Up
-- +goose StatementBegin

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
    status              TEXT NOT NULL CHECK(status IN ('active', 'expired', 'archived')),
    artifact_id         TEXT REFERENCES artifacts(id),
    config_revision     INTEGER NOT NULL DEFAULT 1 CHECK(config_revision > 0),
    config_hash         TEXT NOT NULL,
    active_generation   INTEGER NOT NULL DEFAULT 0 CHECK(active_generation >= 0),
    state               BLOB NOT NULL CHECK(json_valid(state)),
    state_version       INTEGER NOT NULL DEFAULT 1 CHECK(state_version > 0),
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
    retired_at          INTEGER,
    PRIMARY KEY (deployment_id, generation)
) STRICT;

CREATE TABLE checkpoints (
    deployment_id       TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    source              TEXT NOT NULL,
    value               BLOB NOT NULL,
    value_hash          BLOB NOT NULL,
    updated_generation  INTEGER NOT NULL,
    updated_seq         INTEGER NOT NULL,
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

CREATE TABLE dedupe_claims (
    deployment_id   TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    dedupe_key      TEXT NOT NULL,
    claimed_at      INTEGER NOT NULL,
    expires_at      INTEGER NOT NULL,
    event_id        TEXT NOT NULL,
    PRIMARY KEY (deployment_id, dedupe_key)
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
    generation      INTEGER NOT NULL,
    transaction_seq INTEGER NOT NULL,
    event_id        TEXT NOT NULL,
    payload         BLOB NOT NULL CHECK(json_valid(payload)),
    payload_hash    BLOB NOT NULL,
    created_at      INTEGER NOT NULL,
    UNIQUE (deployment_id, event_id),
    FOREIGN KEY (deployment_id, generation, transaction_seq)
        REFERENCES transactions(deployment_id, generation, seq) DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE outbox_deliveries (
    outbox_id              TEXT NOT NULL REFERENCES outbox_events(outbox_id) ON DELETE CASCADE,
    destination_id         TEXT NOT NULL,
    destination_revision   INTEGER NOT NULL,
    destination_deployment_id TEXT NOT NULL,
    rendered_payload       BLOB NOT NULL CHECK(json_valid(rendered_payload)),
    payload_hash           BLOB NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'sending', 'delivered', 'dead')),
    attempt_count          INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
    next_attempt_at        INTEGER NOT NULL,
    lease_owner            TEXT,
    lease_expires_at       INTEGER,
    delivered_at           INTEGER,
    last_error             TEXT NOT NULL DEFAULT '',
    dead_at                INTEGER,
    PRIMARY KEY (outbox_id, destination_id),
    FOREIGN KEY (destination_deployment_id, destination_id, destination_revision)
        REFERENCES destination_bindings(deployment_id, id, revision),
    CHECK(destination_deployment_id <> '')
) STRICT;

CREATE TABLE deployment_runs (
    id                  TEXT PRIMARY KEY,
    deployment_id       TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    generation          INTEGER NOT NULL,
    child_name          TEXT NOT NULL,
    kind                TEXT NOT NULL CHECK(kind IN ('poll', 'continuous')),
    status              TEXT NOT NULL CHECK(status IN ('running', 'success', 'failure', 'canceled')),
    started_at          INTEGER NOT NULL,
    finished_at         INTEGER,
    summary             TEXT NOT NULL DEFAULT '',
    error               TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (deployment_id, generation)
        REFERENCES deployment_generations(deployment_id, generation) ON DELETE CASCADE
) STRICT;

CREATE TABLE deployment_health (
    deployment_id       TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    child_name          TEXT NOT NULL,
    generation          INTEGER NOT NULL,
    status              TEXT NOT NULL CHECK(status IN ('healthy', 'degraded', 'failed', 'stopped')),
    summary             TEXT NOT NULL DEFAULT '',
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_failures >= 0),
    updated_at          INTEGER NOT NULL,
    PRIMARY KEY (deployment_id, child_name),
    FOREIGN KEY (deployment_id, generation)
        REFERENCES deployment_generations(deployment_id, generation) ON DELETE CASCADE
) STRICT;

CREATE INDEX outbox_deliveries_ready
    ON outbox_deliveries(status, next_attempt_at)
    WHERE status = 'pending';
CREATE INDEX transactions_committed
    ON transactions(deployment_id, generation, committed_at);
CREATE INDEX dedupe_claims_expiry
    ON dedupe_claims(expires_at);
CREATE INDEX deployment_runs_history
    ON deployment_runs(deployment_id, started_at DESC);
CREATE INDEX outbox_deliveries_lease
    ON outbox_deliveries(status, lease_expires_at)
    WHERE status = 'sending';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE outbox_deliveries;
DROP TABLE outbox_events;
DROP TABLE deployment_health;
DROP TABLE deployment_runs;
DROP TABLE destination_bindings;
DROP TABLE dedupe_claims;
DROP TABLE transactions;
DROP TABLE checkpoints;
DROP TABLE deployment_generations;
DROP TABLE deployments;
DROP TABLE artifacts;
DROP TABLE routes;
-- +goose StatementEnd

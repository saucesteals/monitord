-- +goose Up
-- +goose StatementBegin

-- V5 deployment identity is deliberately independent of the implementation name.
CREATE TABLE deployments (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    info_name           TEXT NOT NULL,
    source_dir          TEXT NOT NULL,
    status              TEXT NOT NULL,
    artifact_id         TEXT REFERENCES artifacts(id),
    config_revision     INTEGER NOT NULL DEFAULT 1,
    config_hash         TEXT NOT NULL,
    active_generation   INTEGER NOT NULL DEFAULT 0,
    state               BLOB NOT NULL,
    state_version       INTEGER NOT NULL DEFAULT 1,
    state_revision      INTEGER NOT NULL DEFAULT 0,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    expires_at          INTEGER,
    archived_at         INTEGER
) STRICT;

CREATE TABLE artifacts (
    id              TEXT PRIMARY KEY,
    content_hash    TEXT NOT NULL UNIQUE,
    path            TEXT NOT NULL,
    describe_json   BLOB NOT NULL,
    created_at      INTEGER NOT NULL
) STRICT;

CREATE TABLE deployment_generations (
    deployment_id       TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    generation          INTEGER NOT NULL,
    worker_token_hash   BLOB NOT NULL,
    artifact_id         TEXT NOT NULL REFERENCES artifacts(id),
    config_revision     INTEGER NOT NULL,
    secret_fingerprint  BLOB NOT NULL,
    status              TEXT NOT NULL,
    last_transaction_seq INTEGER NOT NULL DEFAULT 0,
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
    revision        INTEGER NOT NULL,
    deployment_id   TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    config          BLOB NOT NULL,
    config_hash     BLOB NOT NULL,
    created_at      INTEGER NOT NULL,
    retired_at      INTEGER,
    PRIMARY KEY (id, revision)
) STRICT;

CREATE TABLE outbox_events (
    outbox_id       TEXT PRIMARY KEY,
    deployment_id   TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    generation      INTEGER NOT NULL,
    transaction_seq INTEGER NOT NULL,
    event_id        TEXT NOT NULL,
    payload         BLOB NOT NULL,
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
    rendered_payload       BLOB NOT NULL,
    payload_hash           BLOB NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'pending',
    attempt_count          INTEGER NOT NULL DEFAULT 0,
    next_attempt_at        INTEGER NOT NULL,
    lease_owner            TEXT,
    lease_expires_at       INTEGER,
    delivered_at           INTEGER,
    last_error             TEXT NOT NULL DEFAULT '',
    dead_at                INTEGER,
    PRIMARY KEY (outbox_id, destination_id),
    FOREIGN KEY (destination_id, destination_revision)
        REFERENCES destination_bindings(id, revision)
) STRICT;

CREATE INDEX outbox_deliveries_ready
    ON outbox_deliveries(status, next_attempt_at)
    WHERE status = 'pending';
CREATE INDEX transactions_committed
    ON transactions(deployment_id, generation, committed_at);
CREATE INDEX dedupe_claims_expiry
    ON dedupe_claims(expires_at);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE outbox_deliveries;
DROP TABLE outbox_events;
DROP TABLE destination_bindings;
DROP TABLE dedupe_claims;
DROP TABLE transactions;
DROP TABLE checkpoints;
DROP TABLE deployment_generations;
DROP TABLE artifacts;
DROP TABLE deployments;
-- +goose StatementEnd

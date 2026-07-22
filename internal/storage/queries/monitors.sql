-- name: GetMonitor :one
SELECT * FROM monitors WHERE name = ?;

-- name: ListMonitors :many
SELECT * FROM monitors ORDER BY name;

-- name: DeleteMonitor :exec
DELETE FROM monitors WHERE name = ?;

-- name: EarliestDue :one
SELECT MIN(next_due_at) FROM monitors WHERE status = 'active' AND next_due_at IS NOT NULL;

-- name: MarkNotified :exec
UPDATE monitors SET notified_status = ? WHERE name = ?;

-- name: ExpireDueMonitors :execrows
UPDATE monitors
SET status = 'expired', expired_at = sqlc.arg(now), updated_at = sqlc.arg(now)
WHERE status = 'active' AND expires_at IS NOT NULL AND expires_at <= sqlc.arg(now);

-- name: ExpireMonitor :execrows
UPDATE monitors
SET status = 'expired', expired_at = sqlc.arg(now), updated_at = sqlc.arg(now)
WHERE name = sqlc.arg(name);

-- name: SetMonitorState :execrows
UPDATE monitors
SET state = sqlc.arg(state), state_version = sqlc.arg(state_version),
    state_revision = state_revision + 1, updated_at = sqlc.arg(updated_at)
WHERE name = sqlc.arg(name);

-- name: SelectDueMonitors :many
SELECT * FROM monitors
WHERE status = 'active'
    AND next_due_at IS NOT NULL
    AND next_due_at <= sqlc.arg(now)
    AND (expires_at IS NULL OR expires_at > sqlc.arg(now))
    AND (running_run_id = '' OR running_expires_at IS NULL OR running_expires_at <= sqlc.arg(now))
ORDER BY next_due_at, name
LIMIT sqlc.arg(lim);

-- name: ClaimMonitor :execrows
UPDATE monitors
SET running_run_id = sqlc.arg(run_id),
    running_started_at = sqlc.arg(started_at),
    running_expires_at = sqlc.arg(lease_expires),
    next_due_at = sqlc.arg(lease_expires),
    updated_at = sqlc.arg(now)
WHERE name = sqlc.arg(name)
    AND status = 'active'
    AND next_due_at IS NOT NULL
    AND next_due_at <= sqlc.arg(now)
    AND (expires_at IS NULL OR expires_at > sqlc.arg(now))
    AND (running_run_id = '' OR running_expires_at IS NULL OR running_expires_at <= sqlc.arg(now));

-- name: AdvanceMonitor :execrows
UPDATE monitors
SET last_run_at = sqlc.arg(finished_at),
    next_due_at = sqlc.arg(next_due),
    updated_at = sqlc.arg(finished_at),
    running_run_id = '', running_started_at = NULL, running_expires_at = NULL,
    last_status = sqlc.arg(status),
    consecutive_failures = CASE WHEN sqlc.arg(failed) = 1 THEN consecutive_failures + 1 ELSE 0 END,
    total_runs = total_runs + 1,
    total_failures = total_failures + sqlc.arg(failed)
WHERE name = sqlc.arg(name) AND status = 'active' AND running_run_id = sqlc.arg(run_id);

-- name: SaveMonitorState :execrows
UPDATE monitors
SET state = sqlc.arg(state), state_revision = state_revision + 1
WHERE name = sqlc.arg(name) AND state_revision = sqlc.arg(revision);

-- name: UpsertMonitor :exec
INSERT INTO monitors (
    name, source_dir, artifact_dir, binary_path, definition, status,
    interval_ms, ttl_ms, timeout_ms, max_events, deliveries, proxy_pool,
    state, state_version, state_revision,
    created_at, updated_at, expires_at, next_due_at, last_run_at, expired_at
) VALUES (
    sqlc.arg(name), sqlc.arg(source_dir), sqlc.arg(artifact_dir), sqlc.arg(binary_path), sqlc.arg(definition), sqlc.arg(status),
    sqlc.arg(interval_ms), sqlc.arg(ttl_ms), sqlc.arg(timeout_ms), sqlc.arg(max_events), sqlc.arg(deliveries), sqlc.arg(proxy_pool),
    sqlc.arg(state), sqlc.arg(state_version), sqlc.arg(state_revision),
    sqlc.arg(created_at), sqlc.arg(updated_at), sqlc.arg(expires_at), sqlc.arg(next_due_at), sqlc.arg(last_run_at), sqlc.arg(expired_at)
)
ON CONFLICT(name) DO UPDATE SET
    source_dir = excluded.source_dir,
    artifact_dir = excluded.artifact_dir,
    binary_path = excluded.binary_path,
    definition = excluded.definition,
    status = excluded.status,
    interval_ms = excluded.interval_ms,
    ttl_ms = excluded.ttl_ms,
    timeout_ms = excluded.timeout_ms,
    max_events = excluded.max_events,
    deliveries = excluded.deliveries,
    proxy_pool = excluded.proxy_pool,
    state = excluded.state,
    state_version = excluded.state_version,
    state_revision = monitors.state_revision + 1,
    updated_at = excluded.updated_at,
    expires_at = excluded.expires_at,
    next_due_at = excluded.next_due_at,
    expired_at = excluded.expired_at,
    running_run_id = '',
    running_started_at = NULL,
    running_expires_at = NULL;

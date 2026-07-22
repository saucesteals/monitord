-- name: EventSuppressed :one
-- Dedupe: was this id delivered within the window?
SELECT EXISTS (
    SELECT 1 FROM events
    WHERE monitor_name = ? AND event_id = ? AND delivered = 1 AND sent_at > ?
);

-- name: UpsertEvent :exec
-- One row per (monitor, event_id): each send updates the row's latest state.
INSERT INTO events (monitor_name, event_id, title, summary, url, severity, sent_at, delivered, error)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(monitor_name, event_id) DO UPDATE SET
    title = excluded.title,
    summary = excluded.summary,
    url = excluded.url,
    severity = excluded.severity,
    sent_at = excluded.sent_at,
    delivered = excluded.delivered,
    error = excluded.error;

-- name: ListEvents :many
SELECT * FROM events
WHERE monitor_name = sqlc.arg(monitor_name)
    AND (sqlc.arg(only_failed) = 0 OR delivered = 0)
    AND (sqlc.arg(since) = 0 OR sent_at >= sqlc.arg(since))
ORDER BY sent_at DESC
LIMIT sqlc.arg(lim);

-- name: PruneEvents :execrows
DELETE FROM events WHERE sent_at < ?;

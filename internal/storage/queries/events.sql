-- name: EventSuppressed :one
-- Dedupe: is there a delivered event with this id in the window?
SELECT EXISTS (
    SELECT 1 FROM events
    WHERE monitor_name = ? AND event_id = ? AND delivered = 1 AND sent_at > ?
);

-- name: InsertEvent :one
INSERT INTO events (monitor_name, event_id, title, summary, url, severity, sent_at, delivered, error)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;

-- name: ListEvents :many
SELECT * FROM events
WHERE monitor_name = ?
    AND (sqlc.arg(only_failed) = 0 OR delivered = 0)
    AND (sqlc.arg(since) = 0 OR sent_at >= sqlc.arg(since))
ORDER BY sent_at DESC
LIMIT ?;

-- name: PruneEvents :execrows
DELETE FROM events WHERE sent_at < ?;

-- name: InsertRun :exec
INSERT INTO runs (
    id, monitor_name, started_at, finished_at, status, exit_code,
    stdout, stderr, error, notified, notify_error
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: GetRun :one
SELECT * FROM runs WHERE id = ?;

-- name: ListRuns :many
SELECT * FROM runs
WHERE monitor_name = sqlc.arg(monitor_name)
    AND (sqlc.arg(only_failed) = 0 OR status = 'failure')
ORDER BY started_at DESC
LIMIT sqlc.arg(lim);

-- name: UpdateRunNotification :execrows
UPDATE runs SET notified = ?, notify_error = ? WHERE id = ?;

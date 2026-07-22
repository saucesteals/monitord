-- name: GetRoute :one
SELECT * FROM routes WHERE name = ?;

-- name: ListRoutes :many
SELECT * FROM routes ORDER BY name;

-- name: UpsertRoute :exec
INSERT INTO routes (name, kind, config, created_at, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    kind = excluded.kind,
    config = excluded.config,
    updated_at = excluded.updated_at;

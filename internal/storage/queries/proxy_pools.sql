-- name: GetProxyPool :one
SELECT * FROM proxy_pools WHERE name = ?;

-- name: ListProxyPools :many
SELECT * FROM proxy_pools ORDER BY name;

-- name: UpsertProxyPool :exec
INSERT INTO proxy_pools (name, strategy, proxies, offset, created_at, updated_at)
VALUES (?, ?, ?, 0, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    strategy = excluded.strategy,
    proxies = excluded.proxies,
    updated_at = excluded.updated_at;

-- name: DeleteProxyPool :exec
DELETE FROM proxy_pools WHERE name = ?;

-- name: CountMonitorsUsingPool :one
SELECT count(*) FROM monitors WHERE proxy_pool = ?;

-- name: GetProxyOffset :one
SELECT offset FROM proxy_pools WHERE name = ?;

-- name: SetProxyOffset :exec
UPDATE proxy_pools SET offset = ?, updated_at = ? WHERE name = ?;

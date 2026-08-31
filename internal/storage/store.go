// Package storage owns SQLite persistence for monitord.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/network"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage/db"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrStateConflict = errors.New("state revision conflict")

type Store struct {
	db *sql.DB
	q  *db.Queries
}
type Route struct {
	Name                 model.RouteName
	Kind                 model.RouteKind
	Options              routes.Options
	CreatedAt, UpdatedAt time.Time
}
type ProxyPool struct {
	Name                 model.PoolName
	Strategy             network.Strategy
	Proxies              []string
	CreatedAt, UpdatedAt time.Time
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	conn.SetMaxOpenConns(1)
	for _, pragma := range []string{`PRAGMA busy_timeout = 5000`, `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = FULL`, `PRAGMA foreign_keys = ON`} {
		if _, err = conn.ExecContext(context.Background(), pragma); err != nil {
			conn.Close()
			return nil, fmt.Errorf("pragma %q: %w", pragma, err)
		}
	}
	if err = runMigrations(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return &Store{db: conn, q: db.New(conn)}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) UpsertRoute(ctx context.Context, route Route) error {
	if err := route.Name.Validate(); err != nil {
		return err
	}
	prepared, err := routes.PrepareRoute(route.Kind, route.Options)
	if err != nil {
		return err
	}
	raw, err := encodeOptions(prepared)
	if err != nil {
		return err
	}
	now := toMs(time.Now().UTC())
	return s.q.UpsertRoute(ctx, db.UpsertRouteParams{Name: route.Name.String(), Kind: route.Kind.String(), Config: raw, CreatedAt: now, UpdatedAt: now})
}
func (s *Store) ListRoutes(ctx context.Context) ([]Route, error) {
	rows, err := s.q.ListRoutes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Route, 0, len(rows))
	for _, row := range rows {
		r, e := toRoute(row)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, nil
}
func (s *Store) GetRoute(ctx context.Context, name model.RouteName) (Route, error) {
	if err := name.Validate(); err != nil {
		return Route{}, err
	}
	row, err := s.q.GetRoute(ctx, name.String())
	if errors.Is(err, sql.ErrNoRows) {
		return Route{}, fmt.Errorf("route %s not found", name)
	}
	if err != nil {
		return Route{}, err
	}
	return toRoute(row)
}
func (s *Store) UpsertProxyPool(ctx context.Context, pool ProxyPool) error {
	if err := pool.Name.Validate(); err != nil {
		return err
	}
	if err := pool.Strategy.Validate(); err != nil {
		return err
	}
	if len(pool.Proxies) == 0 {
		return fmt.Errorf("pool %s has no proxies", pool.Name)
	}
	now := toMs(time.Now().UTC())
	return s.q.UpsertProxyPool(ctx, db.UpsertProxyPoolParams{Name: pool.Name.String(), Strategy: pool.Strategy.String(), Proxies: strings.Join(pool.Proxies, "\n"), CreatedAt: now, UpdatedAt: now})
}
func (s *Store) GetProxyPool(ctx context.Context, name model.PoolName) (ProxyPool, error) {
	if err := name.Validate(); err != nil {
		return ProxyPool{}, err
	}
	row, err := s.q.GetProxyPool(ctx, name.String())
	if errors.Is(err, sql.ErrNoRows) {
		return ProxyPool{}, fmt.Errorf("proxy pool %s not found", name)
	}
	if err != nil {
		return ProxyPool{}, err
	}
	return toProxyPool(row)
}
func (s *Store) ListProxyPools(ctx context.Context) ([]ProxyPool, error) {
	rows, err := s.q.ListProxyPools(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ProxyPool, 0, len(rows))
	for _, row := range rows {
		p, e := toProxyPool(row)
		if e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, nil
}
func (s *Store) DeleteProxyPool(ctx context.Context, name model.PoolName) error {
	if err := name.Validate(); err != nil {
		return err
	}
	return s.q.DeleteProxyPool(ctx, name.String())
}
func (s *Store) TakeProxyOffset(ctx context.Context, pool model.PoolName, advance func(int64) int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	q := s.q.WithTx(tx)
	current, err := q.GetProxyOffset(ctx, pool.String())
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if err = q.SetProxyOffset(ctx, db.SetProxyOffsetParams{Offset: advance(current), UpdatedAt: toMs(time.Now().UTC()), Name: pool.String()}); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return current, nil
}
func toRoute(r db.Route) (Route, error) {
	name, err := model.ParseRouteName(r.Name)
	if err != nil {
		return Route{}, err
	}
	kind, err := model.ParseRouteKind(r.Kind)
	if err != nil {
		return Route{}, err
	}
	opts, err := decodeOptions(r.Config)
	if err != nil {
		return Route{}, err
	}
	opts, err = routes.PrepareRoute(kind, opts)
	if err != nil {
		return Route{}, err
	}
	return Route{Name: name, Kind: kind, Options: opts, CreatedAt: fromMs(r.CreatedAt), UpdatedAt: fromMs(r.UpdatedAt)}, nil
}
func toProxyPool(p db.ProxyPool) (ProxyPool, error) {
	name, err := model.ParsePoolName(p.Name)
	if err != nil {
		return ProxyPool{}, err
	}
	return ProxyPool{Name: name, Strategy: network.Strategy(p.Strategy), Proxies: strings.Split(p.Proxies, "\n"), CreatedAt: fromMs(p.CreatedAt), UpdatedAt: fromMs(p.UpdatedAt)}, nil
}
func toMs(t time.Time) int64    { return t.UTC().UnixMilli() }
func fromMs(ms int64) time.Time { return time.UnixMilli(ms).UTC() }
func nullMs(t *time.Time) sql.NullInt64 {
	if t == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: toMs(*t), Valid: true}
}
func msPtr(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	t := fromMs(n.Int64)
	return &t
}
func nullPool(p model.PoolName) sql.NullString {
	if p == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: p.String(), Valid: true}
}
func encodeOptions(o routes.Options) (string, error) {
	if o == nil {
		o = routes.Options{}
	}
	raw, err := json.Marshal(o)
	return string(raw), err
}
func decodeOptions(raw string) (routes.Options, error) {
	if strings.TrimSpace(raw) == "" {
		raw = "{}"
	}
	var o routes.Options
	err := json.Unmarshal([]byte(raw), &o)
	return o, err
}

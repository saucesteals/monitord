// Package storage owns SQLite persistence for monitord.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/saucesteals/monitord/internal/model"
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
func toMs(t time.Time) int64    { return t.UTC().UnixMilli() }
func fromMs(ms int64) time.Time { return time.UnixMilli(ms).UTC() }
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

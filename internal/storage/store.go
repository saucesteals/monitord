// Package storage owns SQLite persistence for monitord.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"
)

var ErrStateConflict = errors.New("state revision conflict")

const maxStoredErrorBytes = 16 << 10

type Store struct {
	db *sql.DB
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
	if err = initializeSchema(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return &Store{db: conn}, nil
}
func (s *Store) Close() error   { return s.db.Close() }
func toMs(t time.Time) int64    { return t.UTC().UnixMilli() }
func fromMs(ms int64) time.Time { return time.UnixMilli(ms).UTC() }
func boundedText(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

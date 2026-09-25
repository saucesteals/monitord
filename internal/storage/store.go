// Package storage owns SQLite persistence for monitord.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"
)

var ErrStateConflict = errors.New("state revision conflict")

const maxStoredErrorBytes = 16 << 10

type Store struct {
	db                 *sql.DB
	checkpointDB       *sql.DB
	checkpointCancel   context.CancelFunc
	checkpointDone     chan struct{}
	checkpointPressure atomic.Bool
}

// Open opens a standalone store with SQLite's automatic checkpoint policy.
func Open(path string) (*Store, error) {
	return openStore(path, nil)
}

// OpenWithCheckpointer owns a background PASSIVE checkpointer until Close.
// The operational pool remains serialized, with automatic checkpoints disabled.
func OpenWithCheckpointer(path string, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	return openStore(path, logger)
}

func openStore(path string, logger *slog.Logger) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	auto := 1000
	if logger != nil {
		auto = 0
	}
	db, err := openSQLite(path, auto)
	if err != nil {
		return nil, err
	}
	if err := initializeSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if logger != nil {
		checkpointDB, err := openSQLite(path, 1000)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		// Verify status support before returning a store without automatic checkpoints.
		initial, err := readWAL(context.Background(), checkpointDB, false)
		if err != nil {
			_ = checkpointDB.Close()
			_ = db.Close()
			return nil, fmt.Errorf("initialize checkpointer: %w", err)
		}
		var pageSize int64
		if err := checkpointDB.QueryRow("PRAGMA main.page_size").Scan(&pageSize); err != nil {
			_ = checkpointDB.Close()
			_ = db.Close()
			return nil, fmt.Errorf("read page size: %w", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.checkpointPressure.Store(initial.Busy != 0 || initial.backlog()*pageSize >= checkpointWarningBytes)
		s.checkpointDB = checkpointDB
		s.checkpointCancel = cancel
		s.checkpointDone = make(chan struct{})
		go func() {
			defer close(s.checkpointDone)
			s.runCheckpointer(ctx, checkpointDB, logger, pageSize)
		}()
	}
	return s, nil
}

func openSQLite(path string, auto int) (*sql.DB, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve sqlite path: %w", err)
	}
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	q := u.Query()
	// Driver DSN pragmas run for every physical connection, including replacements.
	for _, pragma := range []string{"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)", "foreign_keys(ON)", fmt.Sprintf("wal_autocheckpoint(%d)", auto)} {
		q.Add("_pragma", pragma)
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize sqlite: %w", err)
	}
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("read journal mode: %w", err)
	}
	if mode != "wal" {
		_ = db.Close()
		return nil, fmt.Errorf("expected WAL journal mode, got %q", mode)
	}

	return db, nil
}

// Close joins the checkpointer before closing either pool. No forced checkpoint
// is needed for durability: acknowledged FULL commits are already in the WAL.
func (s *Store) Close() error {
	if s.checkpointCancel != nil {
		s.checkpointCancel()
		<-s.checkpointDone
	}
	err := s.db.Close()
	if s.checkpointDB != nil {
		err = errors.Join(err, s.checkpointDB.Close())
	}
	return err
}
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

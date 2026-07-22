// Package storage owns SQLite persistence for monitord.
package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	monitor "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/network"
	"github.com/saucesteals/monitord/internal/routes"

	_ "modernc.org/sqlite"
)

// ErrStateConflict is returned when stored state moved on since a tick read it.
var ErrStateConflict = errors.New("state revision conflict")

// Store owns SQLite access for monitord.
type Store struct {
	db *sql.DB
}

// Route is a named notification sink.
type Route struct {
	ID        int64
	Name      model.RouteName
	Kind      model.RouteKind
	Options   routes.Options
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ProxyPool is a named set of proxies monitord owns and hands to workers.
type ProxyPool struct {
	Name      model.PoolName
	Strategy  network.Strategy
	Proxies   []string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Monitor is a deployed monitor artifact, schedule, and durable state.
type Monitor struct {
	Name        model.MonitorName
	SourceDir   string
	ArtifactDir string
	BinaryPath  string
	Definition  monitor.Definition

	// State is the monitor's durable cross-tick state as JSON.
	State json.RawMessage
	// StateVersion is the schema version State was written with.
	StateVersion int
	// StateRevision increments on every state write, for conflict detection.
	StateRevision int64

	IntervalSeconds int64
	TTLSeconds      int64
	TimeoutSeconds  int64
	// MaxEvents caps events delivered per tick. Zero means the daemon default.
	MaxEvents  int64
	Deliveries []routes.Delivery
	ProxyPool  model.PoolName
	Status     model.MonitorStatus

	CreatedAt *time.Time
	UpdatedAt *time.Time
	ExpiresAt *time.Time
	NextDueAt *time.Time
	LastRunAt *time.Time
	ExpiredAt *time.Time

	RunningRunID     string
	RunningStartedAt *time.Time
	RunningExpiresAt *time.Time

	// LastStatus is the outcome of the most recent completed run.
	LastStatus monitor.ResultStatus
	// NotifiedStatus is the status all routes were last told about. Notifications
	// are edge-triggered off this so a broken monitor does not page on every
	// tick.
	NotifiedStatus monitor.ResultStatus
	// ConsecutiveFailures counts unbroken failures up to the latest run.
	ConsecutiveFailures int64
	// TotalRuns and TotalFailures are lifetime counters for health reporting.
	TotalRuns     int64
	TotalFailures int64
}

// Run records one monitor tick.
type Run struct {
	ID                string
	MonitorName       model.MonitorName
	StartedAt         time.Time
	FinishedAt        time.Time
	Status            monitor.ResultStatus
	ExitCode          int
	Stdout            string
	Stderr            string
	Error             string
	NotificationSent  bool
	NotificationError string

	// State, when set, is persisted atomically with the run if StateRevision
	// still matches the stored revision.
	State         json.RawMessage
	StateRevision int64
}

const monitorColumns = `name, source_dir, artifact_dir, binary_path, definition_json, state_json, state_version, state_revision,
	interval_seconds, ttl_seconds, timeout_seconds, max_events, deliveries_json, proxy_pool, status,
	created_at, updated_at, expires_at, next_due_at, last_run_at, expired_at,
	running_run_id, running_started_at, running_expires_at,
	last_status, notified_status, consecutive_failures, total_runs, total_failures`

const routeColumns = `id, name, kind, config_json, created_at, updated_at`

// Open opens a SQLite store and runs migrations.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), `PRAGMA busy_timeout = 5000`); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("set sqlite busy timeout: %w", err)
	}

	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()

		return nil, err
	}

	return store, nil
}

// Close closes the SQLite connection.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE IF NOT EXISTS routes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL,
			config_json TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS monitors (
			name TEXT PRIMARY KEY,
			source_dir TEXT NOT NULL,
			artifact_dir TEXT NOT NULL,
			binary_path TEXT NOT NULL,
			definition_json TEXT NOT NULL,
			state_json TEXT NOT NULL DEFAULT '{}',
			state_version INTEGER NOT NULL DEFAULT 1,
			state_revision INTEGER NOT NULL DEFAULT 0,
			interval_seconds INTEGER NOT NULL,
			ttl_seconds INTEGER NOT NULL,
			timeout_seconds INTEGER NOT NULL,
			max_events INTEGER NOT NULL DEFAULT 0,
			deliveries_json TEXT NOT NULL DEFAULT '[]',
			proxy_pool TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			expires_at TEXT,
			next_due_at TEXT,
			last_run_at TEXT,
			expired_at TEXT,
			running_run_id TEXT NOT NULL DEFAULT '',
			running_started_at TEXT,
			running_expires_at TEXT,
			last_status TEXT NOT NULL DEFAULT '',
			notified_status TEXT NOT NULL DEFAULT '',
			consecutive_failures INTEGER NOT NULL DEFAULT 0,
			total_runs INTEGER NOT NULL DEFAULT 0,
			total_failures INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			monitor_name TEXT NOT NULL,
			started_at TEXT NOT NULL,
			finished_at TEXT NOT NULL,
			status TEXT NOT NULL,
			exit_code INTEGER NOT NULL,
			stdout TEXT NOT NULL,
			stderr TEXT NOT NULL,
			error TEXT NOT NULL,
			notification_sent INTEGER NOT NULL,
			notification_error TEXT NOT NULL DEFAULT '',
			FOREIGN KEY (monitor_name) REFERENCES monitors(name)
		)`,
		`CREATE TABLE IF NOT EXISTS event_dedupe (
			monitor_name TEXT NOT NULL,
			dedupe_key TEXT NOT NULL,
			last_sent_at TEXT NOT NULL,
			PRIMARY KEY (monitor_name, dedupe_key)
		)`,
		`CREATE TABLE IF NOT EXISTS proxy_pools (
			name TEXT PRIMARY KEY,
			strategy TEXT NOT NULL,
			proxies TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proxy_offsets (
			pool TEXT PRIMARY KEY,
			offset_value INTEGER NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS runs_monitor_started ON runs(monitor_name, started_at DESC)`,
	}

	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}

	columns := []struct {
		table string
		name  string
		spec  string
	}{
		{table: "monitors", name: "state_json", spec: "TEXT NOT NULL DEFAULT '{}'"},
		{table: "monitors", name: "state_version", spec: "INTEGER NOT NULL DEFAULT 1"},
		{table: "monitors", name: "state_revision", spec: "INTEGER NOT NULL DEFAULT 0"},
		{table: "monitors", name: "running_run_id", spec: "TEXT NOT NULL DEFAULT ''"},
		{table: "monitors", name: "running_started_at", spec: "TEXT"},
		{table: "monitors", name: "running_expires_at", spec: "TEXT"},
		{table: "monitors", name: "last_status", spec: "TEXT NOT NULL DEFAULT ''"},
		{table: "monitors", name: "notified_status", spec: "TEXT NOT NULL DEFAULT ''"},
		{table: "monitors", name: "consecutive_failures", spec: "INTEGER NOT NULL DEFAULT 0"},
		{table: "monitors", name: "total_runs", spec: "INTEGER NOT NULL DEFAULT 0"},
		{table: "monitors", name: "total_failures", spec: "INTEGER NOT NULL DEFAULT 0"},
		{table: "monitors", name: "proxy_pool", spec: "TEXT NOT NULL DEFAULT ''"},
		{table: "monitors", name: "max_events", spec: "INTEGER NOT NULL DEFAULT 0"},
		{table: "runs", name: "notification_error", spec: "TEXT NOT NULL DEFAULT ''"},
	}
	for _, column := range columns {
		if err := s.ensureColumn(ctx, column.table, column.name, column.spec); err != nil {
			return err
		}
	}

	return s.retireNetworkProfiles(ctx)
}

// retireNetworkProfiles carries monitors off the old env-backed proxy profiles.
//
// The rename to proxy pools added a column rather than moving the data, so a
// monitor that named a profile would have silently lost it. This backfills
// first, then drops the dead column and offset table.
func (s *Store) retireNetworkProfiles(ctx context.Context) error {
	has, err := s.hasColumn(ctx, "monitors", "network_profile")
	if err != nil {
		return err
	}
	if has {
		if _, err := s.db.ExecContext(ctx, `UPDATE monitors
			SET proxy_pool = network_profile
			WHERE proxy_pool = '' AND network_profile != ''`); err != nil {
			return fmt.Errorf("backfill proxy_pool: %w", err)
		}
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE monitors DROP COLUMN network_profile`); err != nil {
			return fmt.Errorf("drop network_profile column: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `DROP TABLE IF EXISTS network_offsets`); err != nil {
		return fmt.Errorf("drop network_offsets table: %w", err)
	}

	return nil
}

// hasColumn reports whether a table has the named column.
func (s *Store) hasColumn(ctx context.Context, table string, name string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return false, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var cid, notNull, pk int
		var columnName, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return false, fmt.Errorf("scan %s column: %w", table, err)
		}
		if columnName == name {
			return true, nil
		}
	}

	return false, rows.Err()
}

func (s *Store) ensureColumn(ctx context.Context, table string, name string, spec string) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var cid, notNull, pk int
		var columnName, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("scan %s column: %w", table, err)
		}
		if columnName == name {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %s columns: %w", table, err)
	}

	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, name, spec)); err != nil {
		return fmt.Errorf("add %s.%s column: %w", table, name, err)
	}

	return nil
}

// UpsertRoute creates or updates a notification route.
func (s *Store) UpsertRoute(ctx context.Context, route Route) error {
	if err := route.Name.Validate(); err != nil {
		return err
	}
	if err := route.Kind.Validate(); err != nil {
		return err
	}
	prepared, err := routes.PrepareRoute(route.Kind, route.Options)
	if err != nil {
		return err
	}
	route.Options = prepared
	configJSON, err := encodeOptions(route.Options)
	if err != nil {
		return err
	}
	now := formatTime(time.Now().UTC())
	_, err = s.db.ExecContext(ctx, `INSERT INTO routes (name, kind, config_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			kind = excluded.kind,
			config_json = excluded.config_json,
			updated_at = excluded.updated_at`,
		route.Name.String(), route.Kind.String(), configJSON, now, now)
	if err != nil {
		return fmt.Errorf("upsert route %s: %w", route.Name, err)
	}

	return nil
}

// ListRoutes returns all notification routes.
func (s *Store) ListRoutes(ctx context.Context) ([]Route, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+routeColumns+` FROM routes ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list routes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var routes []Route
	for rows.Next() {
		route, err := scanRoute(rows)
		if err != nil {
			return nil, err
		}
		routes = append(routes, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate routes: %w", err)
	}

	return routes, nil
}

// GetRoute loads a route by name.
func (s *Store) GetRoute(ctx context.Context, name model.RouteName) (Route, error) {
	if err := name.Validate(); err != nil {
		return Route{}, err
	}

	row := s.db.QueryRowContext(ctx, `SELECT `+routeColumns+` FROM routes WHERE name = ?`, name.String())
	route, err := scanRoute(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Route{}, fmt.Errorf("route %s not found", name)
	}
	if err != nil {
		return Route{}, err
	}

	return route, nil
}

// UpsertMonitor creates or updates a monitor. Existing state is preserved
// unless the caller supplies a new one.
func (s *Store) UpsertMonitor(ctx context.Context, m Monitor) error {
	if err := m.Name.Validate(); err != nil {
		return err
	}
	if len(m.Deliveries) == 0 {
		return errors.New("monitor requires at least one delivery")
	}
	for _, delivery := range m.Deliveries {
		if err := delivery.Route.Validate(); err != nil {
			return err
		}
	}
	if err := m.Definition.Validate(); err != nil {
		return err
	}

	now := time.Now().UTC()
	if m.CreatedAt == nil {
		m.CreatedAt = &now
	}
	m.UpdatedAt = &now
	if m.Status == "" {
		m.Status = model.MonitorStatusActive
	}
	if err := m.Status.Validate(); err != nil {
		return err
	}
	if m.NextDueAt == nil {
		m.NextDueAt = &now
	}
	if len(m.State) == 0 {
		m.State = json.RawMessage("{}")
	}
	if m.StateVersion <= 0 {
		m.StateVersion = 1
	}

	definitionJSON, err := json.Marshal(m.Definition)
	if err != nil {
		return fmt.Errorf("marshal monitor definition: %w", err)
	}
	deliveriesJSON, err := json.Marshal(m.Deliveries)
	if err != nil {
		return fmt.Errorf("marshal monitor deliveries: %w", err)
	}

	// A redeploy resets the run claim so a worker from the previous artifact
	// cannot report against the new one.
	_, err = s.db.ExecContext(ctx, `INSERT INTO monitors (`+monitorColumns+`)
		VALUES (
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?, ?, ?, ?, ?,
			?,
			'', NULL, NULL, '', '', 0, 0, 0
		)
		ON CONFLICT(name) DO UPDATE SET
			source_dir = excluded.source_dir,
			artifact_dir = excluded.artifact_dir,
			binary_path = excluded.binary_path,
			definition_json = excluded.definition_json,
			state_json = excluded.state_json,
			state_version = excluded.state_version,
			state_revision = monitors.state_revision + 1,
			interval_seconds = excluded.interval_seconds,
			ttl_seconds = excluded.ttl_seconds,
			timeout_seconds = excluded.timeout_seconds,
			max_events = excluded.max_events,
			deliveries_json = excluded.deliveries_json,
			proxy_pool = excluded.proxy_pool,
			status = excluded.status,
			updated_at = excluded.updated_at,
			expires_at = excluded.expires_at,
			next_due_at = excluded.next_due_at,
			expired_at = excluded.expired_at,
			running_run_id = '',
			running_started_at = NULL,
			running_expires_at = NULL`,
		m.Name.String(), m.SourceDir, m.ArtifactDir, m.BinaryPath, string(definitionJSON), string(m.State), m.StateVersion, m.StateRevision,
		m.IntervalSeconds, m.TTLSeconds, m.TimeoutSeconds, m.MaxEvents, string(deliveriesJSON), m.ProxyPool.String(), m.Status.String(),
		formatTimePtr(m.CreatedAt), formatTimePtr(m.UpdatedAt), formatTimePtr(m.ExpiresAt), formatTimePtr(m.NextDueAt),
		formatTimePtr(m.LastRunAt), formatTimePtr(m.ExpiredAt))
	if err != nil {
		return fmt.Errorf("upsert monitor %s: %w", m.Name, err)
	}

	return nil
}

// ListMonitors returns all monitors.
func (s *Store) ListMonitors(ctx context.Context) ([]Monitor, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+monitorColumns+` FROM monitors ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list monitors: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var monitors []Monitor
	for rows.Next() {
		m, err := scanMonitor(rows)
		if err != nil {
			return nil, err
		}
		monitors = append(monitors, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate monitors: %w", err)
	}

	return monitors, nil
}

// GetMonitor loads a monitor by name.
func (s *Store) GetMonitor(ctx context.Context, name model.MonitorName) (Monitor, error) {
	if err := name.Validate(); err != nil {
		return Monitor{}, err
	}

	row := s.db.QueryRowContext(ctx, `SELECT `+monitorColumns+` FROM monitors WHERE name = ?`, name.String())
	m, err := scanMonitor(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Monitor{}, fmt.Errorf("monitor %s not found", name)
	}
	if err != nil {
		return Monitor{}, err
	}

	return m, nil
}

// SetMonitorState replaces a monitor's state from outside a run, bumping the
// revision so an in-flight tick cannot silently clobber the edit.
func (s *Store) SetMonitorState(ctx context.Context, name model.MonitorName, state json.RawMessage, version int) error {
	if err := name.Validate(); err != nil {
		return err
	}
	if !json.Valid(state) {
		return errors.New("state must be valid JSON")
	}
	if version <= 0 {
		version = 1
	}

	result, err := s.db.ExecContext(ctx, `UPDATE monitors
		SET state_json = ?, state_version = ?, state_revision = state_revision + 1, updated_at = ?
		WHERE name = ?`,
		string(state), version, formatTime(time.Now().UTC()), name.String())
	if err != nil {
		return fmt.Errorf("set state for %s: %w", name, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set state rows for %s: %w", name, err)
	}
	if count == 0 {
		return fmt.Errorf("monitor %s not found", name)
	}

	return nil
}

// ClaimDueMonitors durably leases active due monitors for execution.
func (s *Store) ClaimDueMonitors(ctx context.Context, now time.Time, limit int) ([]Monitor, error) {
	if limit <= 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin claim due monitors: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `SELECT `+monitorColumns+` FROM monitors
		WHERE status = ?
			AND next_due_at IS NOT NULL
			AND next_due_at <= ?
			AND (expires_at IS NULL OR expires_at > ?)
			AND (running_run_id = '' OR running_expires_at IS NULL OR running_expires_at <= ?)
		ORDER BY next_due_at, name
		LIMIT ?`,
		model.MonitorStatusActive.String(), formatTime(now), formatTime(now), formatTime(now), limit)
	if err != nil {
		return nil, fmt.Errorf("select due monitors: %w", err)
	}

	var monitors []Monitor
	for rows.Next() {
		m, scanErr := scanMonitor(rows)
		if scanErr != nil {
			_ = rows.Close()

			return nil, scanErr
		}
		monitors = append(monitors, m)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()

		return nil, fmt.Errorf("iterate due monitors: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close due monitor rows: %w", err)
	}

	for i := range monitors {
		leaseExpires := now.Add(leaseDuration(monitors[i].TimeoutSeconds))
		runID, err := newRunID(monitors[i].Name, now)
		if err != nil {
			return nil, err
		}

		// Push next_due_at out to the lease expiry so a crashed daemon cannot
		// re-run the monitor until the lease lapses.
		result, err := tx.ExecContext(ctx, `UPDATE monitors
			SET running_run_id = ?, running_started_at = ?, running_expires_at = ?, next_due_at = ?, updated_at = ?
			WHERE name = ?
				AND status = ?
				AND next_due_at IS NOT NULL
				AND next_due_at <= ?
				AND (expires_at IS NULL OR expires_at > ?)
				AND (running_run_id = '' OR running_expires_at IS NULL OR running_expires_at <= ?)`,
			runID, formatTime(now), formatTime(leaseExpires), formatTime(leaseExpires), formatTime(now),
			monitors[i].Name.String(), model.MonitorStatusActive.String(), formatTime(now), formatTime(now), formatTime(now))
		if err != nil {
			return nil, fmt.Errorf("claim monitor %s: %w", monitors[i].Name, err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("claim monitor %s rows: %w", monitors[i].Name, err)
		}
		if count != 1 {
			return nil, fmt.Errorf("claim monitor %s affected %d rows", monitors[i].Name, count)
		}

		monitors[i].RunningRunID = runID
		monitors[i].RunningStartedAt = &now
		monitors[i].RunningExpiresAt = &leaseExpires
		monitors[i].NextDueAt = &leaseExpires
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim due monitors: %w", err)
	}

	return monitors, nil
}

// EarliestDue returns the soonest scheduled run among monitors eligible to be
// claimed, so the scheduler can sleep exactly that long instead of polling on a
// fixed cadence.
func (s *Store) EarliestDue(ctx context.Context) (time.Time, bool, error) {
	var due sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT MIN(next_due_at) FROM monitors
		WHERE status = ? AND next_due_at IS NOT NULL`, model.MonitorStatusActive.String()).Scan(&due)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, fmt.Errorf("earliest due: %w", err)
	}
	if !due.Valid {
		return time.Time{}, false, nil
	}

	at, err := parseTime(due.String)
	if err != nil {
		return time.Time{}, false, err
	}

	return at, true, nil
}

// ExpireDueMonitors marks active monitors expired after their TTL.
func (s *Store) ExpireDueMonitors(ctx context.Context, now time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE monitors
		SET status = ?, expired_at = ?, updated_at = ?
		WHERE status = ? AND expires_at IS NOT NULL AND expires_at <= ?`,
		model.MonitorStatusExpired.String(), formatTime(now), formatTime(now), model.MonitorStatusActive.String(), formatTime(now))
	if err != nil {
		return 0, fmt.Errorf("expire due monitors: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("expire due monitors rows: %w", err)
	}

	return count, nil
}

// ExpireMonitor manually expires a monitor.
func (s *Store) ExpireMonitor(ctx context.Context, name model.MonitorName, now time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE monitors
		SET status = ?, expired_at = ?, updated_at = ?
		WHERE name = ?`,
		model.MonitorStatusExpired.String(), formatTime(now), formatTime(now), name.String())
	if err != nil {
		return fmt.Errorf("expire monitor %s: %w", name, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("expire monitor rows: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("monitor %s not found", name)
	}

	return nil
}

// RecordRun stores a run, applies any state the tick produced, and advances the
// schedule. State is written only when the tick's revision still matches, so an
// external edit during the tick wins and is reported as ErrStateConflict.
func (s *Store) RecordRun(ctx context.Context, run Run, nextDue time.Time) error {
	if err := run.MonitorName.Validate(); err != nil {
		return err
	}
	if err := run.Status.Validate(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin record run: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `INSERT INTO runs (
			id, monitor_name, started_at, finished_at, status, exit_code, stdout, stderr, error, notification_sent, notification_error
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.MonitorName.String(), formatTime(run.StartedAt), formatTime(run.FinishedAt), run.Status.String(), run.ExitCode,
		run.Stdout, run.Stderr, run.Error, boolInt(run.NotificationSent), run.NotificationError); err != nil {
		return fmt.Errorf("insert run %s: %w", run.ID, err)
	}

	failed := 0
	if run.Status == monitor.StatusFailure {
		failed = 1
	}
	result, err := tx.ExecContext(ctx, `UPDATE monitors
		SET last_run_at = ?, next_due_at = ?, updated_at = ?,
			running_run_id = '', running_started_at = NULL, running_expires_at = NULL,
			last_status = ?,
			consecutive_failures = CASE WHEN ? = 1 THEN consecutive_failures + 1 ELSE 0 END,
			total_runs = total_runs + 1,
			total_failures = total_failures + ?
		WHERE name = ? AND status = ? AND running_run_id = ?`,
		formatTime(run.FinishedAt), formatTime(nextDue), formatTime(run.FinishedAt),
		run.Status.String(), failed, failed,
		run.MonitorName.String(), model.MonitorStatusActive.String(), run.ID)
	if err != nil {
		return fmt.Errorf("advance monitor %s: %w", run.MonitorName, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("advance monitor %s rows: %w", run.MonitorName, err)
	}
	if count == 0 {
		return fmt.Errorf("monitor %s claim %s is no longer active", run.MonitorName, run.ID)
	}

	conflict := false
	if len(run.State) > 0 {
		stateResult, err := tx.ExecContext(ctx, `UPDATE monitors
			SET state_json = ?, state_revision = state_revision + 1
			WHERE name = ? AND state_revision = ?`,
			string(run.State), run.MonitorName.String(), run.StateRevision)
		if err != nil {
			return fmt.Errorf("save state for %s: %w", run.MonitorName, err)
		}
		stateCount, err := stateResult.RowsAffected()
		if err != nil {
			return fmt.Errorf("save state rows for %s: %w", run.MonitorName, err)
		}
		conflict = stateCount == 0
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit record run: %w", err)
	}
	if conflict {
		return fmt.Errorf("monitor %s run %s: %w", run.MonitorName, run.ID, ErrStateConflict)
	}

	return nil
}

// UpdateRunNotification records best-effort notification delivery state.
func (s *Store) UpdateRunNotification(ctx context.Context, runID string, sent bool, notificationError string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE runs SET notification_sent = ?, notification_error = ? WHERE id = ?`,
		boolInt(sent), notificationError, runID)
	if err != nil {
		return fmt.Errorf("update notification state for run %s: %w", runID, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update notification state rows for run %s: %w", runID, err)
	}
	if count == 0 {
		return fmt.Errorf("run %s not found", runID)
	}

	return nil
}

// MarkNotified records the status a monitor's routes were last told about, so
// notifications can be edge-triggered rather than sent on every failing tick.
func (s *Store) MarkNotified(ctx context.Context, name model.MonitorName, status monitor.ResultStatus) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE monitors SET notified_status = ? WHERE name = ?`,
		status.String(), name.String()); err != nil {
		return fmt.Errorf("mark notified for %s: %w", name, err)
	}

	return nil
}

// ReserveEvent atomically checks and claims a dedupe id. It reports whether the
// event may send and returns the prior last_sent_at ("" when the id was unseen),
// so a delivery that then fails can ReleaseEvent the claim and allow a retry
// rather than burning the whole dedupe window on a send that never landed. An
// empty key always sends and reserves nothing.
func (s *Store) ReserveEvent(ctx context.Context, name model.MonitorName, key string, now time.Time, window time.Duration) (bool, string, error) {
	if key == "" {
		return true, "", nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", fmt.Errorf("begin event dedupe: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var prior string
	err = tx.QueryRowContext(ctx, `SELECT last_sent_at FROM event_dedupe WHERE monitor_name = ? AND dedupe_key = ?`,
		name.String(), key).Scan(&prior)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, "", fmt.Errorf("read event dedupe: %w", err)
	}
	if err == nil {
		sentAt, parseErr := parseTime(prior)
		if parseErr == nil && now.Sub(sentAt) < window {
			return false, "", nil
		}
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO event_dedupe (monitor_name, dedupe_key, last_sent_at)
		VALUES (?, ?, ?)
		ON CONFLICT(monitor_name, dedupe_key) DO UPDATE SET last_sent_at = excluded.last_sent_at`,
		name.String(), key, formatTime(now)); err != nil {
		return false, "", fmt.Errorf("write event dedupe: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, "", fmt.Errorf("commit event dedupe: %w", err)
	}

	return true, prior, nil
}

// ReleaseEvent undoes a ReserveEvent claim after a failed delivery: it restores
// the prior last_sent_at, or removes the row entirely when the id was unseen, so
// the id is free to retry on the next tick instead of being suppressed for the
// full window by a send that never landed.
func (s *Store) ReleaseEvent(ctx context.Context, name model.MonitorName, key, prior string) error {
	if key == "" {
		return nil
	}
	if prior == "" {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM event_dedupe WHERE monitor_name = ? AND dedupe_key = ?`,
			name.String(), key); err != nil {
			return fmt.Errorf("release event dedupe: %w", err)
		}

		return nil
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE event_dedupe SET last_sent_at = ? WHERE monitor_name = ? AND dedupe_key = ?`,
		prior, name.String(), key); err != nil {
		return fmt.Errorf("restore event dedupe: %w", err)
	}

	return nil
}

// ListRuns returns a monitor's most recent runs, newest first.
func (s *Store) ListRuns(ctx context.Context, name model.MonitorName, limit int) ([]Run, error) {
	if limit <= 0 {
		limit = 20
	}

	rows, err := s.db.QueryContext(ctx, `SELECT id, monitor_name, started_at, finished_at, status, exit_code,
		stdout, stderr, error, notification_sent, notification_error
		FROM runs WHERE monitor_name = ? ORDER BY started_at DESC LIMIT ?`, name.String(), limit)
	if err != nil {
		return nil, fmt.Errorf("list runs for %s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()

	var runs []Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runs: %w", err)
	}

	return runs, nil
}

// GetRun loads one run by ID.
func (s *Store) GetRun(ctx context.Context, runID string) (Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, monitor_name, started_at, finished_at, status, exit_code,
		stdout, stderr, error, notification_sent, notification_error FROM runs WHERE id = ?`, runID)

	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("run %s not found", runID)
	}
	if err != nil {
		return Run{}, err
	}

	return run, nil
}

// DeleteMonitor removes a monitor and everything recorded about it.
func (s *Store) DeleteMonitor(ctx context.Context, name model.MonitorName) error {
	if err := name.Validate(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete monitor: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Runs reference monitors, so they go first.
	for _, stmt := range []string{
		`DELETE FROM runs WHERE monitor_name = ?`,
		`DELETE FROM event_dedupe WHERE monitor_name = ?`,
	} {
		if _, err := tx.ExecContext(ctx, stmt, name.String()); err != nil {
			return fmt.Errorf("delete monitor %s: %w", name, err)
		}
	}

	result, err := tx.ExecContext(ctx, `DELETE FROM monitors WHERE name = ?`, name.String())
	if err != nil {
		return fmt.Errorf("delete monitor %s: %w", name, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete monitor rows: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("monitor %s not found", name)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete monitor: %w", err)
	}

	return nil
}

func scanRun(row scanner) (Run, error) {
	var r Run
	var monitorName, status, startedAt, finishedAt string
	var notificationSent int

	if err := row.Scan(&r.ID, &monitorName, &startedAt, &finishedAt, &status, &r.ExitCode,
		&r.Stdout, &r.Stderr, &r.Error, &notificationSent, &r.NotificationError); err != nil {
		return Run{}, err
	}

	var err error
	if r.MonitorName, err = model.ParseMonitorName(monitorName); err != nil {
		return Run{}, err
	}
	if r.StartedAt, err = parseTime(startedAt); err != nil {
		return Run{}, err
	}
	if r.FinishedAt, err = parseTime(finishedAt); err != nil {
		return Run{}, err
	}
	r.Status = monitor.ResultStatus(status)
	r.NotificationSent = notificationSent != 0

	return r, nil
}

// UpsertProxyPool stores a pool, replacing its proxies.
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

	now := formatTime(time.Now().UTC())
	if _, err := s.db.ExecContext(ctx, `INSERT INTO proxy_pools (name, strategy, proxies, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			strategy = excluded.strategy,
			proxies = excluded.proxies,
			updated_at = excluded.updated_at`,
		pool.Name.String(), pool.Strategy.String(), strings.Join(pool.Proxies, "\n"), now, now); err != nil {
		return fmt.Errorf("upsert proxy pool %s: %w", pool.Name, err)
	}

	return nil
}

// GetProxyPool loads a pool by name.
func (s *Store) GetProxyPool(ctx context.Context, name model.PoolName) (ProxyPool, error) {
	if err := name.Validate(); err != nil {
		return ProxyPool{}, err
	}

	var pool ProxyPool
	var strategy, proxies, createdAt, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT strategy, proxies, created_at, updated_at FROM proxy_pools WHERE name = ?`,
		name.String()).Scan(&strategy, &proxies, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ProxyPool{}, fmt.Errorf("proxy pool %s not found", name)
	}
	if err != nil {
		return ProxyPool{}, fmt.Errorf("get proxy pool %s: %w", name, err)
	}

	pool.Name = name
	pool.Strategy = network.Strategy(strategy)
	pool.Proxies = strings.Split(proxies, "\n")
	if pool.CreatedAt, err = parseTime(createdAt); err != nil {
		return ProxyPool{}, err
	}
	if pool.UpdatedAt, err = parseTime(updatedAt); err != nil {
		return ProxyPool{}, err
	}

	return pool, nil
}

// ListProxyPools returns every pool, newest name order.
func (s *Store) ListProxyPools(ctx context.Context) ([]ProxyPool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, strategy, proxies, created_at, updated_at FROM proxy_pools ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list proxy pools: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var pools []ProxyPool
	for rows.Next() {
		var pool ProxyPool
		var name, strategy, proxies, createdAt, updatedAt string
		if err := rows.Scan(&name, &strategy, &proxies, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan proxy pool: %w", err)
		}
		if pool.Name, err = model.ParsePoolName(name); err != nil {
			return nil, err
		}
		pool.Strategy = network.Strategy(strategy)
		pool.Proxies = strings.Split(proxies, "\n")
		if pool.CreatedAt, err = parseTime(createdAt); err != nil {
			return nil, err
		}
		if pool.UpdatedAt, err = parseTime(updatedAt); err != nil {
			return nil, err
		}
		pools = append(pools, pool)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate proxy pools: %w", err)
	}

	return pools, nil
}

// DeleteProxyPool removes a pool and its round-robin offset.
func (s *Store) DeleteProxyPool(ctx context.Context, name model.PoolName) error {
	if err := name.Validate(); err != nil {
		return err
	}

	var used int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM monitors WHERE proxy_pool = ?`, name.String()).Scan(&used); err != nil {
		return fmt.Errorf("check pool usage: %w", err)
	}
	if used > 0 {
		return fmt.Errorf("proxy pool %s is used by %d monitor(s)", name, used)
	}

	result, err := s.db.ExecContext(ctx, `DELETE FROM proxy_pools WHERE name = ?`, name.String())
	if err != nil {
		return fmt.Errorf("delete proxy pool %s: %w", name, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete proxy pool rows: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("proxy pool %s not found", name)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM proxy_offsets WHERE pool = ?`, name.String()); err != nil {
		return fmt.Errorf("delete proxy offset: %w", err)
	}

	return nil
}

// TakeProxyOffset atomically reads and advances a pool's round-robin
// offset so concurrent workers receive different slices of the proxy pool.
func (s *Store) TakeProxyOffset(ctx context.Context, pool model.PoolName, advance func(current int64) int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin take proxy offset: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var current int64
	err = tx.QueryRowContext(ctx, `SELECT offset_value FROM proxy_offsets WHERE pool = ?`, pool.String()).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read proxy offset %s: %w", pool, err)
	}

	next := advance(current)
	if _, err := tx.ExecContext(ctx, `INSERT INTO proxy_offsets (pool, offset_value, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(pool) DO UPDATE SET offset_value = excluded.offset_value, updated_at = excluded.updated_at`,
		pool.String(), next, formatTime(time.Now().UTC())); err != nil {
		return 0, fmt.Errorf("write proxy offset %s: %w", pool, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit take proxy offset: %w", err)
	}

	return current, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanRoute(row scanner) (Route, error) {
	var r Route
	var name, kind, configJSON, createdAt, updatedAt string
	if err := row.Scan(&r.ID, &name, &kind, &configJSON, &createdAt, &updatedAt); err != nil {
		return Route{}, err
	}

	var err error
	if r.Name, err = model.ParseRouteName(name); err != nil {
		return Route{}, err
	}
	if r.Kind, err = model.ParseRouteKind(kind); err != nil {
		return Route{}, err
	}
	r.Options, err = decodeOptions(configJSON)
	if err != nil {
		return Route{}, fmt.Errorf("parse config for route %s: %w", name, err)
	}
	r.Options, err = routes.PrepareRoute(r.Kind, r.Options)
	if err != nil {
		return Route{}, fmt.Errorf("invalid config for route %s: %w", name, err)
	}
	if r.CreatedAt, err = parseTime(createdAt); err != nil {
		return Route{}, err
	}
	if r.UpdatedAt, err = parseTime(updatedAt); err != nil {
		return Route{}, err
	}

	return r, nil
}

func encodeOptions(options routes.Options) (string, error) {
	if options == nil {
		options = routes.Options{}
	}
	raw, err := json.Marshal(options)
	if err != nil {
		return "", fmt.Errorf("marshal route options: %w", err)
	}

	return string(raw), nil
}

func decodeOptions(raw string) (routes.Options, error) {
	if strings.TrimSpace(raw) == "" {
		raw = "{}"
	}

	var options routes.Options
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return nil, err
	}

	return routes.CloneOptions(options), nil
}

func scanMonitor(row scanner) (Monitor, error) {
	var m Monitor
	var name, definitionJSON, stateJSON, deliveriesJSON, proxyPool, status string
	var createdAt, updatedAt string
	var lastStatus, notifiedStatus string
	var expiresAt, nextDueAt, lastRunAt, expiredAt, runningStartedAt, runningExpiresAt sql.NullString

	err := row.Scan(&name, &m.SourceDir, &m.ArtifactDir, &m.BinaryPath, &definitionJSON, &stateJSON, &m.StateVersion, &m.StateRevision,
		&m.IntervalSeconds, &m.TTLSeconds, &m.TimeoutSeconds, &m.MaxEvents, &deliveriesJSON, &proxyPool, &status,
		&createdAt, &updatedAt, &expiresAt, &nextDueAt, &lastRunAt, &expiredAt,
		&m.RunningRunID, &runningStartedAt, &runningExpiresAt,
		&lastStatus, &notifiedStatus, &m.ConsecutiveFailures, &m.TotalRuns, &m.TotalFailures)
	if err != nil {
		return Monitor{}, err
	}
	m.LastStatus = monitor.ResultStatus(lastStatus)
	m.NotifiedStatus = monitor.ResultStatus(notifiedStatus)

	if m.Name, err = model.ParseMonitorName(name); err != nil {
		return Monitor{}, err
	}
	if err := json.Unmarshal([]byte(definitionJSON), &m.Definition); err != nil {
		return Monitor{}, fmt.Errorf("parse monitor definition for %s: %w", m.Name, err)
	}
	m.Definition = m.Definition.WithDefaults()
	if err := m.Definition.Validate(); err != nil {
		return Monitor{}, fmt.Errorf("invalid monitor definition for %s: %w", m.Name, err)
	}
	m.State = json.RawMessage(stateJSON)
	if err := json.Unmarshal([]byte(deliveriesJSON), &m.Deliveries); err != nil {
		return Monitor{}, fmt.Errorf("parse deliveries for monitor %s: %w", m.Name, err)
	}
	if len(m.Deliveries) == 0 {
		return Monitor{}, fmt.Errorf("monitor %s has no deliveries", m.Name)
	}
	for index := range m.Deliveries {
		if err := m.Deliveries[index].Route.Validate(); err != nil {
			return Monitor{}, fmt.Errorf("invalid delivery for monitor %s: %w", m.Name, err)
		}
		m.Deliveries[index].Options = routes.CloneOptions(m.Deliveries[index].Options)
	}
	if m.ProxyPool, err = model.ParsePoolName(proxyPool); err != nil {
		return Monitor{}, err
	}
	if m.Status, err = model.ParseMonitorStatus(status); err != nil {
		return Monitor{}, err
	}
	if m.CreatedAt, err = parseTimePtr(createdAt); err != nil {
		return Monitor{}, err
	}
	if m.UpdatedAt, err = parseTimePtr(updatedAt); err != nil {
		return Monitor{}, err
	}
	if m.ExpiresAt, err = parseNullTime(expiresAt); err != nil {
		return Monitor{}, err
	}
	if m.NextDueAt, err = parseNullTime(nextDueAt); err != nil {
		return Monitor{}, err
	}
	if m.LastRunAt, err = parseNullTime(lastRunAt); err != nil {
		return Monitor{}, err
	}
	if m.ExpiredAt, err = parseNullTime(expiredAt); err != nil {
		return Monitor{}, err
	}
	if m.RunningStartedAt, err = parseNullTime(runningStartedAt); err != nil {
		return Monitor{}, err
	}
	if m.RunningExpiresAt, err = parseNullTime(runningExpiresAt); err != nil {
		return Monitor{}, err
	}

	return m, nil
}

// leaseDuration is the run timeout plus slack for daemon-side bookkeeping.
func leaseDuration(timeoutSeconds int64) time.Duration {
	timeout := time.Duration(timeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	return timeout + 30*time.Second
}

func newRunID(name model.MonitorName, now time.Time) (string, error) {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate run id: %w", err)
	}

	return fmt.Sprintf("%s-%d-%s", name, now.UnixNano(), hex.EncodeToString(suffix[:])), nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func formatTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}

	return formatTime(*t)
}

func parseTime(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse db time %q: %w", value, err)
	}

	return t, nil
}

func parseTimePtr(value string) (*time.Time, error) {
	t, err := parseTime(value)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func parseNullTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}

	return parseTimePtr(value.String)
}

func boolInt(value bool) int {
	if value {
		return 1
	}

	return 0
}

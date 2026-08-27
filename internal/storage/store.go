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
	"github.com/saucesteals/monitord/internal/storage/db"

	_ "modernc.org/sqlite"
)

// ErrStateConflict is returned when stored state moved on since a tick read it.
var ErrStateConflict = errors.New("state revision conflict")

// Store owns SQLite access for monitord, over sqlc-generated queries.
type Store struct {
	db *sql.DB
	q  *db.Queries
}

// Route is a named agentic notification sink. Discord deliveries are inline.
type Route struct {
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

	State         json.RawMessage
	StateVersion  int
	StateRevision int64

	IntervalSeconds int64
	TTLSeconds      int64
	TimeoutSeconds  int64
	MaxEvents       int64
	Deliveries      []routes.Delivery
	ProxyPool       model.PoolName
	Status          model.MonitorStatus

	CreatedAt *time.Time
	UpdatedAt *time.Time
	ExpiresAt *time.Time
	NextDueAt *time.Time
	LastRunAt *time.Time
	ExpiredAt *time.Time

	RunningRunID     string
	RunningStartedAt *time.Time
	RunningExpiresAt *time.Time

	LastStatus          monitor.ResultStatus
	NotifiedStatus      monitor.ResultStatus
	ConsecutiveFailures int64
	TotalRuns           int64
	TotalFailures       int64
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

	State         json.RawMessage
	StateRevision int64
}

// Event is one monitor event: the alert history and dedupe source in one,
// keyed by (MonitorName, EventID).
type Event struct {
	MonitorName model.MonitorName
	EventID     string
	Title       string
	Summary     string
	URL         string
	Severity    string
	SentAt      time.Time
	Delivered   bool
	Error       string
}

// Open opens a SQLite store and applies migrations.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	conn, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	conn.SetMaxOpenConns(1)
	for _, pragma := range []string{
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA journal_mode = WAL`,
		`PRAGMA foreign_keys = ON`,
	} {
		if _, err := conn.ExecContext(context.Background(), pragma); err != nil {
			_ = conn.Close()

			return nil, fmt.Errorf("pragma %q: %w", pragma, err)
		}
	}

	if err := runMigrations(conn); err != nil {
		_ = conn.Close()

		return nil, err
	}

	return &Store{db: conn, q: db.New(conn)}, nil
}

// Close closes the SQLite connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// UpsertRoute creates or updates an agent route.
func (s *Store) UpsertRoute(ctx context.Context, route Route) error {
	if err := route.Name.Validate(); err != nil {
		return err
	}
	prepared, err := routes.PrepareRoute(route.Kind, route.Options)
	if err != nil {
		return err
	}
	configJSON, err := encodeOptions(prepared)
	if err != nil {
		return err
	}
	now := toMs(time.Now().UTC())
	return s.q.UpsertRoute(ctx, db.UpsertRouteParams{Name: route.Name.String(), Kind: route.Kind.String(), Config: configJSON, CreatedAt: now, UpdatedAt: now})
}

// ListRoutes returns all agent routes.
func (s *Store) ListRoutes(ctx context.Context) ([]Route, error) {
	rows, err := s.q.ListRoutes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list routes: %w", err)
	}
	out := make([]Route, 0, len(rows))
	for _, row := range rows {
		route, err := toRoute(row)
		if err != nil {
			return nil, err
		}
		out = append(out, route)
	}
	return out, nil
}

// GetRoute loads one agent route by name.
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

// ---- monitors ----

// UpsertMonitor creates or updates a monitor, preserving state on redeploy.
func (s *Store) UpsertMonitor(ctx context.Context, m Monitor) error {
	if err := m.Name.Validate(); err != nil {
		return err
	}
	if len(m.Deliveries) == 0 {
		return errors.New("monitor requires at least one delivery")
	}
	for _, delivery := range m.Deliveries {
		if err := delivery.Validate(); err != nil {
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

	return s.q.UpsertMonitor(ctx, db.UpsertMonitorParams{
		Name:          m.Name.String(),
		SourceDir:     m.SourceDir,
		ArtifactDir:   m.ArtifactDir,
		BinaryPath:    m.BinaryPath,
		Definition:    string(definitionJSON),
		Status:        m.Status.String(),
		IntervalMs:    m.IntervalSeconds * 1000,
		TtlMs:         m.TTLSeconds * 1000,
		TimeoutMs:     m.TimeoutSeconds * 1000,
		MaxEvents:     m.MaxEvents,
		Deliveries:    string(deliveriesJSON),
		ProxyPool:     nullPool(m.ProxyPool),
		State:         string(m.State),
		StateVersion:  int64(m.StateVersion),
		StateRevision: m.StateRevision,
		CreatedAt:     toMs(*m.CreatedAt),
		UpdatedAt:     toMs(*m.UpdatedAt),
		ExpiresAt:     nullMs(m.ExpiresAt),
		NextDueAt:     nullMs(m.NextDueAt),
		LastRunAt:     nullMs(m.LastRunAt),
		ExpiredAt:     nullMs(m.ExpiredAt),
	})
}

// ListMonitors returns all monitors.
func (s *Store) ListMonitors(ctx context.Context) ([]Monitor, error) {
	rows, err := s.q.ListMonitors(ctx)
	if err != nil {
		return nil, fmt.Errorf("list monitors: %w", err)
	}
	out := make([]Monitor, 0, len(rows))
	for _, row := range rows {
		m, err := toMonitor(row)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}

	return out, nil
}

// GetMonitor loads a monitor by name.
func (s *Store) GetMonitor(ctx context.Context, name model.MonitorName) (Monitor, error) {
	if err := name.Validate(); err != nil {
		return Monitor{}, err
	}
	row, err := s.q.GetMonitor(ctx, name.String())
	if errors.Is(err, sql.ErrNoRows) {
		return Monitor{}, fmt.Errorf("monitor %s not found", name)
	}
	if err != nil {
		return Monitor{}, err
	}

	return toMonitor(row)
}

// SetMonitorState replaces a monitor's state from outside a run.
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

	count, err := s.q.SetMonitorState(ctx, db.SetMonitorStateParams{
		State:        string(state),
		StateVersion: int64(version),
		UpdatedAt:    toMs(time.Now().UTC()),
		Name:         name.String(),
	})
	if err != nil {
		return fmt.Errorf("set state for %s: %w", name, err)
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
	qtx := s.q.WithTx(tx)

	rows, err := qtx.SelectDueMonitors(ctx, db.SelectDueMonitorsParams{Now: nullMs(&now), Lim: int64(limit)})
	if err != nil {
		return nil, fmt.Errorf("select due monitors: %w", err)
	}
	monitors := make([]Monitor, 0, len(rows))
	for _, row := range rows {
		m, err := toMonitor(row)
		if err != nil {
			return nil, err
		}
		monitors = append(monitors, m)
	}

	for i := range monitors {
		lease := now.Add(leaseDuration(monitors[i].TimeoutSeconds))
		runID, err := newRunID(monitors[i].Name, now)
		if err != nil {
			return nil, err
		}
		count, err := qtx.ClaimMonitor(ctx, db.ClaimMonitorParams{
			RunID:        runID,
			StartedAt:    nullMs(&now),
			LeaseExpires: nullMs(&lease),
			Now:          toMs(now),
			Name:         monitors[i].Name.String(),
		})
		if err != nil {
			return nil, fmt.Errorf("claim monitor %s: %w", monitors[i].Name, err)
		}
		if count != 1 {
			return nil, fmt.Errorf("claim monitor %s affected %d rows", monitors[i].Name, count)
		}
		monitors[i].RunningRunID = runID
		monitors[i].RunningStartedAt = &now
		monitors[i].RunningExpiresAt = &lease
		monitors[i].NextDueAt = &lease
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claim due monitors: %w", err)
	}

	return monitors, nil
}

// EarliestDue returns the soonest scheduled run among claimable monitors.
func (s *Store) EarliestDue(ctx context.Context) (time.Time, bool, error) {
	v, err := s.q.EarliestDue(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, fmt.Errorf("earliest due: %w", err)
	}
	switch ms := v.(type) {
	case nil:
		return time.Time{}, false, nil
	case int64:
		return fromMs(ms), true, nil
	default:
		return time.Time{}, false, fmt.Errorf("earliest due: unexpected type %T", v)
	}
}

// ExpireDueMonitors marks active monitors expired after their TTL.
func (s *Store) ExpireDueMonitors(ctx context.Context, now time.Time) (int64, error) {
	count, err := s.q.ExpireDueMonitors(ctx, nullMs(&now))
	if err != nil {
		return 0, fmt.Errorf("expire due monitors: %w", err)
	}

	return count, nil
}

// ExpireMonitor manually expires a monitor.
func (s *Store) ExpireMonitor(ctx context.Context, name model.MonitorName, now time.Time) error {
	count, err := s.q.ExpireMonitor(ctx, db.ExpireMonitorParams{Now: nullMs(&now), Name: name.String()})
	if err != nil {
		return fmt.Errorf("expire monitor %s: %w", name, err)
	}
	if count == 0 {
		return fmt.Errorf("monitor %s not found", name)
	}

	return nil
}

// MarkNotified records the status a monitor's destinations were last told about.
func (s *Store) MarkNotified(ctx context.Context, name model.MonitorName, status monitor.ResultStatus) error {
	if err := s.q.MarkNotified(ctx, db.MarkNotifiedParams{NotifiedStatus: status.String(), Name: name.String()}); err != nil {
		return fmt.Errorf("mark notified for %s: %w", name, err)
	}

	return nil
}

// RecordRun stores a run, advances the schedule, and applies tick state.
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
	qtx := s.q.WithTx(tx)

	if err := qtx.InsertRun(ctx, db.InsertRunParams{
		ID:          run.ID,
		MonitorName: run.MonitorName.String(),
		StartedAt:   toMs(run.StartedAt),
		FinishedAt:  toMs(run.FinishedAt),
		Status:      run.Status.String(),
		ExitCode:    int64(run.ExitCode),
		Stdout:      run.Stdout,
		Stderr:      run.Stderr,
		Error:       run.Error,
		Notified:    b2i(run.NotificationSent),
		NotifyError: run.NotificationError,
	}); err != nil {
		return fmt.Errorf("insert run %s: %w", run.ID, err)
	}
	if err := qtx.PruneRunsBefore(ctx, db.PruneRunsBeforeParams{
		MonitorName: run.MonitorName.String(),
		Cutoff:      toMs(run.FinishedAt.Add(-7 * 24 * time.Hour)),
		Lim:         1000,
	}); err != nil {
		return fmt.Errorf("prune runs for %s: %w", run.MonitorName, err)
	}

	var failed int64
	if run.Status == monitor.StatusFailure {
		failed = 1
	}
	count, err := qtx.AdvanceMonitor(ctx, db.AdvanceMonitorParams{
		FinishedAt: nullMs(&run.FinishedAt),
		NextDue:    nullMs(&nextDue),
		Status:     run.Status.String(),
		Failed:     failed,
		Name:       run.MonitorName.String(),
		RunID:      run.ID,
	})
	if err != nil {
		return fmt.Errorf("advance monitor %s: %w", run.MonitorName, err)
	}
	if count == 0 {
		return fmt.Errorf("monitor %s claim %s is no longer active", run.MonitorName, run.ID)
	}

	conflict := false
	if len(run.State) > 0 {
		stateCount, err := qtx.SaveMonitorState(ctx, db.SaveMonitorStateParams{
			State:    string(run.State),
			Name:     run.MonitorName.String(),
			Revision: run.StateRevision,
		})
		if err != nil {
			return fmt.Errorf("save state for %s: %w", run.MonitorName, err)
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

// DeleteMonitor removes a monitor; runs and events cascade.
func (s *Store) DeleteMonitor(ctx context.Context, name model.MonitorName) error {
	if err := name.Validate(); err != nil {
		return err
	}
	if err := s.q.DeleteMonitor(ctx, name.String()); err != nil {
		return fmt.Errorf("delete monitor %s: %w", name, err)
	}

	return nil
}

// ---- runs ----

// GetRun loads a run by id.
func (s *Store) GetRun(ctx context.Context, runID string) (Run, error) {
	row, err := s.q.GetRun(ctx, runID)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, fmt.Errorf("run %s not found", runID)
	}
	if err != nil {
		return Run{}, err
	}

	return toRun(row)
}

// ListRuns returns a monitor's most recent runs, newest first.
func (s *Store) ListRuns(ctx context.Context, name model.MonitorName, limit int, onlyFailed bool) ([]Run, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.q.ListRuns(ctx, db.ListRunsParams{
		MonitorName: name.String(),
		OnlyFailed:  b2i(onlyFailed),
		Lim:         int64(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	out := make([]Run, 0, len(rows))
	for _, row := range rows {
		r, err := toRun(row)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}

	return out, nil
}

// UpdateRunNotification records best-effort notification delivery state.
func (s *Store) UpdateRunNotification(ctx context.Context, runID string, sent bool, notificationError string) error {
	count, err := s.q.UpdateRunNotification(ctx, db.UpdateRunNotificationParams{
		Notified:    b2i(sent),
		NotifyError: notificationError,
		ID:          runID,
	})
	if err != nil {
		return fmt.Errorf("update notification state for run %s: %w", runID, err)
	}
	if count == 0 {
		return fmt.Errorf("run %s not found", runID)
	}

	return nil
}

// ---- events ----

// EventSuppressed reports whether an event id was delivered within the window.
func (s *Store) EventSuppressed(ctx context.Context, name model.MonitorName, eventID string, after time.Time) (bool, error) {
	if strings.TrimSpace(eventID) == "" {
		return false, errors.New("event id is required")
	}

	return s.q.EventSuppressed(ctx, db.EventSuppressedParams{
		MonitorName: name.String(),
		EventID:     eventID,
		SentAt:      toMs(after),
	})
}

// RecordEvent upserts an event: one row per (monitor, event id), updated with
// each send, for history and dedupe.
func (s *Store) RecordEvent(ctx context.Context, e Event) error {
	if strings.TrimSpace(e.EventID) == "" {
		return errors.New("event id is required")
	}

	if err := s.q.UpsertEvent(ctx, db.UpsertEventParams{
		MonitorName: e.MonitorName.String(),
		EventID:     e.EventID,
		Title:       e.Title,
		Summary:     e.Summary,
		Url:         e.URL,
		Severity:    e.Severity,
		SentAt:      toMs(e.SentAt),
		Delivered:   b2i(e.Delivered),
		Error:       e.Error,
	}); err != nil {
		return fmt.Errorf("record event for %s: %w", e.MonitorName, err)
	}

	return nil
}

// ListEvents returns a monitor's recent events, newest first.
func (s *Store) ListEvents(ctx context.Context, name model.MonitorName, limit int, onlyFailed bool, since time.Time) ([]Event, error) {
	if limit <= 0 {
		limit = 20
	}
	var sinceMs int64
	if !since.IsZero() {
		sinceMs = toMs(since)
	}
	rows, err := s.q.ListEvents(ctx, db.ListEventsParams{
		MonitorName: name.String(),
		OnlyFailed:  b2i(onlyFailed),
		Since:       sinceMs,
		Lim:         int64(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	out := make([]Event, 0, len(rows))
	for _, row := range rows {
		e, err := toEvent(row)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}

	return out, nil
}

// PruneEvents deletes events older than the cutoff and returns the count.
func (s *Store) PruneEvents(ctx context.Context, before time.Time) (int64, error) {
	return s.q.PruneEvents(ctx, toMs(before))
}

// ---- proxy pools ----

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
	now := toMs(time.Now().UTC())

	return s.q.UpsertProxyPool(ctx, db.UpsertProxyPoolParams{
		Name:      pool.Name.String(),
		Strategy:  pool.Strategy.String(),
		Proxies:   strings.Join(pool.Proxies, "\n"),
		CreatedAt: now,
		UpdatedAt: now,
	})
}

// GetProxyPool loads a pool by name.
func (s *Store) GetProxyPool(ctx context.Context, name model.PoolName) (ProxyPool, error) {
	if err := name.Validate(); err != nil {
		return ProxyPool{}, err
	}
	row, err := s.q.GetProxyPool(ctx, name.String())
	if errors.Is(err, sql.ErrNoRows) {
		return ProxyPool{}, fmt.Errorf("proxy pool %s not found", name)
	}
	if err != nil {
		return ProxyPool{}, fmt.Errorf("get proxy pool %s: %w", name, err)
	}

	return toProxyPool(row)
}

// ListProxyPools returns every pool in name order.
func (s *Store) ListProxyPools(ctx context.Context) ([]ProxyPool, error) {
	rows, err := s.q.ListProxyPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("list proxy pools: %w", err)
	}
	out := make([]ProxyPool, 0, len(rows))
	for _, row := range rows {
		p, err := toProxyPool(row)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}

	return out, nil
}

// DeleteProxyPool removes a pool unless a monitor still uses it.
func (s *Store) DeleteProxyPool(ctx context.Context, name model.PoolName) error {
	if err := name.Validate(); err != nil {
		return err
	}
	used, err := s.q.CountMonitorsUsingPool(ctx, nullPool(name))
	if err != nil {
		return fmt.Errorf("check pool usage: %w", err)
	}
	if used > 0 {
		return fmt.Errorf("proxy pool %s is used by %d monitor(s)", name, used)
	}
	if err := s.q.DeleteProxyPool(ctx, name.String()); err != nil {
		return fmt.Errorf("delete proxy pool %s: %w", name, err)
	}

	return nil
}

// TakeProxyOffset reads a pool's round-robin cursor and advances it atomically,
// returning the value in effect before the advance.
func (s *Store) TakeProxyOffset(ctx context.Context, pool model.PoolName, advance func(current int64) int64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin take proxy offset: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	qtx := s.q.WithTx(tx)

	current, err := qtx.GetProxyOffset(ctx, pool.String())
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read proxy offset %s: %w", pool, err)
	}
	if err := qtx.SetProxyOffset(ctx, db.SetProxyOffsetParams{
		Offset:    advance(current),
		UpdatedAt: toMs(time.Now().UTC()),
		Name:      pool.String(),
	}); err != nil {
		return 0, fmt.Errorf("write proxy offset %s: %w", pool, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit take proxy offset: %w", err)
	}

	return current, nil
}

// ---- mapping ----

func toRoute(r db.Route) (Route, error) {
	name, err := model.ParseRouteName(r.Name)
	if err != nil {
		return Route{}, err
	}
	kind, err := model.ParseRouteKind(r.Kind)
	if err != nil {
		return Route{}, err
	}
	options, err := decodeOptions(r.Config)
	if err != nil {
		return Route{}, fmt.Errorf("parse config for route %s: %w", name, err)
	}
	options, err = routes.PrepareRoute(kind, options)
	if err != nil {
		return Route{}, fmt.Errorf("invalid config for route %s: %w", name, err)
	}
	return Route{Name: name, Kind: kind, Options: options, CreatedAt: fromMs(r.CreatedAt), UpdatedAt: fromMs(r.UpdatedAt)}, nil
}

func toMonitor(m db.Monitor) (Monitor, error) {
	name, err := model.ParseMonitorName(m.Name)
	if err != nil {
		return Monitor{}, err
	}
	var def monitor.Definition
	if err := json.Unmarshal([]byte(m.Definition), &def); err != nil {
		return Monitor{}, fmt.Errorf("decode definition for %s: %w", name, err)
	}
	def = def.WithDefaults()
	var deliveries []routes.Delivery
	if err := json.Unmarshal([]byte(m.Deliveries), &deliveries); err != nil {
		return Monitor{}, fmt.Errorf("decode deliveries for %s: %w", name, err)
	}
	pool, err := model.ParsePoolName(m.ProxyPool.String)
	if err != nil {
		return Monitor{}, err
	}
	created := fromMs(m.CreatedAt)
	updated := fromMs(m.UpdatedAt)

	return Monitor{
		Name:                name,
		SourceDir:           m.SourceDir,
		ArtifactDir:         m.ArtifactDir,
		BinaryPath:          m.BinaryPath,
		Definition:          def,
		State:               json.RawMessage(m.State),
		StateVersion:        int(m.StateVersion),
		StateRevision:       m.StateRevision,
		IntervalSeconds:     m.IntervalMs / 1000,
		TTLSeconds:          m.TtlMs / 1000,
		TimeoutSeconds:      m.TimeoutMs / 1000,
		MaxEvents:           m.MaxEvents,
		Deliveries:          deliveries,
		ProxyPool:           pool,
		Status:              model.MonitorStatus(m.Status),
		CreatedAt:           &created,
		UpdatedAt:           &updated,
		ExpiresAt:           msPtr(m.ExpiresAt),
		NextDueAt:           msPtr(m.NextDueAt),
		LastRunAt:           msPtr(m.LastRunAt),
		ExpiredAt:           msPtr(m.ExpiredAt),
		RunningRunID:        m.RunningRunID,
		RunningStartedAt:    msPtr(m.RunningStartedAt),
		RunningExpiresAt:    msPtr(m.RunningExpiresAt),
		LastStatus:          monitor.ResultStatus(m.LastStatus),
		NotifiedStatus:      monitor.ResultStatus(m.NotifiedStatus),
		ConsecutiveFailures: m.ConsecutiveFailures,
		TotalRuns:           m.TotalRuns,
		TotalFailures:       m.TotalFailures,
	}, nil
}

func toRun(r db.Run) (Run, error) {
	name, err := model.ParseMonitorName(r.MonitorName)
	if err != nil {
		return Run{}, err
	}

	return Run{
		ID:                r.ID,
		MonitorName:       name,
		StartedAt:         fromMs(r.StartedAt),
		FinishedAt:        fromMs(r.FinishedAt),
		Status:            monitor.ResultStatus(r.Status),
		ExitCode:          int(r.ExitCode),
		Stdout:            r.Stdout,
		Stderr:            r.Stderr,
		Error:             r.Error,
		NotificationSent:  i2b(r.Notified),
		NotificationError: r.NotifyError,
	}, nil
}

func toEvent(e db.Event) (Event, error) {
	name, err := model.ParseMonitorName(e.MonitorName)
	if err != nil {
		return Event{}, err
	}

	return Event{
		MonitorName: name,
		EventID:     e.EventID,
		Title:       e.Title,
		Summary:     e.Summary,
		URL:         e.Url,
		Severity:    e.Severity,
		SentAt:      fromMs(e.SentAt),
		Delivered:   i2b(e.Delivered),
		Error:       e.Error,
	}, nil
}

func toProxyPool(p db.ProxyPool) (ProxyPool, error) {
	name, err := model.ParsePoolName(p.Name)
	if err != nil {
		return ProxyPool{}, err
	}

	return ProxyPool{
		Name:      name,
		Strategy:  network.Strategy(p.Strategy),
		Proxies:   strings.Split(p.Proxies, "\n"),
		CreatedAt: fromMs(p.CreatedAt),
		UpdatedAt: fromMs(p.UpdatedAt),
	}, nil
}

// ---- helpers ----

func toMs(t time.Time) int64 { return t.UTC().UnixMilli() }

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

func b2i(b bool) int64 {
	if b {
		return 1
	}

	return 0
}

func i2b(i int64) bool { return i != 0 }

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
		return nil, fmt.Errorf("unmarshal route options: %w", err)
	}
	return options, nil
}

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

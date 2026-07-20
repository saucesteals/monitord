// Package daemon schedules deployed monitors and supervises their workers.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	monitor "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/network"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
)

// Defaults for daemon scheduling.
const (
	DefaultInterval    = 5 * time.Second
	DefaultConcurrency = 8
	defaultTimeout     = 30 * time.Second
	notifyTimeout      = 10 * time.Second
	recordTimeout      = 10 * time.Second

	// EventDedupeWindow is how long an event's dedupe key suppresses repeats.
	EventDedupeWindow = time.Hour

	// minWait keeps the scheduler from spinning when a monitor is due now or
	// slightly overdue.
	minWait = 20 * time.Millisecond
)

// Daemon scans the schedule and drives monitor workers.
type Daemon struct {
	store    *storage.Store
	paths    config.Paths
	logger   *slog.Logger
	interval time.Duration
	limit    chan struct{}
	wg       sync.WaitGroup
	// wake nudges the scheduler when a tick finishes, so it recomputes the
	// sleep against the next due time that tick just wrote.
	wake chan struct{}

	workersMu sync.Mutex
	workers   map[model.MonitorName]*worker
}

// New returns a daemon runtime.
func New(store *storage.Store, paths config.Paths, logger *slog.Logger, interval time.Duration, concurrency int) *Daemon {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Daemon{
		store:    store,
		paths:    paths,
		logger:   logger,
		interval: interval,
		limit:    make(chan struct{}, concurrency),
		wake:     make(chan struct{}, 1),
		workers:  map[model.MonitorName]*worker{},
	}
}

// Run scans on an interval until ctx is canceled.
func (d *Daemon) Run(ctx context.Context) error {
	lock, err := acquireDaemonLock(d.paths)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	d.logger.Info("monitord daemon started", "root", d.paths.Root, "max_wait", d.interval)

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		if err := d.scan(ctx); err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("scheduler scan failed", "error", err)
		}

		timer.Reset(d.wait(ctx))

		select {
		case <-ctx.Done():
			d.wg.Wait()
			d.stopWorkers()
			d.logger.Info("monitord daemon stopped")

			return nil
		case <-timer.C:
		case <-d.wake:
		}
	}
}

// wait returns how long to sleep before the next scan.
//
// Sleeping until the soonest due monitor, rather than on a fixed cadence, is
// what makes `--every` mean what it says: a fixed ticker rounds every schedule
// up to its own period, so a 5s monitor whose tick takes any time at all misses
// its slot and waits for the following one.
//
// A claimed monitor's next_due_at holds its lease expiry until the tick
// records, so this can overestimate; finishing ticks send on wake to correct
// it. The configured interval is a ceiling, bounding how long the daemon sleeps
// through TTL expiry and newly deployed monitors.
func (d *Daemon) wait(ctx context.Context) time.Duration {
	due, ok, err := d.store.EarliestDue(ctx)
	if err != nil {
		d.logger.Error("earliest due lookup failed", "error", err)

		return d.interval
	}
	if !ok {
		return d.interval
	}

	return min(max(time.Until(due), minWait), d.interval)
}

func (d *Daemon) scan(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	now := time.Now().UTC()
	expired, err := d.store.ExpireDueMonitors(ctx, now)
	if err != nil {
		return err
	}
	if expired > 0 {
		d.logger.Info("expired monitors", "count", expired)
	}

	capacity := cap(d.limit) - len(d.limit)
	if capacity <= 0 {
		return nil
	}

	monitors, err := d.store.ClaimDueMonitors(ctx, now, capacity)
	if err != nil {
		return err
	}
	for _, m := range monitors {
		d.dispatch(ctx, m)
	}

	return nil
}

func (d *Daemon) dispatch(ctx context.Context, m storage.Monitor) {
	d.limit <- struct{}{}
	d.wg.Add(1)

	go func() {
		defer func() {
			<-d.limit
			d.wg.Done()
		}()

		notifications, err := d.runTick(ctx, m)
		if err != nil {
			d.logger.Error("monitor run failed", "monitor", m.Name, "error", err)
		}
		if len(notifications) > 0 {
			d.deliver(m, notifications)
		}
	}()
}

// notification pairs a rendered message with the route that should receive it.
type notification struct {
	Route   model.RouteName
	Message routes.Message
	// DedupeKey suppresses repeats of the same event within EventDedupeWindow.
	DedupeKey string
	// MarkStatus, when set, is recorded as the status the route was told about
	// once delivery succeeds.
	MarkStatus monitor.ResultStatus
	// Silent records MarkStatus without sending anything, used to seed a
	// monitor's notified status from its first healthy run.
	Silent bool
}

// runTick executes one claimed run and records its outcome.
func (d *Daemon) runTick(ctx context.Context, m storage.Monitor) ([]notification, error) {
	if m.RunningRunID == "" {
		return nil, fmt.Errorf("monitor %s has no run claim", m.Name)
	}

	started := time.Now().UTC()
	timeout := time.Duration(m.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	w, err := d.worker(ctx, m)
	if err != nil {
		return d.recordFailure(m, started, fmt.Sprintf("start monitor worker: %v", err))
	}

	out, tickErr := w.tick(runCtx, d.logger, m, monitor.Tick{
		RunID:     m.RunningRunID,
		StartedAt: started,
		Deadline:  started.Add(timeout),
		State:     m.State,
		Revision:  m.StateRevision,
	})

	stderr := ""
	if tickErr != nil {
		// A tick that failed at the transport level leaves the worker in an
		// unknown state, so it is replaced rather than reused.
		stderr = w.stderr.String()
		d.dropWorker(m.Name, w)
	}

	finished := time.Now().UTC()
	status, exitCode, errorText := classify(out, tickErr)

	record := storage.Run{
		ID:            m.RunningRunID,
		MonitorName:   m.Name,
		StartedAt:     started,
		FinishedAt:    finished,
		Status:        status,
		ExitCode:      exitCode,
		Stdout:        out.Stdout,
		Stderr:        stderr,
		Error:         errorText,
		State:         out.Result.State,
		StateRevision: m.StateRevision,
	}

	nextDue := nextDueAfter(started, time.Duration(m.IntervalSeconds)*time.Second, finished)
	if err := d.record(record, nextDue); err != nil {
		// A state conflict means an external edit won; the run itself stands.
		if !errors.Is(err, storage.ErrStateConflict) {
			return nil, err
		}
		d.logger.Warn("monitor state edited during run, keeping external value", "monitor", m.Name, "run", record.ID)
	}

	d.logger.Info("monitor tick complete",
		"monitor", m.Name,
		"status", status,
		"next_due", nextDue,
	)

	return d.notificationsFor(m, out, status, exitCode, errorText), nil
}

// recordFailure stores a run that never reached the worker, so a monitor that
// cannot start still reports and reschedules instead of leaking its lease.
func (d *Daemon) recordFailure(m storage.Monitor, started time.Time, errorText string) ([]notification, error) {
	finished := time.Now().UTC()
	record := storage.Run{
		ID:          m.RunningRunID,
		MonitorName: m.Name,
		StartedAt:   started,
		FinishedAt:  finished,
		Status:      monitor.StatusFailure,
		ExitCode:    -1,
		Error:       errorText,
	}

	nextDue := nextDueAfter(started, time.Duration(m.IntervalSeconds)*time.Second, finished)
	if err := d.record(record, nextDue); err != nil && !errors.Is(err, storage.ErrStateConflict) {
		return nil, err
	}

	out := tickOutput{
		Result: monitor.Result{
			Status:  monitor.StatusFailure,
			Summary: errorText,
			Notify:  true,
		},
	}

	return d.notificationsFor(m, out, monitor.StatusFailure, -1, errorText), errors.New(errorText)
}

// nextDueAfter schedules the following tick relative to when this one started,
// so the cadence does not drift by however long each tick takes. A tick that
// overruns its interval skips whole periods rather than running back to back.
func nextDueAfter(started time.Time, interval time.Duration, now time.Time) time.Time {
	if interval <= 0 {
		interval = time.Second
	}

	next := started.Add(interval)
	if next.After(now) {
		return next
	}

	missed := now.Sub(started) / interval

	return started.Add((missed + 1) * interval)
}

// classify folds the tick result and any transport error into a final status.
func classify(out tickOutput, tickErr error) (monitor.ResultStatus, int, string) {
	var problems []string
	exitCode := 0

	if tickErr != nil {
		problems = append(problems, tickErr.Error())
		exitCode = -1
	}
	if out.Result.Status == "" {
		problems = append(problems, "worker did not report a result")
	}

	status := out.Result.Status
	if status == "" || len(problems) > 0 {
		status = monitor.StatusFailure
	}

	return status, exitCode, strings.Join(problems, "; ")
}

// worker returns the live worker for a monitor, starting one if the current
// worker is missing, dead, or built from a superseded artifact.
func (d *Daemon) worker(ctx context.Context, m storage.Monitor) (*worker, error) {
	d.workersMu.Lock()
	defer d.workersMu.Unlock()

	if existing := d.workers[m.Name]; existing != nil {
		if existing.binaryPath == m.BinaryPath && !existing.hasExited() {
			return existing, nil
		}
		existing.terminate(d.logger)
		delete(d.workers, m.Name)
	}

	assigned, err := d.assignNetwork(ctx, m)
	if err != nil {
		return nil, err
	}

	w, err := startWorker(ctx, d.logger, m, assigned)
	if err != nil {
		return nil, err
	}
	d.workers[m.Name] = w

	return w, nil
}

// assignNetwork leases the monitor a slice of its proxy pool, sized to the
// client count it asked for. The pool comes from the database, so adding
// proxies never requires restarting the daemon.
func (d *Daemon) assignNetwork(ctx context.Context, m storage.Monitor) (monitor.Network, error) {
	if m.ProxyPool == "" {
		return monitor.Network{}, nil
	}

	pool, err := d.store.GetProxyPool(ctx, m.ProxyPool)
	if err != nil {
		return monitor.Network{}, err
	}

	size := m.Definition.Clients
	offset := int64(0)
	if pool.Strategy == network.StrategyRoundRobin {
		offset, err = d.store.TakeProxyOffset(ctx, m.ProxyPool, func(current int64) int64 {
			return (current + int64(size)) % int64(len(pool.Proxies))
		})
		if err != nil {
			return monitor.Network{}, err
		}
	}

	assignment := network.Assign(pool.Proxies, size, pool.Strategy, m.Name, offset)

	return monitor.Network{Proxies: assignment.Proxies}, nil
}

func (d *Daemon) dropWorker(name model.MonitorName, w *worker) {
	d.workersMu.Lock()
	defer d.workersMu.Unlock()

	if d.workers[name] == w {
		delete(d.workers, name)
	}
	w.terminate(d.logger)
}

func (d *Daemon) stopWorkers() {
	d.workersMu.Lock()
	workers := make([]*worker, 0, len(d.workers))
	for name, w := range d.workers {
		workers = append(workers, w)
		delete(d.workers, name)
	}
	d.workersMu.Unlock()

	for _, w := range workers {
		w.terminate(d.logger)
	}
}

// record persists a run outside the caller's context so a canceled scan still
// releases the monitor's lease.
func (d *Daemon) record(run storage.Run, nextDue time.Time) error {
	ctx, cancel := context.WithTimeout(context.Background(), recordTimeout)
	defer cancel()

	return d.store.RecordRun(ctx, run, nextDue)
}

// notificationsFor decides what a completed tick should send.
//
// Result notifications are edge-triggered: a monitor pages when it first fails
// and again when it recovers, not on every failing tick. Without this a monitor
// that is down on a 30s interval would send 120 messages an hour. Events are
// deduplicated separately, by key, in deliver.
func (d *Daemon) notificationsFor(m storage.Monitor, out tickOutput, status monitor.ResultStatus, exitCode int, errorText string) []notification {
	var notifications []notification

	for _, event := range out.NotifyEvents {
		route := m.Route
		if event.Route != "" {
			parsed, err := model.ParseRouteName(event.Route.String())
			if err == nil {
				route = parsed
			}
		}
		notifications = append(notifications, notification{
			Route:     route,
			DedupeKey: event.DedupeKey,
			Message: routes.Message{
				Title:   event.Title,
				Summary: event.Summary,
				Details: strings.TrimSpace(event.Details),
				URL:     event.URL,
				Level:   eventLevel(event.Severity),
				Fields:  toFields(event.Fields),
				Footer:  m.Name.String(),
			},
		})
	}

	// Result.Notify is an explicit request from the monitor, so it bypasses
	// edge-triggering.
	if out.Result.Notify && status != monitor.StatusFailure {
		return append(notifications, notification{
			Route:   m.Route,
			Message: resultMessage(m, out.Result, status, exitCode, errorText, 0),
		})
	}
	if status == m.NotifiedStatus {
		return notifications
	}

	// A monitor's first observation is only worth reporting if it is bad.
	// Otherwise deploying ten monitors would announce ten successes, which
	// `monitord list` already shows.
	if m.NotifiedStatus == "" && status == monitor.StatusSuccess {
		return append(notifications, notification{
			Route:      m.Route,
			MarkStatus: status,
			Silent:     true,
		})
	}

	// consecutive_failures was already incremented for this run when it was
	// recorded, so it counts this failure too.
	failures := m.ConsecutiveFailures + 1
	if status == monitor.StatusSuccess {
		failures = 0
	}

	return append(notifications, notification{
		Route:      m.Route,
		Message:    resultMessage(m, out.Result, status, exitCode, errorText, failures),
		MarkStatus: status,
	})
}

func resultMessage(m storage.Monitor, result monitor.Result, status monitor.ResultStatus, exitCode int, errorText string, failures int64) routes.Message {
	summary := result.Summary
	if summary == "" {
		summary = errorText
	}
	if summary == "" {
		summary = fmt.Sprintf("exit code %d", exitCode)
	}

	// Three shapes read very differently, so each gets its own headline. A
	// monitor.Alert is "the thing you asked about happened", not a health
	// report, so it leads with the monitor name rather than a status word.
	var (
		title = m.Name.String()
		level = routes.LevelInfo
	)
	switch {
	case status == monitor.StatusFailure:
		title = fmt.Sprintf("%s failed", m.Name)
		level = routes.LevelFailure
	case m.NotifiedStatus == monitor.StatusFailure:
		title = fmt.Sprintf("%s recovered", m.Name)
		level = routes.LevelSuccess
	}

	fields := toFields(result.Fields)
	if failures > 1 {
		fields = append(fields, routes.Field{
			Name:   "Consecutive failures",
			Value:  fmt.Sprintf("%d", failures),
			Inline: true,
		})
	}

	return routes.Message{
		Title:   title,
		Summary: summary,
		Details: strings.TrimSpace(result.Details),
		URL:     result.URL,
		Level:   level,
		Fields:  fields,
		Footer:  m.Name.String(),
	}
}

// toFields converts monitor-declared fields to their route representation.
func toFields(fields []monitor.Field) []routes.Field {
	if len(fields) == 0 {
		return nil
	}

	out := make([]routes.Field, 0, len(fields))
	for _, field := range fields {
		out = append(out, routes.Field{
			Name:   field.Name,
			Value:  field.Value,
			Inline: field.Inline,
		})
	}

	return out
}

// eventLevel maps a monitor event severity to a notification accent.
func eventLevel(severity monitor.Severity) routes.Level {
	switch severity {
	case monitor.SeverityCritical:
		return routes.LevelCritical
	case monitor.SeverityWarn:
		return routes.LevelWarn
	default:
		return routes.LevelInfo
	}
}

func (d *Daemon) deliver(m storage.Monitor, notifications []notification) {
	sent := false
	var failures []string

	for _, item := range notifications {
		ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
		err := d.deliverOne(ctx, m, item)
		cancel()

		if err != nil {
			d.logger.Error("notification failed", "monitor", m.Name, "route", item.Route, "error", err)
			failures = append(failures, err.Error())

			continue
		}
		if !item.Silent {
			sent = true
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()
	if err := d.store.UpdateRunNotification(ctx, m.RunningRunID, sent, strings.Join(failures, "; ")); err != nil {
		d.logger.Error("notification state update failed", "monitor", m.Name, "run", m.RunningRunID, "error", err)
	}
}

// deliverOne sends a single notification, honouring event dedupe keys and
// recording the status the route was told about.
func (d *Daemon) deliverOne(ctx context.Context, m storage.Monitor, item notification) error {
	if item.Silent {
		return d.store.MarkNotified(ctx, m.Name, item.MarkStatus)
	}
	if item.DedupeKey != "" {
		send, err := d.store.ShouldSendEvent(ctx, m.Name, item.DedupeKey, time.Now().UTC(), EventDedupeWindow)
		if err != nil {
			return err
		}
		if !send {
			d.logger.Debug("event suppressed by dedupe key", "monitor", m.Name, "key", item.DedupeKey)

			return nil
		}
	}

	if err := d.notify(ctx, item.Route, item.Message, m.MentionOverride); err != nil {
		return err
	}
	if item.MarkStatus != "" {
		if err := d.store.MarkNotified(ctx, m.Name, item.MarkStatus); err != nil {
			return err
		}
	}

	return nil
}

func (d *Daemon) notify(ctx context.Context, name model.RouteName, msg routes.Message, mentionOverride string) error {
	route, err := d.store.GetRoute(ctx, name)
	if err != nil {
		return err
	}
	if route.Kind != model.RouteKindDiscord {
		return fmt.Errorf("unsupported route kind %q", route.Kind)
	}

	// The monitor's own override wins, so two monitors sharing one channel can
	// ping two different people without duplicating the route.
	mentions, err := routes.ResolveMentions(mentionOverride, route.Mentions)
	if err != nil {
		return err
	}

	return routes.SendDiscord(ctx, route.WebhookURL, msg, mentions)
}

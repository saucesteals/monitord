package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	monitor "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/storage"
)

// notification pairs a rendered message with the destinations that should receive it.
type notification struct {
	Deliveries []routes.Delivery
	Message    routes.Message
	// ID suppresses repeats of the same event within EventDedupeWindow.
	ID string
	// MarkStatus, when set, is recorded once every delivery succeeds.
	MarkStatus monitor.ResultStatus
	// Silent records MarkStatus without sending anything, used to seed a
	// monitor's notified status from its first healthy run.
	Silent bool
}

// runTick carries one claimed run through its lifecycle: execute the worker and
// deliver its events live, record the outcome, then page on the health edge.
func (d *Daemon) runTick(ctx context.Context, m storage.Monitor) error {
	if m.RunningRunID == "" {
		return fmt.Errorf("monitor %s has no run claim", m.Name)
	}

	r := d.newTickRun(m)
	r.execute(ctx)
	r.record()
	if r.err != nil {
		return r.err
	}
	r.reportHealth()
	r.finalize()

	return nil
}

// tickRun is the mutable state of one monitor tick as it moves through the
// lifecycle stages. State lives here rather than being threaded through return
// values, so each stage reads what the previous one produced.
type tickRun struct {
	daemon  *Daemon
	monitor storage.Monitor
	started time.Time
	timeout time.Duration

	// execute
	out     tickOutput
	tickErr error
	stderr  string

	// record
	finished time.Time
	status   monitor.ResultStatus
	exitCode int
	errText  string
	err      error // fatal lifecycle error; aborts the remaining stages

	// notify (events deliver concurrently through the shared send path)
	sent       atomic.Bool
	notifyMu   sync.Mutex
	notifyErrs []string
}

func (d *Daemon) newTickRun(m storage.Monitor) *tickRun {
	timeout := time.Duration(m.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return &tickRun{
		daemon:  d,
		monitor: m,
		started: time.Now().UTC(),
		timeout: timeout,
	}
}

// execute runs the worker for one tick and delivers each event as it streams in.
// A worker that never starts leaves a failure result, which record then treats
// like any other broken tick.
func (r *tickRun) execute(ctx context.Context) {
	w, err := r.daemon.worker(ctx, r.monitor)
	if err != nil {
		r.tickErr = fmt.Errorf("start monitor worker: %v", err)
		r.out.Result = monitor.Result{Status: monitor.StatusFailure, Summary: r.tickErr.Error()}

		return
	}

	runCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	disp := r.daemon.newDispatcher(r.monitor, r.notify)
	r.out, r.tickErr = w.tick(runCtx, r.daemon.logger, r.monitor, monitor.Tick{
		RunID:     r.monitor.RunningRunID,
		StartedAt: r.started,
		Deadline:  r.started.Add(r.timeout),
		State:     r.monitor.State,
		Revision:  r.monitor.StateRevision,
	}, disp.emit)
	disp.wait()

	if r.tickErr != nil {
		// A tick that failed at the transport level leaves the worker in an
		// unknown state, so it is replaced rather than reused.
		r.stderr = w.stderr.String()
		r.daemon.dropWorker(r.monitor.Name, w)
	}
}

// record classifies the outcome and persists the run row plus the next due time.
func (r *tickRun) record() {
	r.finished = time.Now().UTC()
	r.status, r.exitCode, r.errText = classify(r.out, r.tickErr)

	row := storage.Run{
		ID:            r.monitor.RunningRunID,
		MonitorName:   r.monitor.Name,
		StartedAt:     r.started,
		FinishedAt:    r.finished,
		Status:        r.status,
		ExitCode:      r.exitCode,
		Stdout:        r.out.Stdout,
		Stderr:        r.stderr,
		Error:         r.errText,
		State:         r.out.Result.State,
		StateRevision: r.monitor.StateRevision,
	}

	nextDue := nextDueAfter(r.started, time.Duration(r.monitor.IntervalSeconds)*time.Second, r.finished)
	if err := r.daemon.record(row, nextDue); err != nil {
		// A state conflict means an external edit won; the run itself stands.
		if !errors.Is(err, storage.ErrStateConflict) {
			r.err = err

			return
		}
		r.daemon.logger.Warn("monitor state edited during run, keeping external value", "monitor", r.monitor.Name, "run", row.ID)
	}

	r.daemon.logger.Info("monitor tick complete", "monitor", r.monitor.Name, "status", r.status, "next_due", nextDue)
}

// reportHealth pages once on a health edge. Events already delivered live during
// execute; the result only drives failure and recovery notifications, edge-
// triggered off the last status the destinations were told about.
func (r *tickRun) reportHealth() {
	m := r.monitor
	if r.status == m.NotifiedStatus {
		return // steady state, nothing to page
	}

	// A monitor's first observation is only worth reporting if it is bad;
	// otherwise deploying ten monitors would announce ten successes.
	if m.NotifiedStatus == "" && r.status == monitor.StatusSuccess {
		r.notify(notification{MarkStatus: r.status, Silent: true})

		return
	}

	// consecutive_failures was incremented when this run was recorded, so it
	// counts this failure too.
	failures := m.ConsecutiveFailures + 1
	if r.status == monitor.StatusSuccess {
		failures = 0
	}
	r.notify(notification{
		Deliveries: routes.CloneDeliveries(m.Deliveries),
		Message:    resultMessage(m, r.out.Result, r.status, r.exitCode, r.errText, failures),
		MarkStatus: r.status,
	})
}

// finalize stamps the run's notified flag once, reflecting every send the tick
// made — events and the health page alike — along with any delivery errors.
func (r *tickRun) finalize() {
	if !r.sent.Load() && len(r.notifyErrs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()
	if err := r.daemon.store.UpdateRunNotification(ctx, r.monitor.RunningRunID, r.sent.Load(), strings.Join(r.notifyErrs, "; ")); err != nil {
		r.daemon.logger.Error("notification state update failed", "monitor", r.monitor.Name, "run", r.monitor.RunningRunID, "error", err)
	}
}

// notify is the tick's single send primitive: skip suppressed events, fan the
// message out to every destination, log the event for history and dedupe, and record
// the notified status once it lands. It is safe for concurrent use, so events
// deliver in parallel through it.
func (r *tickRun) notify(note notification) {
	// Every notification delivers on its own bounded, uncancelable context, so a
	// slow webhook can't pin a scheduler slot and a shutdown can't drop a health
	// page. Events and health pages take the exact same path.
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()

	if note.Silent {
		r.mark(ctx, note.MarkStatus)

		return
	}

	now := time.Now().UTC()
	if note.ID != "" {
		suppressed, err := r.daemon.store.EventSuppressed(ctx, r.monitor.Name, note.ID, now.Add(-EventDedupeWindow))
		if err != nil {
			// On a dedupe-check failure, prefer sending over silently dropping.
			r.daemon.logger.Error("event dedupe check failed", "monitor", r.monitor.Name, "id", note.ID, "error", err)
		} else if suppressed {
			r.daemon.logger.Debug("event suppressed by id", "monitor", r.monitor.Name, "id", note.ID)

			return
		}
	}

	errs := r.fanOut(ctx, note)
	delivered := len(errs) == 0

	// Every event has a stable ID, so alert history and future dedupe checks use
	// one row per (monitor, id). Health pages do not carry an event ID.
	if err := r.daemon.store.RecordEvent(ctx, storage.Event{
		MonitorName: r.monitor.Name,
		EventID:     note.ID,
		Title:       note.Message.Title,
		Summary:     note.Message.Summary,
		URL:         note.Message.URL,
		Severity:    string(note.Message.Level),
		SentAt:      now,
		Delivered:   delivered,
		Error:       strings.Join(errs, "; "),
	}); err != nil {
		r.daemon.logger.Error("record event failed", "monitor", r.monitor.Name, "error", err)
	}

	if !delivered {
		r.daemon.logger.Error("notification delivery failed", "monitor", r.monitor.Name, "routes", deliveryNames(note.Deliveries), "error", strings.Join(errs, "; "))
		r.recordErrs(errs)

		return
	}

	r.sent.Store(true)
	if note.MarkStatus != "" {
		r.mark(ctx, note.MarkStatus)
	}
}

// fanOut delivers the message to every destination concurrently, returning one
// error string per failed delivery.
func (r *tickRun) fanOut(ctx context.Context, note notification) []string {
	errCh := make(chan string, len(note.Deliveries))
	var wg sync.WaitGroup
	for _, delivery := range note.Deliveries {
		wg.Add(1)
		go func(delivery routes.Delivery) {
			defer wg.Done()
			if err := r.daemon.deliverRoute(ctx, delivery, note.Message); err != nil {
				errCh <- fmt.Sprintf("%s: %v", delivery.Describe(), err)
			}
		}(delivery)
	}
	wg.Wait()
	close(errCh)

	var errs []string
	for e := range errCh {
		errs = append(errs, e)
	}

	return errs
}

func (r *tickRun) mark(ctx context.Context, status monitor.ResultStatus) {
	if err := r.daemon.store.MarkNotified(ctx, r.monitor.Name, status); err != nil {
		r.daemon.logger.Error("mark notified failed", "monitor", r.monitor.Name, "error", err)
	}
}

func (r *tickRun) recordErrs(errs []string) {
	r.notifyMu.Lock()
	r.notifyErrs = append(r.notifyErrs, errs...)
	r.notifyMu.Unlock()
}

// dispatcher delivers a tick's events as the worker streams them: concurrently,
// bounded by a per-tick cap and a concurrency limit, so reading the next worker
// frame never blocks and a runaway monitor can't flood a destination.
type dispatcher struct {
	monitor storage.Monitor
	logger  *slog.Logger
	send    func(notification)
	cap     int64
	slots   chan struct{}
	wg      sync.WaitGroup
	count   atomic.Int64
}

func (d *Daemon) newDispatcher(m storage.Monitor, send func(notification)) *dispatcher {
	limit := int64(maxEventsPerTick)
	if m.MaxEvents > 0 {
		limit = m.MaxEvents
	}

	return &dispatcher{
		monitor: m,
		logger:  d.logger,
		send:    send,
		cap:     limit,
		slots:   make(chan struct{}, maxEventConcurrency),
	}
}

// emit queues one event for delivery and returns at once; the work runs in a
// goroutine bounded by the concurrency limit. Events past the cap are dropped.
func (p *dispatcher) emit(event monitor.Event) {
	if count := p.count.Add(1); count > p.cap {
		if count == p.cap+1 {
			p.logger.Warn("event cap reached, dropping the rest of this tick", "monitor", p.monitor.Name, "cap", p.cap)
		}

		return
	}

	note := notification{
		Deliveries: routes.CloneDeliveries(p.monitor.Deliveries),
		Message:    eventMessage(p.monitor, event),
		ID:         event.ID,
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		// Acquire the slot inside the goroutine, not at the call site, so the
		// worker read loop never blocks; the cap bounds total goroutines.
		p.slots <- struct{}{}
		defer func() { <-p.slots }()

		p.send(note)
	}()
}

// wait blocks until every queued delivery finishes.
func (p *dispatcher) wait() {
	p.wg.Wait()
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

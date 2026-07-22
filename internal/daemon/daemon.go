// Package daemon schedules deployed monitors and supervises their workers.
package daemon

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	monitor "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/model"
	"github.com/saucesteals/monitord/internal/network"
	"github.com/saucesteals/monitord/internal/storage"
)

// Defaults for daemon scheduling.
const (
	DefaultInterval    = 5 * time.Second
	DefaultConcurrency = 8
	defaultTimeout     = 30 * time.Second
	notifyTimeout      = 10 * time.Second
	recordTimeout      = 10 * time.Second

	// EventDedupeWindow is how long an event's ID suppresses repeats.
	EventDedupeWindow = time.Hour

	// eventRetention is how long delivered events are kept for history before a
	// prune reclaims them; pruneInterval throttles how often the prune runs.
	eventRetention = 30 * 24 * time.Hour
	pruneInterval  = time.Hour

	// maxEventsPerTick is the default cap on how many events one tick may
	// deliver, used when a monitor sets no max_events. Emission order wins: the
	// first N send, the rest are dropped and logged. An unbounded stream would
	// let one bad tick flood a route.
	maxEventsPerTick = 20
	// maxEventConcurrency bounds in-flight event deliveries per tick, keeping
	// bursts from tripping a webhook's rate limit.
	maxEventConcurrency = 5

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

	// lastPrune throttles event retention pruning. Only the scan loop touches
	// it, so it needs no lock.
	lastPrune time.Time
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
// what makes a monitor's `every` interval exact: a fixed ticker rounds schedules
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

	if now.Sub(d.lastPrune) >= pruneInterval {
		d.lastPrune = now
		if pruned, err := d.store.PruneEvents(ctx, now.Add(-eventRetention)); err != nil {
			d.logger.Error("prune events failed", "error", err)
		} else if pruned > 0 {
			d.logger.Info("pruned old events", "count", pruned)
		}
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

		if err := d.runTick(ctx, m); err != nil {
			d.logger.Error("monitor run failed", "monitor", m.Name, "error", err)
		}
	}()
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

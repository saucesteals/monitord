// Package daemon supervises durable V5 deployments and their outbox.
package daemon

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/secrets"
	"github.com/saucesteals/monitord/internal/storage"
)

const (
	DefaultInterval    = 5 * time.Second
	DefaultConcurrency = 8
)

type Daemon struct {
	store          *storage.Store
	paths          config.Paths
	logger         *slog.Logger
	interval       time.Duration
	wg             sync.WaitGroup
	workersMu      sync.Mutex
	v5Workers      map[string]*v5Worker
	v5Cancel       map[string]context.CancelFunc
	v5Failures     map[string]int
	v5NextStart    map[string]time.Time
	deliverySender DeliverySender
	secretKey      []byte
}

func New(store *storage.Store, paths config.Paths, logger *slog.Logger, interval time.Duration, _ int) *Daemon {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	d := &Daemon{store: store, paths: paths, logger: logger, interval: interval, v5Workers: map[string]*v5Worker{}, v5Cancel: map[string]context.CancelFunc{}, v5Failures: map[string]int{}, v5NextStart: map[string]time.Time{}, secretKey: key}
	d.deliverySender = daemonDeliverySender{daemon: d}
	return d
}

func (d *Daemon) SetDeliverySender(sender DeliverySender) { d.deliverySender = sender }

func (d *Daemon) Run(ctx context.Context) error {
	lock, err := acquireDaemonLock(d.paths)
	if err != nil {
		return err
	}
	defer lock.Close()
	d.logger.Info("monitord daemon started", "root", d.paths.Root, "max_wait", d.interval)
	return d.runV5(ctx)
}

func (d *Daemon) runV5(ctx context.Context) error {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	defer d.stopV5Workers()
	var outbox *outboxWorker
	if d.deliverySender != nil {
		outbox = newOutboxWorker(d.store, d.deliverySender, fmt.Sprintf("daemon-%d", time.Now().UnixNano()))
	}
	for {
		if err := d.reconcileV5(ctx); err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("v5 reconcile failed", "error", err)
		}
		if outbox != nil {
			if _, err := outbox.process(ctx); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Error("outbox processing failed", "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (d *Daemon) reconcileV5(ctx context.Context) error {
	now := time.Now().UTC()
	_, _ = d.store.ExpireDueDeployments(ctx, now)
	deps, err := d.store.ListRuntimeDeployments(ctx)
	if err != nil {
		return err
	}
	active := map[string]bool{}
	for _, dep := range deps {
		active[dep.ID] = true
		secretMap, fingerprint, err := d.resolveDeploymentSecrets(dep)
		if err != nil {
			return err
		}
		d.workersMu.Lock()
		existing, next := d.v5Workers[dep.ID], d.v5NextStart[dep.ID]
		d.workersMu.Unlock()
		if existing != nil && existing.secretFingerprint != fingerprint {
			d.workersMu.Lock()
			delete(d.v5Workers, dep.ID)
			cancel := d.v5Cancel[dep.ID]
			delete(d.v5Cancel, dep.ID)
			d.workersMu.Unlock()
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = existing.stop(stopCtx, "secret change")
			stopCancel()
			if cancel != nil {
				cancel()
			}
			existing = nil
		}
		if existing != nil || now.Before(next) {
			continue
		}
		if err = d.launchV5(ctx, dep, secretMap, fingerprint); err != nil {
			d.workersMu.Lock()
			d.v5Failures[dep.ID]++
			attempt := min(d.v5Failures[dep.ID], 8)
			d.v5NextStart[dep.ID] = now.Add(time.Second * time.Duration(1<<attempt))
			d.workersMu.Unlock()
			d.logger.Error("v5 worker launch failed", "deployment", dep.Name, "error", err)
		}
	}
	d.workersMu.Lock()
	type stoppingWorker struct {
		worker *v5Worker
		cancel context.CancelFunc
	}
	var stopping []stoppingWorker
	for id, cancel := range d.v5Cancel {
		if !active[id] {
			stopping = append(stopping, stoppingWorker{d.v5Workers[id], cancel})
			delete(d.v5Cancel, id)
			delete(d.v5Workers, id)
		}
	}
	d.workersMu.Unlock()
	for _, item := range stopping {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if item.worker != nil {
			_ = item.worker.stop(stopCtx, "deployment inactive")
		}
		cancel()
		if item.cancel != nil {
			item.cancel()
		}
	}
	return nil
}

func (d *Daemon) resolveDeploymentSecrets(dep storage.RuntimeDeployment) (map[string]map[string]string, string, error) {
	var described monitord.MonitorFrame
	if err := json.Unmarshal(dep.Describe, &described); err != nil {
		return nil, "", fmt.Errorf("decode artifact describe: %w", err)
	}
	refs := described.Plan.SecretRefs()
	requested := make([]secrets.Ref, 0, len(refs))
	for _, r := range refs {
		requested = append(requested, secrets.Ref{Group: r.Group, Key: r.Key, Required: r.Required})
	}
	values, err := secrets.Resolve(requested, secrets.Sources{Root: d.paths.Root, MonitorDir: dep.SourceDir})
	if err != nil {
		return nil, "", err
	}
	result := map[string]map[string]string{}
	for _, v := range values {
		if result[v.Ref.Group] == nil {
			result[v.Ref.Group] = map[string]string{}
		}
		result[v.Ref.Group][v.Ref.Key] = v.Value
	}
	return result, secrets.Fingerprint(d.secretKey, values), nil
}

func (d *Daemon) launchV5(parent context.Context, dep storage.RuntimeDeployment, secretMap map[string]map[string]string, fingerprint string) error {
	gen, err := d.store.ActivateGeneration(parent, storage.GenerationActivation{DeploymentID: dep.ID, ArtifactID: dep.ArtifactID, ConfigRevision: dep.ConfigRevision, SecretFingerprint: []byte(fingerprint)})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	worker, err := startV5Worker(ctx, d.logger, dep, gen, secretMap)
	if err != nil {
		cancel()
		return err
	}
	worker.secretFingerprint = fingerprint
	d.workersMu.Lock()
	d.v5Workers[dep.ID] = worker
	d.v5Cancel[dep.ID] = cancel
	d.v5Failures[dep.ID] = 0
	d.workersMu.Unlock()
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		err := worker.serve(ctx, d.store)
		d.workersMu.Lock()
		if d.v5Workers[dep.ID] == worker {
			delete(d.v5Workers, dep.ID)
			delete(d.v5Cancel, dep.ID)
			if err != nil && !errors.Is(err, context.Canceled) {
				d.v5Failures[dep.ID]++
				attempt := min(d.v5Failures[dep.ID], 8)
				d.v5NextStart[dep.ID] = time.Now().Add(time.Second * time.Duration(1<<attempt))
			}
		}
		d.workersMu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("v5 worker stopped", "deployment", dep.Name, "error", err)
		}
	}()
	return nil
}

func (d *Daemon) stopV5Workers() {
	d.workersMu.Lock()
	workers := make([]*v5Worker, 0, len(d.v5Workers))
	cancels := make([]context.CancelFunc, 0, len(d.v5Workers))
	for id, w := range d.v5Workers {
		workers = append(workers, w)
		cancels = append(cancels, d.v5Cancel[id])
	}
	d.v5Workers = map[string]*v5Worker{}
	d.v5Cancel = map[string]context.CancelFunc{}
	d.workersMu.Unlock()
	for i, w := range workers {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = w.stop(ctx, "daemon shutdown")
		cancel()
		if cancels[i] != nil {
			cancels[i]()
		}
	}
	d.wg.Wait()
}

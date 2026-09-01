// Package daemon supervises durable deployments and their outbox.
package daemon

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/saucesteals/monitord/internal/config"
	"github.com/saucesteals/monitord/internal/storage"
)

const (
	DefaultInterval        = 5 * time.Second
	deliveryAttemptTimeout = 30 * time.Second
)

type Daemon struct {
	store          *storage.Store
	paths          config.Paths
	logger         *slog.Logger
	interval       time.Duration
	wg             sync.WaitGroup
	workersMu      sync.Mutex
	workers        map[string]*workerSlot
	deliverySender DeliverySender
	secretKey      []byte
}

func New(store *storage.Store, paths config.Paths, logger *slog.Logger, interval time.Duration) *Daemon {
	if interval <= 0 {
		interval = DefaultInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	d := &Daemon{store: store, paths: paths, logger: logger, interval: interval, workers: map[string]*workerSlot{}, secretKey: key}
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
	return d.run(ctx)
}

func (d *Daemon) run(ctx context.Context) error {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	defer d.stopWorkers()
	var outboxDone chan struct{}
	if d.deliverySender != nil {
		outboxDone = make(chan struct{})
		outbox := newOutboxWorker(d.store, d.deliverySender, fmt.Sprintf("daemon-%d", time.Now().UnixNano()))
		go func() {
			defer close(outboxDone)
			d.runOutbox(ctx, outbox)
		}()
	}
	defer func() {
		if outboxDone != nil {
			<-outboxDone
		}
	}()
	for {
		if err := d.reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("reconcile failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (d *Daemon) runOutbox(ctx context.Context, outbox *outboxWorker) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		if _, err := outbox.process(ctx); err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("outbox processing failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

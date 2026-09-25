package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/saucesteals/monitord/internal/storage"
)

// runMaintenance is independent of delivery latency. Each store call releases
// its transaction before the next page, with a one-second yield between batches.
func (d *Daemon) runMaintenance(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if d.store.CheckpointPressure() {
			timer.Reset(5 * time.Second)
			continue
		}
		now := time.Now().UTC()
		started := time.Now()
		outbox, outboxErr := d.store.PruneTerminalOutbox(ctx, now)
		outboxDuration := time.Since(started)
		started = time.Now()
		ledger, ledgerErr := d.store.PruneRetiredTransactions(ctx, now)
		ledgerDuration := time.Since(started)
		if ctx.Err() != nil {
			return
		}
		d.logMaintenance("outbox", outbox, outboxDuration, outboxErr)
		d.logMaintenance("transactions", ledger, ledgerDuration, ledgerErr)
		delay := maintenanceDelay(time.Now().UTC(), outbox, ledger, outboxErr, ledgerErr)
		timer.Reset(delay)
	}
}

func maintenanceDelay(now time.Time, outbox, ledger storage.PruneResult, outboxErr, ledgerErr error) time.Duration {
	if outboxErr != nil || ledgerErr != nil {
		return 5 * time.Second
	}
	if outbox.More || ledger.More {
		return time.Second
	}
	next := outbox.NextSweepAt
	if ledger.NextSweepAt.Before(next) {
		next = ledger.NextSweepAt
	}

	return max(time.Second, next.Sub(now))
}

func (d *Daemon) logMaintenance(kind string, result storage.PruneResult, duration time.Duration, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		d.logger.Error("maintenance failed", "kind", kind, "error", err)
	} else if duration >= time.Second {
		d.logger.Warn("slow maintenance page", "kind", kind, "duration", duration, "scanned", result.Scanned, "deleted", result.Deleted, "more", result.More)
	} else if result.Scanned > 0 {
		d.logger.Debug("maintenance page", "kind", kind, "scanned", result.Scanned, "deleted", result.Deleted, "more", result.More)
	}
}

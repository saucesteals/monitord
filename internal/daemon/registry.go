package daemon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/saucesteals/monitord/internal/secrets"
	"github.com/saucesteals/monitord/internal/storage"
)

// A continuous worker must remain alive long enough to prove it is not in a
// crash loop before readiness is promoted to health.
const workerStableAfter = time.Minute

type workerSlot struct {
	worker    *worker
	cancel    context.CancelFunc
	failures  int
	nextStart time.Time
	startedAt time.Time
}

func (d *Daemon) launch(parent context.Context, dep storage.RuntimeDeployment, secretMap map[string]map[string]string, fingerprint string, redactor secrets.Redactor) error {
	gen, err := d.store.ActivateGeneration(parent, storage.GenerationActivation{DeploymentID: dep.ID, ArtifactID: dep.ArtifactID, ConfigRevision: dep.ConfigRevision, StateRevision: dep.StateRevision, SecretFingerprint: []byte(fingerprint)})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	handshakeCtx, handshakeCancel := context.WithTimeout(parent, 10*time.Second)
	defer handshakeCancel()
	worker, err := startWorker(handshakeCtx, d.logger, dep, gen, secretMap, redactor)
	if err != nil {
		cancel()
		safeError := redactor.Redact(err.Error())
		transition, healthErr := d.store.RecordGenerationFailure(context.Background(), dep.ID, gen.Generation, safeError)
		logHealthTransition(d.logger, dep.Name, gen.Generation, transition)
		stopErr := d.store.MarkGenerationStopped(context.Background(), dep.ID, gen.Generation, "launch failed", safeError, time.Now().UTC())
		return errors.Join(errors.New(safeError), healthErr, stopErr)
	}
	if err = d.store.MarkGenerationReady(parent, dep.ID, gen.Generation, time.Now().UTC()); err != nil {
		worker.kill()
		cancel()
		safeError := redactor.Redact(err.Error())
		transition, healthErr := d.store.RecordGenerationFailure(context.Background(), dep.ID, gen.Generation, safeError)
		logHealthTransition(d.logger, dep.Name, gen.Generation, transition)
		stopErr := d.store.MarkGenerationStopped(context.Background(), dep.ID, gen.Generation, "readiness persistence failed", safeError, time.Now().UTC())
		return errors.Join(errors.New(safeError), healthErr, stopErr)
	}
	worker.secretFingerprint = fingerprint
	d.workersMu.Lock()
	slot := d.workers[dep.ID]
	if slot == nil {
		slot = &workerSlot{}
		d.workers[dep.ID] = slot
	}
	slot.worker = worker
	slot.cancel = cancel
	slot.nextStart = time.Time{}
	slot.startedAt = time.Now()
	d.workersMu.Unlock()
	d.logger.Info("worker ready", "deployment", dep.Name, "generation", gen.Generation, "plan", worker.planKind)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer cancel()
		err := worker.serve(ctx, d.store)
		safeError := ""
		if err != nil {
			safeError = worker.redact(err.Error())
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			// A daemon-side persistence or protocol failure can strand a worker
			// waiting for an acknowledgement that will never arrive.
			worker.kill()
		}
		var stoppedErr *workerStoppedError
		isStoppedErr := errors.As(err, &stoppedErr)
		unexpected := err != nil && !errors.Is(err, context.Canceled) && (!isStoppedErr || !stoppedErr.failureReported)
		if unexpected {
			transition, healthErr := d.store.RecordGenerationFailure(context.Background(), dep.ID, gen.Generation, safeError)
			if healthErr != nil && !errors.Is(healthErr, storage.ErrGenerationFenced) {
				d.logger.Error("record worker failure", "deployment", dep.Name, "generation", gen.Generation, "error", healthErr)
			}
			logHealthTransition(d.logger, dep.Name, gen.Generation, transition)
		}
		stopReason := worker.stopReason()
		if stopReason == "" {
			stopReason = "worker exited"
		}
		stopError := ""
		if err != nil && !errors.Is(err, context.Canceled) {
			stopError = safeError
		}
		if stopErr := d.store.MarkGenerationStopped(context.Background(), dep.ID, gen.Generation, stopReason, stopError, time.Now().UTC()); stopErr != nil {
			d.logger.Error("record worker stop", "deployment", dep.Name, "generation", gen.Generation, "error", stopErr)
		}
		d.workersMu.Lock()
		slot := d.workers[dep.ID]
		if slot != nil && slot.worker == worker {
			slot.worker = nil
			slot.cancel = nil
			if err != nil && !errors.Is(err, context.Canceled) {
				if time.Since(slot.startedAt) >= workerStableAfter {
					slot.failures = 0
				}
				slot.failures++
				attempt := min(slot.failures, 8)
				slot.nextStart = time.Now().Add(time.Second * time.Duration(1<<attempt))
			}
		}
		d.workersMu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("worker stopped", "deployment", dep.Name, "generation", gen.Generation, "error", safeError)
		}
	}()
	return nil
}

func (d *Daemon) stopWorkers() {
	d.workersMu.Lock()
	slots := make([]*workerSlot, 0, len(d.workers))
	for _, slot := range d.workers {
		slots = append(slots, slot)
	}
	d.workers = map[string]*workerSlot{}
	d.workersMu.Unlock()
	var stopping sync.WaitGroup
	for _, slot := range slots {
		stopping.Add(1)
		go func(slot *workerSlot) {
			defer stopping.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if slot.worker != nil {
				_ = slot.worker.stop(ctx, "daemon shutdown")
			} else if slot.cancel != nil {
				slot.cancel()
			}
		}(slot)
	}
	stopping.Wait()
	d.wg.Wait()
}

package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/saucesteals/monitord/internal/storage"
)

type workerSlot struct {
	worker    *worker
	cancel    context.CancelFunc
	failures  int
	nextStart time.Time
	startedAt time.Time
}

func (d *Daemon) launch(parent context.Context, dep storage.RuntimeDeployment, secretMap map[string]map[string]string, fingerprint string) error {
	gen, err := d.store.ActivateGeneration(parent, storage.GenerationActivation{DeploymentID: dep.ID, ArtifactID: dep.ArtifactID, ConfigRevision: dep.ConfigRevision, StateRevision: dep.StateRevision, SecretFingerprint: []byte(fingerprint)})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	handshakeCtx, handshakeCancel := context.WithTimeout(ctx, 10*time.Second)
	defer handshakeCancel()
	worker, err := startWorker(handshakeCtx, d.logger, dep, gen, secretMap)
	if err != nil {
		cancel()
		return err
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
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		err := worker.serve(ctx, d.store)
		d.workersMu.Lock()
		slot := d.workers[dep.ID]
		if slot != nil && slot.worker == worker {
			slot.worker = nil
			slot.cancel = nil
			if err != nil && !errors.Is(err, context.Canceled) {
				if time.Since(slot.startedAt) >= time.Minute {
					slot.failures = 0
				}
				slot.failures++
				attempt := min(slot.failures, 8)
				slot.nextStart = time.Now().Add(time.Second * time.Duration(1<<attempt))
			}
		}
		d.workersMu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("worker stopped", "deployment", dep.Name, "error", err)
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
	for _, slot := range slots {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if slot.worker != nil {
			_ = slot.worker.stop(ctx, "daemon shutdown")
		}
		cancel()
		if slot.cancel != nil {
			slot.cancel()
		}
	}
	d.wg.Wait()
}

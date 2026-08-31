package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/saucesteals/monitord/internal/storage"
)

func (d *Daemon) launch(parent context.Context, dep storage.RuntimeDeployment, secretMap map[string]map[string]string, fingerprint string) error {
	gen, err := d.store.ActivateGeneration(parent, storage.GenerationActivation{DeploymentID: dep.ID, ArtifactID: dep.ArtifactID, ConfigRevision: dep.ConfigRevision, SecretFingerprint: []byte(fingerprint)})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	worker, err := startWorker(ctx, d.logger, dep, gen, secretMap)
	if err != nil {
		cancel()
		return err
	}
	worker.secretFingerprint = fingerprint
	d.workersMu.Lock()
	d.workers[dep.ID] = worker
	d.workerCancels[dep.ID] = cancel
	d.workerFailures[dep.ID] = 0
	d.workersMu.Unlock()
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		err := worker.serve(ctx, d.store)
		d.workersMu.Lock()
		if d.workers[dep.ID] == worker {
			delete(d.workers, dep.ID)
			delete(d.workerCancels, dep.ID)
			if err != nil && !errors.Is(err, context.Canceled) {
				d.workerFailures[dep.ID]++
				attempt := min(d.workerFailures[dep.ID], 8)
				d.workerNextStart[dep.ID] = time.Now().Add(time.Second * time.Duration(1<<attempt))
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
	workers := make([]*worker, 0, len(d.workers))
	cancels := make([]context.CancelFunc, 0, len(d.workers))
	for id, w := range d.workers {
		workers = append(workers, w)
		cancels = append(cancels, d.workerCancels[id])
	}
	d.workers = map[string]*worker{}
	d.workerCancels = map[string]context.CancelFunc{}
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

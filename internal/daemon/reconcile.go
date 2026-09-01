package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/secrets"
	"github.com/saucesteals/monitord/internal/storage"
)

func (d *Daemon) reconcile(ctx context.Context) error {
	now := time.Now().UTC()
	var reconcileErrs []error
	if _, err := d.store.DeactivateDueDeployments(ctx, now); err != nil {
		reconcileErrs = append(reconcileErrs, fmt.Errorf("deactivate deployments: %w", err))
	}
	deps, err := d.store.ListRuntimeDeployments(ctx)
	if err != nil {
		return err
	}
	active := map[string]bool{}
	for _, dep := range deps {
		active[dep.ID] = true
		secretMap, fingerprint, err := d.resolveDeploymentSecrets(dep)
		if err != nil {
			d.stopWorker(dep.ID, "secret resolution failed")
			d.logger.Error("resolve worker secrets failed", "deployment", dep.Name, "error", err)
			reconcileErrs = append(reconcileErrs, fmt.Errorf("resolve secrets for %s: %w", dep.Name, err))
			continue
		}
		d.workersMu.Lock()
		slot := d.workers[dep.ID]
		var existing *worker
		var next time.Time
		if slot != nil {
			existing = slot.worker
			next = slot.nextStart
		}
		d.workersMu.Unlock()
		if existing != nil && (existing.secretFingerprint != fingerprint ||
			existing.generation.Generation != dep.ActiveGeneration ||
			existing.deployment.ArtifactID != dep.ArtifactID ||
			existing.deployment.ConfigRevision != dep.ConfigRevision) {
			d.stopWorker(dep.ID, "deployment snapshot changed")
			existing = nil
		}
		if existing != nil || now.Before(next) {
			continue
		}
		if err = d.launch(ctx, dep, secretMap, fingerprint); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			d.workersMu.Lock()
			slot := d.workers[dep.ID]
			if slot == nil {
				slot = &workerSlot{}
				d.workers[dep.ID] = slot
			}
			slot.failures++
			attempt := min(slot.failures, 8)
			slot.nextStart = now.Add(time.Second * time.Duration(1<<attempt))
			d.workersMu.Unlock()
			d.logger.Error("worker launch failed", "deployment", dep.Name, "error", err)
			reconcileErrs = append(reconcileErrs, fmt.Errorf("launch %s: %w", dep.Name, err))
		}
	}
	d.workersMu.Lock()
	var stopping []*workerSlot
	for id, slot := range d.workers {
		if !active[id] {
			stopping = append(stopping, slot)
			delete(d.workers, id)
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
	return errors.Join(reconcileErrs...)
}

func (d *Daemon) stopWorker(id, reason string) {
	d.workersMu.Lock()
	slot := d.workers[id]
	delete(d.workers, id)
	d.workersMu.Unlock()
	if slot == nil {
		return
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if slot.worker != nil {
		_ = slot.worker.stop(stopCtx, reason)
	}
	cancel()
	if slot.cancel != nil {
		slot.cancel()
	}
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

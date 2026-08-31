package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/secrets"
	"github.com/saucesteals/monitord/internal/storage"
)

func (d *Daemon) reconcile(ctx context.Context) error {
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
		existing, next := d.workers[dep.ID], d.workerNextStart[dep.ID]
		d.workersMu.Unlock()
		if existing != nil && existing.secretFingerprint != fingerprint {
			d.workersMu.Lock()
			delete(d.workers, dep.ID)
			cancel := d.workerCancels[dep.ID]
			delete(d.workerCancels, dep.ID)
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
		if err = d.launch(ctx, dep, secretMap, fingerprint); err != nil {
			d.workersMu.Lock()
			d.workerFailures[dep.ID]++
			attempt := min(d.workerFailures[dep.ID], 8)
			d.workerNextStart[dep.ID] = now.Add(time.Second * time.Duration(1<<attempt))
			d.workersMu.Unlock()
			d.logger.Error("worker launch failed", "deployment", dep.Name, "error", err)
		}
	}
	d.workersMu.Lock()
	type stoppingWorker struct {
		worker *worker
		cancel context.CancelFunc
	}
	var stopping []stoppingWorker
	for id, cancel := range d.workerCancels {
		if !active[id] {
			stopping = append(stopping, stoppingWorker{d.workers[id], cancel})
			delete(d.workerCancels, id)
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

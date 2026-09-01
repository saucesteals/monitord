package daemon

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/monitor"
	"github.com/saucesteals/monitord/internal/routes"
	"github.com/saucesteals/monitord/internal/secrets"
	"github.com/saucesteals/monitord/internal/storage"
)

type worker struct {
	deployment        storage.RuntimeDeployment
	generation        storage.ActiveGeneration
	cmd               *exec.Cmd
	in                io.WriteCloser
	out               *bufio.Reader
	readCh            chan readResult
	pgid              int
	logger            *slog.Logger
	writeMu           sync.Mutex
	lifecycleMu       sync.Mutex
	done              chan error
	secretFingerprint string
	planKind          string
	requestedStop     string
	redactor          secrets.Redactor
}
type readResult struct {
	v   monitord.WorkerFrame
	err error
}
type workerStoppedError struct {
	message         string
	failureReported bool
}

func (e *workerStoppedError) Error() string { return e.message }

func startWorker(ctx context.Context, logger *slog.Logger, dep storage.RuntimeDeployment, generation storage.ActiveGeneration, secretMap map[string]map[string]string, redactor secrets.Redactor) (*worker, error) {
	cmd := exec.Command(dep.ArtifactPath, monitord.FlagWorker)
	setMonitorProcessGroup(cmd)
	cmd.Env = monitor.Env()
	// Workers execute beside the immutable artifact, never from mutable source.
	cmd.Dir = filepath.Dir(dep.ArtifactPath)
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	w := &worker{deployment: dep, generation: generation, cmd: cmd, in: in, out: bufio.NewReaderSize(out, 64<<10), readCh: make(chan readResult, 1), logger: logger, done: make(chan error, 1), redactor: redactor}
	w.pgid, _ = monitorProcessGroup(cmd)
	go func() {
		digest := sha256.New()
		captured, _ := io.Copy(digest, io.LimitReader(stderr, 64<<10))
		discarded, _ := io.Copy(io.Discard, stderr)
		if captured+discarded > 0 {
			logger.Warn("worker wrote stderr; content suppressed", "deployment", dep.Name, "generation", generation.Generation, "bytes", captured+discarded, "captured_bytes", captured, "sha256", hex.EncodeToString(digest.Sum(nil)))
		}
	}()
	go func() { w.done <- cmd.Wait() }()
	go w.readLoop()
	hello := monitord.Hello{Version: monitord.ProtocolVersion{Major: monitord.ProtocolMajor}, DeploymentID: dep.ID, DeploymentName: dep.Name, Generation: uint64(generation.Generation), WorkerToken: hex.EncodeToString(generation.WorkerToken), ArtifactHash: dep.ArtifactHash, ConfigHash: dep.ConfigHash, StateRevision: dep.StateRevision, State: dep.State, Checkpoints: dep.Checkpoints, Secrets: secretMap, Policy: monitord.DeploymentPolicy{Health: monitord.HealthPolicy{FailureThreshold: dep.FailureThreshold}, Events: monitord.EventPolicy{MaxPerTransaction: dep.MaxEventsPerTransaction, Retention: dep.EventRetention}}}
	if err = w.send(ctx, monitord.DaemonFrame{Type: "hello", Hello: &hello}); err != nil {
		w.kill()
		return nil, err
	}
	msg, err := w.read(ctx)
	if err != nil {
		w.kill()
		return nil, fmt.Errorf("worker monitor handshake: %w", err)
	}
	if msg.Type != "monitor" {
		w.kill()
		return nil, fmt.Errorf("expected monitor frame, got %q", msg.Type)
	}
	var persisted monitord.MonitorFrame
	if err = json.Unmarshal(dep.Describe, &persisted); err != nil {
		w.kill()
		return nil, fmt.Errorf("decode persisted describe: %w", err)
	}
	w.planKind = persisted.Plan.Kind
	want, marshalErr := json.Marshal(struct {
		Info monitord.Info
		Plan monitord.PlanDescription
	}{persisted.Info, persisted.Plan})
	if marshalErr != nil {
		w.kill()
		return nil, marshalErr
	}
	got, _ := json.Marshal(struct {
		Info monitord.Info
		Plan monitord.PlanDescription
	}{msg.Monitor.Info, msg.Monitor.Plan})
	if !bytes.Equal(want, got) {
		w.kill()
		return nil, errors.New("worker monitor frame differs from persisted artifact description")
	}
	if err = w.send(ctx, monitord.DaemonFrame{Type: "start", Start: &monitord.Start{Plan: persisted.Plan}}); err != nil {
		w.kill()
		return nil, err
	}
	msg, err = w.read(ctx)
	if err != nil {
		w.kill()
		return nil, err
	}
	if msg.Type == "stopped" && msg.Stopped.Generation == uint64(generation.Generation) && msg.Stopped.Error != "" {
		w.kill()
		return nil, fmt.Errorf("worker start: %s", msg.Stopped.Error)
	}
	if msg.Type != "ready" || msg.Ready.Generation != uint64(generation.Generation) {
		w.kill()
		return nil, errors.New("worker ready generation mismatch")
	}
	return w, nil
}

func (w *worker) serve(ctx context.Context, store *storage.Store) error {
	var stable <-chan time.Time
	var stableTimer *time.Timer
	if w.planKind == "continuous" {
		stableTimer = time.NewTimer(workerStableAfter)
		stable = stableTimer.C
		defer stableTimer.Stop()
	}
	for {
		var msg monitord.WorkerFrame
		select {
		case result := <-w.readCh:
			if result.err != nil {
				return result.err
			}
			msg = result.v
		case <-stable:
			stable = nil
			transition, err := store.MarkGenerationStable(ctx, w.deployment.ID, w.generation.Generation, time.Now().UTC())
			if err != nil {
				return err
			}
			logHealthTransition(w.logger, w.deployment.Name, w.generation.Generation, transition)
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
		var err error
		switch msg.Type {
		case "transaction":
			if err = w.transaction(ctx, store, *msg.Transaction); err != nil {
				return err
			}
		case "run":
			if msg.Run.Generation != uint64(w.generation.Generation) {
				return storage.ErrGenerationFenced
			}
			transition, healthErr := store.RecordRun(ctx, storage.RunOutcome{DeploymentID: w.deployment.ID, Generation: w.generation.Generation, Status: storage.RunStatus(msg.Run.Status), Duration: msg.Run.Duration, Error: w.redact(msg.Run.Error), FinishedAt: time.Now().UTC()})
			if healthErr != nil {
				return healthErr
			}
			logHealthTransition(w.logger, w.deployment.Name, w.generation.Generation, transition)
		case "stopped":
			if msg.Stopped.Generation != uint64(w.generation.Generation) {
				return storage.ErrGenerationFenced
			}
			if msg.Stopped.Error != "" {
				return &workerStoppedError{message: msg.Stopped.Error, failureReported: msg.Stopped.RunFailureReported}
			}
			if !msg.Stopped.Clean {
				return &workerStoppedError{message: "worker stopped unsuccessfully", failureReported: msg.Stopped.RunFailureReported}
			}
			return nil
		default:
			return fmt.Errorf("unexpected worker frame %q", msg.Type)
		}
	}
}

func logHealthTransition(logger *slog.Logger, deployment string, generation int64, transition storage.HealthTransition) {
	switch transition {
	case storage.HealthBecameUnhealthy:
		logger.Warn("deployment unhealthy", "deployment", deployment, "generation", generation)
	case storage.HealthRecovered:
		logger.Info("deployment recovered", "deployment", deployment, "generation", generation)
	}
}

func (w *worker) redact(message string) string { return w.redactor.Redact(message) }

func (w *worker) transaction(ctx context.Context, store *storage.Store, wire monitord.TransactionFrame) error {
	if wire.DeploymentID != w.deployment.ID || wire.Generation != uint64(w.generation.Generation) || wire.WorkerToken != hex.EncodeToString(w.generation.WorkerToken) {
		return storage.ErrGenerationFenced
	}
	if err := verifyWireHash(wire); err != nil {
		return err
	}
	if len(wire.Events) > w.deployment.MaxEventsPerTransaction {
		return fmt.Errorf("transaction has %d events; deployment limit is %d", len(wire.Events), w.deployment.MaxEventsPerTransaction)
	}
	bindings, err := store.ListActiveBindings(ctx, w.deployment.ID)
	if err != nil {
		return err
	}
	checkpoints := make([]storage.CheckpointMutation, 0, len(wire.Checkpoints))
	keys := make([]string, 0, len(wire.Checkpoints))
	for k := range wire.Checkpoints {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		checkpoints = append(checkpoints, storage.CheckpointMutation{Source: k, Value: wire.Checkpoints[k]})
	}
	events := make([]storage.OutboxEvent, 0, len(wire.Events))
	for i, event := range wire.Events {
		message := routes.Message{
			Title: event.Title, Message: event.Body, URL: event.URL,
			Level: eventLevel(event.Severity), Fields: dataFields(event.Data),
			Footer: w.deployment.Name,
		}
		deliveries := make([]storage.OutboxDelivery, 0, len(bindings))
		for _, b := range bindings {
			deliveries = append(deliveries, storage.OutboxDelivery{DestinationID: b.ID, DestinationRevision: b.Revision})
		}
		sum := sha256.Sum256([]byte(w.deployment.ID + "\x00" + strconv.FormatUint(wire.Generation, 10) + "\x00" + strconv.FormatUint(wire.Sequence, 10) + "\x00" + strconv.Itoa(i)))
		events = append(events, storage.OutboxEvent{OutboxID: hex.EncodeToString(sum[:]), EventID: event.ID, Message: message, Deliveries: deliveries})
	}
	hash, _ := hex.DecodeString(wire.PayloadHash)
	var fixed [sha256.Size]byte
	copy(fixed[:], hash)
	frame := storage.TransactionFrame{DeploymentID: wire.DeploymentID, Generation: int64(wire.Generation), WorkerToken: w.generation.WorkerToken, Sequence: int64(wire.Sequence), BaseStateRevision: wire.BaseStateRevision, NextState: wire.NextState, Checkpoints: checkpoints, Events: events, PayloadHash: fixed}
	ack, err := store.ApplyTransaction(ctx, frame)
	if err != nil {
		return err
	}
	return w.send(ctx, monitord.DaemonFrame{Type: "ack", Ack: &monitord.TransactionAck{DeploymentID: ack.DeploymentID, Generation: uint64(ack.Generation), Sequence: uint64(ack.Sequence), PayloadHash: wire.PayloadHash, ResultRevision: ack.ResultRevision, Status: ack.Status}})
}

func verifyWireHash(frame monitord.TransactionFrame) error {
	sum := monitord.HashTransactionFrame(frame)
	want := hex.EncodeToString(sum[:])
	if !bytes.Equal([]byte(frame.PayloadHash), []byte(want)) {
		return fmt.Errorf("%w: got %s want %s", storage.ErrPayloadConflict, frame.PayloadHash, want)
	}
	return nil
}
func (w *worker) send(ctx context.Context, v monitord.DaemonFrame) error {
	if err := v.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if len(raw) > monitord.MaxFrameBytes {
		return errors.New("outbound frame too large")
	}
	result := make(chan error, 1)
	go func() {
		w.writeMu.Lock()
		defer w.writeMu.Unlock()
		_, writeErr := io.Copy(w.in, bytes.NewReader(raw))
		result <- writeErr
	}()
	select {
	case err = <-result:
		return err
	case <-ctx.Done():
		// Closing the pipe releases a write blocked on a worker that no longer
		// reads stdin. The caller will retire this worker.
		_ = w.in.Close()
		return ctx.Err()
	}
}
func (w *worker) read(ctx context.Context) (monitord.WorkerFrame, error) {
	select {
	case r := <-w.readCh:
		return r.v, r.err
	case <-ctx.Done():
		return monitord.WorkerFrame{}, ctx.Err()
	}
}
func (w *worker) readLoop() {
	for {
		raw := make([]byte, 0, 64<<10)
		for {
			part, err := w.out.ReadSlice('\n')
			if len(raw)+len(part) > monitord.MaxFrameBytes {
				w.readCh <- readResult{err: errors.New("protocol frame exceeds maximum size")}
				return
			}
			raw = append(raw, part...)
			if err == nil {
				break
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			w.readCh <- readResult{err: err}
			return
		}
		v, decodeErr := monitord.DecodeWorkerFrame(bytes.NewReader(raw))
		w.readCh <- readResult{v: v, err: decodeErr}
		if decodeErr != nil {
			return
		}
	}
}
func (w *worker) stop(ctx context.Context, reason string) error {
	w.lifecycleMu.Lock()
	w.requestedStop = reason
	w.lifecycleMu.Unlock()
	deadline, _ := ctx.Deadline()
	_ = w.send(ctx, monitord.DaemonFrame{Type: "stop", Stop: &monitord.Stop{Reason: reason, Deadline: deadline.UTC().Format(time.RFC3339Nano)}})
	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
		w.kill()
		return ctx.Err()
	}
}
func (w *worker) stopReason() string {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	return w.requestedStop
}
func (w *worker) kill() {
	_ = w.in.Close()
	terminateMonitorProcessGroup(w.pgid, w.logger)
	if w.cmd != nil && w.cmd.Process != nil {
		_ = w.cmd.Process.Kill()
	}
}

package daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	monitord "github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/internal/monitor"
	"github.com/saucesteals/monitord/internal/storage"
)

// maxCapturedOutput bounds per-run stdout/stderr retained for the runs table.
const maxCapturedOutput = 1024 * 1024

// handshakeTimeout bounds how long a freshly started worker may take to accept
// its network assignment.
const handshakeTimeout = 30 * time.Second

// worker is a long-lived monitor process. It is started once per artifact and
// handles every tick for that monitor, so HTTP clients, connection pools, and
// TLS sessions survive between runs.
type worker struct {
	binaryPath string
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stdout     *bufio.Scanner
	pgid       int
	clients    int

	stderr   buffer
	exit     chan error
	runMu    sync.Mutex
	killOnce sync.Once
	exited   bool
	exitErr  error
}

// tickOutput collects everything one tick produced. Events are not collected
// here: they are delivered as they arrive, so only the final result and the
// captured stdout survive the tick.
type tickOutput struct {
	Result monitord.Result
	Stdout string
}

// start launches a worker process and completes the handshake.
func startWorker(ctx context.Context, logger *slog.Logger, m storage.Monitor, network monitord.Network) (*worker, error) {
	if err := m.Definition.Validate(); err != nil {
		return nil, fmt.Errorf("monitor artifact requires redeploy: %w", err)
	}

	cmd := exec.Command(m.BinaryPath, monitord.FlagWorker)
	setMonitorProcessGroup(cmd)
	// Proxies travel in the handshake, not the environment, so credentials
	// never show up in the process table.
	cmd.Env = monitor.Env()
	// Run from the monitor's source directory so it can read data files that
	// live beside its source.
	cmd.Dir = m.SourceDir

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open worker stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("open worker stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("open worker stderr: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start worker process: %w", err)
	}

	w := &worker{
		binaryPath: m.BinaryPath,
		cmd:        cmd,
		stdin:      stdin,
		stdout:     bufio.NewScanner(stdout),
		exit:       make(chan error, 1),
	}
	w.stdout.Buffer(make([]byte, 0, 64*1024), maxCapturedOutput)

	pgid, err := monitorProcessGroup(cmd)
	if err != nil {
		logger.Warn("worker process group lookup failed", "monitor", m.Name, "pid", cmd.Process.Pid, "error", err)
		_ = cmd.Process.Kill()

		return nil, fmt.Errorf("resolve worker process group: %w", err)
	}
	w.pgid = pgid

	go func() { _, _ = io.Copy(&w.stderr, stderr) }()
	go func() { w.exit <- cmd.Wait() }()

	handshakeCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	if err := w.handshake(handshakeCtx, logger, m, network); err != nil {
		w.terminate(logger)

		return nil, err
	}

	logger.Info("monitor worker started",
		"monitor", m.Name,
		"pid", cmd.Process.Pid,
		"clients", w.clients,
		"proxies", len(network.Proxies),
	)

	return w, nil
}

func (w *worker) handshake(ctx context.Context, logger *slog.Logger, m storage.Monitor, network monitord.Network) error {
	if err := w.send(monitord.Inbound{
		Type: monitord.InboundHello,
		Hello: &monitord.Hello{
			Monitor: monitord.MonitorName(m.Name.String()),
			Dir:     m.SourceDir,
			Network: network,
		},
	}); err != nil {
		return err
	}

	line, err := w.readLine(ctx, logger)
	if err != nil {
		return fmt.Errorf("read worker handshake: %w (stderr: %s)", err, w.stderr.String())
	}

	var msg monitord.Outbound
	if err := json.Unmarshal(line, &msg); err != nil {
		return fmt.Errorf("decode worker handshake: %w", err)
	}
	if err := msg.Validate(); err != nil {
		return err
	}
	if msg.Type != monitord.OutboundReady {
		return fmt.Errorf("expected %q from worker, got %q", monitord.OutboundReady, msg.Type)
	}
	w.clients = msg.Ready.Clients

	return nil
}

// tick sends one run to the worker and consumes messages until its result.
func (w *worker) tick(ctx context.Context, logger *slog.Logger, m storage.Monitor, t monitord.Tick, emit func(monitord.Event)) (tickOutput, error) {
	w.runMu.Lock()
	defer w.runMu.Unlock()

	var out tickOutput
	if w.hasExited() {
		return out, fmt.Errorf("worker exited before tick: %v", w.exitErr)
	}
	if err := w.send(monitord.Inbound{
		Type: monitord.InboundTick,
		Tick: &t,
	}); err != nil {
		return out, err
	}

	var captured buffer
	for {
		line, err := w.readLine(ctx, logger)
		if err != nil {
			out.Stdout = captured.String()

			return out, err
		}
		captured.Write(append(line, '\n'))
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var msg monitord.Outbound
		if err := json.Unmarshal(line, &msg); err != nil {
			out.Stdout = captured.String()

			return out, fmt.Errorf("decode worker message: %w", err)
		}
		if err := msg.Validate(); err != nil {
			out.Stdout = captured.String()

			return out, err
		}
		if msg.RunID != "" && msg.RunID != t.RunID {
			out.Stdout = captured.String()

			return out, fmt.Errorf("worker reported run %s while %s was active", msg.RunID, t.RunID)
		}

		w.consume(logger, m, msg, &out, emit)
		if msg.Type == monitord.OutboundResult {
			out.Stdout = captured.String()

			return out, nil
		}
	}
}

func (w *worker) consume(logger *slog.Logger, m storage.Monitor, msg monitord.Outbound, out *tickOutput, emit func(monitord.Event)) {
	switch msg.Type {
	case monitord.OutboundLog:
		logger.Info("monitor log", "monitor", m.Name, "level", msg.Log.Level, "message", msg.Log.Message)
	case monitord.OutboundEvent:
		logger.Info("monitor event", "monitor", m.Name, "severity", msg.Event.Severity, "title", msg.Event.Title)
		emit(*msg.Event)
	case monitord.OutboundResult:
		out.Result = *msg.Result
	}
}

func (w *worker) send(msg monitord.Inbound) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode worker message: %w", err)
	}
	if _, err := w.stdin.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write worker message: %w", err)
	}

	return nil
}

// readLine reads one NDJSON line, terminating the worker if ctx expires first.
func (w *worker) readLine(ctx context.Context, logger *slog.Logger) ([]byte, error) {
	type result struct {
		line []byte
		err  error
	}

	done := make(chan result, 1)
	go func() {
		if w.stdout.Scan() {
			done <- result{line: append([]byte(nil), w.stdout.Bytes()...)}

			return
		}
		if err := w.stdout.Err(); err != nil {
			done <- result{err: fmt.Errorf("read worker stdout: %w", err)}

			return
		}
		done <- result{err: io.EOF}
	}()

	select {
	case r := <-done:
		return r.line, r.err
	case <-ctx.Done():
		// The scan goroutine is unblocked by killing the process, which closes
		// stdout.
		w.terminate(logger)

		return nil, ctx.Err()
	}
}

// hasExited reports whether the process has already been reaped.
func (w *worker) hasExited() bool {
	if w.exited {
		return true
	}

	select {
	case err := <-w.exit:
		w.exited = true
		w.exitErr = err

		return true
	default:
		return false
	}
}

// terminate stops the worker and its whole process group.
func (w *worker) terminate(logger *slog.Logger) {
	w.killOnce.Do(func() {
		_ = w.stdin.Close()
		terminateMonitorProcessGroup(w.pgid, logger)
		if w.cmd != nil && w.cmd.Process != nil {
			_ = w.cmd.Process.Kill()
		}
	})
}

// buffer is a bounded, concurrency-safe output capture.
type buffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if remaining := maxCapturedOutput - b.buf.Len(); remaining > 0 {
		b.buf.Write(p[:min(len(p), remaining)])
	}

	return len(p), nil
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

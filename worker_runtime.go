package monitord

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Permanent marks a continuous-source error as terminal.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

type permanentError struct{ error }

func (permanentError) Permanent() {}
func isPermanent(err error) bool  { var p interface{ Permanent() }; return errors.As(err, &p) }

func dispatchMonitor[S any](m Monitor[S]) error {
	d, err := validateMonitor(m)
	if err != nil {
		return fmt.Errorf("invalid monitor: %w", err)
	}
	if len(os.Args) != 2 {
		return errors.New("usage: monitor --monitord-describe | --monitord-worker")
	}
	switch os.Args[1] {
	case FlagDescribe:
		var input DescribeInput
		if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("decode describe input: %w", err)
		}
		state, err := decodeState[S](input.State, input.Version)
		if err != nil {
			return err
		}
		raw, err := encodeState(state)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(MonitorFrame{Info: m.Info(), Plan: d, StateVersion: stateVersion[S](), State: raw})
	case FlagWorker:
		return serveV5(m, d, os.Stdin, os.Stdout)
	default:
		return fmt.Errorf("unknown monitor flag %q", os.Args[1])
	}
}

type wire struct {
	r  *bufio.Reader
	w  io.Writer
	mu sync.Mutex
}

func newWire(r io.Reader, w io.Writer) *wire { return &wire{r: bufio.NewReaderSize(r, 64<<10), w: w} }
func (w *wire) readInbound() (V5Inbound, []byte, error) {
	raw := make([]byte, 0, 64<<10)
	for {
		part, err := w.r.ReadSlice('\n')
		if len(raw)+len(part) > MaxFrameBytes {
			return V5Inbound{}, nil, errors.New("protocol frame exceeds maximum size")
		}
		raw = append(raw, part...)
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return V5Inbound{}, nil, err
	}
	var v V5Inbound
	if err := strictDecode(bytes.NewReader(raw), &v); err != nil {
		return v, nil, err
	}
	return v, raw, v.Validate()
}
func (w *wire) send(v V5Outbound) error {
	if err := v.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return w.sendBytes(raw)
}
func (w *wire) sendBytes(raw []byte) error {
	if len(raw) > MaxFrameBytes {
		return errors.New("protocol frame exceeds maximum size")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.w.Write(raw)
	return err
}

type workerCoordinator struct {
	wire        *wire
	hello       V5Hello
	mu          sync.Mutex
	seq         uint64
	revision    int64
	acks        chan TransactionAck
	stop        chan V5Stop
	fatal       chan error
	progress    chan struct{}
	outstanding atomic.Bool
}

func (c *workerCoordinator) Commit(ctx context.Context, tx transactionCommit) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outstanding.Store(true)
	defer c.outstanding.Store(false)
	nextSequence := c.seq + 1
	frame := TransactionFrame{DeploymentID: c.hello.DeploymentID, Generation: c.hello.Generation, WorkerToken: c.hello.WorkerToken, Sequence: nextSequence, BaseStateRevision: c.revision, NextState: tx.NextState, Checkpoints: tx.Checkpoints, Events: tx.Events, Progress: tx.Progress}
	sum := HashTransactionFrame(frame)
	frame.PayloadHash = hex.EncodeToString(sum[:])
	out := V5Outbound{Type: "transaction", Transaction: &frame}
	if err := out.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	if len(raw) > MaxFrameBytes {
		return nil, errors.New("transaction frame exceeds maximum size")
	}
	if err = c.wire.sendBytes(raw); err != nil {
		return nil, err
	}
	c.seq = nextSequence
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	for {
		select {
		case ack := <-c.acks:
			if ack.DeploymentID != frame.DeploymentID || ack.Generation != frame.Generation || ack.Sequence != frame.Sequence || ack.PayloadHash != frame.PayloadHash {
				return nil, errors.New("transaction ACK identity mismatch")
			}
			if ack.Status != "accepted" && ack.Status != "replayed" {
				return nil, fmt.Errorf("transaction rejected with status %q", ack.Status)
			}
			c.revision = ack.ResultRevision
			if tx.Progress {
				select {
				case c.progress <- struct{}{}:
				default:
				}
			}
			return append(json.RawMessage(nil), tx.NextState...), nil
		case <-retry.C:
			if err = c.wire.sendBytes(raw); err != nil {
				return nil, err
			}
		case err := <-c.fatal:
			return nil, err
		}
	}
}

// HashTransactionFrame returns the canonical V5 semantic payload hash.
func HashTransactionFrame(frame TransactionFrame) [32]byte {
	h := sha256.New()
	fields := []any{frame.DeploymentID, frame.Generation, frame.WorkerToken, frame.Sequence, frame.BaseStateRevision, frame.NextState, frame.Checkpoints, frame.Events, frame.Progress}
	var size [8]byte
	for _, field := range fields {
		raw, _ := json.Marshal(field)
		binary.BigEndian.PutUint64(size[:], uint64(len(raw)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(raw)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func serveV5[S any](m Monitor[S], desc PlanDescription, in io.Reader, out io.Writer) error {
	w := newWire(in, out)
	helloMsg, _, err := w.readInbound()
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if helloMsg.Type != "hello" {
		return fmt.Errorf("expected hello, got %q", helloMsg.Type)
	}
	hello := *helloMsg.Hello
	if err = w.send(V5Outbound{Type: "monitor", Monitor: &MonitorFrame{Info: m.Info(), Plan: desc, StateVersion: stateVersion[S](), State: json.RawMessage("null")}}); err != nil {
		return err
	}
	start, _, err := w.readInbound()
	if err != nil {
		return fmt.Errorf("read start: %w", err)
	}
	if start.Type != "start" {
		return fmt.Errorf("expected start, got %q", start.Type)
	}
	want, _ := json.Marshal(desc)
	got, _ := json.Marshal(start.Start.Plan)
	if !bytes.Equal(want, got) {
		return errors.New("start plan differs from described plan")
	}
	coord := &workerCoordinator{wire: w, hello: hello, revision: hello.StateRevision, acks: make(chan TransactionAck, 1), stop: make(chan V5Stop, 1), fatal: make(chan error, 1), progress: make(chan struct{}, 1)}
	if err := validateHandshakeSecrets(desc, hello.Secrets); err != nil {
		return err
	}
	secrets := map[string]string{}
	for g, keys := range hello.Secrets {
		for k, v := range keys {
			secrets[g+"\x00"+k] = v
		}
	}
	session, err := newSession[S](hello.State, secrets, coord)
	if err != nil {
		return err
	}
	for source, raw := range hello.Checkpoints {
		session.checkpoints[source] = append(json.RawMessage(nil), raw...)
	}
	if err = w.send(V5Outbound{Type: "ready", Ready: &ReadyFrame{Generation: hello.Generation}}); err != nil {
		return err
	}
	go readControl(w, coord)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan childResult[S], 8)
	children := runtimeChildren(m.Plan())
	for _, child := range children {
		if child.check != nil {
			_ = sendHealth(w, child.name, "ready", "schedule activated")
		}
		go runChild(ctx, w, session, hello.Generation, child, done)
	}
	remaining := len(children)
	clean := true
	for remaining > 0 {
		select {
		case result := <-done:
			remaining--
			if result.err != nil {
				clean = false
				_ = sendHealth(w, result.child.name, "terminal", result.err.Error())
				if !result.child.optional {
					cancel()
				}
			}
		case <-coord.stop:
			cancel()
		case err := <-coord.fatal:
			cancel()
			return err
		case <-coord.progress:
			for _, child := range children {
				if child.continuous != nil {
					_ = sendHealth(w, child.name, "ready", "progress committed")
				}
			}
		}
	}
	return w.send(V5Outbound{Type: "stopped", Stopped: &StoppedFrame{Generation: hello.Generation, Clean: clean}})
}

func validateHandshakeSecrets(desc PlanDescription, supplied map[string]map[string]string) error {
	allowed := map[string]SecretRef{}
	for _, ref := range desc.SecretRefs() {
		allowed[ref.Group+"\x00"+ref.Key] = ref
	}
	seen := map[string]bool{}
	for group, keys := range supplied {
		for key := range keys {
			id := group + "\x00" + key
			if _, ok := allowed[id]; !ok {
				return fmt.Errorf("unauthorized secret %s/%s in handshake", group, key)
			}
			seen[id] = true
		}
	}
	for id, ref := range allowed {
		if ref.Required && !seen[id] {
			return fmt.Errorf("required secret %s/%s is unavailable", ref.Group, ref.Key)
		}
	}
	return nil
}
func readControl(w *wire, c *workerCoordinator) {
	for {
		m, _, err := w.readInbound()
		if err != nil {
			c.reportFatal(err)
			return
		}
		switch m.Type {
		case "ack":
			if !c.outstanding.Load() {
				c.reportFatal(errors.New("unsolicited transaction ACK"))
				return
			}
			select {
			case c.acks <- *m.Ack:
			default:
				c.reportFatal(errors.New("unexpected transaction ACK"))
				return
			}
		case "stop":
			select {
			case c.stop <- *m.Stop:
			default:
				{
				}
			}
		default:
			c.reportFatal(fmt.Errorf("unexpected control frame %q", m.Type))
			return
		}
	}
}
func (c *workerCoordinator) reportFatal(err error) {
	select {
	case c.fatal <- err:
	default:
	}
}

type runtimeChild[S any] struct {
	name              string
	optional          bool
	continuous        ContinuousFunc[S]
	check             CheckFunc[S]
	interval, timeout time.Duration
}

func runtimeChildren[S any](p Plan[S]) []runtimeChild[S] {
	n := p.node
	if n.kind == planCombined {
		var out []runtimeChild[S]
		for _, named := range n.children {
			c := named.child
			out = append(out, runtimeChild[S]{named.name, named.optional, c.continuous, c.check, c.interval, c.options.timeout})
		}
		return out
	}
	return []runtimeChild[S]{{name: "default", continuous: n.continuous, check: n.check, interval: n.interval, timeout: n.options.timeout}}
}

type childResult[S any] struct {
	child runtimeChild[S]
	err   error
}

func runChild[S any](ctx context.Context, w *wire, s *Session[S], generation uint64, child runtimeChild[S], done chan<- childResult[S]) {
	var err error
	if child.continuous != nil {
		err = runContinuous(ctx, w, s, child)
	} else {
		err = runPolling(ctx, w, s, generation, child)
	}
	done <- childResult[S]{child, err}
}
func runContinuous[S any](ctx context.Context, w *wire, s *Session[S], child runtimeChild[S]) error {
	backoff := time.Second
	failures := 0
	for {
		started := time.Now()
		attempt := ctx
		cancel := func() {}
		if child.timeout > 0 {
			attempt, cancel = context.WithTimeout(ctx, child.timeout)
		}
		err := safeCallback(func() error { return child.continuous(attempt, s) })
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return errors.New("continuous callback returned nil while active")
		}
		if time.Since(started) >= time.Minute {
			failures = 0
			backoff = time.Second
		}
		failures++
		if isPermanent(err) || failures >= 6 {
			return err
		}
		if sendErr := sendHealth(w, child.name, "restarting", err.Error()); sendErr != nil {
			return sendErr
		}
		delay := backoff + time.Duration(rand.Int64N(int64(backoff/4+1)))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
func runPolling[S any](ctx context.Context, w *wire, s *Session[S], generation uint64, child runtimeChild[S]) error {
	for attempt := uint64(1); ; attempt++ {
		id := fmt.Sprintf("%d-%s-%d", generation, child.name, attempt)
		if err := w.send(V5Outbound{Type: "run", Run: &RunFrame{AttemptID: id, Child: child.name, Phase: "started"}}); err != nil {
			return err
		}
		runctx := ctx
		cancel := func() {}
		if child.timeout > 0 {
			runctx, cancel = context.WithTimeout(ctx, child.timeout)
		}
		err := safeCallback(func() error { return child.check(runctx, s) })
		cancel()
		finish := &RunFrame{AttemptID: id, Child: child.name, Phase: "finished"}
		if err != nil {
			finish.Error = err.Error()
		}
		if sendErr := w.send(V5Outbound{Type: "run", Run: finish}); sendErr != nil {
			return sendErr
		}
		status := "healthy"
		if err != nil {
			status = "degraded"
		}
		if sendErr := sendHealth(w, child.name, status, finish.Error); sendErr != nil {
			return sendErr
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(child.interval):
		}
	}
}
func safeCallback(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("callback panicked: %v", r)
		}
	}()
	return fn()
}
func sendHealth(w *wire, child, status, message string) error {
	return w.send(V5Outbound{Type: "health", Health: &HealthFrame{Child: child, Status: status, Message: message}})
}

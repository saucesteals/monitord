package monitord

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type workerCoordinator struct {
	wire        *wire
	hello       Hello
	mu          sync.Mutex
	ackMu       sync.Mutex
	seq         uint64
	revision    int64
	ackReady    chan struct{}
	expectedAck *TransactionAck
	pendingAck  *TransactionAck
	lastAck     *TransactionAck
	stop        chan Stop
	commitStop  chan Stop
	fatalOnce   sync.Once
	fatalDone   chan struct{}
	fatalErr    error
}

func (c *workerCoordinator) Commit(ctx context.Context, tx transactionCommit) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nextSequence := c.seq + 1
	frame := TransactionFrame{DeploymentID: c.hello.DeploymentID, Generation: c.hello.Generation, WorkerToken: c.hello.WorkerToken, Sequence: nextSequence, BaseStateRevision: c.revision, NextState: tx.NextState, Checkpoints: tx.Checkpoints, Events: tx.Events}
	sum := HashTransactionFrame(frame)
	frame.PayloadHash = hex.EncodeToString(sum[:])
	out := WorkerFrame{Type: "transaction", Transaction: &frame}
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
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	c.expectTransactionAck(frame)
	defer c.clearExpectedAck()
	if err = c.wire.sendBytes(raw); err != nil {
		return nil, err
	}
	c.seq = nextSequence
	retry := time.NewTicker(2 * time.Second)
	defer retry.Stop()
	var stopTimer *time.Timer
	var stopDeadline <-chan time.Time
	defer func() {
		if stopTimer != nil {
			stopTimer.Stop()
		}
	}()
	for {
		select {
		case <-c.ackReady:
			ack, err := c.acceptTransactionAck()
			if err != nil {
				return nil, err
			}
			if ack.Status != "accepted" && ack.Status != "replayed" {
				return nil, fmt.Errorf("transaction rejected with status %q", ack.Status)
			}
			c.revision = ack.ResultRevision
			return append(json.RawMessage(nil), tx.NextState...), nil
		case <-retry.C:
			if err = c.wire.sendBytes(raw); err != nil {
				return nil, err
			}
		case <-c.fatalDone:
			return nil, c.fatalError()
		case request := <-c.commitStop:
			if stopDeadline != nil {
				continue
			}
			deadline, err := time.Parse(time.RFC3339Nano, request.Deadline)
			if err != nil {
				return nil, errors.New("transaction stop deadline is invalid")
			}
			// Leave time for the worker to report an unclean stop before the
			// daemon's process deadline expires.
			remaining := time.Until(deadline.Add(-stopReportGrace))
			if remaining <= 0 {
				return nil, errors.New("transaction remained unacknowledged at stop deadline")
			}
			stopTimer = time.NewTimer(remaining)
			stopDeadline = stopTimer.C
		case <-stopDeadline:
			return nil, errors.New("transaction remained unacknowledged at stop deadline")
		}
	}
}

// HashTransactionFrame returns the canonical semantic payload hash.
func HashTransactionFrame(frame TransactionFrame) [32]byte {
	h := sha256.New()
	fields := []any{frame.DeploymentID, frame.Generation, frame.WorkerToken, frame.Sequence, frame.BaseStateRevision, frame.NextState, frame.Checkpoints, frame.Events}
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

func readControl(w *wire, c *workerCoordinator) {
	for {
		m, _, err := w.readInbound()
		if err != nil {
			c.reportFatal(err)
			return
		}
		switch m.Type {
		case "ack":
			if err := c.receiveTransactionAck(*m.Ack); err != nil {
				c.reportFatal(err)
				return
			}
		case "stop":
			select {
			case c.commitStop <- *m.Stop:
			default:
			}
			select {
			case c.stop <- *m.Stop:
			default:
			}
		default:
			c.reportFatal(fmt.Errorf("unexpected control frame %q", m.Type))
			return
		}
	}
}

func (c *workerCoordinator) expectTransactionAck(frame TransactionFrame) {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	c.expectedAck = &TransactionAck{DeploymentID: frame.DeploymentID, Generation: frame.Generation, Sequence: frame.Sequence, PayloadHash: frame.PayloadHash}
	c.pendingAck = nil
}

func (c *workerCoordinator) receiveTransactionAck(ack TransactionAck) error {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	if c.lastAck != nil && sameTransactionAck(*c.lastAck, ack) {
		return nil
	}
	if c.expectedAck == nil {
		return errors.New("unsolicited transaction ACK")
	}
	if !sameTransactionAckIdentity(*c.expectedAck, ack) {
		return errors.New("transaction ACK identity mismatch")
	}
	if c.pendingAck != nil {
		if sameTransactionAck(*c.pendingAck, ack) {
			return nil
		}
		return errors.New("conflicting transaction ACK")
	}
	c.pendingAck = &ack
	select {
	case c.ackReady <- struct{}{}:
	default:
	}
	return nil
}

func (c *workerCoordinator) acceptTransactionAck() (TransactionAck, error) {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	if c.pendingAck == nil {
		return TransactionAck{}, errors.New("transaction ACK signal without payload")
	}
	ack := *c.pendingAck
	c.lastAck = &ack
	c.pendingAck = nil
	c.expectedAck = nil
	return ack, nil
}

func (c *workerCoordinator) clearExpectedAck() {
	c.ackMu.Lock()
	defer c.ackMu.Unlock()
	c.expectedAck = nil
	c.pendingAck = nil
}

func sameTransactionAckIdentity(a, b TransactionAck) bool {
	return a.DeploymentID == b.DeploymentID && a.Generation == b.Generation && a.Sequence == b.Sequence && a.PayloadHash == b.PayloadHash
}

func sameTransactionAck(a, b TransactionAck) bool {
	return sameTransactionAckIdentity(a, b) && a.ResultRevision == b.ResultRevision
}
func (c *workerCoordinator) reportFatal(err error) {
	c.fatalOnce.Do(func() {
		c.fatalErr = err
		close(c.fatalDone)
	})
}

func (c *workerCoordinator) fatalError() error {
	if c.fatalErr == nil {
		return errors.New("worker control channel failed")
	}
	return c.fatalErr
}

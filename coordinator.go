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
	"sync/atomic"
	"time"
)

type workerCoordinator struct {
	wire        *wire
	hello       Hello
	mu          sync.Mutex
	seq         uint64
	revision    int64
	acks        chan TransactionAck
	stop        chan Stop
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

// HashTransactionFrame returns the canonical semantic payload hash.
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

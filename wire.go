package monitord

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"
)

type wire struct {
	r            *bufio.Reader
	w            io.Writer
	mu           sync.Mutex
	failureMu    sync.Mutex
	writeFailure error
}

func newWire(r io.Reader, w io.Writer) *wire { return &wire{r: bufio.NewReaderSize(r, 64<<10), w: w} }
func (w *wire) readInbound() (DaemonFrame, []byte, error) {
	raw := make([]byte, 0, 64<<10)
	for {
		part, err := w.r.ReadSlice('\n')
		if len(raw)+len(part) > MaxFrameBytes {
			return DaemonFrame{}, nil, errors.New("protocol frame exceeds maximum size")
		}
		raw = append(raw, part...)
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return DaemonFrame{}, nil, err
	}
	var v DaemonFrame
	if err := strictDecode(bytes.NewReader(raw), &v); err != nil {
		return v, nil, err
	}
	return v, raw, v.Validate()
}
func (w *wire) send(v WorkerFrame) error {
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
	// A daemon that stops reading must not hold the protocol mutex forever and
	// prevent the supervisor from exiting. At most one blocked write survives
	// until process exit; subsequent sends fail closed on this poisoned wire.
	w.failureMu.Lock()
	failure := w.writeFailure
	w.failureMu.Unlock()
	if failure != nil {
		return failure
	}
	result := make(chan error, 1)
	go func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.failureMu.Lock()
		failure := w.writeFailure
		w.failureMu.Unlock()
		if failure != nil {
			result <- failure
			return
		}
		n, err := w.w.Write(raw)
		if err == nil && n != len(raw) {
			err = io.ErrShortWrite
		}
		result <- err
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	var err error
	select {
	case err = <-result:
	case <-timer.C:
		err = errors.New("worker protocol write deadline exceeded")
	}
	if err != nil {
		w.failureMu.Lock()
		if w.writeFailure == nil {
			w.writeFailure = err
		}
		w.failureMu.Unlock()
	}
	return err
}

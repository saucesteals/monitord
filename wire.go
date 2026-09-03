package monitord

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"
)

type wire struct {
	r  *bufio.Reader
	w  io.Writer
	mu sync.Mutex
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
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := w.w.Write(raw)
	return err
}

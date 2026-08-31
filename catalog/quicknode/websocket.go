package quicknode

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type Subscription[T any] struct {
	client *Client
	mu     sync.RWMutex
	conn   *websocket.Conn
	id     string
	kind   string
	args   []any
	values chan T
	errs   chan error
	once   sync.Once
	decode func(json.RawMessage) (T, error)
	closed atomic.Bool
	done   chan struct{}
}

func (s *Subscription[T]) C() <-chan T       { return s.values }
func (s *Subscription[T]) Err() <-chan error { return s.errs }
func (s *Subscription[T]) Close() error {
	var err error
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.done)
		s.mu.RLock()
		conn := s.conn
		s.mu.RUnlock()
		err = conn.Close(websocket.StatusNormalClosure, "subscription closed")
	})
	return err
}

func subscribe[T any](ctx context.Context, c *Client, kind string, decode func(json.RawMessage) (T, error), args ...any) (*Subscription[T], error) {
	if c.wssURL == nil {
		return nil, errors.New("quicknode: WSS endpoint is required")
	}
	conn, subscriptionID, err := openSubscription(ctx, c, kind, args...)
	if err != nil {
		return nil, err
	}
	s := &Subscription[T]{client: c, conn: conn, id: subscriptionID, kind: kind, args: append([]any(nil), args...), values: make(chan T, 16), errs: make(chan error, 1), decode: decode, done: make(chan struct{})}
	go s.read()
	return s, nil
}

func openSubscription(ctx context.Context, c *Client, kind string, args ...any) (*websocket.Conn, string, error) {
	conn, _, err := websocket.Dial(ctx, c.wssURL.String(), nil)
	if err != nil {
		return nil, "", errors.New("quicknode: websocket dial failed")
	}
	id := c.seq.Add(1)
	params := append([]any{kind}, args...)
	if err := wsWrite(ctx, conn, rpcRequest{JSONRPC: "2.0", ID: id, Method: "eth_subscribe", Params: params}); err != nil {
		conn.CloseNow()
		return nil, "", err
	}
	_, payload, err := conn.Read(ctx)
	if err != nil {
		conn.CloseNow()
		return nil, "", errors.New("quicknode: subscription handshake failed")
	}
	var response rpcResponse
	var subscriptionID string
	if json.Unmarshal(payload, &response) != nil || response.ID != id || response.Error != nil || json.Unmarshal(response.Result, &subscriptionID) != nil || subscriptionID == "" {
		conn.CloseNow()
		if response.Error != nil {
			return nil, "", c.safeRPCError(response.Error)
		}
		return nil, "", errors.New("quicknode: invalid subscription response")
	}
	return conn, subscriptionID, nil
}

func wsWrite(ctx context.Context, c *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, b)
}
func (s *Subscription[T]) read() {
	defer close(s.values)
	defer close(s.errs)
	for {
		s.mu.RLock()
		conn := s.conn
		s.mu.RUnlock()
		_, b, err := conn.Read(context.Background())
		if err != nil {
			if s.closed.Load() || websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				return
			}
			if reconnectErr := s.reconnect(); reconnectErr != nil {
				s.errs <- reconnectErr
				return
			}
			continue
		}
		var n struct {
			JSONRPC string `json:"jsonrpc"`
			Method  string `json:"method"`
			Params  struct {
				Subscription string          `json:"subscription"`
				Result       json.RawMessage `json:"result"`
			} `json:"params"`
		}
		s.mu.RLock()
		subscriptionID := s.id
		s.mu.RUnlock()
		if json.Unmarshal(b, &n) != nil || n.Method != "eth_subscription" || n.Params.Subscription != subscriptionID {
			s.errs <- errors.New("quicknode: invalid subscription notification")
			return
		}
		var v T
		var decodeErr error
		if s.decode != nil {
			v, decodeErr = s.decode(n.Params.Result)
		} else {
			decodeErr = json.Unmarshal(n.Params.Result, &v)
		}
		if decodeErr != nil {
			s.errs <- errors.New("quicknode: invalid subscription record")
			return
		}
		select {
		case s.values <- v:
		case <-s.done:
			return
		}
	}
}

func (s *Subscription[T]) reconnect() error {
	for attempt := 0; attempt < 4; attempt++ {
		if s.closed.Load() {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		conn, id, err := openSubscription(ctx, s.client, s.kind, s.args...)
		cancel()
		if err == nil {
			s.mu.Lock()
			s.conn = conn
			s.id = id
			s.mu.Unlock()
			return nil
		}
		if err := backoff(context.Background(), attempt, 0); err != nil {
			return err
		}
	}
	return errors.New("quicknode: subscription reconnect failed")
}

type Head struct {
	Number     string `json:"number"`
	Hash       Hash   `json:"hash"`
	ParentHash Hash   `json:"parentHash"`
}

func (c *Client) SubscribeHeads(ctx context.Context) (*Subscription[Head], error) {
	return subscribe[Head](ctx, c, "newHeads", nil)
}
func (c *Client) SubscribePending(ctx context.Context) (*Subscription[Hash], error) {
	return subscribe[Hash](ctx, c, "newPendingTransactions", nil)
}
func (c *Client) SubscribeLogs(ctx context.Context, filter Logs) (*Subscription[Log], error) {
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	arg := map[string]any{}
	if len(filter.Addresses) > 0 {
		arg["address"] = filter.Addresses
	}
	if len(filter.Topics) > 0 {
		arg["topics"] = filter.Topics
	}
	return subscribe[Log](ctx, c, "logs", func(raw json.RawMessage) (Log, error) {
		var wire rpcLog
		if err := json.Unmarshal(raw, &wire); err != nil {
			return Log{}, err
		}
		return decodeRPCLog(wire, c.chainID)
	}, arg)
}

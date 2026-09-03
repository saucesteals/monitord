package quicknode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// SubscriptionSpec describes a JSON-RPC pubsub protocol. It deliberately does
// not assign meaning to notification payloads or promise replay across gaps.
type SubscriptionSpec struct {
	SubscribeMethod    string
	NotificationMethod string
	Params             any
}

type Subscription[T any] struct {
	client *Client
	spec   SubscriptionSpec
	decode func(json.RawMessage) (T, error)

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	conn   *websocket.Conn
	id     json.RawMessage
	values chan T
	errs   chan error
	once   sync.Once
}

func (s *Subscription[T]) C() <-chan T       { return s.values }
func (s *Subscription[T]) Err() <-chan error { return s.errs }

func (s *Subscription[T]) Close() error {
	var err error
	s.once.Do(func() {
		s.cancel()
		s.mu.Lock()
		conn := s.conn
		s.conn = nil
		s.mu.Unlock()
		if conn != nil {
			err = conn.Close(websocket.StatusNormalClosure, "subscription closed")
		}
	})
	return err
}

// Subscribe uses ctx to bound the opening handshake. The returned subscription
// owns its reconnecting lifetime and must be closed by its lifecycle owner.
func Subscribe[T any](ctx context.Context, client *Client, spec SubscriptionSpec, decode func(json.RawMessage) (T, error)) (*Subscription[T], error) {
	if client == nil {
		return nil, errors.New("quicknode: subscription client is required")
	}
	if client.wssURL == nil {
		return nil, errors.New("quicknode: WSS endpoint is required")
	}
	if spec.SubscribeMethod == "" || spec.NotificationMethod == "" {
		return nil, errors.New("quicknode: subscription and notification methods are required")
	}
	subscriptionCtx, cancel := context.WithCancel(context.Background())
	s := &Subscription[T]{
		client: client,
		spec:   spec,
		decode: decode,
		ctx:    subscriptionCtx,
		cancel: cancel,
		values: make(chan T, 256),
		errs:   make(chan error, 1),
	}
	conn, id, err := s.open(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	s.conn, s.id = conn, id
	go s.read()
	return s, nil
}

func (s *Subscription[T]) open(ctx context.Context) (*websocket.Conn, json.RawMessage, error) {
	if err := s.client.waitRate(ctx); err != nil {
		return nil, nil, err
	}
	conn, _, err := websocket.Dial(ctx, s.client.wssURL.String(), nil)
	if err != nil {
		return nil, nil, errors.New("quicknode: websocket dial failed")
	}
	conn.SetReadLimit(s.client.webSocketReadLimit)
	id := s.client.seq.Add(1)
	if err := writeJSON(ctx, conn, rpcRequest{JSONRPC: "2.0", ID: id, Method: s.spec.SubscribeMethod, Params: s.spec.Params}); err != nil {
		conn.CloseNow()
		return nil, nil, errors.New("quicknode: subscription write failed")
	}
	_, payload, err := conn.Read(ctx)
	if err != nil {
		conn.CloseNow()
		return nil, nil, errors.New("quicknode: subscription handshake failed")
	}
	var response rpcResponse
	if json.Unmarshal(payload, &response) != nil || response.JSONRPC != "2.0" || response.ID != id || response.Error != nil {
		conn.CloseNow()
		if response.Error != nil {
			return nil, nil, s.client.safeRPCError(response.Error)
		}
		return nil, nil, errors.New("quicknode: invalid subscription response")
	}
	if !validSubscriptionID(response.Result) {
		conn.CloseNow()
		return nil, nil, errors.New("quicknode: invalid subscription ID")
	}
	return conn, append(json.RawMessage(nil), response.Result...), nil
}

func validSubscriptionID(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return false
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return false
	}
	switch value.(type) {
	case string, json.Number:
		return true
	default:
		return false
	}
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

func (s *Subscription[T]) read() {
	defer close(s.values)
	defer close(s.errs)
	defer s.Close()
	for {
		s.mu.Lock()
		conn, subscriptionID := s.conn, append(json.RawMessage(nil), s.id...)
		s.mu.Unlock()
		_, payload, err := conn.Read(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			if err := s.reconnect(); err != nil {
				s.sendError(err)
				return
			}
			continue
		}
		var notification struct {
			JSONRPC string `json:"jsonrpc"`
			Method  string `json:"method"`
			Params  struct {
				Subscription json.RawMessage `json:"subscription"`
				Result       json.RawMessage `json:"result"`
			} `json:"params"`
		}
		if json.Unmarshal(payload, &notification) != nil || notification.JSONRPC != "2.0" || notification.Method != s.spec.NotificationMethod || !bytes.Equal(notification.Params.Subscription, subscriptionID) {
			s.sendError(errors.New("quicknode: invalid subscription notification"))
			return
		}
		value, err := s.decodeValue(notification.Params.Result)
		if err != nil {
			s.sendError(errors.New("quicknode: invalid subscription record"))
			return
		}
		select {
		case s.values <- value:
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Subscription[T]) decodeValue(raw json.RawMessage) (T, error) {
	if s.decode != nil {
		return s.decode(raw)
	}
	var value T
	err := json.Unmarshal(raw, &value)
	return value, err
}

func (s *Subscription[T]) reconnect() error {
	for attempt := 0; attempt < 4; attempt++ {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		openCtx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
		conn, id, err := s.open(openCtx)
		cancel()
		if err == nil {
			s.mu.Lock()
			if s.ctx.Err() != nil {
				s.mu.Unlock()
				conn.CloseNow()
				return s.ctx.Err()
			}
			old := s.conn
			s.conn, s.id = conn, id
			s.mu.Unlock()
			if old != nil {
				old.CloseNow()
			}
			return nil
		}
		if err := backoff(s.ctx, attempt, 0); err != nil {
			return err
		}
	}
	return errors.New("quicknode: subscription reconnect failed")
}

func (s *Subscription[T]) sendError(err error) {
	select {
	case s.errs <- err:
	default:
	}
}

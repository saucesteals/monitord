package quicknode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type Config struct {
	WSSURL            string
	HTTPURL           string
	HTTPClient        *http.Client
	RequestsPerSecond float64
	Burst             int
}

type Client struct {
	cfg         Config
	httpURL     *url.URL
	wssURL      *url.URL
	http        *http.Client
	chainID     ChainID
	seq         atomic.Uint64
	rateMu      sync.Mutex
	nextRequest time.Time
}

func Open(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.HTTPURL == "" && cfg.WSSURL == "" {
		return nil, errors.New("quicknode: HTTPURL or WSSURL is required")
	}
	c := &Client{cfg: cfg, http: cfg.HTTPClient}
	if c.http == nil {
		c.http = &http.Client{Timeout: 30 * time.Second}
	}
	var err error
	if cfg.HTTPURL != "" {
		c.httpURL, err = endpoint(cfg.HTTPURL, "http", "https")
		if err != nil {
			return nil, fmt.Errorf("quicknode HTTP endpoint: %w", err)
		}
	}
	if cfg.WSSURL != "" {
		c.wssURL, err = endpoint(cfg.WSSURL, "ws", "wss")
		if err != nil {
			return nil, fmt.Errorf("quicknode WSS endpoint: %w", err)
		}
	}
	if c.httpURL != nil {
		var raw string
		if err := c.call(ctx, "eth_chainId", []any{}, &raw); err != nil {
			return nil, fmt.Errorf("quicknode chain handshake: %w", err)
		}
		c.chainID, err = ParseChainID(raw)
		if err != nil {
			return nil, fmt.Errorf("quicknode chain handshake: %w", err)
		}
	} else {
		var raw string
		if err := c.callWSOnce(ctx, "eth_chainId", []any{}, &raw); err != nil {
			return nil, fmt.Errorf("quicknode chain handshake: %w", err)
		}
		c.chainID, err = ParseChainID(raw)
		if err != nil {
			return nil, fmt.Errorf("quicknode chain handshake: %w", err)
		}
	}
	return c, nil
}

func (c *Client) callWSOnce(ctx context.Context, method string, params any, out any) error {
	conn, _, err := websocket.Dial(ctx, c.wssURL.String(), nil)
	if err != nil {
		return errors.New("quicknode: websocket dial failed")
	}
	defer conn.CloseNow()
	id := c.seq.Add(1)
	if err = wsWrite(ctx, conn, rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		return errors.New("quicknode: websocket write failed")
	}
	_, payload, err := conn.Read(ctx)
	if err != nil {
		return errors.New("quicknode: websocket response failed")
	}
	var response rpcResponse
	if json.Unmarshal(payload, &response) != nil || response.ID != id || response.Error != nil {
		if response.Error != nil {
			return c.safeRPCError(response.Error)
		}
		return errors.New("quicknode: invalid websocket JSON-RPC response")
	}
	if err = json.Unmarshal(response.Result, out); err != nil {
		return errors.New("quicknode: invalid websocket JSON-RPC result")
	}
	return nil
}

func endpoint(raw string, schemes ...string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid endpoint URL")
	}
	for _, s := range schemes {
		if u.Scheme == s {
			return u, nil
		}
	}
	return nil, errors.New("invalid endpoint scheme")
}

func (c *Client) ChainID() ChainID { return c.chainID }
func (c *Client) Close() error {
	if t, ok := c.http.Transport.(interface{ CloseIdleConnections() }); ok {
		t.CloseIdleConnections()
	}
	return nil
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("JSON-RPC error %d: %s", e.Code, e.Message) }

func (c *Client) safeRPCError(e *rpcError) error {
	message := e.Message
	for _, raw := range []string{c.cfg.HTTPURL, c.cfg.WSSURL} {
		if raw == "" {
			continue
		}
		message = strings.ReplaceAll(message, raw, RedactEndpoint(raw))
		if u, err := url.Parse(raw); err == nil {
			for _, part := range strings.Split(strings.Trim(u.Path, "/"), "/") {
				if part != "" {
					message = strings.ReplaceAll(message, part, "<redacted>")
				}
			}
			for _, values := range u.Query() {
				for _, value := range values {
					if value != "" {
						message = strings.ReplaceAll(message, value, "<redacted>")
					}
				}
			}
		}
	}
	return fmt.Errorf("JSON-RPC error %d: %s", e.Code, message)
}

func (c *Client) call(ctx context.Context, method string, params any, out any) error {
	if c.httpURL == nil {
		return errors.New("quicknode: HTTP endpoint is required")
	}
	id := c.seq.Add(1)
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 4; attempt++ {
		if err := c.waitRate(ctx); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.httpURL.String(), bytes.NewReader(body))
		if err != nil {
			return errors.New("quicknode: create request")
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			if attempt < 3 && ctx.Err() == nil {
				if err := backoff(ctx, attempt, 0); err != nil {
					return err
				}
				continue
			}
			return errors.New("quicknode: request failed")
		}
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if readErr != nil {
			return errors.New("quicknode: read response")
		}
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			if attempt < 3 {
				if err := backoff(ctx, attempt, retryAfter(resp.Header.Get("Retry-After"))); err != nil {
					return err
				}
				continue
			}
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("quicknode: HTTP status %d", resp.StatusCode)
		}
		var msg rpcResponse
		dec := json.NewDecoder(bytes.NewReader(payload))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&msg); err != nil || msg.JSONRPC != "2.0" || msg.ID != id {
			return errors.New("quicknode: invalid JSON-RPC response")
		}
		if msg.Error != nil {
			return c.safeRPCError(msg.Error)
		}
		if len(msg.Result) == 0 {
			return errors.New("quicknode: JSON-RPC response has no result")
		}
		if err := json.Unmarshal(msg.Result, out); err != nil {
			return errors.New("quicknode: invalid JSON-RPC result")
		}
		return nil
	}
	return errors.New("quicknode: retries exhausted")
}

func (c *Client) waitRate(ctx context.Context) error {
	if c.cfg.RequestsPerSecond <= 0 {
		return nil
	}
	c.rateMu.Lock()
	now := time.Now()
	wait := time.Duration(0)
	if now.Before(c.nextRequest) {
		wait = c.nextRequest.Sub(now)
		now = c.nextRequest
	}
	c.nextRequest = now.Add(time.Duration(float64(time.Second) / c.cfg.RequestsPerSecond))
	c.rateMu.Unlock()
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func retryAfter(v string) time.Duration {
	n, err := strconv.Atoi(v)
	if err == nil && n >= 0 && n <= 30 {
		return time.Duration(n) * time.Second
	}
	return 0
}
func backoff(ctx context.Context, attempt int, requested time.Duration) error {
	d := time.Duration(1<<attempt) * 100 * time.Millisecond
	if requested > d {
		d = requested
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func RedactEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<redacted-endpoint>"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	if u.Path != "" && u.Path != "/" {
		u.Path = "/<redacted>"
		u.RawPath = ""
	}
	return u.String()
}

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

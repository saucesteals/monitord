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
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
)

// Endpoint contains the exact provider URLs copied from QuickNode. Network
// names and exceptional URL paths belong in configuration, not URL builders.
type Endpoint struct {
	HTTPURL string
	WSSURL  string
}

type Config struct {
	Endpoint           Endpoint
	HTTPClient         *http.Client
	RequestsPerSecond  float64
	Burst              int
	MaxResponseBytes   int64
	WebSocketReadLimit int64
}

// Client is a chain-neutral QuickNode JSON-RPC transport. Chain packages own
// handshakes, methods, types, finality, and source progress.
type Client struct {
	cfg                Config
	httpURL            *url.URL
	wssURL             *url.URL
	http               *http.Client
	ownHTTP            bool
	limiter            *rate.Limiter
	maxResponseBytes   int64
	webSocketReadLimit int64
	seq                atomic.Uint64
}

func Open(cfg Config) (*Client, error) {
	if cfg.Endpoint.HTTPURL == "" && cfg.Endpoint.WSSURL == "" {
		return nil, errors.New("quicknode: HTTPURL or WSSURL is required")
	}
	if cfg.RequestsPerSecond < 0 {
		return nil, errors.New("quicknode: requests per second cannot be negative")
	}
	if cfg.Burst < 0 {
		return nil, errors.New("quicknode: burst cannot be negative")
	}
	if cfg.RequestsPerSecond == 0 && cfg.Burst != 0 {
		return nil, errors.New("quicknode: burst requires a positive request rate")
	}
	c := &Client{cfg: cfg, http: cfg.HTTPClient, maxResponseBytes: cfg.MaxResponseBytes, webSocketReadLimit: cfg.WebSocketReadLimit}
	if c.http == nil {
		transport := http.DefaultTransport
		if standard, ok := transport.(*http.Transport); ok {
			transport = standard.Clone()
			c.ownHTTP = true
		}
		c.http = &http.Client{Transport: transport, Timeout: 30 * time.Second}
	}
	if c.maxResponseBytes == 0 {
		c.maxResponseBytes = 32 << 20
	}
	if c.maxResponseBytes < 0 {
		return nil, errors.New("quicknode: max response bytes cannot be negative")
	}
	if c.webSocketReadLimit == 0 {
		c.webSocketReadLimit = 32 << 20
	}
	if c.webSocketReadLimit < 0 {
		return nil, errors.New("quicknode: websocket read limit cannot be negative")
	}
	var err error
	if cfg.Endpoint.HTTPURL != "" {
		c.httpURL, err = parseEndpoint(cfg.Endpoint.HTTPURL, "http", "https")
		if err != nil {
			return nil, fmt.Errorf("quicknode HTTP endpoint: %w", err)
		}
	}
	if cfg.Endpoint.WSSURL != "" {
		c.wssURL, err = parseEndpoint(cfg.Endpoint.WSSURL, "ws", "wss")
		if err != nil {
			return nil, fmt.Errorf("quicknode WSS endpoint: %w", err)
		}
	}
	if cfg.RequestsPerSecond > 0 {
		burst := cfg.Burst
		if burst == 0 {
			burst = 1
		}
		c.limiter = rate.NewLimiter(rate.Limit(cfg.RequestsPerSecond), burst)
	}
	return c, nil
}

// Call makes one JSON-RPC call. HTTP is preferred when both endpoint forms are
// configured; WSS-only clients use a short-lived connection.
func (c *Client) Call(ctx context.Context, method string, params, out any) error {
	if method == "" {
		return errors.New("quicknode: JSON-RPC method is required")
	}
	if out == nil {
		return errors.New("quicknode: JSON-RPC result target is required")
	}
	if c.httpURL != nil {
		return c.callHTTP(ctx, method, params, out, false)
	}
	return c.callWSOnce(ctx, method, params, out)
}

// CallRead retries a read-only JSON-RPC call after transient transport, 429,
// and server failures. Callers must not use it for submissions with side effects.
func (c *Client) CallRead(ctx context.Context, method string, params, out any) error {
	if method == "" || out == nil {
		return c.Call(ctx, method, params, out)
	}
	if c.httpURL != nil {
		return c.callHTTP(ctx, method, params, out, true)
	}
	return c.callWSOnce(ctx, method, params, out)
}

// CallWebSocketRead performs one read-only JSON-RPC call against the configured
// WebSocket endpoint even when HTTP is also available. Chain clients use it to
// verify that paired HTTP and WSS URLs address the same network.
func (c *Client) CallWebSocketRead(ctx context.Context, method string, params, out any) error {
	if method == "" {
		return errors.New("quicknode: JSON-RPC method is required")
	}
	if out == nil {
		return errors.New("quicknode: JSON-RPC result target is required")
	}
	return c.callWSOnce(ctx, method, params, out)
}

func (c *Client) callWSOnce(ctx context.Context, method string, params, out any) error {
	if c.wssURL == nil {
		return errors.New("quicknode: WSS endpoint is required")
	}
	if err := c.waitRate(ctx); err != nil {
		return err
	}
	conn, _, err := websocket.Dial(ctx, c.wssURL.String(), nil)
	if err != nil {
		return errors.New("quicknode: websocket dial failed")
	}
	conn.SetReadLimit(c.webSocketReadLimit)
	defer conn.CloseNow()
	id := c.seq.Add(1)
	if err = writeJSON(ctx, conn, rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		return errors.New("quicknode: websocket write failed")
	}
	_, payload, err := conn.Read(ctx)
	if err != nil {
		return errors.New("quicknode: websocket response failed")
	}
	return c.decodeResponse(payload, id, out)
}

func parseEndpoint(raw string, schemes ...string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid endpoint URL")
	}
	for _, scheme := range schemes {
		if u.Scheme == scheme {
			return u, nil
		}
	}
	return nil, errors.New("invalid endpoint scheme")
}

func (c *Client) Close() error {
	if !c.ownHTTP {
		return nil
	}
	if transport, ok := c.http.Transport.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
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

// RPCError is a credential-safe error returned by the remote JSON-RPC method.
// Transport and HTTP failures use different error types.
type RPCError struct {
	Code    int
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("JSON-RPC error %d: %s", e.Code, e.Message)
}

func (c *Client) safeRPCError(e *rpcError) error {
	message := e.Message
	for _, raw := range []string{c.cfg.Endpoint.HTTPURL, c.cfg.Endpoint.WSSURL} {
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
	return &RPCError{Code: e.Code, Message: message}
}

func (c *Client) callHTTP(ctx context.Context, method string, params, out any, retry bool) error {
	id := c.seq.Add(1)
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return err
	}
	attempts := 1
	if retry {
		attempts = 4
	}
	for attempt := 0; attempt < attempts; attempt++ {
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
			if attempt+1 < attempts && ctx.Err() == nil {
				if err := backoff(ctx, attempt, 0); err != nil {
					return err
				}
				continue
			}
			return errors.New("quicknode: request failed")
		}
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, c.maxResponseBytes+1))
		resp.Body.Close()
		if readErr != nil {
			return errors.New("quicknode: read response")
		}
		if int64(len(payload)) > c.maxResponseBytes {
			return fmt.Errorf("quicknode: response exceeds %d bytes", c.maxResponseBytes)
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			if attempt+1 < attempts {
				if err := backoff(ctx, attempt, retryAfter(resp.Header.Get("Retry-After"))); err != nil {
					return err
				}
				continue
			}
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("quicknode: HTTP status %d", resp.StatusCode)
		}
		return c.decodeResponse(payload, id, out)
	}
	return errors.New("quicknode: retries exhausted")
}

func (c *Client) decodeResponse(payload []byte, id uint64, out any) error {
	var response rpcResponse
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&response); err != nil || response.JSONRPC != "2.0" || response.ID != id {
		return errors.New("quicknode: invalid JSON-RPC response")
	}
	if response.Error != nil {
		return c.safeRPCError(response.Error)
	}
	if len(response.Result) == 0 {
		return errors.New("quicknode: JSON-RPC response has no result")
	}
	if err := json.Unmarshal(response.Result, out); err != nil {
		return errors.New("quicknode: invalid JSON-RPC result")
	}
	return nil
}

func (c *Client) waitRate(ctx context.Context) error {
	if c.limiter == nil {
		return nil
	}
	return c.limiter.Wait(ctx)
}

func retryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(value)
	if err == nil && seconds >= 0 && seconds <= 30 {
		return time.Duration(seconds) * time.Second
	}
	return 0
}

func backoff(ctx context.Context, attempt int, requested time.Duration) error {
	delay := time.Duration(1<<attempt) * 100 * time.Millisecond
	if requested > delay {
		delay = requested
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
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

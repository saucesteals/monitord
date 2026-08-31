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

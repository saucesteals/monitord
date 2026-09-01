package evm

import (
	"context"
	"fmt"
	"net/http"

	"github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/catalog/quicknode"
)

var (
	quicknodeEthereumMainnetHTTPURL = monitord.RequiredSecret("quicknode", "ethereum-mainnet-http-url")
	quicknodeEthereumMainnetWSSURL  = monitord.RequiredSecret("quicknode", "ethereum-mainnet-websocket-url")
)

func httpSecret(configured monitord.SecretRef) (monitord.SecretRef, bool) {
	if configured.Group == "" && configured.Key == "" {
		return quicknodeEthereumMainnetHTTPURL, true
	}
	return configured, false
}

func wssSecret(configured monitord.SecretRef) (monitord.SecretRef, bool) {
	if configured.Group == "" && configured.Key == "" {
		return quicknodeEthereumMainnetWSSURL, true
	}
	return configured, false
}

type Config struct {
	Endpoint           quicknode.Endpoint
	HTTPClient         *http.Client
	RequestsPerSecond  float64
	Burst              int
	MaxResponseBytes   int64
	WebSocketReadLimit int64
}

type Client struct {
	provider *quicknode.Client
	chainID  ChainID
}

func Open(ctx context.Context, cfg Config) (*Client, error) {
	provider, err := quicknode.Open(quicknode.Config{
		Endpoint:           cfg.Endpoint,
		HTTPClient:         cfg.HTTPClient,
		RequestsPerSecond:  cfg.RequestsPerSecond,
		Burst:              cfg.Burst,
		MaxResponseBytes:   cfg.MaxResponseBytes,
		WebSocketReadLimit: cfg.WebSocketReadLimit,
	})
	if err != nil {
		return nil, err
	}
	var raw string
	if err := provider.CallRead(ctx, "eth_chainId", []any{}, &raw); err != nil {
		provider.Close()
		return nil, fmt.Errorf("quicknode evm chain handshake: %w", err)
	}
	chainID, err := ParseChainID(raw)
	if err != nil {
		provider.Close()
		return nil, fmt.Errorf("quicknode evm chain handshake: %w", err)
	}
	return &Client{provider: provider, chainID: chainID}, nil
}

func (c *Client) ChainID() ChainID { return c.chainID }
func (c *Client) Close() error     { return c.provider.Close() }

func (c *Client) call(ctx context.Context, method string, params, out any) error {
	return c.provider.CallRead(ctx, method, params, out)
}

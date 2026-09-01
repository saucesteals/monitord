package solana

import (
	"context"
	"fmt"
	"net/http"

	"github.com/saucesteals/monitord/catalog/quicknode"
)

type Config struct {
	Endpoint            quicknode.Endpoint
	HTTPClient          *http.Client
	RequestsPerSecond   float64
	Burst               int
	MaxResponseBytes    int64
	WebSocketReadLimit  int64
	Commitment          Commitment
	ExpectedGenesisHash GenesisHash
}

type Client struct {
	provider    *quicknode.Client
	commitment  Commitment
	genesisHash GenesisHash
}

func Open(ctx context.Context, cfg Config) (*Client, error) {
	commitment := cfg.Commitment
	if commitment == "" {
		commitment = Finalized
	}
	if err := commitment.Validate(); err != nil {
		return nil, err
	}
	if cfg.ExpectedGenesisHash != "" {
		if err := cfg.ExpectedGenesisHash.Validate(); err != nil {
			return nil, err
		}
	}
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
	var genesisHash GenesisHash
	if err := provider.CallRead(ctx, "getGenesisHash", []any{}, &genesisHash); err != nil {
		provider.Close()
		return nil, fmt.Errorf("quicknode solana cluster handshake: %w", err)
	}
	if cfg.ExpectedGenesisHash != "" && genesisHash != cfg.ExpectedGenesisHash {
		provider.Close()
		return nil, fmt.Errorf("quicknode solana cluster handshake: genesis hash is %s, expected %s", genesisHash, cfg.ExpectedGenesisHash)
	}
	return &Client{provider: provider, commitment: commitment, genesisHash: genesisHash}, nil
}

func (c *Client) Commitment() Commitment   { return c.commitment }
func (c *Client) GenesisHash() GenesisHash { return c.genesisHash }
func (c *Client) Close() error             { return c.provider.Close() }

func (c *Client) call(ctx context.Context, method string, params, out any) error {
	return c.provider.CallRead(ctx, method, params, out)
}

func (c *Client) selectCommitment(value Commitment) (Commitment, error) {
	if value == "" {
		return c.commitment, nil
	}
	if err := value.Validate(); err != nil {
		return "", err
	}
	return value, nil
}

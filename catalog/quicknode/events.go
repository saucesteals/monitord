package quicknode

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/saucesteals/monitord"
)

const (
	defaultPollInterval = 2 * time.Second
	checkpointSource    = "quicknode.confirmed-blocks"
)

var _ monitord.Monitor[WalletState] = Wallet{}
var _ monitord.Monitor[struct{}] = Events[struct{}]{}

type Events[S any] struct {
	Name            string
	Filter          Logs
	ExpectedChainID ChainID
	Confirmations   uint64
	Handle          func(*monitord.Tx[S], Log) error
	WSSURL          string
	HTTPURL         string
}

func (e Events[S]) Info() monitord.Info {
	name := e.Name
	if name == "" {
		name = "quicknode-events"
	}
	return monitord.Info{Name: name, Description: "Confirmed QuickNode Ethereum logs"}
}

func (e Events[S]) Plan() monitord.Plan[S] {
	refs := []monitord.SecretRef{}
	if e.WSSURL == "" {
		refs = append(refs, monitord.RequiredSecret("quicknode", "QUICKNODE_WSS_URL"))
	}
	if e.HTTPURL == "" {
		refs = append(refs, monitord.OptionalSecret("quicknode", "QUICKNODE_HTTP_URL"))
	}
	return monitord.Continuous(e.run, monitord.WithSecrets(refs...))
}

func (e Events[S]) run(ctx context.Context, s *monitord.Session[S]) error {
	if e.Handle == nil {
		return errors.New("quicknode: Events.Handle is required")
	}
	if err := e.Filter.Validate(); err != nil {
		return err
	}
	httpURL := e.HTTPURL
	if httpURL == "" {
		httpURL, _ = s.Secrets().Get("quicknode", "QUICKNODE_HTTP_URL")
	}
	if httpURL == "" {
		wss := e.WSSURL
		if wss == "" {
			wss, _ = s.Secrets().Get("quicknode", "QUICKNODE_WSS_URL")
		}
		var err error
		httpURL, err = HTTPFromWSS(wss)
		if err != nil {
			return err
		}
	}
	c, err := Open(ctx, Config{HTTPURL: httpURL})
	if err != nil {
		return err
	}
	defer c.Close()
	if e.ExpectedChainID != "" && c.ChainID() != e.ExpectedChainID {
		return fmt.Errorf("quicknode: expected chain %s, endpoint is %s", e.ExpectedChainID, c.ChainID())
	}
	confirmations, err := confirmationDepth(c.ChainID(), e.Confirmations)
	if err != nil {
		return err
	}
	// Checkpoints are daemon-owned but not exposed as a Session read API. Start
	// inclusively from genesis; durable event IDs make restarts safe. The daemon
	// can optimize this once checkpoint snapshots are exposed to catalog plans.
	var durable Checkpoint
	found, err := s.Checkpoint(checkpointSource, &durable)
	if err != nil {
		return err
	}
	var next uint64
	if found {
		if durable.ChainID != "" && durable.ChainID != c.ChainID() {
			return fmt.Errorf("quicknode: checkpoint chain %s differs from endpoint %s", durable.ChainID, c.ChainID())
		}
		next = durable.NextBlock
		if next > 0 && durable.CanonicalParent != "" {
			prior, loadErr := c.blockByNumber(ctx, next-1, false)
			if loadErr != nil {
				return loadErr
			}
			if prior.Hash != durable.CanonicalParent {
				if next > confirmations {
					next -= confirmations
				} else {
					next = 0
				}
			}
		}
	}
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()
	for {
		head, err := c.blockNumber(ctx)
		if err != nil {
			return err
		}
		if head >= confirmations {
			target := head - confirmations
			for next <= target {
				if err := e.processBlock(ctx, s, c, next); err != nil {
					return err
				}
				next++
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e Events[S]) processBlock(ctx context.Context, s *monitord.Session[S], c *Client, n uint64) error {
	block, err := c.blockByNumber(ctx, n, false)
	if err != nil {
		return err
	}
	logs, err := c.logs(ctx, e.Filter, n, n)
	if err != nil {
		return err
	}
	for _, log := range logs {
		log.ChainID = c.ChainID()
		log = log.Clone()
		if err := s.Commit(ctx, func(tx *monitord.Tx[S]) error {
			if err := e.Handle(tx, log); err != nil {
				return err
			}
			if err := tx.Checkpoint(checkpointSource, Checkpoint{ChainID: c.ChainID(), NextBlock: n, CanonicalParent: block.Hash}); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return s.Commit(ctx, func(tx *monitord.Tx[S]) error {
		if err := tx.Checkpoint(checkpointSource, Checkpoint{ChainID: c.ChainID(), NextBlock: n + 1, CanonicalParent: block.Hash}); err != nil {
			return err
		}
		return nil
	})
}

func confirmationDepth(chain ChainID, configured uint64) (uint64, error) {
	if configured > 0 {
		return configured, nil
	}
	if chain == "0x1" {
		return 12, nil
	}
	return 0, fmt.Errorf("quicknode: confirmations are required for chain %s", chain)
}

func HTTPFromWSS(raw string) (string, error) {
	u, err := endpoint(raw, "ws", "wss")
	if err != nil {
		return "", fmt.Errorf("quicknode: derive HTTP endpoint: %w", err)
	}
	if u.Scheme == "wss" {
		u.Scheme = "https"
	} else {
		u.Scheme = "http"
	}
	return u.String(), nil
}

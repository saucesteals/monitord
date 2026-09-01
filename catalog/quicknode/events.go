package quicknode

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/saucesteals/monitord"
)

var (
	quicknodeWebSocketURL = monitord.RequiredSecret("quicknode", "websocket-url")
	quicknodeHTTPURL      = monitord.OptionalSecret("quicknode", "http-url")
)

const (
	defaultPollInterval = 2 * time.Second
	checkpointSource    = "quicknode.confirmed-blocks"
	eventsCursorSource  = "quicknode.events-cursor"
)

type eventsCursor struct {
	ChainID         ChainID `json:"chain_id"`
	NextBlock       uint64  `json:"next_block"`
	CanonicalParent Hash    `json:"canonical_parent,omitempty"`
	CurrentBlock    Hash    `json:"current_block,omitempty"`
	NextLog         uint64  `json:"next_log,omitempty"`
}

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
		refs = append(refs, quicknodeWebSocketURL)
	}
	if e.HTTPURL == "" {
		refs = append(refs, quicknodeHTTPURL)
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
		httpURL, _ = s.Secrets().Get(quicknodeHTTPURL)
	}
	if httpURL == "" {
		wss := e.WSSURL
		if wss == "" {
			wss, _ = s.Secrets().Get(quicknodeWebSocketURL)
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
	var cursor eventsCursor
	found, err := s.Checkpoint(eventsCursorSource, &cursor)
	if err != nil {
		return err
	}
	if found {
		if cursor.ChainID != c.ChainID() {
			return fmt.Errorf("quicknode: events cursor chain %s differs from endpoint %s", cursor.ChainID, c.ChainID())
		}
		if (cursor.CurrentBlock == "") != (cursor.NextLog == 0) {
			return errors.New("quicknode: events cursor has an inconsistent current block and log position")
		}
		if cursor.CurrentBlock != "" && cursor.CanonicalParent == "" {
			return errors.New("quicknode: events cursor has a current block without its canonical parent")
		}
	} else {
		cursor.ChainID = c.ChainID()
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
			for cursor.NextBlock <= target {
				if err := e.processBlock(ctx, s, c, &cursor, confirmations); err != nil {
					return err
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e Events[S]) processBlock(ctx context.Context, s *monitord.Session[S], c *Client, cursor *eventsCursor, confirmations uint64) error {
	n := cursor.NextBlock
	block, err := c.blockByNumber(ctx, n, false)
	if err != nil {
		return err
	}
	if cursor.CanonicalParent != "" && block.ParentHash != cursor.CanonicalParent {
		return rewindEventsCursor(ctx, s, cursor, confirmations)
	}
	if cursor.CurrentBlock != "" && block.Hash != cursor.CurrentBlock {
		return rewindEventsCursor(ctx, s, cursor, confirmations)
	}
	logs, err := c.logs(ctx, e.Filter, n, n)
	if err != nil {
		return err
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i].LogIndex < logs[j].LogIndex })
	for _, log := range logs {
		if log.BlockNumber != n || log.BlockHash != block.Hash {
			return fmt.Errorf("quicknode: log does not belong to canonical block %d", n)
		}
		if uint64(log.LogIndex) < cursor.NextLog {
			continue
		}
		log.ChainID = c.ChainID()
		log = log.Clone()
		nextCursor := eventsCursor{
			ChainID:         c.ChainID(),
			NextBlock:       n,
			CanonicalParent: block.ParentHash,
			CurrentBlock:    block.Hash,
			NextLog:         uint64(log.LogIndex) + 1,
		}
		if err := s.Commit(ctx, func(tx *monitord.Tx[S]) error {
			if err := e.Handle(tx, log); err != nil {
				return err
			}
			return tx.Checkpoint(eventsCursorSource, nextCursor)
		}); err != nil {
			return err
		}
		*cursor = nextCursor
	}
	nextCursor := eventsCursor{ChainID: c.ChainID(), NextBlock: n + 1, CanonicalParent: block.Hash}
	if err := s.Commit(ctx, func(tx *monitord.Tx[S]) error {
		return tx.Checkpoint(eventsCursorSource, nextCursor)
	}); err != nil {
		return err
	}
	*cursor = nextCursor
	return nil
}

func rewindEventsCursor[S any](ctx context.Context, s *monitord.Session[S], cursor *eventsCursor, depth uint64) error {
	next := uint64(0)
	if cursor.NextBlock > depth {
		next = cursor.NextBlock - depth
	}
	// The cursor can replay confirmed source records after a finality breach, but
	// generic state changed by Handle is intentionally not treated as reversible.
	rewound := eventsCursor{ChainID: cursor.ChainID, NextBlock: next}
	if err := s.Commit(ctx, func(tx *monitord.Tx[S]) error {
		return tx.Checkpoint(eventsCursorSource, rewound)
	}); err != nil {
		return err
	}
	*cursor = rewound
	return nil
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

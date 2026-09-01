package evm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/catalog/quicknode"
)

const (
	defaultPollInterval = 2 * time.Second
	eventsCursorSource  = "quicknode.evm.events-cursor"
)

type eventsCursor struct {
	ChainID         ChainID `json:"chain_id"`
	Filter          Logs    `json:"filter"`
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
	BackfillFrom    *uint64
	Handle          func(*monitord.Tx[S], Log) error
	HTTPURL         string
	HTTPSecret      monitord.SecretRef
}

func (e Events[S]) Info() monitord.Info {
	name := e.Name
	if name == "" {
		name = "quicknode-events"
	}
	return monitord.Info{Name: name, Description: "Confirmed QuickNode EVM logs"}
}

func (e Events[S]) Plan() monitord.Plan[S] {
	refs := []monitord.SecretRef{}
	if e.HTTPURL == "" {
		ref, _ := httpSecret(e.HTTPSecret)
		refs = append(refs, ref)
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
	_, defaultEndpoint := httpSecret(e.HTTPSecret)
	if httpURL == "" {
		ref, _ := httpSecret(e.HTTPSecret)
		var err error
		httpURL, err = s.Secrets().Require(ref)
		if err != nil {
			return err
		}
	}
	c, err := Open(ctx, Config{Endpoint: quicknode.Endpoint{HTTPURL: httpURL}})
	if err != nil {
		return err
	}
	defer c.Close()
	expectedChainID := e.ExpectedChainID
	if expectedChainID == "" && e.HTTPURL == "" && defaultEndpoint {
		expectedChainID = "0x1"
	}
	if expectedChainID != "" && c.ChainID() != expectedChainID {
		return fmt.Errorf("quicknode evm: expected chain %s, endpoint is %s", expectedChainID, c.ChainID())
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
		if !reflect.DeepEqual(cursor.Filter, e.Filter.Clone()) {
			return errors.New("quicknode evm: event filter differs from the saved cursor")
		}
		if (cursor.CurrentBlock == "") != (cursor.NextLog == 0) {
			return errors.New("quicknode: events cursor has an inconsistent current block and log position")
		}
		if cursor.CurrentBlock != "" && cursor.CanonicalParent == "" {
			return errors.New("quicknode: events cursor has a current block without its canonical parent")
		}
	} else {
		cursor.ChainID = c.ChainID()
		cursor.Filter = e.Filter.Clone()
		if e.BackfillFrom != nil {
			cursor.NextBlock = *e.BackfillFrom
		} else {
			head, err := c.blockNumber(ctx)
			if err != nil {
				return err
			}
			if head >= confirmations {
				cursor.NextBlock = head - confirmations + 1
			}
		}
		if err := s.Commit(ctx, func(tx *monitord.Tx[S]) error {
			return tx.Checkpoint(eventsCursorSource, cursor)
		}); err != nil {
			return err
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
			for cursor.NextBlock <= target {
				if err := e.processBlock(ctx, s, c, &cursor); err != nil {
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

func (e Events[S]) processBlock(ctx context.Context, s *monitord.Session[S], c *Client, cursor *eventsCursor) error {
	n := cursor.NextBlock
	block, err := c.blockByNumber(ctx, n, false)
	if err != nil {
		return err
	}
	if cursor.CanonicalParent != "" && block.ParentHash != cursor.CanonicalParent {
		return fmt.Errorf("quicknode evm: finalized chain changed before block %d; operator review required", n)
	}
	if cursor.CurrentBlock != "" && block.Hash != cursor.CurrentBlock {
		return fmt.Errorf("quicknode evm: finalized block %d changed while processing; operator review required", n)
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
			Filter:          cursor.Filter.Clone(),
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
	nextCursor := eventsCursor{ChainID: c.ChainID(), Filter: cursor.Filter.Clone(), NextBlock: n + 1, CanonicalParent: block.Hash}
	if err := s.Commit(ctx, func(tx *monitord.Tx[S]) error {
		return tx.Checkpoint(eventsCursorSource, nextCursor)
	}); err != nil {
		return err
	}
	*cursor = nextCursor
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

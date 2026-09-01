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
	eventsCursorSource     = "quicknode.evm.events-cursor"
	defaultBackfillRange   = uint64(10_000)
	defaultReconcilePeriod = 2 * time.Second
)

type eventsCursor struct {
	ChainID         ChainID `json:"chain_id"`
	Filter          Logs    `json:"filter"`
	NextBlock       uint64  `json:"next_block"`
	CanonicalParent Hash    `json:"canonical_parent,omitempty"`
	CurrentBlock    Hash    `json:"current_block,omitempty"`
	NextLog         uint64  `json:"next_log,omitempty"`
}

type EventUpdate[S any] func(*monitord.Tx[S]) error

// Events sends matching WebSocket logs to Handle immediately and repairs any
// subscription gap with batched HTTP history. Handle must produce stable event
// IDs and deterministic content so inclusive replay safely coalesces.
type Events[S any] struct {
	Name              string
	Filter            Logs
	ExpectedChainID   ChainID
	Confirmations     uint64
	BackfillFrom      *uint64
	BackfillRange     uint64
	ReconcileInterval time.Duration
	Handle            func(context.Context, *Client, Log) (EventUpdate[S], error)
	HTTPURL           string
	WSSURL            string
	HTTPSecret        monitord.SecretRef
	WSSSecret         monitord.SecretRef
}

var _ monitord.Monitor[struct{}] = Events[struct{}]{}

func (e Events[S]) Info() monitord.Info {
	name := e.Name
	if name == "" {
		name = "quicknode-events"
	}
	return monitord.Info{Name: name, Description: "Live filtered EVM logs with durable HTTP recovery"}
}

func (e Events[S]) Plan() monitord.Plan[S] {
	refs := make([]monitord.SecretRef, 0, 2)
	if e.HTTPURL == "" {
		ref, _ := httpSecret(e.HTTPSecret)
		refs = append(refs, ref)
	}
	if e.WSSURL == "" {
		ref, _ := wssSecret(e.WSSSecret)
		refs = append(refs, ref)
	}
	return monitord.Continuous(e.run, monitord.WithSecrets(refs...))
}

func (e Events[S]) run(ctx context.Context, session *monitord.Session[S]) error {
	if e.Handle == nil {
		return errors.New("quicknode evm: Events.Handle is required")
	}
	if err := e.Filter.Validate(); err != nil {
		return err
	}
	httpURL, wssURL, defaultEndpoint, err := e.endpoints(session)
	if err != nil {
		return err
	}
	client, err := Open(ctx, Config{Endpoint: quicknode.Endpoint{HTTPURL: httpURL, WSSURL: wssURL}})
	if err != nil {
		return err
	}
	defer client.Close()
	expected := e.ExpectedChainID
	if expected == "" && defaultEndpoint {
		expected = "0x1"
	}
	if expected != "" && client.ChainID() != expected {
		return fmt.Errorf("quicknode evm: expected chain %s, endpoint is %s", expected, client.ChainID())
	}
	confirmations, err := confirmationDepth(client.ChainID(), e.Confirmations)
	if err != nil {
		return err
	}

	// Open the filtered live lane first; replay then closes the startup race.
	subscription, err := client.SubscribeLogs(ctx, e.Filter)
	if err != nil {
		return err
	}
	defer subscription.Close()
	cursor, err := e.loadCursor(ctx, session, client, confirmations)
	if err != nil {
		return err
	}
	interval := e.ReconcileInterval
	if interval <= 0 {
		interval = defaultReconcilePeriod
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// A bounded drain gives live matches priority without starving recovery.
		for range 64 {
			select {
			case log, ok := <-subscription.C():
				if !ok {
					return errors.New("quicknode evm: log subscription closed")
				}
				if err := e.handle(ctx, session, client, log, nil); err != nil {
					return err
				}
			default:
				goto reconcile
			}
		}
	reconcile:
		caughtUp, err := e.reconcileOne(ctx, session, client, confirmations, &cursor)
		if err != nil {
			return err
		}
		if !caughtUp {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case log, ok := <-subscription.C():
			if !ok {
				return errors.New("quicknode evm: log subscription closed")
			}
			if err := e.handle(ctx, session, client, log, nil); err != nil {
				return err
			}
		case err, ok := <-subscription.Err():
			if ok && err != nil {
				return err
			}
			return errors.New("quicknode evm: log subscription stopped")
		case <-ticker.C:
		}
	}
}

func (e Events[S]) endpoints(session *monitord.Session[S]) (string, string, bool, error) {
	httpURL, wssURL := e.HTTPURL, e.WSSURL
	_, defaultHTTP := httpSecret(e.HTTPSecret)
	_, defaultWSS := wssSecret(e.WSSSecret)
	if httpURL == "" {
		ref, _ := httpSecret(e.HTTPSecret)
		var err error
		httpURL, err = session.Secrets().Require(ref)
		if err != nil {
			return "", "", false, err
		}
	}
	if wssURL == "" {
		ref, _ := wssSecret(e.WSSSecret)
		var err error
		wssURL, err = session.Secrets().Require(ref)
		if err != nil {
			return "", "", false, err
		}
	}
	return httpURL, wssURL, e.HTTPURL == "" && e.WSSURL == "" && defaultHTTP && defaultWSS, nil
}

func (e Events[S]) loadCursor(ctx context.Context, session *monitord.Session[S], client *Client, confirmations uint64) (eventsCursor, error) {
	var cursor eventsCursor
	found, err := session.Checkpoint(eventsCursorSource, &cursor)
	if err != nil {
		return cursor, err
	}
	if found {
		if cursor.ChainID != client.ChainID() || !reflect.DeepEqual(cursor.Filter, e.Filter.Clone()) {
			return cursor, errors.New("quicknode evm: saved event source differs from this chain or filter")
		}
		if (cursor.CurrentBlock == "") != (cursor.NextLog == 0) {
			return cursor, errors.New("quicknode evm: event cursor has inconsistent block and log progress")
		}
		return cursor, nil
	}
	cursor = eventsCursor{ChainID: client.ChainID(), Filter: e.Filter.Clone()}
	if e.BackfillFrom != nil {
		cursor.NextBlock = *e.BackfillFrom
	} else {
		head, err := client.blockNumber(ctx)
		if err != nil {
			return cursor, err
		}
		if head >= confirmations {
			cursor.NextBlock = head - confirmations + 1
		}
	}
	return cursor, e.commit(ctx, session, cursor, nil)
}

func (e Events[S]) reconcileOne(ctx context.Context, session *monitord.Session[S], client *Client, confirmations uint64, cursor *eventsCursor) (bool, error) {
	head, err := client.blockNumber(ctx)
	if err != nil {
		return false, err
	}
	if head < confirmations || cursor.NextBlock > head-confirmations {
		return true, nil
	}
	target := head - confirmations
	rangeSize := e.BackfillRange
	if rangeSize == 0 {
		rangeSize = defaultBackfillRange
	}
	to := min(cursor.NextBlock+rangeSize-1, target)
	if err := client.validateBoundary(ctx, *cursor); err != nil {
		return false, err
	}
	logs, err := client.logs(ctx, e.Filter, cursor.NextBlock, to)
	if err != nil {
		return false, err
	}
	sort.Slice(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		if logs[i].TxIndex != logs[j].TxIndex {
			return logs[i].TxIndex < logs[j].TxIndex
		}
		return logs[i].LogIndex < logs[j].LogIndex
	})
	for _, log := range logs {
		if reconciled(*cursor, log) {
			continue
		}
		next := *cursor
		next.NextBlock, next.CurrentBlock, next.NextLog = log.BlockNumber, log.BlockHash, uint64(log.LogIndex)+1
		if err := e.handle(ctx, session, client, log, &next); err != nil {
			return false, err
		}
		*cursor = next
	}
	end, err := client.blockByNumber(ctx, to)
	if err != nil {
		return false, err
	}
	next := eventsCursor{ChainID: client.ChainID(), Filter: cursor.Filter.Clone(), NextBlock: to + 1, CanonicalParent: end.Hash}
	if err := e.commit(ctx, session, next, nil); err != nil {
		return false, err
	}
	*cursor = next
	return to == target, nil
}

func (c *Client) validateBoundary(ctx context.Context, cursor eventsCursor) error {
	if cursor.CurrentBlock != "" {
		block, err := c.blockByNumber(ctx, cursor.NextBlock)
		if err != nil {
			return err
		}
		if block.Hash != cursor.CurrentBlock {
			return fmt.Errorf("quicknode evm: block %d changed during replay", cursor.NextBlock)
		}
		return nil
	}
	if cursor.NextBlock == 0 || cursor.CanonicalParent == "" {
		return nil
	}
	parent, err := c.blockByNumber(ctx, cursor.NextBlock-1)
	if err != nil {
		return err
	}
	if parent.Hash != cursor.CanonicalParent {
		return fmt.Errorf("quicknode evm: canonical history changed before block %d", cursor.NextBlock)
	}
	return nil
}

func (e Events[S]) handle(ctx context.Context, session *monitord.Session[S], client *Client, log Log, cursor *eventsCursor) error {
	update, err := e.Handle(ctx, client, log.Clone())
	if err != nil {
		return err
	}
	if update == nil && cursor == nil {
		return nil
	}
	return e.commit(ctx, session, valueOrZero(cursor), update)
}

func (e Events[S]) commit(ctx context.Context, session *monitord.Session[S], cursor eventsCursor, update EventUpdate[S]) error {
	return session.Commit(ctx, func(tx *monitord.Tx[S]) error {
		if update != nil {
			if err := update(tx); err != nil {
				return err
			}
		}
		if cursor.ChainID != "" {
			return tx.Checkpoint(eventsCursorSource, cursor)
		}
		return nil
	})
}

func reconciled(cursor eventsCursor, log Log) bool {
	if log.BlockNumber < cursor.NextBlock {
		return true
	}
	return log.BlockNumber == cursor.NextBlock && cursor.CurrentBlock == log.BlockHash && uint64(log.LogIndex) < cursor.NextLog
}

func valueOrZero[T any](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}

func confirmationDepth(chain ChainID, configured uint64) (uint64, error) {
	if configured > 0 {
		return configured, nil
	}
	if chain == "0x1" {
		return 12, nil
	}
	return 0, fmt.Errorf("quicknode evm: confirmations are required for chain %s", chain)
}

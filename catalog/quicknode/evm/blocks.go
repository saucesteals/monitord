package evm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/catalog/quicknode"
)

const (
	defaultBlockReconcilePeriod  = 2 * time.Second
	defaultMaxLiveBlocks         = 256
	defaultBlockReplayBatch      = 32
	defaultBlockFetchConcurrency = 8
)

type blockCursor struct {
	ChainID         ChainID `json:"chain_id"`
	NextBlock       uint64  `json:"next_block"`
	CanonicalParent Hash    `json:"canonical_parent,omitempty"`
}

type blockReference struct {
	Number     uint64 `json:"number"`
	Hash       Hash   `json:"hash"`
	ParentHash Hash   `json:"parent_hash"`
}

func (r blockReference) block(chain ChainID) Block {
	return Block{ChainID: chain, Number: r.Number, Hash: r.Hash, ParentHash: r.ParentHash}
}

type blockLive struct {
	ChainID   ChainID          `json:"chain_id"`
	NextBlock uint64           `json:"next_block"`
	Blocks    []blockReference `json:"blocks,omitempty"`
}

func (l blockLive) clone() blockLive {
	l.Blocks = append([]blockReference(nil), l.Blocks...)
	return l
}

type BlockUpdate[S any] func(*monitord.Tx[S]) error

// Blocks delivers canonical EVM blocks immediately after a WebSocket head and
// independently replays canonical HTTP history. Live and replay observations
// are inclusive: Handle must be deterministic and deduplicate by Block.ID or
// transaction identity. Removed live blocks are delivered in reverse order.
//
// Confirmations bounds the live reorganization journal and controls the HTTP
// replay target. It does not delay live delivery.
type Blocks[S any] struct {
	Name              string
	ExpectedChainID   ChainID
	Confirmations     uint64
	BackfillFrom      *uint64
	ReconcileInterval time.Duration
	ReplayBatch       uint64
	FetchConcurrency  int
	MaxLiveBlocks     int
	Handle            func(context.Context, *Client, Block) (BlockUpdate[S], error)
	HTTPURL           string
	WSSURL            string
	HTTPSecret        monitord.SecretRef
	WSSSecret         monitord.SecretRef
}

var _ monitord.Monitor[struct{}] = Blocks[struct{}]{}

func (b Blocks[S]) Info() monitord.Info {
	name := b.Name
	if name == "" {
		name = "quicknode-blocks"
	}
	return monitord.Info{Name: name, Description: "Live canonical EVM blocks with durable HTTP recovery"}
}

func (b Blocks[S]) Plan() monitord.Plan[S] {
	refs := make([]monitord.SecretRef, 0, 2)
	if b.HTTPURL == "" {
		ref, _ := httpSecret(b.HTTPSecret)
		refs = append(refs, ref)
	}
	if b.WSSURL == "" {
		ref, _ := wssSecret(b.WSSSecret)
		refs = append(refs, ref)
	}
	return monitord.Continuous(b.run, monitord.WithSecrets(refs...))
}

func (b Blocks[S]) run(ctx context.Context, session *monitord.Session[S]) error {
	if b.Handle == nil {
		return errors.New("quicknode evm: Blocks.Handle is required")
	}
	httpURL, wssURL, defaultEndpoint, err := b.endpoints(session)
	if err != nil {
		return err
	}
	client, err := Open(ctx, Config{Endpoint: quicknode.Endpoint{HTTPURL: httpURL, WSSURL: wssURL}})
	if err != nil {
		return err
	}
	defer client.Close()
	expected := b.ExpectedChainID
	if expected == "" && defaultEndpoint {
		expected = "0x1"
	}
	if expected != "" && client.ChainID() != expected {
		return fmt.Errorf("quicknode evm: expected chain %s, endpoint is %s", expected, client.ChainID())
	}
	confirmations, err := confirmationDepth(client.ChainID(), b.Confirmations)
	if err != nil {
		return err
	}
	maxLive := b.MaxLiveBlocks
	if maxLive == 0 {
		maxLive = defaultMaxLiveBlocks
	}
	if maxLive < int(confirmations)+2 {
		return errors.New("quicknode evm: MaxLiveBlocks must exceed confirmations by at least two")
	}
	if b.ReplayBatch > 10_000 {
		return errors.New("quicknode evm: ReplayBatch cannot exceed 10000")
	}
	if b.FetchConcurrency < 0 {
		return errors.New("quicknode evm: FetchConcurrency cannot be negative")
	}

	// Subscribe before taking any snapshots so queued heads close the startup gap.
	subscription, err := client.SubscribeHeads(ctx)
	if err != nil {
		return err
	}
	defer subscription.Close()
	cursor, err := b.loadCursor(ctx, session, client, confirmations)
	if err != nil {
		return err
	}
	live, err := b.loadLive(ctx, session, client, maxLive)
	if err != nil {
		return err
	}
	interval := b.ReconcileInterval
	if interval <= 0 {
		interval = defaultBlockReconcilePeriod
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		for range 64 {
			select {
			case head, ok := <-subscription.C():
				if !ok {
					return errors.New("quicknode evm: head subscription closed")
				}
				if err := b.syncLive(ctx, session, client, &live, head, confirmations, maxLive); err != nil {
					return err
				}
			default:
				goto reconcile
			}
		}
	reconcile:
		caughtUp, err := b.reconcile(ctx, session, client, confirmations, &cursor)
		if err != nil {
			return err
		}
		if !caughtUp {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case head, ok := <-subscription.C():
			if !ok {
				return errors.New("quicknode evm: head subscription closed")
			}
			if err := b.syncLive(ctx, session, client, &live, head, confirmations, maxLive); err != nil {
				return err
			}
		case err, ok := <-subscription.Err():
			if ok && err != nil {
				return err
			}
			return errors.New("quicknode evm: head subscription stopped")
		case <-ticker.C:
		}
	}
}

func (b Blocks[S]) endpoints(session *monitord.Session[S]) (string, string, bool, error) {
	httpURL, wssURL := b.HTTPURL, b.WSSURL
	_, defaultHTTP := httpSecret(b.HTTPSecret)
	_, defaultWSS := wssSecret(b.WSSSecret)
	if httpURL == "" {
		ref, _ := httpSecret(b.HTTPSecret)
		var err error
		httpURL, err = session.Secrets().Require(ref)
		if err != nil {
			return "", "", false, err
		}
	}
	if wssURL == "" {
		ref, _ := wssSecret(b.WSSSecret)
		var err error
		wssURL, err = session.Secrets().Require(ref)
		if err != nil {
			return "", "", false, err
		}
	}
	return httpURL, wssURL, b.HTTPURL == "" && b.WSSURL == "" && defaultHTTP && defaultWSS, nil
}

func (b Blocks[S]) loadCursor(ctx context.Context, session *monitord.Session[S], client *Client, confirmations uint64) (blockCursor, error) {
	var cursor blockCursor
	found, err := session.Checkpoint(b.cursorSource(), &cursor)
	if err != nil {
		return cursor, err
	}
	if found {
		if cursor.ChainID != client.ChainID() {
			return cursor, errors.New("quicknode evm: saved block cursor belongs to another chain")
		}
		return cursor, nil
	}
	cursor.ChainID = client.ChainID()
	if b.BackfillFrom != nil {
		cursor.NextBlock = *b.BackfillFrom
	} else {
		head, err := client.blockNumber(ctx)
		if err != nil {
			return cursor, err
		}
		if head >= confirmations {
			cursor.NextBlock = head - confirmations + 1
		}
	}
	return cursor, b.commit(ctx, session, &cursor, nil, nil)
}

func (b Blocks[S]) loadLive(ctx context.Context, session *monitord.Session[S], client *Client, maxLive int) (blockLive, error) {
	var live blockLive
	found, err := session.Checkpoint(b.liveSource(), &live)
	if err != nil {
		return live, err
	}
	if found {
		if live.ChainID != client.ChainID() {
			return live, errors.New("quicknode evm: saved live blocks belong to another chain")
		}
		if len(live.Blocks) > maxLive {
			return live, errors.New("quicknode evm: live block journal exceeds its bound")
		}
		return live, nil
	}
	head, err := client.blockNumber(ctx)
	if err != nil {
		return live, err
	}
	live = blockLive{ChainID: client.ChainID(), NextBlock: head + 1}
	return live, b.commit(ctx, session, nil, &live, nil)
}

func (b Blocks[S]) syncLive(ctx context.Context, session *monitord.Session[S], client *Client, live *blockLive, head Head, confirmations uint64, maxLive int) error {
	headNumber, err := parseUintQuantity(head.Number)
	if err != nil {
		return err
	}
	if _, err = ParseHash(string(head.Hash)); err != nil {
		return err
	}
	if _, err = ParseHash(string(head.ParentHash)); err != nil {
		return err
	}
	if err = b.removeOrphans(ctx, session, client, live); err != nil {
		return err
	}
	if headNumber < live.NextBlock {
		return b.trimLive(ctx, session, client, live, headNumber, confirmations)
	}
	for number := live.NextBlock; number <= headNumber; number++ {
		if len(live.Blocks) == maxLive {
			if err := b.trimLive(ctx, session, client, live, headNumber, confirmations); err != nil {
				return err
			}
			if len(live.Blocks) == maxLive {
				return errors.New("quicknode evm: live block journal is full")
			}
		}
		block, err := client.BlockByNumber(ctx, number)
		if err != nil {
			return err
		}
		if len(live.Blocks) > 0 && block.ParentHash != live.Blocks[len(live.Blocks)-1].Hash {
			return errors.New("quicknode evm: live block does not extend canonical journal")
		}
		block.Confirmed = false
		update, err := b.Handle(ctx, client, block.Clone())
		if err != nil {
			return err
		}
		next := live.clone()
		next.NextBlock = block.Number + 1
		next.Blocks = append(next.Blocks, blockReference{Number: block.Number, Hash: block.Hash, ParentHash: block.ParentHash})
		if err = b.commit(ctx, session, nil, &next, update); err != nil {
			return err
		}
		*live = next
	}
	return b.trimLive(ctx, session, client, live, headNumber, confirmations)
}

func (b Blocks[S]) removeOrphans(ctx context.Context, session *monitord.Session[S], client *Client, live *blockLive) error {
	for len(live.Blocks) > 0 {
		last := live.Blocks[len(live.Blocks)-1]
		canonical, err := client.blockByNumber(ctx, last.Number)
		if err != nil {
			return err
		}
		if canonical.Hash == last.Hash {
			return nil
		}
		removed := last.block(client.ChainID())
		removed.Removed = true
		update, err := b.Handle(ctx, client, removed)
		if err != nil {
			return err
		}
		next := live.clone()
		next.Blocks = next.Blocks[:len(next.Blocks)-1]
		next.NextBlock = last.Number
		if err = b.commit(ctx, session, nil, &next, update); err != nil {
			return err
		}
		*live = next
	}
	return nil
}

func (b Blocks[S]) trimLive(ctx context.Context, session *monitord.Session[S], client *Client, live *blockLive, head, confirmations uint64) error {
	if head < confirmations || len(live.Blocks) == 0 {
		return nil
	}
	finalized := head - confirmations
	cut := 0
	for cut < len(live.Blocks) && live.Blocks[cut].Number <= finalized {
		canonical, err := client.blockByNumber(ctx, live.Blocks[cut].Number)
		if err != nil {
			return err
		}
		if canonical.Hash != live.Blocks[cut].Hash {
			return errors.New("quicknode evm: finalized live block changed before trimming")
		}
		cut++
	}
	if cut == 0 {
		return nil
	}
	next := live.clone()
	next.Blocks = append([]blockReference(nil), next.Blocks[cut:]...)
	if err := b.commit(ctx, session, nil, &next, nil); err != nil {
		return err
	}
	*live = next
	return nil
}

func (b Blocks[S]) reconcile(ctx context.Context, session *monitord.Session[S], client *Client, confirmations uint64, cursor *blockCursor) (bool, error) {
	head, err := client.blockNumber(ctx)
	if err != nil {
		return false, err
	}
	if head < confirmations {
		return true, nil
	}
	target := head - confirmations
	if cursor.NextBlock > target {
		return true, nil
	}
	if cursor.NextBlock > 0 && cursor.CanonicalParent != "" {
		parent, err := client.blockByNumber(ctx, cursor.NextBlock-1)
		if err != nil {
			return false, err
		}
		if parent.Hash != cursor.CanonicalParent {
			return false, fmt.Errorf("quicknode evm: canonical history changed before block %d", cursor.NextBlock)
		}
	}
	batch := b.ReplayBatch
	if batch == 0 {
		batch = defaultBlockReplayBatch
	}
	to := target
	if remaining := target - cursor.NextBlock + 1; batch < remaining {
		to = cursor.NextBlock + batch - 1
	}
	concurrency := b.FetchConcurrency
	if concurrency == 0 {
		concurrency = defaultBlockFetchConcurrency
	}
	blocks, err := client.blocksByNumber(ctx, cursor.NextBlock, to, concurrency)
	if err != nil {
		return false, err
	}
	if cursor.CanonicalParent != "" && len(blocks) > 0 && blocks[0].ParentHash != cursor.CanonicalParent {
		return false, fmt.Errorf("quicknode evm: replay range does not extend block %d", cursor.NextBlock-1)
	}
	for i := range blocks {
		block := blocks[i]
		block.Confirmed = true
		update, err := b.Handle(ctx, client, block.Clone())
		if err != nil {
			return false, err
		}
		next := blockCursor{ChainID: client.ChainID(), NextBlock: block.Number + 1, CanonicalParent: block.Hash}
		if err = b.commit(ctx, session, &next, nil, update); err != nil {
			return false, err
		}
		*cursor = next
	}
	return to == target, nil
}

func (b Blocks[S]) commit(ctx context.Context, session *monitord.Session[S], cursor *blockCursor, live *blockLive, update BlockUpdate[S]) error {
	return session.Commit(ctx, func(tx *monitord.Tx[S]) error {
		if update != nil {
			if err := update(tx); err != nil {
				return err
			}
		}
		if cursor != nil {
			if err := tx.Checkpoint(b.cursorSource(), *cursor); err != nil {
				return err
			}
		}
		if live != nil {
			return tx.Checkpoint(b.liveSource(), *live)
		}
		return nil
	})
}

func (b Blocks[S]) cursorSource() string { return b.checkpointPrefix() + ".cursor" }
func (b Blocks[S]) liveSource() string   { return b.checkpointPrefix() + ".live" }
func (b Blocks[S]) checkpointPrefix() string {
	if b.Name == "" {
		return "quicknode.evm.blocks"
	}
	return "quicknode.evm.blocks." + b.Name
}

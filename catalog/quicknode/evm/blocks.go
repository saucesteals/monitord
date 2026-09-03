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
	defaultMaxRecentBlocks       = 256
	defaultBlockReplayBatch      = 32
	defaultBlockFetchConcurrency = 8
)

type blockReference struct {
	Number     uint64 `json:"number"`
	Hash       Hash   `json:"hash"`
	ParentHash Hash   `json:"parent_hash"`
}

func (r blockReference) block(chain ChainID) Block {
	return Block{ChainID: chain, Number: r.Number, Hash: r.Hash, ParentHash: r.ParentHash}
}

type blockProgress struct {
	ChainID         ChainID          `json:"chain_id"`
	NextBlock       uint64           `json:"next_block"`
	CanonicalParent Hash             `json:"canonical_parent,omitempty"`
	Recent          []blockReference `json:"recent,omitempty"`
}

func (p blockProgress) clone() blockProgress {
	p.Recent = append([]blockReference(nil), p.Recent...)
	return p
}

type BlockUpdate[S any] func(*monitord.Tx[S]) error

// Blocks advances one canonical EVM block sequence. WebSocket heads wake the
// source immediately; HTTP block reads provide ordered content and reconnect
// recovery. Each block is applied once. Recent orphaned blocks are removed in
// reverse order before their canonical replacements are applied.
//
// Handle receives a detached durable state snapshot for selecting relevant
// transactions and performing network reads before returning a short atomic
// update. The update must recheck predicates against tx.State.
type Blocks[S any] struct {
	Name              string
	ExpectedChainID   ChainID
	Confirmations     uint64
	BackfillFrom      *uint64
	ReconcileInterval time.Duration
	ReplayBatch       uint64
	FetchConcurrency  int
	MaxRecentBlocks   int
	Handle            func(context.Context, *Client, S, Block) (BlockUpdate[S], error)
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
	return monitord.Info{Name: name, Description: "Ordered canonical EVM blocks with immediate WebSocket wakeups"}
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
	confirmations, maxRecent, err := b.validate(client.ChainID())
	if err != nil {
		return err
	}

	// Open the subscription before the snapshot, then process the snapshot block
	// itself. Transactions published during startup cannot fall behind finality.
	subscription, err := client.SubscribeHeads(ctx)
	if err != nil {
		return err
	}
	defer subscription.Close()
	progress, head, err := b.loadProgress(ctx, session, client, maxRecent)
	if err != nil {
		return err
	}
	if err = b.advance(ctx, session, client, &progress, head, confirmations, maxRecent, false); err != nil {
		return err
	}

	interval := b.ReconcileInterval
	if interval <= 0 {
		interval = defaultBlockReconcilePeriod
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case head, ok := <-subscription.C():
			if !ok {
				return errors.New("quicknode evm: head subscription closed")
			}
			announcement, err := decodeHead(head)
			if err != nil {
				return err
			}
			extends := announcement.Number == progress.NextBlock && announcement.ParentHash == progress.CanonicalParent
			if err = b.advance(ctx, session, client, &progress, announcement.Number, confirmations, maxRecent, extends); err != nil {
				return err
			}
		case err, ok := <-subscription.Err():
			if ok && err != nil {
				return err
			}
			return errors.New("quicknode evm: head subscription stopped")
		case <-ticker.C:
			head, err := client.blockNumber(ctx)
			if err != nil {
				return err
			}
			if err = b.advance(ctx, session, client, &progress, head, confirmations, maxRecent, false); err != nil {
				return err
			}
		}
	}
}

func (b Blocks[S]) validate(chain ChainID) (uint64, int, error) {
	confirmations, err := confirmationDepth(chain, b.Confirmations)
	if err != nil {
		return 0, 0, err
	}
	maxRecent := b.MaxRecentBlocks
	if maxRecent == 0 {
		maxRecent = defaultMaxRecentBlocks
	}
	if maxRecent < 2 || confirmations > uint64(maxRecent-2) {
		return 0, 0, errors.New("quicknode evm: MaxRecentBlocks must exceed confirmations by at least two")
	}
	if b.ReplayBatch > 10_000 {
		return 0, 0, errors.New("quicknode evm: ReplayBatch cannot exceed 10000")
	}
	if b.FetchConcurrency < 0 || b.FetchConcurrency > 256 {
		return 0, 0, errors.New("quicknode evm: FetchConcurrency must be between 0 and 256")
	}
	return confirmations, maxRecent, nil
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

func (b Blocks[S]) loadProgress(ctx context.Context, session *monitord.Session[S], client *Client, maxRecent int) (blockProgress, uint64, error) {
	head, err := client.blockNumber(ctx)
	if err != nil {
		return blockProgress{}, 0, err
	}
	var progress blockProgress
	found, err := session.Checkpoint(b.progressSource(), &progress)
	if err != nil {
		return progress, 0, err
	}
	if found {
		if err = validateBlockProgress(progress, client.ChainID(), maxRecent); err != nil {
			return progress, 0, err
		}
		return progress, head, nil
	}
	start := head
	if b.BackfillFrom != nil {
		start = *b.BackfillFrom
		if start > head {
			return progress, 0, errors.New("quicknode evm: BackfillFrom is above the current head")
		}
	}
	progress = blockProgress{ChainID: client.ChainID(), NextBlock: start}
	if start > 0 {
		parent, err := client.blockByNumber(ctx, start-1)
		if err != nil {
			return progress, 0, err
		}
		progress.CanonicalParent = parent.Hash
	}
	return progress, head, b.commit(ctx, session, progress, nil)
}

func validateBlockProgress(progress blockProgress, chain ChainID, maxRecent int) error {
	if progress.ChainID != chain {
		return errors.New("quicknode evm: saved block progress belongs to another chain")
	}
	if len(progress.Recent) > maxRecent {
		return errors.New("quicknode evm: recent block journal exceeds its bound")
	}
	for i := range progress.Recent {
		ref := progress.Recent[i]
		if _, err := ParseHash(string(ref.Hash)); err != nil {
			return err
		}
		if _, err := ParseHash(string(ref.ParentHash)); err != nil {
			return err
		}
		if i > 0 {
			previous := progress.Recent[i-1]
			if ref.Number != previous.Number+1 || ref.ParentHash != previous.Hash {
				return errors.New("quicknode evm: recent block journal is not contiguous")
			}
		}
	}
	if len(progress.Recent) > 0 {
		last := progress.Recent[len(progress.Recent)-1]
		if progress.NextBlock != last.Number+1 || progress.CanonicalParent != last.Hash {
			return errors.New("quicknode evm: saved block progress does not follow its journal")
		}
	} else if progress.NextBlock > 0 && progress.CanonicalParent == "" {
		return errors.New("quicknode evm: saved block progress lacks its canonical parent")
	}
	return nil
}

type headAnnouncement struct {
	Number     uint64
	ParentHash Hash
}

func decodeHead(head Head) (headAnnouncement, error) {
	number, err := parseUintQuantity(head.Number)
	if err != nil {
		return headAnnouncement{}, err
	}
	if _, err = ParseHash(string(head.Hash)); err != nil {
		return headAnnouncement{}, err
	}
	if _, err = ParseHash(string(head.ParentHash)); err != nil {
		return headAnnouncement{}, err
	}
	return headAnnouncement{Number: number, ParentHash: head.ParentHash}, nil
}

func (b Blocks[S]) advance(ctx context.Context, session *monitord.Session[S], client *Client, progress *blockProgress, target, confirmations uint64, maxRecent int, knownExtension bool) error {
	if !knownExtension {
		if err := b.rollbackOrphans(ctx, session, client, progress, confirmations); err != nil {
			return err
		}
	}
	if progress.NextBlock > target {
		return nil
	}
	batchSize := b.ReplayBatch
	if batchSize == 0 {
		batchSize = defaultBlockReplayBatch
	}
	concurrency := b.FetchConcurrency
	if concurrency == 0 {
		concurrency = defaultBlockFetchConcurrency
	}
	for progress.NextBlock <= target {
		to := target
		if remaining := target - progress.NextBlock + 1; batchSize < remaining {
			to = progress.NextBlock + batchSize - 1
		}
		blocks, err := client.blocksByNumber(ctx, progress.NextBlock, to, concurrency)
		if err != nil {
			return err
		}
		if len(blocks) == 0 {
			return errors.New("quicknode evm: canonical block range is empty")
		}
		if progress.NextBlock > 0 && blocks[0].ParentHash != progress.CanonicalParent {
			return fmt.Errorf("quicknode evm: canonical range does not extend block %d", progress.NextBlock-1)
		}
		for i := range blocks {
			block := blocks[i]
			state := session.State()
			update, err := b.Handle(ctx, client, state, block.Clone())
			if err != nil {
				return err
			}
			next := progress.clone()
			next.NextBlock = block.Number + 1
			next.CanonicalParent = block.Hash
			next.Recent = append(next.Recent, blockReference{Number: block.Number, Hash: block.Hash, ParentHash: block.ParentHash})
			next.trim(target, confirmations)
			if len(next.Recent) > maxRecent {
				return errors.New("quicknode evm: recent block journal is full")
			}
			if err = b.commit(ctx, session, next, update); err != nil {
				return err
			}
			*progress = next
		}
	}
	return nil
}

func (p *blockProgress) trim(head, confirmations uint64) {
	if head < confirmations {
		return
	}
	finalized := head - confirmations
	cut := 0
	for cut < len(p.Recent) && p.Recent[cut].Number <= finalized {
		cut++
	}
	if cut > 0 {
		p.Recent = append([]blockReference(nil), p.Recent[cut:]...)
	}
}

func (b Blocks[S]) rollbackOrphans(ctx context.Context, session *monitord.Session[S], client *Client, progress *blockProgress, confirmations uint64) error {
	for progress.NextBlock > 0 && progress.CanonicalParent != "" {
		canonical, err := client.blockByNumber(ctx, progress.NextBlock-1)
		if err != nil {
			return err
		}
		if canonical.Hash == progress.CanonicalParent {
			return nil
		}
		if len(progress.Recent) == 0 {
			return fmt.Errorf("quicknode evm: canonical history changed beyond the %d-block rollback journal", confirmations)
		}
		last := progress.Recent[len(progress.Recent)-1]
		if last.Number+1 != progress.NextBlock || last.Hash != progress.CanonicalParent {
			return errors.New("quicknode evm: recent block journal cannot roll back current progress")
		}
		removed := last.block(client.ChainID())
		removed.Removed = true
		state := session.State()
		update, err := b.Handle(ctx, client, state, removed)
		if err != nil {
			return err
		}
		next := progress.clone()
		next.Recent = next.Recent[:len(next.Recent)-1]
		next.NextBlock = last.Number
		next.CanonicalParent = last.ParentHash
		if err = b.commit(ctx, session, next, update); err != nil {
			return err
		}
		*progress = next
	}
	return nil
}

func (b Blocks[S]) commit(ctx context.Context, session *monitord.Session[S], progress blockProgress, update BlockUpdate[S]) error {
	return session.Commit(ctx, func(tx *monitord.Tx[S]) error {
		if update != nil {
			if err := update(tx); err != nil {
				return err
			}
		}
		return tx.Checkpoint(b.progressSource(), progress)
	})
}

func (b Blocks[S]) progressSource() string {
	if b.Name == "" {
		return "quicknode.evm.blocks"
	}
	return "quicknode.evm.blocks." + b.Name
}

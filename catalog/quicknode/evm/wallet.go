package evm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/saucesteals/monitord"
	"github.com/saucesteals/monitord/catalog/quicknode"
)

type Wallet struct {
	Name            string
	HTTPURL         string
	HTTPSecret      monitord.SecretRef
	ExpectedChainID ChainID
	Address         Address
	Events          TransferKinds
	Confirmations   uint64
	BackfillFrom    *uint64
	Map             func(Transfer) monitord.Event
}
type WalletState struct{}
type walletJournal struct {
	Blocks []walletJournalBlock `json:"blocks"`
	Reorg  *walletReorgCursor   `json:"reorg,omitempty"`
}
type walletJournalBlock struct {
	Number  uint64 `json:"number"`
	Hash    Hash   `json:"hash"`
	Emitted uint64 `json:"emitted,omitempty"`
}
type walletReorgCursor struct {
	Rewind      uint64 `json:"rewind"`
	FirstBlock  int    `json:"first_block"`
	BlockOffset int    `json:"block_offset,omitempty"`
}

const (
	walletCursorSource  = "quicknode.evm.wallet-cursor"
	walletJournalSource = "quicknode.evm.wallet-canonical-journal"
	walletJournalLimit  = 256
)

func (w Wallet) Info() monitord.Info {
	name := w.Name
	if name == "" {
		name = "quicknode-wallet"
	}
	return monitord.Info{Name: name, Description: "Confirmed native and token transfers for an EVM wallet"}
}
func (w Wallet) Plan() monitord.Plan[WalletState] {
	return monitord.Continuous(w.run, monitord.WithSecrets(w.secretRefs()...))
}
func (w Wallet) secretRefs() []monitord.SecretRef {
	r := []monitord.SecretRef{}
	if w.HTTPURL == "" {
		ref, _ := httpSecret(w.HTTPSecret)
		r = append(r, ref)
	}
	return r
}

func (w Wallet) run(ctx context.Context, s *monitord.Session[WalletState]) error {
	address := w.Address
	if address == "" {
		return errors.New("quicknode evm: Wallet.Address is required")
	}
	if _, err := ParseAddress(string(address)); err != nil {
		return err
	}
	kinds := w.Events
	if kinds == 0 {
		kinds = AllTransfers
	}
	if kinds&^AllTransfers != 0 {
		return errors.New("quicknode evm: invalid wallet transfer kinds")
	}
	httpURL := w.HTTPURL
	_, defaultEndpoint := httpSecret(w.HTTPSecret)
	if httpURL == "" {
		ref, _ := httpSecret(w.HTTPSecret)
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
	expectedChainID := w.ExpectedChainID
	if expectedChainID == "" && w.HTTPURL == "" && defaultEndpoint {
		expectedChainID = "0x1"
	}
	if expectedChainID != "" && expectedChainID != c.ChainID() {
		return fmt.Errorf("quicknode evm: expected chain %s, endpoint is %s", expectedChainID, c.ChainID())
	}
	var checkpoint Checkpoint
	found, err := s.Checkpoint(walletCursorSource, &checkpoint)
	if err != nil {
		return err
	}
	if checkpoint.ChainID != "" && checkpoint.ChainID != c.ChainID() {
		return fmt.Errorf("quicknode: checkpoint chain %s differs from endpoint %s", checkpoint.ChainID, c.ChainID())
	}
	if found && (checkpoint.Address != address || checkpoint.Events != kinds) {
		return errors.New("quicknode evm: wallet address or transfer selection differs from the saved cursor")
	}
	if (checkpoint.CurrentBlock == "") != (checkpoint.TransferOffset == 0) {
		return errors.New("quicknode evm: wallet block identity and transfer cursor are inconsistent")
	}
	depth, err := confirmationDepth(c.ChainID(), w.Confirmations)
	if err != nil {
		return err
	}
	var journal walletJournal
	journalFound, err := s.Checkpoint(walletJournalSource, &journal)
	if err != nil {
		return err
	}
	if found != journalFound {
		return errors.New("quicknode evm: wallet cursor and journal must both be present or absent")
	}
	if found {
		if err := validateWalletProgress(checkpoint, journal); err != nil {
			return err
		}
	}
	if !found {
		next := uint64(0)
		if w.BackfillFrom != nil {
			next = *w.BackfillFrom
		} else {
			head, err := c.blockNumber(ctx)
			if err != nil {
				return err
			}
			if head >= depth {
				next = head - depth + 1
			}
		}
		nextCheckpoint := Checkpoint{ChainID: c.ChainID(), NextBlock: next, Address: address, Events: kinds}
		if err := commitWallet(ctx, s, nextCheckpoint, journal, nil); err != nil {
			return err
		}
		checkpoint = nextCheckpoint
	}
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()
	for {
		if journal.Reorg != nil {
			if err := correctReorg(ctx, s, &checkpoint, &journal); err != nil {
				return err
			}
			continue
		}
		head, err := c.blockNumber(ctx)
		if err != nil {
			return err
		}
		if head >= depth {
			target := head - depth
			for checkpoint.NextBlock <= target {
				canonical, err := walletCursorCanonical(ctx, c, checkpoint)
				if err != nil {
					return err
				}
				if !canonical {
					rewind, first, err := reconcileJournal(ctx, c, journal)
					if err != nil {
						return err
					}
					nextJournal := cloneWalletJournal(journal)
					nextJournal.Reorg = &walletReorgCursor{Rewind: rewind, FirstBlock: first}
					nextCheckpoint := checkpoint
					nextCheckpoint.NextBlock = rewind
					nextCheckpoint.CurrentBlock = ""
					nextCheckpoint.TransferOffset = 0
					if first > 0 {
						nextCheckpoint.CanonicalParent = nextJournal.Blocks[first-1].Hash
					} else {
						nextCheckpoint.CanonicalParent = ""
					}
					if err := commitWallet(ctx, s, nextCheckpoint, nextJournal, nil); err != nil {
						return err
					}
					checkpoint, journal = nextCheckpoint, nextJournal
					break
				}
				block, err := c.blockByNumber(ctx, checkpoint.NextBlock, true)
				if err != nil {
					return err
				}
				if (checkpoint.CurrentBlock != "" && block.Hash != checkpoint.CurrentBlock) || (checkpoint.CanonicalParent != "" && block.ParentHash != checkpoint.CanonicalParent) {
					continue
				}
				transfers, err := w.blockTransfers(ctx, c, address, kinds, block)
				if err != nil {
					return err
				}
				if checkpoint.TransferOffset > uint64(len(transfers)) {
					return errors.New("quicknode evm: wallet transfer cursor exceeds block transfers")
				}
				if checkpoint.CurrentBlock != "" && checkpoint.TransferOffset == uint64(len(transfers)) {
					return errors.New("quicknode evm: wallet transfer cursor has no remaining block transfer")
				}
				if err := w.commitBlock(ctx, s, address, block, transfers, &checkpoint, &journal); err != nil {
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

func validateWalletProgress(checkpoint Checkpoint, journal walletJournal) error {
	if checkpoint.CurrentBlock != "" {
		if len(journal.Blocks) == 0 {
			return errors.New("quicknode evm: in-block wallet cursor is missing its journal entry")
		}
		last := journal.Blocks[len(journal.Blocks)-1]
		if last.Number != checkpoint.NextBlock || last.Hash != checkpoint.CurrentBlock || last.Emitted != checkpoint.TransferOffset {
			return errors.New("quicknode evm: in-block wallet cursor differs from its journal entry")
		}
	}
	if journal.Reorg != nil && (checkpoint.NextBlock != journal.Reorg.Rewind || checkpoint.CurrentBlock != "" || checkpoint.TransferOffset != 0) {
		return errors.New("quicknode evm: wallet reorganization cursor differs from block cursor")
	}
	return nil
}

func walletCursorCanonical(ctx context.Context, c *Client, checkpoint Checkpoint) (bool, error) {
	if checkpoint.CurrentBlock != "" {
		block, err := c.blockByNumber(ctx, checkpoint.NextBlock, false)
		if err != nil {
			return false, err
		}
		if block.Hash != checkpoint.CurrentBlock {
			return false, nil
		}
		return checkpoint.CanonicalParent == "" || block.ParentHash == checkpoint.CanonicalParent, nil
	}
	if checkpoint.NextBlock == 0 || checkpoint.CanonicalParent == "" {
		return true, nil
	}
	parent, err := c.blockByNumber(ctx, checkpoint.NextBlock-1, false)
	if err != nil {
		return false, err
	}
	return parent.Hash == checkpoint.CanonicalParent, nil
}

func (w Wallet) commitBlock(ctx context.Context, s *monitord.Session[WalletState], address Address, block rpcBlock, transfers []Transfer, checkpoint *Checkpoint, journal *walletJournal) error {
	number, err := parseUintQuantity(block.Number)
	if err != nil || number != checkpoint.NextBlock {
		return errors.New("quicknode evm: wallet block number differs from cursor")
	}
	if err := validateWalletProgress(*checkpoint, *journal); err != nil {
		return err
	}
	for i := checkpoint.TransferOffset; i < uint64(len(transfers)); i++ {
		nextCheckpoint := *checkpoint
		nextCheckpoint.CurrentBlock = block.Hash
		nextCheckpoint.TransferOffset = i + 1
		nextJournal := cloneWalletJournal(*journal)
		if err := recordWalletBlock(&nextJournal, number, block.Hash, i+1); err != nil {
			return err
		}
		if i+1 == uint64(len(transfers)) {
			finishWalletBlock(&nextCheckpoint, block.Hash)
		}
		event := w.mapEventFor(transfers[i], address)
		if err := commitWallet(ctx, s, nextCheckpoint, nextJournal, &event); err != nil {
			return err
		}
		*checkpoint, *journal = nextCheckpoint, nextJournal
	}
	if checkpoint.NextBlock != number {
		return nil
	}
	// Empty blocks still advance durably.
	nextCheckpoint := *checkpoint
	nextJournal := cloneWalletJournal(*journal)
	if err := recordWalletBlock(&nextJournal, number, block.Hash, uint64(len(transfers))); err != nil {
		return err
	}
	finishWalletBlock(&nextCheckpoint, block.Hash)
	if err := commitWallet(ctx, s, nextCheckpoint, nextJournal, nil); err != nil {
		return err
	}
	*checkpoint, *journal = nextCheckpoint, nextJournal
	return nil
}

func finishWalletBlock(checkpoint *Checkpoint, hash Hash) {
	checkpoint.NextBlock++
	checkpoint.CanonicalParent = hash
	checkpoint.CurrentBlock = ""
	checkpoint.TransferOffset = 0
}

func recordWalletBlock(journal *walletJournal, number uint64, hash Hash, emitted uint64) error {
	if len(journal.Blocks) > 0 && journal.Blocks[len(journal.Blocks)-1].Number == number {
		last := &journal.Blocks[len(journal.Blocks)-1]
		if last.Hash != hash || emitted < last.Emitted {
			return errors.New("quicknode evm: wallet journal block identity or offset changed")
		}
		last.Emitted = emitted
		return nil
	}
	journal.Blocks = append(journal.Blocks, walletJournalBlock{Number: number, Hash: hash, Emitted: emitted})
	if len(journal.Blocks) > walletJournalLimit {
		journal.Blocks = append([]walletJournalBlock(nil), journal.Blocks[len(journal.Blocks)-walletJournalLimit:]...)
	}
	return nil
}

func correctReorg(ctx context.Context, s *monitord.Session[WalletState], checkpoint *Checkpoint, journal *walletJournal) error {
	for journal.Reorg != nil {
		reorg := journal.Reorg
		index := reorg.FirstBlock + reorg.BlockOffset
		if reorg.FirstBlock < 0 || reorg.FirstBlock > len(journal.Blocks) || index < reorg.FirstBlock || index > len(journal.Blocks) {
			return errors.New("quicknode evm: invalid wallet reorganization cursor")
		}
		if index == len(journal.Blocks) {
			nextJournal := cloneWalletJournal(*journal)
			nextJournal.Blocks = append([]walletJournalBlock(nil), nextJournal.Blocks[:reorg.FirstBlock]...)
			nextJournal.Reorg = nil
			nextCheckpoint := *checkpoint
			nextCheckpoint.NextBlock = reorg.Rewind
			nextCheckpoint.CurrentBlock = ""
			nextCheckpoint.TransferOffset = 0
			if len(nextJournal.Blocks) > 0 {
				nextCheckpoint.CanonicalParent = nextJournal.Blocks[len(nextJournal.Blocks)-1].Hash
			} else {
				nextCheckpoint.CanonicalParent = ""
			}
			if err := commitWallet(ctx, s, nextCheckpoint, nextJournal, nil); err != nil {
				return err
			}
			*checkpoint, *journal = nextCheckpoint, nextJournal
			return nil
		}
		entry := journal.Blocks[index]
		nextJournal := cloneWalletJournal(*journal)
		nextJournal.Reorg.BlockOffset++
		var event *monitord.Event
		if entry.Emitted > 0 {
			correction := correctionEvent(checkpoint.ChainID, entry)
			event = &correction
		}
		if err := commitWallet(ctx, s, *checkpoint, nextJournal, event); err != nil {
			return err
		}
		*journal = nextJournal
	}
	return nil
}

func cloneWalletJournal(journal walletJournal) walletJournal {
	clone := walletJournal{Blocks: append([]walletJournalBlock(nil), journal.Blocks...)}
	if journal.Reorg != nil {
		reorg := *journal.Reorg
		clone.Reorg = &reorg
	}
	return clone
}

func commitWallet(ctx context.Context, s *monitord.Session[WalletState], checkpoint Checkpoint, journal walletJournal, event *monitord.Event) error {
	cursorRaw, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	journalRaw, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	if len(cursorRaw) > monitord.MaxCheckpointBytes || len(journalRaw) > monitord.MaxCheckpointBytes {
		return errors.New("quicknode evm: wallet checkpoint exceeds protocol size limit")
	}
	return s.Commit(ctx, func(tx *monitord.Tx[WalletState]) error {
		if event != nil {
			if err := tx.Emit(*event); err != nil {
				return err
			}
		}
		if err := tx.Checkpoint(walletCursorSource, checkpoint); err != nil {
			return err
		}
		return tx.Checkpoint(walletJournalSource, journal)
	})
}

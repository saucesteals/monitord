package evm

import (
	"context"
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
}
type walletJournalBlock struct {
	Number    uint64     `json:"number"`
	Hash      Hash       `json:"hash"`
	Transfers []Transfer `json:"transfers"`
}

const (
	walletCursorSource  = "quicknode.evm.wallet-cursor"
	walletJournalSource = "quicknode.evm.wallet-canonical-journal"
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
	depth, err := confirmationDepth(c.ChainID(), w.Confirmations)
	if err != nil {
		return err
	}
	next := checkpoint.NextBlock
	var journal walletJournal
	_, err = s.Checkpoint(walletJournalSource, &journal)
	if err != nil {
		return err
	}
	if !found {
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
		if err := s.Commit(ctx, func(tx *monitord.Tx[WalletState]) error {
			checkpoint = Checkpoint{ChainID: c.ChainID(), NextBlock: next, Address: address, Events: kinds}
			return tx.Checkpoint(walletCursorSource, checkpoint)
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
		if head >= depth {
			target := head - depth
			if next > 0 && checkpoint.CanonicalParent != "" {
				prior, loadErr := c.blockByNumber(ctx, next-1, false)
				if loadErr != nil {
					return loadErr
				}
				if prior.Hash != checkpoint.CanonicalParent {
					rewind, orphaned, reconcileErr := reconcileJournal(ctx, c, journal, next)
					if reconcileErr != nil {
						return reconcileErr
					}
					journal.Blocks = journal.Blocks[:rewindJournalIndex(journal, rewind)]
					if err = s.Commit(ctx, func(tx *monitord.Tx[WalletState]) error {
						for _, tr := range orphaned {
							ev := w.correctionEvent(tr)
							if emitErr := tx.Emit(ev); emitErr != nil {
								return emitErr
							}
						}
						checkpoint = Checkpoint{ChainID: c.ChainID(), NextBlock: rewind, Address: address, Events: kinds}
						if cpErr := tx.Checkpoint(walletCursorSource, checkpoint); cpErr != nil {
							return cpErr
						}
						return tx.Checkpoint(walletJournalSource, journal)
					}); err != nil {
						return err
					}
					next = rewind
				}
			}
			for next <= target {
				block, err := c.blockByNumber(ctx, next, true)
				if err != nil {
					return err
				}
				transfers, err := w.blockTransfers(ctx, c, address, kinds, block)
				if err != nil {
					return err
				}
				nextJournal := walletJournal{Blocks: append([]walletJournalBlock(nil), journal.Blocks...)}
				nextJournal.Blocks = append(nextJournal.Blocks, walletJournalBlock{Number: next, Hash: block.Hash, Transfers: append([]Transfer(nil), transfers...)})
				if len(nextJournal.Blocks) > 256 {
					nextJournal.Blocks = append([]walletJournalBlock(nil), nextJournal.Blocks[len(nextJournal.Blocks)-256:]...)
				}
				nextCheckpoint := Checkpoint{ChainID: c.ChainID(), NextBlock: next + 1, CanonicalParent: block.Hash, Address: address, Events: kinds}
				if err = s.Commit(ctx, func(tx *monitord.Tx[WalletState]) error {
					for _, tr := range transfers {
						ev := w.mapEventFor(tr, address)
						if err := tx.Emit(ev); err != nil {
							return err
						}
					}
					if err := tx.Checkpoint(walletCursorSource, nextCheckpoint); err != nil {
						return err
					}
					if err := tx.Checkpoint(walletJournalSource, nextJournal); err != nil {
						return err
					}
					return nil
				}); err != nil {
					return err
				}
				journal = nextJournal
				checkpoint = nextCheckpoint
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

package quicknode

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/saucesteals/monitord"
)

var quicknodeWalletAddress = monitord.RequiredSecret("quicknode", "wallet-address")

type Wallet struct {
	Name            string
	WSSURL          string
	HTTPURL         string
	ExpectedChainID ChainID
	Address         Address
	Events          TransferKinds
	Confirmations   uint64
	Map             func(Transfer) monitord.Event
}
type WalletState struct {
	Checkpoint Checkpoint `json:"checkpoint"`
}
type walletJournal struct {
	Blocks []walletJournalBlock `json:"blocks"`
}
type walletJournalBlock struct {
	Number    uint64     `json:"number"`
	Hash      Hash       `json:"hash"`
	Transfers []Transfer `json:"transfers"`
}

const walletJournalSource = "quicknode.wallet-canonical-journal"

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
	if w.WSSURL == "" {
		r = append(r, quicknodeWebSocketURL)
	}
	if w.HTTPURL == "" {
		r = append(r, quicknodeHTTPURL)
	}
	if w.Address == "" {
		r = append(r, quicknodeWalletAddress)
	}
	return r
}

func (w Wallet) run(ctx context.Context, s *monitord.Session[WalletState]) error {
	address := w.Address
	if address == "" {
		v, err := s.Secrets().Require(quicknodeWalletAddress)
		if err != nil {
			return err
		}
		address, err = ParseAddress(v)
		if err != nil {
			return err
		}
	}
	if _, err := ParseAddress(string(address)); err != nil {
		return err
	}
	kinds := w.Events
	if kinds == 0 {
		kinds = AllTransfers
	}
	if kinds&^AllTransfers != 0 {
		return errors.New("quicknode: invalid wallet transfer kinds")
	}
	httpURL := w.HTTPURL
	if httpURL == "" {
		httpURL, _ = s.Secrets().Get(quicknodeHTTPURL)
	}
	if httpURL == "" {
		ws := w.WSSURL
		if ws == "" {
			ws, _ = s.Secrets().Get(quicknodeWebSocketURL)
		}
		var err error
		httpURL, err = HTTPFromWSS(ws)
		if err != nil {
			return err
		}
	}
	c, err := Open(ctx, Config{HTTPURL: httpURL})
	if err != nil {
		return err
	}
	defer c.Close()
	if w.ExpectedChainID != "" && w.ExpectedChainID != c.ChainID() {
		return fmt.Errorf("quicknode: expected chain %s, endpoint is %s", w.ExpectedChainID, c.ChainID())
	}
	checkpoint := s.State().Checkpoint
	if checkpoint.ChainID != "" && checkpoint.ChainID != c.ChainID() {
		return fmt.Errorf("quicknode: checkpoint chain %s differs from endpoint %s", checkpoint.ChainID, c.ChainID())
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
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()
	for {
		head, err := c.blockNumber(ctx)
		if err != nil {
			return err
		}
		if head >= depth {
			target := head - depth
			if next > 0 && s.State().Checkpoint.CanonicalParent != "" {
				prior, loadErr := c.blockByNumber(ctx, next-1, false)
				if loadErr != nil {
					return loadErr
				}
				if prior.Hash != s.State().Checkpoint.CanonicalParent {
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
						tx.State.Checkpoint = Checkpoint{ChainID: c.ChainID(), NextBlock: rewind}
						if cpErr := tx.Checkpoint(checkpointSource, tx.State.Checkpoint); cpErr != nil {
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
				if err = s.Commit(ctx, func(tx *monitord.Tx[WalletState]) error {
					for _, tr := range transfers {
						ev := w.mapEventFor(tr, address)
						if err := tx.Emit(ev); err != nil {
							return err
						}
					}
					tx.State.Checkpoint = Checkpoint{ChainID: c.ChainID(), NextBlock: next + 1, CanonicalParent: block.Hash}
					if err := tx.Checkpoint(checkpointSource, tx.State.Checkpoint); err != nil {
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

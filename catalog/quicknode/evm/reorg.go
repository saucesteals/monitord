package evm

import (
	"context"
	"errors"
	"fmt"

	"github.com/saucesteals/monitord"
)

// reconcileJournal returns the first journal entry that is no longer canonical.
func reconcileJournal(ctx context.Context, c *Client, j walletJournal) (uint64, int, error) {
	for i := len(j.Blocks) - 1; i >= 0; i-- {
		canonical, err := c.blockByNumber(ctx, j.Blocks[i].Number, false)
		if err != nil {
			return 0, 0, err
		}
		if canonical.Hash == j.Blocks[i].Hash {
			return j.Blocks[i].Number + 1, i + 1, nil
		}
	}
	if len(j.Blocks) == 0 {
		return 0, 0, errors.New("quicknode evm: cannot reconcile an empty wallet journal")
	}
	if len(j.Blocks) == walletJournalLimit {
		return 0, 0, errors.New("quicknode evm: chain reorganization exceeds the retained wallet journal; pause the deployment and clear all checkpoints")
	}
	return j.Blocks[0].Number, 0, nil
}
func correctionEvent(chain ChainID, block walletJournalBlock) monitord.Event {
	return monitord.Event{
		ID:    fmt.Sprintf("evm:%s:correction:block:%s:%d", chain, block.Hash, block.Emitted),
		Title: "Chain reorganization correction",
		Body:  "Previously reported wallet events from an orphaned block are no longer canonical",
		Data: map[string]string{
			"chain":         string(chain),
			"block_number":  fmt.Sprint(block.Number),
			"block_hash":    string(block.Hash),
			"emitted_count": fmt.Sprint(block.Emitted),
		},
	}
}

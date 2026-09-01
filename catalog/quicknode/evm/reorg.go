package evm

import (
	"context"

	"github.com/saucesteals/monitord"
)

func rewindJournalIndex(j walletJournal, block uint64) int {
	for i, b := range j.Blocks {
		if b.Number >= block {
			return i
		}
	}
	return len(j.Blocks)
}
func reconcileJournal(ctx context.Context, c *Client, j walletJournal, next uint64) (uint64, []Transfer, error) {
	for i := len(j.Blocks) - 1; i >= 0; i-- {
		canonical, err := c.blockByNumber(ctx, j.Blocks[i].Number, false)
		if err != nil {
			return 0, nil, err
		}
		if canonical.Hash == j.Blocks[i].Hash {
			orphaned := []Transfer{}
			for _, b := range j.Blocks[i+1:] {
				orphaned = append(orphaned, b.Transfers...)
			}
			return j.Blocks[i].Number + 1, orphaned, nil
		}
	}
	orphaned := []Transfer{}
	for _, b := range j.Blocks {
		orphaned = append(orphaned, b.Transfers...)
	}
	rewind := uint64(0)
	if len(j.Blocks) > 0 {
		rewind = j.Blocks[0].Number
	}
	return rewind, orphaned, nil
}
func (w Wallet) correctionEvent(t Transfer) monitord.Event {
	original := w.mapEvent(t)
	return monitord.Event{
		ID: "correction:" + t.ID(), Title: "Chain reorganization correction",
		Body: "A previously reported wallet transfer is no longer canonical",
		Data: map[string]string{"original_event_id": t.ID(), "original_title": original.Title},
	}
}

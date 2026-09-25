package storage

import (
	"context"
	"fmt"
	"time"
)

const maintenanceBatchSize = 1000

// PruneResult describes one bounded candidate page, including protected rows.
type PruneResult struct {
	Scanned     int
	Deleted     int64
	More        bool
	NextSweepAt time.Time
}

// PruneTerminalOutbox visits at most 1,000 events, deleting only expired events
// whose deliveries are all terminal. Pending and leased work is preserved.
func (s *Store) PruneTerminalOutbox(ctx context.Context, now time.Time) (PruneResult, error) {
	return s.prunePage(ctx, now, "outbox", "outbox_events", `
 DELETE FROM outbox_events AS e WHERE e.rowid > ? AND e.rowid <= ?
 AND e.created_at < (SELECT ? - p.event_retention_ms FROM deployments p WHERE p.id=e.deployment_id)
 AND NOT EXISTS (SELECT 1 FROM outbox_deliveries d WHERE d.outbox_id=e.outbox_id AND d.status IN ('pending','sending'))`, toMs(now))
}

// PruneRetiredTransactions visits at most 1,000 ACKs, deleting only unreferenced
// records from generations retired for at least seven days. Active ACKs remain.
func (s *Store) PruneRetiredTransactions(ctx context.Context, now time.Time) (PruneResult, error) {
	return s.prunePage(ctx, now, "transactions", "transactions", `
 DELETE FROM transactions AS t WHERE t.rowid > ? AND t.rowid <= ?
 AND EXISTS (SELECT 1 FROM deployment_generations g
  WHERE g.deployment_id=t.deployment_id AND g.generation=t.generation
  AND g.status='retired' AND g.retired_at < ?)
 AND NOT EXISTS (SELECT 1 FROM outbox_events e
  WHERE e.deployment_id=t.deployment_id AND e.generation=t.generation AND e.transaction_seq=t.seq)`, toMs(now.Add(-7*24*time.Hour)))
}

// prunePage uses only internal constant table names and SQL. A fixed high-water
// rowid makes each sweep finite even while producers append new records. Cursor
// advancement and deletion commit together, so failures retry the same page.
func (s *Store) prunePage(ctx context.Context, now time.Time, name, table, deleteSQL string, cutoff int64) (PruneResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PruneResult{}, fmt.Errorf("begin %s maintenance: %w", name, err)
	}
	defer func() { _ = tx.Rollback() }()

	// Acquire the SQLite writer reservation before reading the cursor or page.
	// This also serializes maintenance callers using separate Store instances.
	if _, err := tx.ExecContext(ctx, `INSERT INTO maintenance_cursors(name) VALUES (?) ON CONFLICT(name) DO NOTHING`, name); err != nil {
		return PruneResult{}, fmt.Errorf("initialize %s cursor: %w", name, err)
	}
	var after, through, nextSweep int64
	if err := tx.QueryRowContext(ctx, `SELECT after_rowid,through_rowid,next_sweep_at FROM maintenance_cursors WHERE name=?`, name).Scan(&after, &through, &nextSweep); err != nil {
		return PruneResult{}, fmt.Errorf("read %s cursor: %w", name, err)
	}
	if through == 0 && nextSweep > toMs(now) {
		return PruneResult{NextSweepAt: fromMs(nextSweep)}, nil
	}
	if through == 0 {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM `+table).Scan(&through); err != nil {
			return PruneResult{}, fmt.Errorf("read %s high water: %w", name, err)
		}
		after = 0
	}
	rows, err := tx.QueryContext(ctx, `SELECT rowid FROM `+table+` WHERE rowid > ? AND rowid <= ? ORDER BY rowid LIMIT ?`, after, through, maintenanceBatchSize)
	if err != nil {
		return PruneResult{}, fmt.Errorf("read %s candidate page: %w", name, err)
	}
	result := PruneResult{}
	last := after
	for rows.Next() {
		if err := rows.Scan(&last); err != nil {
			_ = rows.Close()
			return PruneResult{}, err
		}
		result.Scanned++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return PruneResult{}, err
	}
	if err := rows.Close(); err != nil {
		return PruneResult{}, err
	}
	if result.Scanned > 0 {
		deleted, err := tx.ExecContext(ctx, deleteSQL, after, last, cutoff)
		if err != nil {
			return PruneResult{}, fmt.Errorf("delete %s candidate page: %w", name, err)
		}
		result.Deleted, err = deleted.RowsAffected()
		if err != nil {
			return PruneResult{}, err
		}
	}
	result.More = result.Scanned == maintenanceBatchSize && last < through
	if result.More {
		after = last
		nextSweep = 0
	} else {
		after, through = 0, 0
		nextSweep = toMs(now.Add(time.Hour))
		result.NextSweepAt = fromMs(nextSweep)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE maintenance_cursors SET after_rowid=?,through_rowid=?,next_sweep_at=? WHERE name=?`, after, through, nextSweep, name); err != nil {
		return PruneResult{}, fmt.Errorf("advance %s cursor: %w", name, err)
	}
	if err := tx.Commit(); err != nil {
		return PruneResult{}, fmt.Errorf("commit %s maintenance: %w", name, err)
	}

	return result, nil
}

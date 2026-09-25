package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	maintenanceBatchSize   = 64
	maintenanceWriteBudget = 10 * time.Millisecond
)

// PruneResult describes one bounded candidate slice, including protected rows.
type PruneResult struct {
	Scanned     int
	Deleted     int64
	More        bool
	NextSweepAt time.Time
}

type maintenanceCursor struct {
	after, through, next, generation, sequence int64
}

type maintenanceCandidate struct {
	id       int64
	eligible bool
}

type maintenancePage struct {
	before, next maintenanceCursor
	ids          []maintenanceCandidate
	deployment   string
	generation   int64
	complete     bool
}

// PruneTerminalOutbox discovers a bounded page without reserving the writer.
// Expiry and delivery status are checked again when each event is deleted.
func (s *Store) PruneTerminalOutbox(ctx context.Context, now time.Time) (PruneResult, error) {
	return s.prune(ctx, now, "outbox")
}

// PruneRetiredTransactions visits only old retired generations. Discovery uses
// the covering transaction primary key, never reading ACK payloads. Active and
// recently retired generations retain every receipt for replay.
func (s *Store) PruneRetiredTransactions(ctx context.Context, now time.Time) (PruneResult, error) {
	return s.prune(ctx, now, "transactions")
}

func (s *Store) discoverMaintenance(ctx context.Context, now time.Time, name string) (maintenancePage, error) {
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return maintenancePage{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var p maintenancePage
	err = tx.QueryRowContext(ctx, `SELECT after_rowid,through_rowid,next_sweep_at,generation_rowid,after_seq FROM maintenance_cursors WHERE name=?`, name).
		Scan(&p.before.after, &p.before.through, &p.before.next, &p.before.generation, &p.before.sequence)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return p, err
	}
	p.next = p.before
	if p.next.through == 0 && p.next.next > toMs(now) {
		return p, nil
	}
	table := "outbox_events"
	if name == "transactions" {
		table = "deployment_generations"
	}
	if p.next.through == 0 {
		if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM `+table).Scan(&p.next.through); err != nil {
			return p, err
		}
		p.next.after, p.next.generation, p.next.sequence, p.next.next = 0, 0, 0, 0
	}
	if name == "transactions" {
		// Walk a bounded generation page, not the transaction history of live workers.
		if p.next.generation == 0 {
			rows, err := tx.QueryContext(ctx, `SELECT rowid,deployment_id,generation,status,retired_at FROM deployment_generations WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?`, p.next.after, p.next.through, maintenanceBatchSize)
			if err != nil {
				return p, err
			}
			for rows.Next() {
				var id, gen int64
				var dep, status string
				var retired sql.NullInt64
				if err = rows.Scan(&id, &dep, &gen, &status, &retired); err != nil {
					_ = rows.Close()
					return p, err
				}
				if status == "retired" && retired.Valid && retired.Int64 < toMs(now.Add(-7*24*time.Hour)) {
					p.next.generation = id
					p.deployment = dep
					p.generation = gen
					break
				}
				p.next.after = id
			}
			err = errors.Join(rows.Err(), rows.Close())
			if err != nil {
				return p, err
			}
			if p.next.generation == 0 {
				// If fewer generations were visited, the next read proves completion; at
				// most one additional empty slice is needed when the last row was removed.
				var more int
				if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM deployment_generations WHERE rowid>? AND rowid<=?)`, p.next.after, p.next.through).Scan(&more); err != nil {
					return p, err
				}
				p.complete = more == 0
				return p, nil
			}
		} else {
			// A deployment can be purged between slices. Recheck eligibility as
			// well as identity in case SQLite has reused its generation rowid.
			err = tx.QueryRowContext(ctx, `SELECT deployment_id,generation FROM deployment_generations WHERE rowid=? AND status='retired' AND retired_at<?`, p.next.generation, toMs(now.Add(-7*24*time.Hour))).Scan(&p.deployment, &p.generation)
			if errors.Is(err, sql.ErrNoRows) {
				p.next.after = p.next.generation
				p.next.generation, p.next.sequence = 0, 0
				return p, nil
			}
			if err != nil {
				return p, err
			}
		}
		rows, err := tx.QueryContext(ctx, `SELECT t.seq,NOT EXISTS(SELECT 1 FROM outbox_events e WHERE e.deployment_id=t.deployment_id AND e.generation=t.generation AND e.transaction_seq=t.seq)
 FROM transactions t WHERE t.deployment_id=? AND t.generation=? AND t.seq>? ORDER BY t.seq LIMIT ?`, p.deployment, p.generation, p.next.sequence, maintenanceBatchSize)
		if err != nil {
			return p, err
		}
		p.ids, err = maintenanceIDs(rows)
		if err != nil {
			return p, err
		}
		if len(p.ids) == 0 {
			p.next.after = p.next.generation
			p.next.generation, p.next.sequence = 0, 0
		}
	} else {
		rows, err := tx.QueryContext(ctx, `SELECT e.rowid,
 e.created_at < (SELECT ?-d.event_retention_ms FROM deployments d WHERE d.id=e.deployment_id)
 AND NOT EXISTS(SELECT 1 FROM outbox_deliveries d WHERE d.outbox_id=e.outbox_id AND d.status IN ('pending','sending'))
 FROM outbox_events e WHERE e.rowid>? AND e.rowid<=? ORDER BY e.rowid LIMIT ?`, toMs(now), p.next.after, p.next.through, maintenanceBatchSize)
		if err != nil {
			return p, err
		}
		p.ids, err = maintenanceIDs(rows)
		if err != nil {
			return p, err
		}
		p.complete = len(p.ids) == 0
	}
	// Rollback releases the WAL read snapshot before any writer acquisition.
	return p, nil
}

func maintenanceIDs(rows *sql.Rows) ([]maintenanceCandidate, error) {
	defer func() { _ = rows.Close() }()
	var ids []maintenanceCandidate
	for rows.Next() {
		var id maintenanceCandidate
		if err := rows.Scan(&id.id, &id.eligible); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) prune(ctx context.Context, now time.Time, name string) (PruneResult, error) {
	p, err := s.discoverMaintenance(ctx, now, name)
	if err != nil {
		return PruneResult{}, fmt.Errorf("discover %s maintenance: %w", name, err)
	}
	if p.before.through == 0 && p.before.next > toMs(now) {
		return PruneResult{NextSweepAt: fromMs(p.before.next)}, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PruneResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	// Reserve the writer before rechecking the cursor. Concurrent stores may have
	// discovered the same page; only its current owner can delete and advance it.
	if _, err = tx.ExecContext(ctx, `INSERT INTO maintenance_cursors(name) VALUES (?) ON CONFLICT(name) DO NOTHING`, name); err != nil {
		return PruneResult{}, err
	}
	var current maintenanceCursor
	if err = tx.QueryRowContext(ctx, `SELECT after_rowid,through_rowid,next_sweep_at,generation_rowid,after_seq FROM maintenance_cursors WHERE name=?`, name).
		Scan(&current.after, &current.through, &current.next, &current.generation, &current.sequence); err != nil {
		return PruneResult{}, err
	}
	if current != p.before {
		return PruneResult{More: true}, nil
	}
	started := time.Now()
	result := PruneResult{}
	for _, candidate := range p.ids {
		id := candidate.id
		if name == "outbox" {
			p.next.after = id
		} else {
			p.next.sequence = id
		}
		result.Scanned++
		if !candidate.eligible {
			continue
		}
		var deleted sql.Result
		if name == "outbox" {
			deleted, err = tx.ExecContext(ctx, `DELETE FROM outbox_events AS e WHERE e.rowid=?
    AND e.created_at < (SELECT ?-d.event_retention_ms FROM deployments d WHERE d.id=e.deployment_id)
    AND NOT EXISTS(SELECT 1 FROM outbox_deliveries d WHERE d.outbox_id=e.outbox_id AND d.status IN ('pending','sending'))`, id, toMs(now))
		} else {
			deleted, err = tx.ExecContext(ctx, `DELETE FROM transactions AS t WHERE deployment_id=? AND generation=? AND seq=?
    AND EXISTS(SELECT 1 FROM deployment_generations g WHERE g.deployment_id=t.deployment_id AND g.generation=t.generation AND g.status='retired' AND g.retired_at<?)
    AND NOT EXISTS(SELECT 1 FROM outbox_events e WHERE e.deployment_id=t.deployment_id AND e.generation=t.generation AND e.transaction_seq=t.seq)`, p.deployment, p.generation, id, toMs(now.Add(-7*24*time.Hour)))
		}
		if err != nil {
			return PruneResult{}, fmt.Errorf("delete %s candidate: %w", name, err)
		}
		n, err := deleted.RowsAffected()
		if err != nil {
			return PruneResult{}, err
		}
		result.Deleted += n
		// This is a scheduling budget, not a promise to bound one SQLite operation or
		// fsync. A slow operation ends the slice instead of multiplying its cost.
		if time.Since(started) >= maintenanceWriteBudget {
			break
		}
	}
	if p.complete {
		p.next = maintenanceCursor{next: toMs(now.Add(time.Hour))}
		result.NextSweepAt = fromMs(p.next.next)
	} else {
		result.More = true
	}
	if _, err = tx.ExecContext(ctx, `UPDATE maintenance_cursors SET after_rowid=?,through_rowid=?,next_sweep_at=?,generation_rowid=?,after_seq=? WHERE name=?`, p.next.after, p.next.through, p.next.next, p.next.generation, p.next.sequence, name); err != nil {
		return PruneResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return PruneResult{}, fmt.Errorf("commit %s maintenance: %w", name, err)
	}
	return result, nil
}

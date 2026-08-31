package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func (s *Store) ListActiveBindings(ctx context.Context, deploymentID string) ([]DestinationBinding, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,revision,deployment_id,config,created_at,retired_at FROM destination_bindings WHERE deployment_id=? AND retired_at IS NULL ORDER BY id`, deploymentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DestinationBinding
	for rows.Next() {
		var b DestinationBinding
		var created int64
		var retired sql.NullInt64
		if err = rows.Scan(&b.ID, &b.Revision, &b.DeploymentID, &b.Config, &created, &retired); err != nil {
			return nil, err
		}
		b.CreatedAt = fromMs(created)
		b.RetiredAt = nullTime(retired)
		out = append(out, b)
	}
	return out, rows.Err()
}

type DestinationBinding struct {
	ID           string
	Revision     int64
	DeploymentID string
	Config       json.RawMessage
	CreatedAt    time.Time
	RetiredAt    *time.Time
}

// PutDestinationBinding creates an immutable revision, reusing the current revision when unchanged.
func (s *Store) PutDestinationBinding(ctx context.Context, deploymentID, id string, config json.RawMessage) (DestinationBinding, error) {
	if deploymentID == "" || id == "" || !json.Valid(config) {
		return DestinationBinding{}, errors.New("binding requires deployment, id, and valid JSON")
	}
	hash := hashJSON(config)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DestinationBinding{}, err
	}
	defer tx.Rollback()
	var revision int64
	var currentHash []byte
	err = tx.QueryRowContext(ctx, `SELECT revision,config_hash FROM destination_bindings WHERE deployment_id=? AND id=? AND retired_at IS NULL ORDER BY revision DESC LIMIT 1`, deploymentID, id).Scan(&revision, &currentHash)
	if err == nil && string(currentHash) == string(hash) {
		_ = tx.Rollback()
		return s.getBinding(ctx, deploymentID, id, revision)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return DestinationBinding{}, err
	}
	now := toMs(time.Now().UTC())
	if revision > 0 {
		if _, err = tx.ExecContext(ctx, `UPDATE destination_bindings SET retired_at=? WHERE deployment_id=? AND id=? AND revision=? AND retired_at IS NULL`, now, deploymentID, id, revision); err != nil {
			return DestinationBinding{}, err
		}
	}
	var max int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0) FROM destination_bindings WHERE deployment_id=? AND id=?`, deploymentID, id).Scan(&max); err != nil {
		return DestinationBinding{}, err
	}
	revision = max + 1
	_, err = tx.ExecContext(ctx, `INSERT INTO destination_bindings(id,revision,deployment_id,config,config_hash,created_at) VALUES(?,?,?,?,?,?)`, id, revision, deploymentID, config, hash, now)
	if err != nil {
		return DestinationBinding{}, err
	}
	if err = tx.Commit(); err != nil {
		return DestinationBinding{}, err
	}
	return s.getBinding(ctx, deploymentID, id, revision)
}
func (s *Store) RetireDestinationBindingsExcept(ctx context.Context, id string, count int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE destination_bindings SET retired_at=? WHERE deployment_id=? AND retired_at IS NULL AND CAST(SUBSTR(id,LENGTH('destination-')+1) AS INTEGER)>?`, toMs(time.Now().UTC()), id, count)
	return err
}

func (s *Store) getBinding(ctx context.Context, deploymentID, id string, revision int64) (DestinationBinding, error) {
	var b DestinationBinding
	var created int64
	var retired sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT id,revision,deployment_id,config,created_at,retired_at FROM destination_bindings WHERE deployment_id=? AND id=? AND revision=?`, deploymentID, id, revision).Scan(&b.ID, &b.Revision, &b.DeploymentID, &b.Config, &created, &retired)
	if err != nil {
		return b, err
	}
	b.CreatedAt = fromMs(created)
	b.RetiredAt = nullTime(retired)
	return b, nil
}

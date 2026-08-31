package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalidStatus = errors.New("invalid deployment status")
	ErrRunFenced     = errors.New("run generation is fenced")
)

type Deployment struct {
	ID, Name, InfoName, SourceDir, Status, ArtifactID, ConfigHash string
	ConfigRevision, ActiveGeneration, StateRevision               int64
	StateVersion                                                  int
	State                                                         json.RawMessage
	CreatedAt, UpdatedAt                                          time.Time
	ExpiresAt, ArchivedAt                                         *time.Time
}

type CreateDeployment struct {
	Name, InfoName, SourceDir, ArtifactID, ConfigHash string
	State                                             json.RawMessage
	StateVersion                                      int
	ExpiresAt                                         *time.Time
}

func (s *Store) ListDeployments(ctx context.Context) ([]Deployment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM deployments ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	out := make([]Deployment, 0, len(ids))
	for _, id := range ids {
		d, e := s.GetDeployment(ctx, id)
		if e != nil {
			return nil, e
		}
		out = append(out, d)
	}
	return out, nil
}
func (s *Store) HasQueuedDeliveries(ctx context.Context, id string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_events e JOIN outbox_deliveries d ON d.outbox_id=e.outbox_id WHERE e.deployment_id=? AND d.status IN ('pending','sending')`, id).Scan(&n)
	return n > 0, err
}

func (s *Store) ExpireDueDeployments(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE deployments SET status='expired',updated_at=? WHERE status='active' AND expires_at IS NOT NULL AND expires_at<=?`, toMs(now), toMs(now))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) CreateDeployment(ctx context.Context, in CreateDeployment) (Deployment, error) {
	if strings.TrimSpace(in.Name) == "" || strings.TrimSpace(in.InfoName) == "" || in.ArtifactID == "" || in.ConfigHash == "" || !json.Valid(in.State) || in.StateVersion < 1 {
		return Deployment{}, errors.New("deployment requires name, info, artifact, config hash, and valid versioned state")
	}
	id, err := randomID()
	if err != nil {
		return Deployment{}, err
	}
	now := time.Now().UTC()
	var expires any
	if in.ExpiresAt != nil {
		expires = toMs(*in.ExpiresAt)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO deployments(id,name,info_name,source_dir,status,artifact_id,config_revision,config_hash,state,state_version,created_at,updated_at,expires_at)
		VALUES(?,?,?,?, 'active', ?,1,?,?,?,?,?,?)`, id, in.Name, in.InfoName, in.SourceDir, in.ArtifactID, in.ConfigHash, in.State, in.StateVersion, toMs(now), toMs(now), expires)
	if err != nil {
		return Deployment{}, fmt.Errorf("create deployment: %w", err)
	}
	return s.GetDeployment(ctx, id)
}

// Redeploy updates implementation/config metadata while preserving immutable identity, state, and history.
func (s *Store) Redeploy(ctx context.Context, id, infoName, sourceDir, artifactID, configHash string, expiresAt *time.Time) (Deployment, error) {
	if id == "" || infoName == "" || artifactID == "" || configHash == "" {
		return Deployment{}, errors.New("redeploy requires identity, info, artifact, and config hash")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Deployment{}, err
	}
	defer tx.Rollback()
	var oldHash string
	if err = tx.QueryRowContext(ctx, `SELECT config_hash FROM deployments WHERE id=? AND status!='archived'`, id).Scan(&oldHash); errors.Is(err, sql.ErrNoRows) {
		return Deployment{}, ErrNotFound
	}
	if err != nil {
		return Deployment{}, err
	}
	revisionBump := int64(0)
	if oldHash != configHash {
		revisionBump = 1
	}
	var expires any
	if expiresAt != nil {
		expires = toMs(*expiresAt)
	}
	_, err = tx.ExecContext(ctx, `UPDATE deployments SET info_name=?,source_dir=?,artifact_id=?,config_hash=?,config_revision=config_revision+?,status='active',expires_at=?,archived_at=NULL,updated_at=? WHERE id=?`, infoName, sourceDir, artifactID, configHash, revisionBump, expires, toMs(time.Now().UTC()), id)
	if err != nil {
		return Deployment{}, err
	}
	// Redeploy never revives an old worker capability, even when configuration
	// bytes happen to be unchanged. The caller activates a fresh generation.
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=? WHERE deployment_id=? AND status='active'`, toMs(time.Now().UTC()), id); err != nil {
		return Deployment{}, err
	}
	if err = tx.Commit(); err != nil {
		return Deployment{}, err
	}
	return s.GetDeployment(ctx, id)
}

func (s *Store) GetDeployment(ctx context.Context, selector string) (Deployment, error) {
	var d Deployment
	var state []byte
	var created, updated int64
	var expires, archived sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT id,name,info_name,source_dir,status,COALESCE(artifact_id,''),config_revision,config_hash,active_generation,state,state_version,state_revision,created_at,updated_at,expires_at,archived_at
		FROM deployments WHERE id=? OR name=? ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END LIMIT 1`, selector, selector, selector).Scan(&d.ID, &d.Name, &d.InfoName, &d.SourceDir, &d.Status, &d.ArtifactID, &d.ConfigRevision, &d.ConfigHash, &d.ActiveGeneration, &state, &d.StateVersion, &d.StateRevision, &created, &updated, &expires, &archived)
	if errors.Is(err, sql.ErrNoRows) {
		return Deployment{}, fmt.Errorf("deployment %q: %w", selector, ErrNotFound)
	}
	if err != nil {
		return Deployment{}, err
	}
	d.State = append(json.RawMessage(nil), state...)
	d.CreatedAt = fromMs(created)
	d.UpdatedAt = fromMs(updated)
	d.ExpiresAt = nullTime(expires)
	d.ArchivedAt = nullTime(archived)
	return d, nil
}

func (s *Store) setDeploymentStatus(ctx context.Context, id, from, to string, expires *time.Time) error {
	var expiry any
	if expires != nil {
		expiry = toMs(*expires)
	}
	archived := any(nil)
	if to == "archived" {
		archived = toMs(time.Now().UTC())
	}
	res, err := s.db.ExecContext(ctx, `UPDATE deployments SET status=?,expires_at=?,archived_at=?,updated_at=? WHERE id=? AND status=?`, to, expiry, archived, toMs(time.Now().UTC()), id, from)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrInvalidStatus
	}
	return nil
}

func (s *Store) ExpireDeployment(ctx context.Context, id string) error {
	return s.setDeploymentStatus(ctx, id, "active", "expired", nil)
}
func (s *Store) ResumeDeployment(ctx context.Context, id string, expires *time.Time) error {
	return s.setDeploymentStatus(ctx, id, "expired", "active", expires)
}
func (s *Store) ArchiveDeployment(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := toMs(time.Now().UTC())
	res, err := tx.ExecContext(ctx, `UPDATE deployments SET status='archived',expires_at=NULL,archived_at=?,updated_at=? WHERE id=? AND status!='archived'`, now, now, id)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrInvalidStatus
	}
	_, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=? WHERE deployment_id=? AND status='active'`, now, id)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) PurgeDeployment(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM deployments WHERE id=? AND status='archived'`, id)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrInvalidStatus
	}
	return nil
}

func (s *Store) PurgeDeploymentSafe(ctx context.Context, id string, force bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if !force {
		var n int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_events e JOIN outbox_deliveries d ON d.outbox_id=e.outbox_id WHERE e.deployment_id=? AND d.status IN ('pending','sending')`, id).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return errors.New("queued deliveries remain")
		}
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM deployments WHERE id=? AND status='archived'`, id)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrInvalidStatus
	}
	return tx.Commit()
}

// ReplaceState performs an operator CAS and immediately fences the active worker.
func (s *Store) ReplaceState(ctx context.Context, id string, base int64, state json.RawMessage, version int) (int64, error) {
	if !json.Valid(state) || version < 1 {
		return 0, errors.New("state must be valid JSON with a positive version")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := toMs(time.Now().UTC())
	res, err := tx.ExecContext(ctx, `UPDATE deployments SET state=?,state_version=?,state_revision=state_revision+1,updated_at=? WHERE id=? AND state_revision=?`, state, version, now, id, base)
	if err != nil {
		return 0, err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return 0, ErrStateConflict
	}
	_, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=? WHERE deployment_id=? AND status='active'`, now, id)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return base + 1, nil
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
func nullTime(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := fromMs(v.Int64)
	return &t
}

func hashJSON(raw json.RawMessage) []byte { sum := sha256.Sum256(raw); return sum[:] }

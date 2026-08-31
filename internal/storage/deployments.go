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
	ErrLeaseLost     = errors.New("delivery lease is no longer owned")
)

type Deployment struct {
	ID, Name, InfoName, SourceDir, Status, ArtifactID, ConfigHash string
	ConfigRevision, ActiveGeneration, StateRevision               int64
	StateVersion                                                  int
	State                                                         json.RawMessage
	CreatedAt, UpdatedAt                                          time.Time
	ExpiresAt, ArchivedAt                                         *time.Time
}

// DeployInput is the complete durable snapshot produced by one deploy. The
// deployment and its destinations become visible together or not at all.
type DeployInput struct {
	Name, InfoName, SourceDir, ArtifactID, ConfigHash string
	State                                             json.RawMessage
	StateVersion                                      int
	ExpiresAt                                         *time.Time
	Destinations                                      []json.RawMessage
	// ExpectedStateRevision prevents a build from overwriting state committed
	// while it was compiling. Nil means this is a new deployment.
	ExpectedStateRevision *int64
}

func (s *Store) Deploy(ctx context.Context, in DeployInput) (Deployment, error) {
	if strings.TrimSpace(in.Name) == "" || strings.TrimSpace(in.InfoName) == "" || in.ArtifactID == "" || in.ConfigHash == "" || !json.Valid(in.State) || in.StateVersion < 1 || len(in.Destinations) == 0 {
		return Deployment{}, errors.New("deploy requires name, info, artifact, config hash, and valid versioned state")
	}
	for _, destination := range in.Destinations {
		if !json.Valid(destination) {
			return Deployment{}, errors.New("deploy requires valid destination JSON")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Deployment{}, err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	nowMS := toMs(now)
	var expires any
	if in.ExpiresAt != nil {
		expires = toMs(*in.ExpiresAt)
	}
	var id, oldHash, status string
	var stateRevision int64
	err = tx.QueryRowContext(ctx, `SELECT id,config_hash,status,state_revision FROM deployments WHERE name=?`, in.Name).Scan(&id, &oldHash, &status, &stateRevision)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if in.ExpectedStateRevision != nil {
			return Deployment{}, ErrStateConflict
		}
		id, err = randomID()
		if err != nil {
			return Deployment{}, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO deployments(id,name,info_name,source_dir,status,artifact_id,config_revision,config_hash,state,state_version,created_at,updated_at,expires_at)
			VALUES(?,?,?,?, 'active', ?,1,?,?,?,?,?,?)`, id, in.Name, in.InfoName, in.SourceDir, in.ArtifactID, in.ConfigHash, in.State, in.StateVersion, nowMS, nowMS, expires)
	case err != nil:
		return Deployment{}, err
	default:
		if status == "archived" {
			return Deployment{}, ErrInvalidStatus
		}
		if in.ExpectedStateRevision == nil || *in.ExpectedStateRevision != stateRevision {
			return Deployment{}, ErrStateConflict
		}
		revisionBump := 0
		if oldHash != in.ConfigHash {
			revisionBump = 1
		}
		_, err = tx.ExecContext(ctx, `UPDATE deployments SET info_name=?,source_dir=?,artifact_id=?,config_hash=?,config_revision=config_revision+?,active_generation=0,state=?,state_version=?,state_revision=state_revision+1,status='active',expires_at=?,archived_at=NULL,updated_at=? WHERE id=?`, in.InfoName, in.SourceDir, in.ArtifactID, in.ConfigHash, revisionBump, in.State, in.StateVersion, expires, nowMS, id)
	}
	if err != nil {
		return Deployment{}, fmt.Errorf("deploy: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=? WHERE deployment_id=? AND status='active'`, nowMS, id); err != nil {
		return Deployment{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE destination_bindings SET retired_at=? WHERE deployment_id=? AND retired_at IS NULL`, nowMS, id); err != nil {
		return Deployment{}, err
	}
	for i, destination := range in.Destinations {
		destinationID := fmt.Sprintf("destination-%d", i+1)
		var revision int64
		if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision),0)+1 FROM destination_bindings WHERE deployment_id=? AND id=?`, id, destinationID).Scan(&revision); err != nil {
			return Deployment{}, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO destination_bindings(id,revision,deployment_id,config,config_hash,created_at) VALUES(?,?,?,?,?,?)`, destinationID, revision, id, destination, hashJSON(destination), nowMS); err != nil {
			return Deployment{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return Deployment{}, err
	}
	return s.GetDeployment(ctx, id)
}

func (s *Store) ListDeployments(ctx context.Context) ([]Deployment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,info_name,source_dir,status,COALESCE(artifact_id,''),config_revision,config_hash,active_generation,state,state_version,state_revision,created_at,updated_at,expires_at,archived_at FROM deployments ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Deployment
	for rows.Next() {
		var d Deployment
		var state []byte
		var created, updated int64
		var expires, archived sql.NullInt64
		if err = rows.Scan(&d.ID, &d.Name, &d.InfoName, &d.SourceDir, &d.Status, &d.ArtifactID, &d.ConfigRevision, &d.ConfigHash, &d.ActiveGeneration, &state, &d.StateVersion, &d.StateRevision, &created, &updated, &expires, &archived); err != nil {
			return nil, err
		}
		d.State = append(json.RawMessage(nil), state...)
		d.CreatedAt = fromMs(created)
		d.UpdatedAt = fromMs(updated)
		d.ExpiresAt = nullTime(expires)
		d.ArchivedAt = nullTime(archived)
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) DeactivateDueDeployments(ctx context.Context, now time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	nowMS := toMs(now)
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=? WHERE status='active' AND deployment_id IN (SELECT id FROM deployments WHERE status='active' AND expires_at IS NOT NULL AND expires_at<=?)`, nowMS, nowMS); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE deployments SET status='inactive',active_generation=0,updated_at=? WHERE status='active' AND expires_at IS NOT NULL AND expires_at<=?`, nowMS, nowMS)
	if err != nil {
		return 0, err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return count, nil
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := toMs(time.Now().UTC())
	res, err := tx.ExecContext(ctx, `UPDATE deployments SET status=?,active_generation=0,expires_at=?,archived_at=?,updated_at=? WHERE id=? AND status=?`, to, expiry, archived, now, id, from)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows != 1 {
		return ErrInvalidStatus
	}
	if from == "active" {
		if _, err = tx.ExecContext(ctx, `UPDATE deployment_generations SET status='retired',retired_at=? WHERE deployment_id=? AND status='active'`, now, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) PauseDeployment(ctx context.Context, id string) error {
	return s.setDeploymentStatus(ctx, id, "active", "inactive", nil)
}
func (s *Store) ResumeDeployment(ctx context.Context, id string, expires *time.Time) error {
	return s.setDeploymentStatus(ctx, id, "inactive", "active", expires)
}
func (s *Store) ArchiveDeployment(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := toMs(time.Now().UTC())
	res, err := tx.ExecContext(ctx, `UPDATE deployments SET status='archived',expires_at=NULL,archived_at=?,updated_at=? WHERE id=? AND status='inactive'`, now, now, id)
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
	res, err := tx.ExecContext(ctx, `UPDATE deployments SET state=?,state_version=?,state_revision=state_revision+1,active_generation=0,updated_at=? WHERE id=? AND state_revision=?`, state, version, now, id, base)
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

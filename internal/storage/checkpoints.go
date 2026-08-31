package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

type RuntimeDeployment struct {
	Deployment
	ArtifactPath, ArtifactHash string
	Describe                   json.RawMessage
	Checkpoints                map[string]json.RawMessage
}

func (s *Store) GetRuntimeDeployment(ctx context.Context, selector string) (RuntimeDeployment, error) {
	d, err := s.GetDeployment(ctx, selector)
	if err != nil {
		return RuntimeDeployment{}, err
	}
	var r RuntimeDeployment
	r.Deployment = d
	err = s.db.QueryRowContext(ctx, `SELECT path,content_hash,describe_json FROM artifacts WHERE id=?`, d.ArtifactID).Scan(&r.ArtifactPath, &r.ArtifactHash, &r.Describe)
	return r, err
}

func (s *Store) ListRuntimeDeployments(ctx context.Context) ([]RuntimeDeployment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT d.id,d.name,d.info_name,d.source_dir,d.status,COALESCE(d.artifact_id,''),d.config_revision,d.config_hash,d.active_generation,d.state,d.state_version,d.state_revision,d.created_at,d.updated_at,d.expires_at,d.archived_at,a.path,a.content_hash,a.describe_json FROM deployments d JOIN artifacts a ON a.id=d.artifact_id WHERE d.status='active' AND (d.expires_at IS NULL OR d.expires_at>?) ORDER BY d.name`, toMs(time.Now().UTC()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RuntimeDeployment
	for rows.Next() {
		var r RuntimeDeployment
		var state []byte
		var created, updated int64
		var expires, archived sql.NullInt64
		if err = rows.Scan(&r.ID, &r.Name, &r.InfoName, &r.SourceDir, &r.Status, &r.ArtifactID, &r.ConfigRevision, &r.ConfigHash, &r.ActiveGeneration, &state, &r.StateVersion, &r.StateRevision, &created, &updated, &expires, &archived, &r.ArtifactPath, &r.ArtifactHash, &r.Describe); err != nil {
			return nil, err
		}
		r.State = append(json.RawMessage(nil), state...)
		r.CreatedAt = fromMs(created)
		r.UpdatedAt = fromMs(updated)
		r.ExpiresAt = nullTime(expires)
		r.ArchivedAt = nullTime(archived)
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Checkpoints = map[string]json.RawMessage{}
		checkpointRows, queryErr := s.db.QueryContext(ctx, `SELECT source,value FROM checkpoints WHERE deployment_id=? ORDER BY source`, out[i].ID)
		if queryErr != nil {
			return nil, queryErr
		}
		for checkpointRows.Next() {
			var source string
			var value []byte
			if queryErr = checkpointRows.Scan(&source, &value); queryErr != nil {
				checkpointRows.Close()
				return nil, queryErr
			}
			out[i].Checkpoints[source] = append(json.RawMessage(nil), value...)
		}
		if queryErr = checkpointRows.Close(); queryErr != nil {
			return nil, queryErr
		}
	}
	return out, nil
}

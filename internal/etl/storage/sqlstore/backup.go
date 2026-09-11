package sqlstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// These writers are deliberately separate from runtime upserts. Restoring a
// historical row must not allocate a new ID/version, renew a lease, normalize a
// completed run, or replace its timestamp with the current clock.
func (s *Store) insertBackupRow(ctx context.Context, table, columns string, values ...any) error {
	if s.txFromContext(ctx) == nil {
		return fmt.Errorf("restore %s requires a transaction", table)
	}
	if err := s.injectFailure("backup.restore." + table); err != nil {
		return err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")
	_, err := s.exec(ctx, "INSERT INTO "+table+" ("+columns+") VALUES ("+placeholders+")", values...)
	return err
}

func (s *Store) WritePipelineFidelity(ctx context.Context, p *storage.PipelineRow) error {
	row := *p
	storage.NormalizePipelineLifecycle(&row) // v1 predates the split state columns.
	return s.insertBackupRow(ctx, "pipelines",
		"id,name,spec_yaml,status,desired_state,observed_state,generation,restore_error,created_at,updated_at",
		row.ID, row.Name, row.SpecYAML, row.Status, row.DesiredState, row.ObservedState,
		row.Generation, row.RestoreError, row.CreatedAt, row.UpdatedAt)
}

func (s *Store) WritePipelineVersionFidelity(ctx context.Context, v *storage.PipelineVersion) error {
	return s.insertBackupRow(ctx, "pipeline_versions", "id,pipeline,version,spec_yaml,created_at",
		v.ID, v.Pipeline, v.Version, v.SpecYAML, v.CreatedAt)
}

func (s *Store) WriteCheckpointFidelity(ctx context.Context, r *storage.CheckpointRecord) error {
	return s.insertBackupRow(ctx, "checkpoints", "job_name,source,position,generation,timestamp,updated_at",
		r.JobName, r.Source, string(r.Position), r.Generation, r.Timestamp, r.UpdatedAt)
}

func (s *Store) WriteDeadLetterFidelity(ctx context.Context, r *storage.DLQRecord) error {
	record, err := json.Marshal(r.Record)
	if err != nil {
		return fmt.Errorf("marshal restored DLQ record: %w", err)
	}
	identity, err := json.Marshal(r.IdentityContext)
	if err != nil {
		return fmt.Errorf("marshal restored DLQ identity: %w", err)
	}
	return s.insertBackupRow(ctx, "dead_letters",
		"id,job_name,record_json,error,error_class,identity_context_json,attempt,record_hash,pipeline_version,dag_node,created_at",
		r.ID, r.JobName, string(record), r.Error, r.ErrorClass, string(identity), r.Attempt,
		r.RecordHash, r.PipelineVersion, r.DAGNode, r.CreatedAt)
}

func (s *Store) WriteWorkerFidelity(ctx context.Context, w *storage.WorkerInfo) error {
	labels, err := json.Marshal(w.Labels)
	if err != nil {
		return err
	}
	return s.insertBackupRow(ctx, "workers", "id,host,port,slots,status,labels,last_heartbeat,registered_at",
		w.ID, w.Host, w.Port, w.Slots, w.Status, string(labels), w.LastHeartbeat, w.RegisteredAt)
}

func (s *Store) WriteTaskFidelity(ctx context.Context, t *storage.TaskAssignment) error {
	labels, err := json.Marshal(t.RequiredLabels)
	if err != nil {
		return err
	}
	return s.insertBackupRow(ctx, "task_assignments",
		"id,task_id,pipeline,shard_index,shard_total,worker_id,status,generation,attempt,lease_expires_at,last_error,required_labels,assigned_at,started_at,finished_at",
		t.ID, t.TaskID, t.Pipeline, t.ShardIndex, t.ShardTotal, t.WorkerID, t.Status,
		t.Generation, t.Attempt, t.LeaseExpiresAt, t.LastError, string(labels), t.AssignedAt, t.StartedAt, t.FinishedAt)
}

func (s *Store) WritePluginFidelity(ctx context.Context, p *storage.PluginEntry) error {
	return s.insertBackupRow(ctx, "plugins",
		"name,kind,wasm_path,version,abi,min_runtime_version,manifest_json,manifest_validated,enabled,installed_at",
		p.Name, p.Kind, p.WASMPath, p.Version, p.ABI, p.MinRuntimeVersion, p.ManifestJSON,
		p.ManifestValidated, p.Enabled, p.InstalledAt)
}

func (s *Store) WriteConnectionFidelity(ctx context.Context, c *storage.ConnectionEntry) error {
	config, err := json.Marshal(c.Config)
	if err != nil {
		return err
	}
	return s.insertBackupRow(ctx, "connections",
		"name,kind,type,config_json,last_status,last_error,last_tested_at,created_at,updated_at",
		c.Name, c.Kind, c.Type, string(config), c.LastStatus, c.LastError, c.LastTestedAt, c.CreatedAt, c.UpdatedAt)
}

// AdvanceBackupSequences makes the next generated PostgreSQL ID exceed every
// restored ID. setval is not transactional, so never move a sequence backwards:
// a failed commit may leave a harmless gap, never a future ID collision.
func (s *Store) AdvanceBackupSequences(ctx context.Context) error {
	if _, ok := s.dialect.(PostgresDialect); !ok {
		return nil // SQLite/MySQL advance their counters on explicit ID inserts.
	}
	for _, table := range []string{"pipeline_versions", "dead_letters", "audit_logs", "run_history", "task_assignments"} {
		sequence := "pg_get_serial_sequence('" + table + "', 'id')"
		query := "SELECT setval(" + sequence + ", GREATEST(COALESCE((SELECT MAX(id) FROM " + table + "), 0), nextval(" + sequence + ")), true)"
		var id int64
		if err := s.queryRow(ctx, query).Scan(&id); err != nil {
			return fmt.Errorf("advance restored %s sequence: %w", table, err)
		}
	}
	return nil
}

// ListAllPipelineVersions includes ID-keyed, legacy name-keyed and orphaned
// history. A successful empty name lookup must never hide ID-keyed history.
func (s *Store) ListAllPipelineVersions(ctx context.Context) ([]*storage.PipelineVersion, error) {
	rows, err := s.query(ctx, "SELECT id,pipeline,version,spec_yaml,created_at FROM pipeline_versions ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []*storage.PipelineVersion
	for rows.Next() {
		v := new(storage.PipelineVersion)
		if err := rows.Scan(&v.ID, &v.Pipeline, &v.Version, &v.SpecYAML, &v.CreatedAt); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

const schemaVersionCode = "v1"

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

func (s *Store) Ping() error {
	return s.pool.Ping(context.Background())
}

// Pool exposes the underlying connection pool for tests / maintenance.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_version (
			code       TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS pipelines (
			name        TEXT PRIMARY KEY,
			spec_yaml   TEXT NOT NULL,
			status      TEXT NOT NULL DEFAULT 'stopped',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_versions (
			id          BIGSERIAL PRIMARY KEY,
			pipeline    TEXT NOT NULL,
			version     INT NOT NULL,
			spec_yaml   TEXT NOT NULL,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE (pipeline, version)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			job_name    TEXT PRIMARY KEY,
			source      TEXT,
			position    JSONB,
			timestamp   TIMESTAMPTZ,
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS dead_letters (
			id           BIGSERIAL PRIMARY KEY,
			job_name     TEXT NOT NULL,
			record_json  TEXT NOT NULL,
			error        TEXT,
			error_class  TEXT,
			attempt      INT DEFAULT 0,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_dlq_job   ON dead_letters (job_name, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_dlq_class ON dead_letters (error_class)`,
		`CREATE TABLE IF NOT EXISTS audit_logs (
			id          BIGSERIAL PRIMARY KEY,
			action      TEXT NOT NULL,
			method      TEXT,
			path        TEXT,
			target      TEXT,
			remote      TEXT,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs (created_at)`,
		`CREATE TABLE IF NOT EXISTS run_history (
			id              BIGSERIAL PRIMARY KEY,
			job_name        TEXT NOT NULL,
			status          TEXT NOT NULL,
			started_at      TIMESTAMPTZ,
			finished_at     TIMESTAMPTZ,
			records_read    BIGINT DEFAULT 0,
			records_written BIGINT DEFAULT 0,
			records_failed  BIGINT DEFAULT 0,
			records_dlq     BIGINT DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_run_job ON run_history (job_name, started_at)`,
		`CREATE TABLE IF NOT EXISTS workers (
			id              TEXT PRIMARY KEY,
			host            TEXT NOT NULL,
			port            INT NOT NULL,
			slots           INT NOT NULL DEFAULT 4,
			status          TEXT NOT NULL DEFAULT 'online',
			labels          JSONB,
			last_heartbeat  TIMESTAMPTZ,
			registered_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS task_assignments (
			id          BIGSERIAL PRIMARY KEY,
			task_id     TEXT NOT NULL,
			pipeline    TEXT NOT NULL,
			worker_id   TEXT,
			status      TEXT NOT NULL DEFAULT 'pending',
			assigned_at TIMESTAMPTZ,
			started_at  TIMESTAMPTZ,
			finished_at TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_task_pipeline ON task_assignments (pipeline, status)`,
		`CREATE TABLE IF NOT EXISTS plugins (
			name         TEXT PRIMARY KEY,
			kind         TEXT NOT NULL,
			wasm_path    TEXT NOT NULL,
			version      TEXT NOT NULL DEFAULT '1.0.0',
			enabled      BOOLEAN DEFAULT TRUE,
			installed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS connections (
			name           TEXT PRIMARY KEY,
			kind           TEXT NOT NULL,
			type           TEXT NOT NULL,
			config_json    JSONB NOT NULL,
			last_status    TEXT,
			last_error     TEXT,
			last_tested_at TIMESTAMPTZ,
			created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS settings (
			key        TEXT PRIMARY KEY,
			value      TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS plugin_state (
			pipeline   TEXT NOT NULL,
			plugin     TEXT NOT NULL,
			key        TEXT NOT NULL,
			value      BYTEA,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (pipeline, plugin, key)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migration failed [%s]: %w", firstLine(stmt), err)
		}
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO schema_version (code) VALUES ($1) ON CONFLICT (code) DO NOTHING`,
		schemaVersionCode,
	); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}

	// Versioned incremental migrations (additive ALTERs)
	return s.runVersionedMigrations(ctx)
}

// runVersionedMigrations applies incremental schema changes tracked by
// the _schema_version table.
func (s *Store) runVersionedMigrations(ctx context.Context) error {
	s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS _schema_version (
		version     INTEGER PRIMARY KEY,
		description TEXT,
		applied_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
	)`)

	type migration struct {
		version     int
		description string
		sql         string
	}

	migrations := []migration{
		{1, "add duration_ms to run_history", "ALTER TABLE run_history ADD COLUMN IF NOT EXISTS duration_ms BIGINT DEFAULT 0"},
		// A11-redo: shard metadata on task_assignments so a worker knows which
		// shard to execute.
		{2, "add shard_index to task_assignments", "ALTER TABLE task_assignments ADD COLUMN IF NOT EXISTS shard_index INTEGER DEFAULT 0"},
		{3, "add shard_total to task_assignments", "ALTER TABLE task_assignments ADD COLUMN IF NOT EXISTS shard_total INTEGER DEFAULT 0"},
		{4, "add record_hash to dead_letters", "ALTER TABLE dead_letters ADD COLUMN IF NOT EXISTS record_hash TEXT"},
		{5, "add pipeline_version to dead_letters", "ALTER TABLE dead_letters ADD COLUMN IF NOT EXISTS pipeline_version INTEGER DEFAULT 0"},
		{6, "add dag_node to dead_letters", "ALTER TABLE dead_letters ADD COLUMN IF NOT EXISTS dag_node TEXT"},
	}

	for _, m := range migrations {
		var exists int
		s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM _schema_version WHERE version = $1", m.version).Scan(&exists)
		if exists > 0 {
			continue
		}
		if _, err := s.pool.Exec(ctx, m.sql); err != nil {
			return fmt.Errorf("versioned migration %d failed: %w", m.version, err)
		}
		s.pool.Exec(ctx, "INSERT INTO _schema_version (version, description) VALUES ($1, $2)", m.version, m.description)
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

var errNoRows = pgx.ErrNoRows

// ── Pipeline definitions ─────────────────────────────────────────────

func (s *Store) SavePipeline(ctx context.Context, row *storage.PipelineRow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO pipelines (name, spec_yaml, status, updated_at)
		 VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
		 ON CONFLICT (name) DO UPDATE SET
		   spec_yaml = EXCLUDED.spec_yaml,
		   status    = EXCLUDED.status,
		   updated_at = CURRENT_TIMESTAMP`,
		row.Name, row.SpecYAML, row.Status,
	)
	if err != nil {
		return fmt.Errorf("save pipeline: %w", err)
	}
	return nil
}

func (s *Store) GetPipeline(ctx context.Context, name string) (*storage.PipelineRow, error) {
	row := &storage.PipelineRow{}
	err := s.pool.QueryRow(ctx,
		`SELECT name, spec_yaml, status, created_at, updated_at FROM pipelines WHERE name=$1`, name,
	).Scan(&row.Name, &row.SpecYAML, &row.Status, &row.CreatedAt, &row.UpdatedAt)
	if errors.Is(err, errNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}
	return row, nil
}

func (s *Store) ListPipelines(ctx context.Context) ([]*storage.PipelineRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, spec_yaml, status, created_at, updated_at FROM pipelines ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	defer rows.Close()
	var result []*storage.PipelineRow
	for rows.Next() {
		row := &storage.PipelineRow{}
		if err := rows.Scan(&row.Name, &row.SpecYAML, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) DeletePipeline(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM pipelines WHERE name=$1`, name)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	return nil
}

// ── Pipeline versions ────────────────────────────────────────────────

func (s *Store) UpdatePipelineStatus(ctx context.Context, name string, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE pipelines SET status=$1, updated_at=NOW() WHERE name=$2`, status, name)
	if err != nil {
		return fmt.Errorf("update pipeline status: %w", err)
	}
	return nil
}

func (s *Store) SavePipelineVersion(ctx context.Context, name string, specYAML string) (int, error) {
	var maxVer *int
	_ = s.pool.QueryRow(ctx,
		`SELECT MAX(version) FROM pipeline_versions WHERE pipeline=$1`, name,
	).Scan(&maxVer)
	version := 1
	if maxVer != nil {
		version = *maxVer + 1
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO pipeline_versions (pipeline, version, spec_yaml) VALUES ($1, $2, $3)`,
		name, version, specYAML)
	if err != nil {
		return 0, fmt.Errorf("save pipeline version: %w", err)
	}
	return version, nil
}

func (s *Store) GetPipelineVersion(ctx context.Context, name string, version int) (*storage.PipelineVersion, error) {
	v := &storage.PipelineVersion{}
	err := s.pool.QueryRow(ctx,
		`SELECT id, pipeline, version, spec_yaml, created_at FROM pipeline_versions WHERE pipeline=$1 AND version=$2`,
		name, version,
	).Scan(&v.ID, &v.Pipeline, &v.Version, &v.SpecYAML, &v.CreatedAt)
	if errors.Is(err, errNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline version: %w", err)
	}
	return v, nil
}

func (s *Store) ListPipelineVersions(ctx context.Context, name string) ([]*storage.PipelineVersion, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, pipeline, version, spec_yaml, created_at FROM pipeline_versions WHERE pipeline=$1 ORDER BY version DESC`,
		name)
	if err != nil {
		return nil, fmt.Errorf("list pipeline versions: %w", err)
	}
	defer rows.Close()
	var result []*storage.PipelineVersion
	for rows.Next() {
		v := &storage.PipelineVersion{}
		if err := rows.Scan(&v.ID, &v.Pipeline, &v.Version, &v.SpecYAML, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan pipeline version: %w", err)
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

// ── Checkpoints ──────────────────────────────────────────────────────

func (s *Store) SaveCheckpoint(ctx context.Context, rec *storage.CheckpointRecord) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO checkpoints (job_name, source, position, timestamp, updated_at)
		 VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
		 ON CONFLICT (job_name) DO UPDATE SET
		   source     = EXCLUDED.source,
		   position   = EXCLUDED.position,
		   timestamp  = EXCLUDED.timestamp,
		   updated_at = CURRENT_TIMESTAMP`,
		rec.JobName, rec.Source, []byte(rec.Position), rec.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

func (s *Store) LoadCheckpoint(ctx context.Context, jobName string) (*storage.CheckpointRecord, error) {
	rec := &storage.CheckpointRecord{}
	var pos []byte
	err := s.pool.QueryRow(ctx,
		`SELECT job_name, source, position, timestamp, updated_at FROM checkpoints WHERE job_name=$1`,
		jobName,
	).Scan(&rec.JobName, &rec.Source, &pos, &rec.Timestamp, &rec.UpdatedAt)
	if errors.Is(err, errNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}
	rec.Position = append(json.RawMessage(nil), pos...)
	return rec, nil
}

func (s *Store) DeleteCheckpoint(ctx context.Context, jobName string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM checkpoints WHERE job_name=$1`, jobName)
	if err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

func (s *Store) ListCheckpoints(ctx context.Context) ([]*storage.CheckpointRecord, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT job_name, source, position, timestamp, updated_at FROM checkpoints ORDER BY job_name`)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer rows.Close()
	var result []*storage.CheckpointRecord
	for rows.Next() {
		rec := &storage.CheckpointRecord{}
		var pos []byte
		if err := rows.Scan(&rec.JobName, &rec.Source, &pos, &rec.Timestamp, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		rec.Position = append(json.RawMessage(nil), pos...)
		result = append(result, rec)
	}
	return result, rows.Err()
}

// ── Dead letters ─────────────────────────────────────────────────────

func (s *Store) WriteDeadLetter(ctx context.Context, rec *storage.DLQRecord) error {
	recJSON, err := json.Marshal(rec.Record)
	if err != nil {
		return fmt.Errorf("marshal dlq record: %w", err)
	}
	if rec.RecordHash == "" {
		rec.RecordHash = storage.RecordHashJSON(recJSON)
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO dead_letters (job_name, record_json, error, error_class, attempt, record_hash, pipeline_version, dag_node, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		rec.JobName, string(recJSON), rec.Error, rec.ErrorClass, rec.Attempt, rec.RecordHash, rec.PipelineVersion, rec.DAGNode, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("write dlq: %w", err)
	}
	return nil
}

func (s *Store) GetDeadLetterByID(ctx context.Context, jobName string, id int64) (*storage.DLQRecord, error) {
	rec := &storage.DLQRecord{}
	var recJSON string
	err := s.pool.QueryRow(ctx,
		`SELECT id, job_name, record_json, error, error_class, attempt,
		        COALESCE(record_hash, ''), COALESCE(pipeline_version, 0), COALESCE(dag_node, ''), created_at
		 FROM dead_letters WHERE job_name=$1 AND id=$2`,
		jobName, id,
	).Scan(&rec.ID, &rec.JobName, &recJSON, &rec.Error, &rec.ErrorClass, &rec.Attempt, &rec.RecordHash, &rec.PipelineVersion, &rec.DAGNode, &rec.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get dlq by id: %w", err)
	}
	if err := json.Unmarshal([]byte(recJSON), &rec.Record); err != nil {
		return nil, fmt.Errorf("unmarshal dlq record: %w", err)
	}
	return rec, nil
}

func (s *Store) ListDeadLetters(ctx context.Context, filter storage.DLQFilter) ([]*storage.DLQRecord, error) {
	qb := newDLQQueryBuilder(filter)
	rows, err := s.pool.Query(ctx, qb.query, qb.args...)
	if err != nil {
		return nil, fmt.Errorf("list dlq: %w", err)
	}
	defer rows.Close()
	var result []*storage.DLQRecord
	for rows.Next() {
		rec := &storage.DLQRecord{}
		var recJSON string
		if err := rows.Scan(&rec.ID, &rec.JobName, &recJSON, &rec.Error, &rec.ErrorClass, &rec.Attempt, &rec.RecordHash, &rec.PipelineVersion, &rec.DAGNode, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan dlq: %w", err)
		}
		if err := json.Unmarshal([]byte(recJSON), &rec.Record); err != nil {
			continue
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

func (s *Store) DeleteDeadLettersByFilter(ctx context.Context, filter storage.DLQFilter) (int64, error) {
	qb := newDLQDeleteBuilder(filter)
	ct, err := s.pool.Exec(ctx, qb.query, qb.args...)
	if err != nil {
		return 0, fmt.Errorf("delete dlq by filter: %w", err)
	}
	return ct.RowsAffected(), nil
}

func (s *Store) DeleteDeadLetterByID(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM dead_letters WHERE id=$1`, id)
	if err != nil {
		return fmt.Errorf("delete dlq by id: %w", err)
	}
	return nil
}

func (s *Store) DeleteAllDeadLetters(ctx context.Context, jobName string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM dead_letters WHERE job_name=$1`, jobName)
	if err != nil {
		return fmt.Errorf("delete all dlq: %w", err)
	}
	return nil
}

// CountDeadLetters returns the total number of DLQ rows for a job via COUNT(*).
func (s *Store) CountDeadLetters(ctx context.Context, jobName string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM dead_letters WHERE job_name=$1`, jobName).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count dlq: %w", err)
	}
	return n, nil
}

// dlqClause holds the assembled WHERE fragment and bound args.
type dlqClause struct {
	where string
	args  []any
}

func buildDLQWhere(f storage.DLQFilter) dlqClause {
	var where []string
	var args []any
	n := 0
	add := func(clause string, val any) {
		n++
		where = append(where, fmt.Sprintf(clause, n))
		args = append(args, val)
	}
	add("job_name = $%d", f.JobName)
	if !f.From.IsZero() {
		add("created_at >= $%d", f.From)
	}
	if !f.Until.IsZero() {
		add("created_at <= $%d", f.Until)
	}
	if f.ErrorClass != "" {
		add("error_class = $%d", f.ErrorClass)
	}
	if f.ErrorContains != "" {
		add("error LIKE $%d", "%"+f.ErrorContains+"%")
	}
	if f.Contains != "" {
		add("record_json LIKE $%d", "%"+f.Contains+"%")
	}
	return dlqClause{where: strings.Join(where, " AND "), args: args}
}

type dlqQueryBuilder struct {
	query string
	args  []any
}

func newDLQQueryBuilder(f storage.DLQFilter) *dlqQueryBuilder {
	c := buildDLQWhere(f)
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	n := len(c.args)
	limitIdx := n + 1
	offsetIdx := n + 2
	q := fmt.Sprintf(
		`SELECT id, job_name, record_json, error, error_class, attempt,
		        COALESCE(record_hash, ''), COALESCE(pipeline_version, 0), COALESCE(dag_node, ''), created_at
		 FROM dead_letters WHERE %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
		c.where, limitIdx, offsetIdx,
	)
	args := append(append([]any{}, c.args...), limit, f.Offset)
	return &dlqQueryBuilder{query: q, args: args}
}

type dlqDeleteBuilder struct {
	query string
	args  []any
}

func newDLQDeleteBuilder(f storage.DLQFilter) *dlqDeleteBuilder {
	c := buildDLQWhere(f)
	q := fmt.Sprintf(`DELETE FROM dead_letters WHERE %s`, c.where)
	return &dlqDeleteBuilder{query: q, args: c.args}
}

// ── Audit ────────────────────────────────────────────────────────────

func (s *Store) WriteAudit(ctx context.Context, entry *storage.AuditEntry) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO audit_logs (action, method, path, target, remote, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		entry.Action, entry.Method, entry.Path, entry.Target, entry.Remote, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("write audit: %w", err)
	}
	return nil
}

func (s *Store) ListAudit(ctx context.Context, limit int) ([]*storage.AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, action, method, path, target, remote, created_at
		 FROM audit_logs ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list audit: %w", err)
	}
	defer rows.Close()
	var result []*storage.AuditEntry
	for rows.Next() {
		e := &storage.AuditEntry{}
		if err := rows.Scan(&e.ID, &e.Action, &e.Method, &e.Path, &e.Target, &e.Remote, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan audit: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// ── Run history ──────────────────────────────────────────────────────

func (s *Store) RecordRunStart(ctx context.Context, jobName string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO run_history (job_name, status, started_at)
		 VALUES ($1, 'running', CURRENT_TIMESTAMP) RETURNING id`,
		jobName).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("record run start: %w", err)
	}
	return id, nil
}

func (s *Store) RecordRunEnd(ctx context.Context, runID int64, status string, read, written, failed, dlq, durationMs int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE run_history SET
		   status=$1, finished_at=CURRENT_TIMESTAMP,
		   duration_ms=$2,
		   records_read=$3, records_written=$4, records_failed=$5, records_dlq=$6
		 WHERE id=$7`,
		status, durationMs, read, written, failed, dlq, runID)
	if err != nil {
		return fmt.Errorf("record run end: %w", err)
	}
	return nil
}

func (s *Store) ListRunHistory(ctx context.Context, jobName string, limit int) ([]*storage.RunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, job_name, status, started_at, finished_at,
		        duration_ms,
		        records_read, records_written, records_failed, records_dlq
		 FROM run_history WHERE job_name=$1 ORDER BY started_at DESC LIMIT $2`, jobName, limit)
	if err != nil {
		return nil, fmt.Errorf("list run history: %w", err)
	}
	defer rows.Close()
	var result []*storage.RunRecord
	for rows.Next() {
		r := &storage.RunRecord{}
		var finishedAt *time.Time
		if err := rows.Scan(&r.ID, &r.JobName, &r.Status, &r.StartedAt, &finishedAt,
			&r.DurationMs,
			&r.RecordsRead, &r.RecordsWritten, &r.RecordsFailed, &r.RecordsDLQ); err != nil {
			return nil, fmt.Errorf("scan run history: %w", err)
		}
		if finishedAt != nil {
			r.FinishedAt = finishedAt
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ── Worker registry ──────────────────────────────────────────────────

func (s *Store) RegisterWorker(ctx context.Context, info *storage.WorkerInfo) error {
	labelsJSON := []byte("{}")
	if info.Labels != nil {
		b, err := json.Marshal(info.Labels)
		if err != nil {
			return fmt.Errorf("marshal worker labels: %w", err)
		}
		labelsJSON = b
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO workers (id, host, port, slots, status, labels, last_heartbeat, registered_at)
		 VALUES ($1, $2, $3, $4, 'online', $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		 ON CONFLICT (id) DO UPDATE SET
		   host           = EXCLUDED.host,
		   port           = EXCLUDED.port,
		   slots          = EXCLUDED.slots,
		   status         = 'online',
		   labels         = EXCLUDED.labels,
		   last_heartbeat = CURRENT_TIMESTAMP`,
		info.ID, info.Host, info.Port, info.Slots, labelsJSON)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE workers SET last_heartbeat=CURRENT_TIMESTAMP, status='online' WHERE id=$1`, workerID)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]*storage.WorkerInfo, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, host, port, slots, status, labels, last_heartbeat, registered_at
		 FROM workers ORDER BY registered_at`)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()
	var result []*storage.WorkerInfo
	for rows.Next() {
		w := &storage.WorkerInfo{}
		var labelsBytes []byte
		if err := rows.Scan(&w.ID, &w.Host, &w.Port, &w.Slots, &w.Status, &labelsBytes, &w.LastHeartbeat, &w.RegisteredAt); err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		if len(labelsBytes) > 0 && string(labelsBytes) != "{}" {
			_ = json.Unmarshal(labelsBytes, &w.Labels)
		}
		result = append(result, w)
	}
	return result, rows.Err()
}

func (s *Store) DeregisterWorker(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM workers WHERE id=$1`, workerID)
	if err != nil {
		return fmt.Errorf("deregister worker: %w", err)
	}
	return nil
}

// ── Task assignments ─────────────────────────────────────────────────

func (s *Store) CreateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO task_assignments (task_id, pipeline, worker_id, status, assigned_at, shard_index, shard_total)
		 VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, $5, $6)`,
		task.TaskID, task.Pipeline, task.WorkerID, task.Status, task.ShardIndex, task.ShardTotal)
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}
	return nil
}

func (s *Store) UpdateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE task_assignments SET
		   status=$1, worker_id=$2, started_at=$3, finished_at=$4
		 WHERE task_id=$5`,
		task.Status, task.WorkerID, task.StartedAt, task.FinishedAt, task.TaskID)
	if err != nil {
		return fmt.Errorf("update task: %w", err)
	}
	return nil
}

func (s *Store) ListTasks(ctx context.Context, pipeline string) ([]*storage.TaskAssignment, error) {
	var rows pgx.Rows
	var err error
	if pipeline == "" {
		// All-pipelines view is used by dispatch (AssignNextTask,
		// ReassignStaleTasks, worker poll) which only needs ACTIVE tasks.
		// Filter to non-terminal statuses so completed/failed rows don't crowd
		// out pending ones under the LIMIT (ST-1). The active-task count is
		// bounded by total in-flight shards, so 1000 is effectively unlimited.
		rows, err = s.pool.Query(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at
			 FROM task_assignments
			 WHERE status IN ('pending','assigned','running')
			 ORDER BY assigned_at DESC LIMIT 1000`)
	} else {
		rows, err = s.pool.Query(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at
			 FROM task_assignments WHERE pipeline=$1 ORDER BY assigned_at DESC LIMIT 1000`, pipeline)
	}
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()
	var result []*storage.TaskAssignment
	for rows.Next() {
		t := &storage.TaskAssignment{}
		var workerID *string
		var assignedAt, startedAt, finishedAt *time.Time
		if err := rows.Scan(&t.ID, &t.TaskID, &t.Pipeline, &t.ShardIndex, &t.ShardTotal, &workerID, &t.Status, &assignedAt, &startedAt, &finishedAt); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		if workerID != nil {
			t.WorkerID = *workerID
		}
		if assignedAt != nil {
			t.AssignedAt = assignedAt
		}
		if startedAt != nil {
			t.StartedAt = startedAt
		}
		if finishedAt != nil {
			t.FinishedAt = finishedAt
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// ── Plugin registry ──────────────────────────────────────────────────

func (s *Store) SavePlugin(ctx context.Context, p *storage.PluginEntry) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO plugins (name, kind, wasm_path, version, enabled, installed_at)
		 VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
		 ON CONFLICT (name) DO UPDATE SET
		   kind      = EXCLUDED.kind,
		   wasm_path = EXCLUDED.wasm_path,
		   version   = EXCLUDED.version,
		   enabled   = EXCLUDED.enabled`,
		p.Name, p.Kind, p.WASMPath, p.Version, p.Enabled)
	if err != nil {
		return fmt.Errorf("save plugin: %w", err)
	}
	return nil
}

func (s *Store) GetPlugin(ctx context.Context, name string) (*storage.PluginEntry, error) {
	p := &storage.PluginEntry{}
	err := s.pool.QueryRow(ctx,
		`SELECT name, kind, wasm_path, version, enabled, installed_at FROM plugins WHERE name=$1`, name,
	).Scan(&p.Name, &p.Kind, &p.WASMPath, &p.Version, &p.Enabled, &p.InstalledAt)
	if errors.Is(err, errNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get plugin: %w", err)
	}
	return p, nil
}

func (s *Store) ListPlugins(ctx context.Context) ([]*storage.PluginEntry, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, kind, wasm_path, version, enabled, installed_at FROM plugins ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list plugins: %w", err)
	}
	defer rows.Close()
	var result []*storage.PluginEntry
	for rows.Next() {
		p := &storage.PluginEntry{}
		if err := rows.Scan(&p.Name, &p.Kind, &p.WASMPath, &p.Version, &p.Enabled, &p.InstalledAt); err != nil {
			return nil, fmt.Errorf("scan plugin: %w", err)
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *Store) DeletePlugin(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM plugins WHERE name=$1`, name)
	if err != nil {
		return fmt.Errorf("delete plugin: %w", err)
	}
	return nil
}

// ── Connection catalog ───────────────────────────────────────────────

func (s *Store) SaveConnection(ctx context.Context, c *storage.ConnectionEntry) error {
	cfg, err := json.Marshal(c.Config)
	if err != nil {
		return fmt.Errorf("marshal connection config: %w", err)
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO connections (name, kind, type, config_json, last_status, last_error, last_tested_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
		 ON CONFLICT (name) DO UPDATE SET
		   kind        = EXCLUDED.kind,
		   type        = EXCLUDED.type,
		   config_json = EXCLUDED.config_json,
		   updated_at  = CURRENT_TIMESTAMP`,
		c.Name, c.Kind, c.Type, cfg, c.LastStatus, c.LastError, c.LastTestedAt)
	if err != nil {
		return fmt.Errorf("save connection: %w", err)
	}
	return nil
}

func (s *Store) GetConnection(ctx context.Context, name string) (*storage.ConnectionEntry, error) {
	c := &storage.ConnectionEntry{}
	var cfg []byte
	var lastTestedAt *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT name, kind, type, config_json, COALESCE(last_status, ''), COALESCE(last_error, ''), last_tested_at, created_at, updated_at
		 FROM connections WHERE name=$1`, name,
	).Scan(&c.Name, &c.Kind, &c.Type, &cfg, &c.LastStatus, &c.LastError, &lastTestedAt, &c.CreatedAt, &c.UpdatedAt)
	if errors.Is(err, errNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	if len(cfg) > 0 {
		if err := json.Unmarshal(cfg, &c.Config); err != nil {
			return nil, fmt.Errorf("unmarshal connection config: %w", err)
		}
	}
	if c.Config == nil {
		c.Config = map[string]any{}
	}
	c.LastTestedAt = lastTestedAt
	return c, nil
}

func (s *Store) ListConnections(ctx context.Context) ([]*storage.ConnectionEntry, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, kind, type, config_json, COALESCE(last_status, ''), COALESCE(last_error, ''), last_tested_at, created_at, updated_at
		 FROM connections ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	defer rows.Close()
	var result []*storage.ConnectionEntry
	for rows.Next() {
		c := &storage.ConnectionEntry{}
		var cfg []byte
		var lastTestedAt *time.Time
		if err := rows.Scan(&c.Name, &c.Kind, &c.Type, &cfg, &c.LastStatus, &c.LastError, &lastTestedAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan connection: %w", err)
		}
		if len(cfg) > 0 {
			if err := json.Unmarshal(cfg, &c.Config); err != nil {
				return nil, fmt.Errorf("unmarshal connection config: %w", err)
			}
		}
		if c.Config == nil {
			c.Config = map[string]any{}
		}
		c.LastTestedAt = lastTestedAt
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *Store) DeleteConnection(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM connections WHERE name=$1`, name)
	if err != nil {
		return fmt.Errorf("delete connection: %w", err)
	}
	return nil
}

func (s *Store) UpdateConnectionHealth(ctx context.Context, name, status, lastError string, testedAt time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE connections SET last_status=$1, last_error=$2, last_tested_at=$3, updated_at=CURRENT_TIMESTAMP WHERE name=$4`,
		status, lastError, testedAt, name)
	if err != nil {
		return fmt.Errorf("update connection health: %w", err)
	}
	return nil
}

// ── Settings ─────────────────────────────────────────────────────────

func (s *Store) GetSetting(ctx context.Context, key string) (string, error) {
	var val string
	err := s.pool.QueryRow(ctx, `SELECT value FROM settings WHERE key=$1`, key).Scan(&val)
	if errors.Is(err, errNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get setting: %w", err)
	}
	return val, nil
}

func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO settings (key, value, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)
		 ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=CURRENT_TIMESTAMP`,
		key, value)
	if err != nil {
		return fmt.Errorf("set setting: %w", err)
	}
	return nil
}

func (s *Store) ListSettings(ctx context.Context) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, value FROM settings`)
	if err != nil {
		return nil, fmt.Errorf("list settings: %w", err)
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("scan setting: %w", err)
		}
		result[k] = v
	}
	return result, rows.Err()
}

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"openetl-go/internal/etl/storage"
)

// Store implements storage.Storage backed by SQLite.
type Store struct {
	db *sql.DB
}

// New opens (or creates) a SQLite database at path and runs migrations.
func New(path string) (*Store, error) {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping() error {
	var v int
	return s.db.QueryRow("SELECT 1").Scan(&v)
}

// ── Migrations ───────────────────────────────────────────────────────

func (s *Store) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS pipelines (
			name        TEXT PRIMARY KEY,
			spec_yaml   TEXT NOT NULL,
			status      TEXT NOT NULL DEFAULT 'stopped',
			created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_versions (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			pipeline    TEXT NOT NULL,
			version     INTEGER NOT NULL,
			spec_yaml   TEXT NOT NULL,
			created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(pipeline, version)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			job_name    TEXT PRIMARY KEY,
			source      TEXT,
			position    TEXT,
			timestamp   DATETIME,
			updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS dead_letters (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			job_name    TEXT NOT NULL,
			record_json TEXT NOT NULL,
			error       TEXT,
			error_class TEXT,
			attempt     INTEGER DEFAULT 0,
			created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_dlq_job ON dead_letters(job_name, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_dlq_class ON dead_letters(error_class)`,
		`CREATE TABLE IF NOT EXISTS audit_logs (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			action      TEXT NOT NULL,
			method      TEXT,
			path        TEXT,
			target      TEXT,
			remote      TEXT,
			created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at DESC)`,
		`CREATE TABLE IF NOT EXISTS run_history (
			id              INTEGER PRIMARY KEY AUTOINCREMENT,
			job_name        TEXT NOT NULL,
			status          TEXT NOT NULL,
			started_at      DATETIME,
			finished_at     DATETIME,
			records_read    INTEGER DEFAULT 0,
			records_written INTEGER DEFAULT 0,
			records_failed  INTEGER DEFAULT 0,
			records_dlq     INTEGER DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_run_job ON run_history(job_name, started_at DESC)`,
		`CREATE TABLE IF NOT EXISTS workers (
			id              TEXT PRIMARY KEY,
			host            TEXT NOT NULL,
			port            INTEGER NOT NULL,
			slots           INTEGER NOT NULL DEFAULT 4,
			status          TEXT NOT NULL DEFAULT 'online',
			labels          TEXT DEFAULT '{}',
			last_heartbeat  DATETIME,
			registered_at   DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS task_assignments (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			task_id     TEXT NOT NULL,
			pipeline    TEXT NOT NULL,
			worker_id   TEXT,
			status      TEXT NOT NULL DEFAULT 'pending',
			assigned_at DATETIME,
			started_at  DATETIME,
			finished_at DATETIME
		)`,
		`CREATE INDEX IF NOT EXISTS idx_task_pipeline ON task_assignments(pipeline, status)`,
		`CREATE TABLE IF NOT EXISTS plugins (
			name         TEXT PRIMARY KEY,
			kind         TEXT NOT NULL,
			wasm_path    TEXT NOT NULL,
			version      TEXT NOT NULL DEFAULT '1.0.0',
			enabled      INTEGER DEFAULT 1,
			installed_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS settings (
			key    TEXT PRIMARY KEY,
			value  TEXT NOT NULL,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS plugin_state (
			pipeline   TEXT NOT NULL,
			plugin     TEXT NOT NULL,
			key        TEXT NOT NULL,
			value      BLOB,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (pipeline, plugin, key)
		)`,
	}
	for _, m := range migrations {
		if _, err := s.db.Exec(m); err != nil {
			return fmt.Errorf("migration failed [%s]: %w", firstLine(m), err)
		}
	}

	// Versioned incremental migrations (additive ALTERs)
	if err := s.runVersionedMigrations(); err != nil {
		return err
	}

	return nil
}

// runVersionedMigrations applies incremental schema changes tracked by
// the _schema_version table. Each migration is idempotent and recorded
// so it only runs once.
func (s *Store) runVersionedMigrations() error {
	s.db.Exec(`CREATE TABLE IF NOT EXISTS _schema_version (
		version     INTEGER PRIMARY KEY,
		description TEXT,
		applied_at  DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)

	type migration struct {
		version     int
		description string
		sql         string
	}

	migrations := []migration{
		{1, "add duration_ms to run_history", "ALTER TABLE run_history ADD COLUMN duration_ms INTEGER DEFAULT 0"},
		// A11-redo: shard metadata on task_assignments so a worker knows which
		// shard to execute. SQLite cannot add multiple columns in one ALTER, so
		// each column is its own versioned migration.
		{2, "add shard_index to task_assignments", "ALTER TABLE task_assignments ADD COLUMN shard_index INTEGER DEFAULT 0"},
		{3, "add shard_total to task_assignments", "ALTER TABLE task_assignments ADD COLUMN shard_total INTEGER DEFAULT 0"},
	}

	for _, m := range migrations {
		var exists int
		s.db.QueryRow("SELECT COUNT(*) FROM _schema_version WHERE version = ?", m.version).Scan(&exists)
		if exists > 0 {
			continue
		}
		if _, err := s.db.Exec(m.sql); err != nil {
			return fmt.Errorf("versioned migration %d failed: %w", m.version, err)
		}
		s.db.Exec("INSERT INTO _schema_version (version, description) VALUES (?, ?)", m.version, m.description)
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

// ── Pipeline definitions ─────────────────────────────────────────────

func (s *Store) SavePipeline(ctx context.Context, row *storage.PipelineRow) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pipelines (name, spec_yaml, status, updated_at)
		 VALUES (?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(name) DO UPDATE SET spec_yaml=excluded.spec_yaml, status=excluded.status, updated_at=CURRENT_TIMESTAMP`,
		row.Name, row.SpecYAML, row.Status,
	)
	return err
}

func (s *Store) GetPipeline(ctx context.Context, name string) (*storage.PipelineRow, error) {
	row := &storage.PipelineRow{}
	err := s.db.QueryRowContext(ctx,
		`SELECT name, spec_yaml, status, created_at, updated_at FROM pipelines WHERE name=?`, name,
	).Scan(&row.Name, &row.SpecYAML, &row.Status, &row.CreatedAt, &row.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return row, err
}

func (s *Store) ListPipelines(ctx context.Context) ([]*storage.PipelineRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, spec_yaml, status, created_at, updated_at FROM pipelines ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.PipelineRow
	for rows.Next() {
		row := &storage.PipelineRow{}
		if err := rows.Scan(&row.Name, &row.SpecYAML, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) DeletePipeline(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE name=?`, name)
	return err
}

func (s *Store) UpdatePipelineStatus(ctx context.Context, name string, status string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE pipelines SET status=?, updated_at=CURRENT_TIMESTAMP WHERE name=?`,
		status, name)
	return err
}

// ── Pipeline versions ────────────────────────────────────────────────

func (s *Store) SavePipelineVersion(ctx context.Context, name string, specYAML string) (int, error) {
	var maxVer sql.NullInt64
	_ = s.db.QueryRowContext(ctx, `SELECT MAX(version) FROM pipeline_versions WHERE pipeline=?`, name).Scan(&maxVer)
	version := 1
	if maxVer.Valid {
		version = int(maxVer.Int64) + 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pipeline_versions (pipeline, version, spec_yaml) VALUES (?, ?, ?)`,
		name, version, specYAML)
	return version, err
}

func (s *Store) GetPipelineVersion(ctx context.Context, name string, version int) (*storage.PipelineVersion, error) {
	v := &storage.PipelineVersion{}
	err := s.db.QueryRowContext(ctx,
		`SELECT id, pipeline, version, spec_yaml, created_at FROM pipeline_versions WHERE pipeline=? AND version=?`,
		name, version,
	).Scan(&v.ID, &v.Pipeline, &v.Version, &v.SpecYAML, &v.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return v, err
}

func (s *Store) ListPipelineVersions(ctx context.Context, name string) ([]*storage.PipelineVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pipeline, version, spec_yaml, created_at FROM pipeline_versions WHERE pipeline=? ORDER BY version DESC`,
		name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.PipelineVersion
	for rows.Next() {
		v := &storage.PipelineVersion{}
		if err := rows.Scan(&v.ID, &v.Pipeline, &v.Version, &v.SpecYAML, &v.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

// ── Checkpoints ──────────────────────────────────────────────────────

func (s *Store) SaveCheckpoint(ctx context.Context, rec *storage.CheckpointRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO checkpoints (job_name, source, position, timestamp, updated_at)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(job_name) DO UPDATE SET source=excluded.source, position=excluded.position, timestamp=excluded.timestamp, updated_at=CURRENT_TIMESTAMP`,
		rec.JobName, rec.Source, string(rec.Position), rec.Timestamp,
	)
	return err
}

func (s *Store) LoadCheckpoint(ctx context.Context, jobName string) (*storage.CheckpointRecord, error) {
	rec := &storage.CheckpointRecord{}
	var pos string
	err := s.db.QueryRowContext(ctx,
		`SELECT job_name, source, position, timestamp, updated_at FROM checkpoints WHERE job_name=?`,
		jobName,
	).Scan(&rec.JobName, &rec.Source, &pos, &rec.Timestamp, &rec.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	rec.Position = json.RawMessage(pos)
	return rec, err
}

func (s *Store) DeleteCheckpoint(ctx context.Context, jobName string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM checkpoints WHERE job_name=?`, jobName)
	return err
}

func (s *Store) ListCheckpoints(ctx context.Context) ([]*storage.CheckpointRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT job_name, source, position, timestamp, updated_at FROM checkpoints ORDER BY job_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.CheckpointRecord
	for rows.Next() {
		rec := &storage.CheckpointRecord{}
		var pos string
		if err := rows.Scan(&rec.JobName, &rec.Source, &pos, &rec.Timestamp, &rec.UpdatedAt); err != nil {
			return nil, err
		}
		rec.Position = json.RawMessage(pos)
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
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO dead_letters (job_name, record_json, error, error_class, attempt, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		rec.JobName, string(recJSON), rec.Error, rec.ErrorClass, rec.Attempt, time.Now(),
	)
	return err
}

func (s *Store) ListDeadLetters(ctx context.Context, filter storage.DLQFilter) ([]*storage.DLQRecord, error) {
	qb := newDLQQueryBuilder(filter)
	rows, err := s.db.QueryContext(ctx, qb.query, qb.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.DLQRecord
	for rows.Next() {
		rec := &storage.DLQRecord{}
		var recJSON string
		if err := rows.Scan(&rec.ID, &rec.JobName, &recJSON, &rec.Error, &rec.ErrorClass, &rec.Attempt, &rec.CreatedAt); err != nil {
			return nil, err
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
	res, err := s.db.ExecContext(ctx, qb.query, qb.args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) DeleteDeadLetterByID(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dead_letters WHERE id=?`, id)
	return err
}

func (s *Store) DeleteAllDeadLetters(ctx context.Context, jobName string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dead_letters WHERE job_name=?`, jobName)
	return err
}

// CountDeadLetters returns the total number of DLQ rows for a job. Uses COUNT(*)
// instead of loading rows into memory.
func (s *Store) CountDeadLetters(ctx context.Context, jobName string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE job_name=?`, jobName).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ── DLQ query builders ───────────────────────────────────────────────

type dlqQueryBuilder struct {
	query string
	args  []any
}

func newDLQQueryBuilder(f storage.DLQFilter) *dlqQueryBuilder {
	where := []string{"job_name = ?"}
	args := []any{f.JobName}
	if !f.From.IsZero() {
		where = append(where, "created_at >= ?")
		args = append(args, f.From)
	}
	if !f.Until.IsZero() {
		where = append(where, "created_at <= ?")
		args = append(args, f.Until)
	}
	if f.ErrorClass != "" {
		where = append(where, "error_class = ?")
		args = append(args, f.ErrorClass)
	}
	if f.ErrorContains != "" {
		where = append(where, "error LIKE ?")
		args = append(args, "%"+f.ErrorContains+"%")
	}
	if f.Contains != "" {
		where = append(where, "record_json LIKE ?")
		args = append(args, "%"+f.Contains+"%")
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	q := fmt.Sprintf(
		`SELECT id, job_name, record_json, error, error_class, attempt, created_at
		 FROM dead_letters WHERE %s ORDER BY created_at DESC LIMIT %d OFFSET %d`,
		strings.Join(where, " AND "), limit, f.Offset,
	)
	return &dlqQueryBuilder{query: q, args: args}
}

type dlqDeleteBuilder struct {
	query string
	args  []any
}

func newDLQDeleteBuilder(f storage.DLQFilter) *dlqDeleteBuilder {
	where := []string{"job_name = ?"}
	args := []any{f.JobName}
	if !f.From.IsZero() {
		where = append(where, "created_at >= ?")
		args = append(args, f.From)
	}
	if !f.Until.IsZero() {
		where = append(where, "created_at <= ?")
		args = append(args, f.Until)
	}
	if f.ErrorClass != "" {
		where = append(where, "error_class = ?")
		args = append(args, f.ErrorClass)
	}
	if f.ErrorContains != "" {
		where = append(where, "error LIKE ?")
		args = append(args, "%"+f.ErrorContains+"%")
	}
	if f.Contains != "" {
		where = append(where, "record_json LIKE ?")
		args = append(args, "%"+f.Contains+"%")
	}
	q := fmt.Sprintf(`DELETE FROM dead_letters WHERE %s`, strings.Join(where, " AND "))
	return &dlqDeleteBuilder{query: q, args: args}
}

// ── Audit ────────────────────────────────────────────────────────────

func (s *Store) WriteAudit(ctx context.Context, entry *storage.AuditEntry) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit_logs (action, method, path, target, remote, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		entry.Action, entry.Method, entry.Path, entry.Target, entry.Remote, time.Now(),
	)
	return err
}

func (s *Store) ListAudit(ctx context.Context, limit int) ([]*storage.AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, action, method, path, target, remote, created_at
		 FROM audit_logs ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.AuditEntry
	for rows.Next() {
		e := &storage.AuditEntry{}
		if err := rows.Scan(&e.ID, &e.Action, &e.Method, &e.Path, &e.Target, &e.Remote, &e.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// ── Run history ──────────────────────────────────────────────────────

func (s *Store) RecordRunStart(ctx context.Context, jobName string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO run_history (job_name, status, started_at) VALUES (?, 'running', CURRENT_TIMESTAMP)`,
		jobName)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) RecordRunEnd(ctx context.Context, runID int64, status string, read, written, failed, dlq, durationMs int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE run_history SET status=?, finished_at=CURRENT_TIMESTAMP, duration_ms=?, records_read=?, records_written=?, records_failed=?, records_dlq=? WHERE id=?`,
		status, durationMs, read, written, failed, dlq, runID)
	return err
}

func (s *Store) ListRunHistory(ctx context.Context, jobName string, limit int) ([]*storage.RunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_name, status, started_at, finished_at, duration_ms, records_read, records_written, records_failed, records_dlq
		 FROM run_history WHERE job_name=? ORDER BY started_at DESC LIMIT ?`, jobName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.RunRecord
	for rows.Next() {
		r := &storage.RunRecord{}
		var finishedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.JobName, &r.Status, &r.StartedAt, &finishedAt, &r.DurationMs, &r.RecordsRead, &r.RecordsWritten, &r.RecordsFailed, &r.RecordsDLQ); err != nil {
			return nil, err
		}
		if finishedAt.Valid {
			r.FinishedAt = &finishedAt.Time
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ── Worker registry ──────────────────────────────────────────────────

func (s *Store) RegisterWorker(ctx context.Context, info *storage.WorkerInfo) error {
	labelsJSON := "{}"
	if info.Labels != nil {
		if b, err := json.Marshal(info.Labels); err == nil {
			labelsJSON = string(b)
		}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO workers (id, host, port, slots, status, labels, last_heartbeat, registered_at)
		 VALUES (?, ?, ?, ?, 'online', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		 ON CONFLICT(id) DO UPDATE SET host=excluded.host, port=excluded.port, slots=excluded.slots, status='online', labels=excluded.labels, last_heartbeat=CURRENT_TIMESTAMP`,
		info.ID, info.Host, info.Port, info.Slots, labelsJSON)
	return err
}

func (s *Store) Heartbeat(ctx context.Context, workerID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workers SET last_heartbeat=CURRENT_TIMESTAMP, status='online' WHERE id=?`, workerID)
	return err
}

func (s *Store) ListWorkers(ctx context.Context) ([]*storage.WorkerInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, host, port, slots, status, labels, last_heartbeat, registered_at FROM workers ORDER BY registered_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.WorkerInfo
	for rows.Next() {
		w := &storage.WorkerInfo{}
		var labelsStr string
		if err := rows.Scan(&w.ID, &w.Host, &w.Port, &w.Slots, &w.Status, &labelsStr, &w.LastHeartbeat, &w.RegisteredAt); err != nil {
			return nil, err
		}
		if labelsStr != "" && labelsStr != "{}" {
			_ = json.Unmarshal([]byte(labelsStr), &w.Labels)
		}
		result = append(result, w)
	}
	return result, rows.Err()
}

func (s *Store) DeregisterWorker(ctx context.Context, workerID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM workers WHERE id=?`, workerID)
	return err
}

// ── Task assignments ─────────────────────────────────────────────────

func (s *Store) CreateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO task_assignments (task_id, pipeline, worker_id, status, assigned_at, shard_index, shard_total)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)`,
		task.TaskID, task.Pipeline, task.WorkerID, task.Status, task.ShardIndex, task.ShardTotal)
	return err
}

func (s *Store) UpdateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE task_assignments SET status=?, worker_id=?, started_at=?, finished_at=? WHERE task_id=?`,
		task.Status, task.WorkerID, task.StartedAt, task.FinishedAt, task.TaskID)
	return err
}

func (s *Store) ListTasks(ctx context.Context, pipeline string) ([]*storage.TaskAssignment, error) {
	var rows *sql.Rows
	var err error
	if pipeline == "" {
		// All-pipelines view is used by dispatch (AssignNextTask,
		// ReassignStaleTasks, worker poll) which only needs ACTIVE tasks.
		// Filter to non-terminal statuses so completed/failed rows don't crowd
		// out pending ones under the LIMIT (ST-1). The active-task count is
		// bounded by total in-flight shards, so 1000 is effectively unlimited.
		rows, err = s.db.QueryContext(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at
			 FROM task_assignments
			 WHERE status IN ('pending','assigned','running')
			 ORDER BY assigned_at DESC LIMIT 1000`)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at
			 FROM task_assignments WHERE pipeline=? ORDER BY assigned_at DESC LIMIT 1000`, pipeline)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.TaskAssignment
	for rows.Next() {
		t := &storage.TaskAssignment{}
		var workerID sql.NullString
		var assignedAt, startedAt, finishedAt sql.NullTime
		if err := rows.Scan(&t.ID, &t.TaskID, &t.Pipeline, &t.ShardIndex, &t.ShardTotal, &workerID, &t.Status, &assignedAt, &startedAt, &finishedAt); err != nil {
			return nil, err
		}
		t.WorkerID = workerID.String
		if assignedAt.Valid {
			t.AssignedAt = &assignedAt.Time
		}
		if startedAt.Valid {
			t.StartedAt = &startedAt.Time
		}
		if finishedAt.Valid {
			t.FinishedAt = &finishedAt.Time
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// ── Plugin registry ──────────────────────────────────────────────────

func (s *Store) SavePlugin(ctx context.Context, p *storage.PluginEntry) error {
	enabled := 0
	if p.Enabled {
		enabled = 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO plugins (name, kind, wasm_path, version, enabled, installed_at)
		 VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(name) DO UPDATE SET kind=excluded.kind, wasm_path=excluded.wasm_path, version=excluded.version, enabled=excluded.enabled`,
		p.Name, p.Kind, p.WASMPath, p.Version, enabled)
	return err
}

func (s *Store) GetPlugin(ctx context.Context, name string) (*storage.PluginEntry, error) {
	p := &storage.PluginEntry{}
	var enabled int
	err := s.db.QueryRowContext(ctx,
		`SELECT name, kind, wasm_path, version, enabled, installed_at FROM plugins WHERE name=?`, name,
	).Scan(&p.Name, &p.Kind, &p.WASMPath, &p.Version, &enabled, &p.InstalledAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	p.Enabled = enabled == 1
	return p, err
}

func (s *Store) ListPlugins(ctx context.Context) ([]*storage.PluginEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, kind, wasm_path, version, enabled, installed_at FROM plugins ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.PluginEntry
	for rows.Next() {
		p := &storage.PluginEntry{}
		var enabled int
		if err := rows.Scan(&p.Name, &p.Kind, &p.WASMPath, &p.Version, &enabled, &p.InstalledAt); err != nil {
			return nil, err
		}
		p.Enabled = enabled == 1
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *Store) DeletePlugin(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM plugins WHERE name=?`, name)
	return err
}

// ── Settings ─────────────────────────────────────────────────────────

func (s *Store) GetSetting(ctx context.Context, key string) (string, error) {
	var val string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=?`, key).Scan(&val)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return val, err
}

func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP`,
		key, value)
	return err
}

func (s *Store) ListSettings(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, value FROM settings`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		result[k] = v
	}
	return result, rows.Err()
}

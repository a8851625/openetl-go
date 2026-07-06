package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// Dialect renders the small set of SQL differences between SQLite, MySQL, and
// PostgreSQL. Store methods use SQLite-style ? placeholders and common SQL
// fragments; the dialect converts them at execution time.
type Dialect interface {
	Bind(query string) string
	Now() string
	PipelineUpsert() string
	CheckpointUpsert() string
	WorkerUpsert() string
	PluginUpsert() string
	ConnectionUpsert() string
	SettingUpsert() string
	SettingKeyColumn() string
	BoolValue(v bool) any
	RunHistoryInsertReturningID() bool
}

// Store implements storage.Storage with one shared SQL code path.
type Store struct {
	db      *sql.DB
	dialect Dialect
}

func New(db *sql.DB, dialect Dialect) *Store {
	return &Store{db: db, dialect: dialect}
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.dialect.Bind(query), args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, s.dialect.Bind(query), args...)
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, s.dialect.Bind(query), args...)
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping() error {
	var v int
	return s.db.QueryRow("SELECT 1").Scan(&v)
}

func (s *Store) MigrateSQLite() error {
	return s.migrate()
}

// ── Migrations ───────────────────────────────────────────────────────

func (s *Store) migrate() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS pipelines (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			spec_yaml   TEXT NOT NULL,
			status      TEXT NOT NULL DEFAULT 'stopped',
			created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name)`,
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
			name                  TEXT PRIMARY KEY,
			kind                  TEXT NOT NULL,
			wasm_path             TEXT NOT NULL,
			version               TEXT NOT NULL DEFAULT '1.0.0',
			abi                   TEXT NOT NULL DEFAULT '',
			min_runtime_version   TEXT NOT NULL DEFAULT '',
			manifest_json         TEXT NOT NULL DEFAULT '',
			manifest_validated    INTEGER DEFAULT 0,
			enabled               INTEGER DEFAULT 1,
			installed_at          DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS connections (
			name           TEXT PRIMARY KEY,
			kind           TEXT NOT NULL,
			type           TEXT NOT NULL,
			config_json    TEXT NOT NULL,
			last_status    TEXT,
			last_error     TEXT,
			last_tested_at DATETIME,
			created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP
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
		{4, "add record_hash to dead_letters", "ALTER TABLE dead_letters ADD COLUMN record_hash TEXT"},
		{5, "add pipeline_version to dead_letters", "ALTER TABLE dead_letters ADD COLUMN pipeline_version INTEGER DEFAULT 0"},
		{6, "add dag_node to dead_letters", "ALTER TABLE dead_letters ADD COLUMN dag_node TEXT"},
		{7, "add uuid id to pipelines", "ALTER TABLE pipelines ADD COLUMN id TEXT"},
		{8, "add required_labels to task_assignments", "ALTER TABLE task_assignments ADD COLUMN required_labels TEXT DEFAULT '{}'"},
		{9, "add abi to plugins", "ALTER TABLE plugins ADD COLUMN abi TEXT NOT NULL DEFAULT ''"},
		{10, "add min_runtime_version to plugins", "ALTER TABLE plugins ADD COLUMN min_runtime_version TEXT NOT NULL DEFAULT ''"},
		{11, "add manifest_json to plugins", "ALTER TABLE plugins ADD COLUMN manifest_json TEXT NOT NULL DEFAULT ''"},
		{12, "add manifest_validated to plugins", "ALTER TABLE plugins ADD COLUMN manifest_validated INTEGER DEFAULT 0"},
	}

	for _, m := range migrations {
		var exists int
		s.db.QueryRow("SELECT COUNT(*) FROM _schema_version WHERE version = ?", m.version).Scan(&exists)
		if exists > 0 {
			continue
		}
		if _, err := s.db.Exec(m.sql); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("versioned migration %d failed: %w", m.version, err)
			}
		}
		if m.version == 7 {
			if err := s.backfillPipelineIDs(); err != nil {
				return err
			}
			if err := s.migratePipelinePrimaryKey(); err != nil {
				return err
			}
			if _, err := s.db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pipelines_id ON pipelines(id)`); err != nil {
				return fmt.Errorf("create pipeline id index: %w", err)
			}
			if _, err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name)`); err != nil {
				return fmt.Errorf("create pipeline name index: %w", err)
			}
		}
		s.db.Exec("INSERT INTO _schema_version (version, description) VALUES (?, ?)", m.version, m.description)
	}
	if err := s.backfillPipelineIDs(); err != nil {
		return err
	}
	if err := s.migratePipelinePrimaryKey(); err != nil {
		return err
	}
	_, _ = s.db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pipelines_id ON pipelines(id)`)
	_, _ = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name)`)
	return nil
}

func (s *Store) backfillPipelineIDs() error {
	rows, err := s.db.Query(`SELECT name FROM pipelines WHERE id IS NULL OR id = ''`)
	if err != nil {
		if strings.Contains(err.Error(), "no such column: id") {
			return nil
		}
		return fmt.Errorf("list pipelines missing ids: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range names {
		row := &storage.PipelineRow{Name: name}
		storage.EnsurePipelineID(row)
		if _, err := s.db.Exec(`UPDATE pipelines SET id=? WHERE name=? AND (id IS NULL OR id='')`, row.ID, name); err != nil {
			return fmt.Errorf("backfill pipeline id for %s: %w", name, err)
		}
	}
	return nil
}

func (s *Store) migratePipelinePrimaryKey() error {
	rows, err := s.db.Query(`PRAGMA table_info(pipelines)`)
	if err != nil {
		return fmt.Errorf("inspect pipelines table: %w", err)
	}
	nameIsPK := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == "name" && pk > 0 {
			nameIsPK = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !nameIsPK {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmts := []string{
		`CREATE TABLE pipelines_new (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			spec_yaml   TEXT NOT NULL,
			status      TEXT NOT NULL DEFAULT 'stopped',
			created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
		`INSERT INTO pipelines_new (id, name, spec_yaml, status, created_at, updated_at)
		 SELECT id, name, spec_yaml, status, created_at, updated_at FROM pipelines`,
		`DROP TABLE pipelines`,
		`ALTER TABLE pipelines_new RENAME TO pipelines`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("migrate pipelines primary key [%s]: %w", firstLine(stmt), err)
		}
	}
	return tx.Commit()
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

// ── Pipeline definitions ─────────────────────────────────────────────

func (s *Store) SavePipeline(ctx context.Context, row *storage.PipelineRow) error {
	if row != nil && row.ID == "" {
		if existing, err := s.GetPipeline(ctx, row.Name); err == nil && existing != nil {
			row.ID = existing.ID
		}
	}
	storage.EnsurePipelineID(row)
	_, err := s.exec(ctx, s.dialect.PipelineUpsert(), row.ID, row.Name, row.SpecYAML, row.Status)
	return err
}

func (s *Store) GetPipeline(ctx context.Context, ref string) (*storage.PipelineRow, error) {
	row := &storage.PipelineRow{}
	err := s.queryRow(ctx,
		`SELECT id, name, spec_yaml, status, created_at, updated_at FROM pipelines
		 WHERE id=? OR name=?
		 ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END, created_at
		 LIMIT 1`, ref, ref, ref,
	).Scan(&row.ID, &row.Name, &row.SpecYAML, &row.Status, &row.CreatedAt, &row.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return row, err
}

func (s *Store) ListPipelines(ctx context.Context) ([]*storage.PipelineRow, error) {
	rows, err := s.query(ctx,
		`SELECT id, name, spec_yaml, status, created_at, updated_at FROM pipelines ORDER BY name, created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.PipelineRow
	for rows.Next() {
		row := &storage.PipelineRow{}
		if err := rows.Scan(&row.ID, &row.Name, &row.SpecYAML, &row.Status, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) DeletePipeline(ctx context.Context, ref string) error {
	_, err := s.exec(ctx, `DELETE FROM pipelines WHERE id=? OR name=?`, ref, ref)
	return err
}

func (s *Store) UpdatePipelineStatus(ctx context.Context, ref string, status string) error {
	_, err := s.exec(ctx,
		`UPDATE pipelines SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=? OR name=?`,
		status, ref, ref)
	return err
}

// ── Pipeline versions ────────────────────────────────────────────────

func (s *Store) SavePipelineVersion(ctx context.Context, name string, specYAML string) (int, error) {
	var maxVer sql.NullInt64
	_ = s.queryRow(ctx, `SELECT MAX(version) FROM pipeline_versions WHERE pipeline=?`, name).Scan(&maxVer)
	version := 1
	if maxVer.Valid {
		version = int(maxVer.Int64) + 1
	}
	_, err := s.exec(ctx,
		`INSERT INTO pipeline_versions (pipeline, version, spec_yaml) VALUES (?, ?, ?)`,
		name, version, specYAML)
	return version, err
}

func (s *Store) GetPipelineVersion(ctx context.Context, name string, version int) (*storage.PipelineVersion, error) {
	v := &storage.PipelineVersion{}
	err := s.queryRow(ctx,
		`SELECT id, pipeline, version, spec_yaml, created_at FROM pipeline_versions WHERE pipeline=? AND version=?`,
		name, version,
	).Scan(&v.ID, &v.Pipeline, &v.Version, &v.SpecYAML, &v.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return v, err
}

func (s *Store) ListPipelineVersions(ctx context.Context, name string) ([]*storage.PipelineVersion, error) {
	rows, err := s.query(ctx,
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
	_, err := s.exec(ctx, s.dialect.CheckpointUpsert(), rec.JobName, rec.Source, string(rec.Position), rec.Timestamp)
	return err
}

func (s *Store) LoadCheckpoint(ctx context.Context, jobName string) (*storage.CheckpointRecord, error) {
	rec := &storage.CheckpointRecord{}
	var pos string
	err := s.queryRow(ctx,
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
	_, err := s.exec(ctx, `DELETE FROM checkpoints WHERE job_name=?`, jobName)
	return err
}

func (s *Store) ListCheckpoints(ctx context.Context) ([]*storage.CheckpointRecord, error) {
	rows, err := s.query(ctx,
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
	if rec.RecordHash == "" {
		rec.RecordHash = storage.RecordHashJSON(recJSON)
	}
	_, err = s.exec(ctx,
		`INSERT INTO dead_letters (job_name, record_json, error, error_class, attempt, record_hash, pipeline_version, dag_node, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.JobName, string(recJSON), rec.Error, rec.ErrorClass, rec.Attempt, rec.RecordHash, rec.PipelineVersion, rec.DAGNode, time.Now(),
	)
	return err
}

func (s *Store) GetDeadLetterByID(ctx context.Context, jobName string, id int64) (*storage.DLQRecord, error) {
	rec := &storage.DLQRecord{}
	var recJSON string
	err := s.queryRow(ctx,
		`SELECT id, job_name, record_json, error, error_class, attempt,
		        COALESCE(record_hash, ''), COALESCE(pipeline_version, 0), COALESCE(dag_node, ''), created_at
		 FROM dead_letters WHERE job_name=? AND id=?`,
		jobName, id,
	).Scan(&rec.ID, &rec.JobName, &recJSON, &rec.Error, &rec.ErrorClass, &rec.Attempt, &rec.RecordHash, &rec.PipelineVersion, &rec.DAGNode, &rec.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(recJSON), &rec.Record); err != nil {
		return nil, err
	}
	return rec, nil
}

func (s *Store) ListDeadLetters(ctx context.Context, filter storage.DLQFilter) ([]*storage.DLQRecord, error) {
	qb := newDLQQueryBuilder(filter)
	rows, err := s.query(ctx, qb.query, qb.args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.DLQRecord
	for rows.Next() {
		rec := &storage.DLQRecord{}
		var recJSON string
		if err := rows.Scan(&rec.ID, &rec.JobName, &recJSON, &rec.Error, &rec.ErrorClass, &rec.Attempt, &rec.RecordHash, &rec.PipelineVersion, &rec.DAGNode, &rec.CreatedAt); err != nil {
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
	res, err := s.exec(ctx, qb.query, qb.args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) DeleteDeadLetterByID(ctx context.Context, id int64) error {
	_, err := s.exec(ctx, `DELETE FROM dead_letters WHERE id=?`, id)
	return err
}

func (s *Store) DeleteAllDeadLetters(ctx context.Context, jobName string) error {
	_, err := s.exec(ctx, `DELETE FROM dead_letters WHERE job_name=?`, jobName)
	return err
}

// CountDeadLetters returns the total number of DLQ rows for a job. Uses COUNT(*)
// instead of loading rows into memory.
func (s *Store) CountDeadLetters(ctx context.Context, jobName string) (int64, error) {
	var n int64
	err := s.queryRow(ctx, `SELECT COUNT(*) FROM dead_letters WHERE job_name=?`, jobName).Scan(&n)
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
		`SELECT id, job_name, record_json, error, error_class, attempt,
		        COALESCE(record_hash, ''), COALESCE(pipeline_version, 0), COALESCE(dag_node, ''), created_at
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
	_, err := s.exec(ctx,
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
	rows, err := s.query(ctx,
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
	query := `INSERT INTO run_history (job_name, status, started_at) VALUES (?, 'running', CURRENT_TIMESTAMP)`
	if s.dialect.RunHistoryInsertReturningID() {
		var id int64
		err := s.queryRow(ctx, query+` RETURNING id`, jobName).Scan(&id)
		return id, err
	}
	res, err := s.exec(ctx, query, jobName)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) RecordRunEnd(ctx context.Context, runID int64, status string, read, written, failed, dlq, durationMs int64) error {
	_, err := s.exec(ctx,
		`UPDATE run_history SET status=?, finished_at=CURRENT_TIMESTAMP, duration_ms=?, records_read=?, records_written=?, records_failed=?, records_dlq=? WHERE id=?`,
		status, durationMs, read, written, failed, dlq, runID)
	return err
}

func (s *Store) ListRunHistory(ctx context.Context, jobName string, limit int) ([]*storage.RunRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.query(ctx,
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
	_, err := s.exec(ctx, s.dialect.WorkerUpsert(), info.ID, info.Host, info.Port, info.Slots, labelsJSON)
	return err
}

func (s *Store) Heartbeat(ctx context.Context, workerID string) error {
	_, err := s.exec(ctx,
		`UPDATE workers SET last_heartbeat=CURRENT_TIMESTAMP, status='online' WHERE id=?`, workerID)
	return err
}

func (s *Store) ListWorkers(ctx context.Context) ([]*storage.WorkerInfo, error) {
	rows, err := s.query(ctx,
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
	_, err := s.exec(ctx, `DELETE FROM workers WHERE id=?`, workerID)
	return err
}

// ── Task assignments ─────────────────────────────────────────────────

func (s *Store) CreateTask(ctx context.Context, task *storage.TaskAssignment) error {
	labelsJSON := "{}"
	if task.RequiredLabels != nil {
		if b, err := json.Marshal(task.RequiredLabels); err == nil {
			labelsJSON = string(b)
		}
	}
	_, err := s.exec(ctx,
		`INSERT INTO task_assignments (task_id, pipeline, worker_id, status, assigned_at, shard_index, shard_total, required_labels)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?)`,
		task.TaskID, task.Pipeline, task.WorkerID, task.Status, task.ShardIndex, task.ShardTotal, labelsJSON)
	return err
}

func (s *Store) UpdateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.exec(ctx,
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
		rows, err = s.query(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at, required_labels
			 FROM task_assignments
			 WHERE status IN ('pending','assigned','running')
			 ORDER BY assigned_at DESC LIMIT 1000`)
	} else {
		rows, err = s.query(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at, required_labels
			 FROM task_assignments WHERE pipeline=? ORDER BY assigned_at DESC LIMIT 1000`, pipeline)
	}
	if err != nil {
		// Fallback for DBs where the migration hasn't been applied yet:
		// retry without the required_labels column.
		if strings.Contains(err.Error(), "no such column: required_labels") {
			return s.listTasksNoLabels(ctx, pipeline)
		}
		return nil, err
	}
	defer rows.Close()
	var result []*storage.TaskAssignment
	for rows.Next() {
		t := &storage.TaskAssignment{}
		var workerID sql.NullString
		var assignedAt, startedAt, finishedAt sql.NullTime
		var labelsStr string
		if err := rows.Scan(&t.ID, &t.TaskID, &t.Pipeline, &t.ShardIndex, &t.ShardTotal, &workerID, &t.Status, &assignedAt, &startedAt, &finishedAt, &labelsStr); err != nil {
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
		if labelsStr != "" && labelsStr != "{}" {
			_ = json.Unmarshal([]byte(labelsStr), &t.RequiredLabels)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

// listTasksNoLabels is a fallback for databases that haven't yet applied
// migration 8 (required_labels). It returns tasks without label info so the
// dispatcher treats them as unconstrained (backwards-compatible).
func (s *Store) listTasksNoLabels(ctx context.Context, pipeline string) ([]*storage.TaskAssignment, error) {
	var rows *sql.Rows
	var err error
	if pipeline == "" {
		rows, err = s.query(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at
			 FROM task_assignments
			 WHERE status IN ('pending','assigned','running')
			 ORDER BY assigned_at DESC LIMIT 1000`)
	} else {
		rows, err = s.query(ctx,
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
	_, err := s.exec(ctx, s.dialect.PluginUpsert(),
		p.Name,
		p.Kind,
		p.WASMPath,
		p.Version,
		p.ABI,
		p.MinRuntimeVersion,
		p.ManifestJSON,
		s.dialect.BoolValue(p.ManifestValidated),
		s.dialect.BoolValue(p.Enabled),
	)
	return err
}

func (s *Store) GetPlugin(ctx context.Context, name string) (*storage.PluginEntry, error) {
	p := &storage.PluginEntry{}
	var enabled, manifestValidated any
	err := s.queryRow(ctx,
		`SELECT name, kind, wasm_path, version, COALESCE(abi, ''), COALESCE(min_runtime_version, ''), COALESCE(manifest_json, ''), manifest_validated, enabled, installed_at FROM plugins WHERE name=?`, name,
	).Scan(&p.Name, &p.Kind, &p.WASMPath, &p.Version, &p.ABI, &p.MinRuntimeVersion, &p.ManifestJSON, &manifestValidated, &enabled, &p.InstalledAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	p.ManifestValidated = dbBool(manifestValidated)
	p.Enabled = dbBool(enabled)
	return p, err
}

func (s *Store) ListPlugins(ctx context.Context) ([]*storage.PluginEntry, error) {
	rows, err := s.query(ctx,
		`SELECT name, kind, wasm_path, version, COALESCE(abi, ''), COALESCE(min_runtime_version, ''), COALESCE(manifest_json, ''), manifest_validated, enabled, installed_at FROM plugins ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.PluginEntry
	for rows.Next() {
		p := &storage.PluginEntry{}
		var enabled, manifestValidated any
		if err := rows.Scan(&p.Name, &p.Kind, &p.WASMPath, &p.Version, &p.ABI, &p.MinRuntimeVersion, &p.ManifestJSON, &manifestValidated, &enabled, &p.InstalledAt); err != nil {
			return nil, err
		}
		p.ManifestValidated = dbBool(manifestValidated)
		p.Enabled = dbBool(enabled)
		result = append(result, p)
	}
	return result, rows.Err()
}

func dbBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int:
		return x != 0
	case int8:
		return x != 0
	case int16:
		return x != 0
	case int32:
		return x != 0
	case int64:
		return x != 0
	case []byte:
		return string(x) == "1" || strings.EqualFold(string(x), "true")
	case string:
		return x == "1" || strings.EqualFold(x, "true")
	default:
		s := fmt.Sprint(v)
		return s == "1" || strings.EqualFold(s, "true")
	}
}

func (s *Store) DeletePlugin(ctx context.Context, name string) error {
	_, err := s.exec(ctx, `DELETE FROM plugins WHERE name=?`, name)
	return err
}

// ── Connection catalog ───────────────────────────────────────────────

func (s *Store) SaveConnection(ctx context.Context, c *storage.ConnectionEntry) error {
	cfg, err := json.Marshal(c.Config)
	if err != nil {
		return fmt.Errorf("marshal connection config: %w", err)
	}
	_, err = s.exec(ctx, s.dialect.ConnectionUpsert(), c.Name, c.Kind, c.Type, string(cfg), c.LastStatus, c.LastError, c.LastTestedAt)
	return err
}

func (s *Store) GetConnection(ctx context.Context, name string) (*storage.ConnectionEntry, error) {
	c := &storage.ConnectionEntry{}
	var cfg string
	var lastTestedAt sql.NullTime
	err := s.queryRow(ctx,
		`SELECT name, kind, type, config_json, COALESCE(last_status, ''), COALESCE(last_error, ''), last_tested_at, created_at, updated_at
		 FROM connections WHERE name=?`, name,
	).Scan(&c.Name, &c.Kind, &c.Type, &cfg, &c.LastStatus, &c.LastError, &lastTestedAt, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if cfg != "" {
		if err := json.Unmarshal([]byte(cfg), &c.Config); err != nil {
			return nil, fmt.Errorf("unmarshal connection config: %w", err)
		}
	}
	if c.Config == nil {
		c.Config = map[string]any{}
	}
	if lastTestedAt.Valid {
		c.LastTestedAt = &lastTestedAt.Time
	}
	return c, nil
}

func (s *Store) ListConnections(ctx context.Context) ([]*storage.ConnectionEntry, error) {
	rows, err := s.query(ctx,
		`SELECT name, kind, type, config_json, COALESCE(last_status, ''), COALESCE(last_error, ''), last_tested_at, created_at, updated_at
		 FROM connections ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*storage.ConnectionEntry
	for rows.Next() {
		c := &storage.ConnectionEntry{}
		var cfg string
		var lastTestedAt sql.NullTime
		if err := rows.Scan(&c.Name, &c.Kind, &c.Type, &cfg, &c.LastStatus, &c.LastError, &lastTestedAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		if cfg != "" {
			if err := json.Unmarshal([]byte(cfg), &c.Config); err != nil {
				return nil, fmt.Errorf("unmarshal connection config: %w", err)
			}
		}
		if c.Config == nil {
			c.Config = map[string]any{}
		}
		if lastTestedAt.Valid {
			c.LastTestedAt = &lastTestedAt.Time
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (s *Store) DeleteConnection(ctx context.Context, name string) error {
	_, err := s.exec(ctx, `DELETE FROM connections WHERE name=?`, name)
	return err
}

func (s *Store) UpdateConnectionHealth(ctx context.Context, name, status, lastError string, testedAt time.Time) error {
	_, err := s.exec(ctx,
		`UPDATE connections SET last_status=?, last_error=?, last_tested_at=?, updated_at=CURRENT_TIMESTAMP WHERE name=?`,
		status, lastError, testedAt, name)
	return err
}

// ── Settings ─────────────────────────────────────────────────────────

func (s *Store) GetSetting(ctx context.Context, key string) (string, error) {
	var val string
	err := s.queryRow(ctx, fmt.Sprintf(`SELECT value FROM settings WHERE %s=?`, s.dialect.SettingKeyColumn()), key).Scan(&val)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return val, err
}

func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	_, err := s.exec(ctx, s.dialect.SettingUpsert(), key, value)
	return err
}

func (s *Store) ListSettings(ctx context.Context) (map[string]string, error) {
	rows, err := s.query(ctx, fmt.Sprintf(`SELECT %s, value FROM settings`, s.dialect.SettingKeyColumn()))
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

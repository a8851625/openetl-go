package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

const schemaVersionCode = "v1"

type Store struct {
	db *sql.DB
}

func New(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ping() error { return s.db.Ping() }

// DB exposes the underlying connection pool for tests / maintenance.
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_version (
			code       VARCHAR(32) PRIMARY KEY,
			applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS pipelines (
			name        VARCHAR(255) PRIMARY KEY,
			spec_yaml   LONGTEXT NOT NULL,
			status      VARCHAR(32) NOT NULL DEFAULT 'stopped',
			created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_versions (
			id          BIGINT NOT NULL AUTO_INCREMENT,
			pipeline    VARCHAR(255) NOT NULL,
			version     INT NOT NULL,
			spec_yaml   LONGTEXT NOT NULL,
			created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (id),
			UNIQUE KEY uq_pipeline_version (pipeline, version)
		)`,
		`CREATE TABLE IF NOT EXISTS checkpoints (
			job_name    VARCHAR(255) PRIMARY KEY,
			source      VARCHAR(255),
			position    JSON,
			timestamp   DATETIME(3),
			updated_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS dead_letters (
			id           BIGINT NOT NULL AUTO_INCREMENT,
			job_name     VARCHAR(255) NOT NULL,
			record_json  LONGTEXT NOT NULL,
			error        LONGTEXT,
			error_class  VARCHAR(255),
			attempt      INT DEFAULT 0,
			created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (id),
			KEY idx_dlq_job (job_name, created_at),
			KEY idx_dlq_class (error_class)
		)`,
		`CREATE TABLE IF NOT EXISTS audit_logs (
			id          BIGINT NOT NULL AUTO_INCREMENT,
			action      VARCHAR(255) NOT NULL,
			method      VARCHAR(16),
			path        VARCHAR(512),
			target      VARCHAR(255),
			remote      VARCHAR(64),
			created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (id),
			KEY idx_audit_created (created_at)
		)`,
		`CREATE TABLE IF NOT EXISTS run_history (
			id              BIGINT NOT NULL AUTO_INCREMENT,
			job_name        VARCHAR(255) NOT NULL,
			status          VARCHAR(32) NOT NULL,
			started_at      DATETIME(3),
			finished_at     DATETIME(3),
			records_read    BIGINT DEFAULT 0,
			records_written BIGINT DEFAULT 0,
			records_failed  BIGINT DEFAULT 0,
			records_dlq     BIGINT DEFAULT 0,
			PRIMARY KEY (id),
			KEY idx_run_job (job_name, started_at)
		)`,
		`CREATE TABLE IF NOT EXISTS workers (
			id              VARCHAR(255) PRIMARY KEY,
			host            VARCHAR(255) NOT NULL,
			port            INT NOT NULL,
			slots           INT NOT NULL DEFAULT 4,
			status          VARCHAR(32) NOT NULL DEFAULT 'online',
			labels          JSON,
			last_heartbeat  DATETIME(3),
			registered_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS task_assignments (
			id          BIGINT NOT NULL AUTO_INCREMENT,
			task_id     VARCHAR(255) NOT NULL,
			pipeline    VARCHAR(255) NOT NULL,
			worker_id   VARCHAR(255),
			status      VARCHAR(32) NOT NULL DEFAULT 'pending',
			assigned_at DATETIME(3),
			started_at  DATETIME(3),
			finished_at DATETIME(3),
			PRIMARY KEY (id),
			KEY idx_task_pipeline (pipeline, status)
		)`,
		`CREATE TABLE IF NOT EXISTS plugins (
			name         VARCHAR(255) PRIMARY KEY,
			kind         VARCHAR(64) NOT NULL,
			wasm_path    VARCHAR(512) NOT NULL,
			version      VARCHAR(32) NOT NULL DEFAULT '1.0.0',
			enabled      TINYINT(1) DEFAULT 1,
			installed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS connections (
			name           VARCHAR(255) PRIMARY KEY,
			kind           VARCHAR(64) NOT NULL,
			type           VARCHAR(128) NOT NULL,
			config_json    LONGTEXT NOT NULL,
			last_status    VARCHAR(32),
			last_error     LONGTEXT,
			last_tested_at DATETIME(3),
			created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS settings (` +
			"`key` VARCHAR(255) PRIMARY KEY, " +
			`value      LONGTEXT NOT NULL,
			updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
		)`,
		`CREATE TABLE IF NOT EXISTS plugin_state (
			pipeline   VARCHAR(255) NOT NULL,
			plugin     VARCHAR(255) NOT NULL,
			` + "`key` VARCHAR(255) NOT NULL, " + `
			value      LONGBLOB,
			updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			PRIMARY KEY (pipeline, plugin, ` + "`key`" + `)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("migration failed [%s]: %w", firstLine(stmt), err)
		}
	}
	if _, err := s.db.Exec(
		`INSERT IGNORE INTO schema_version (code) VALUES (?)`, schemaVersionCode,
	); err != nil {
		return fmt.Errorf("record schema version: %w", err)
	}

	// Versioned incremental migrations (additive ALTERs)
	return s.runVersionedMigrations()
}

// runVersionedMigrations applies incremental schema changes tracked by
// the _schema_version table.
func (s *Store) runVersionedMigrations() error {
	s.db.Exec(`CREATE TABLE IF NOT EXISTS _schema_version (
		version     INT PRIMARY KEY,
		description VARCHAR(255),
		applied_at  DATETIME DEFAULT CURRENT_TIMESTAMP
	) ENGINE=InnoDB`)

	type migration struct {
		version     int
		description string
		sql         string
	}

	migrations := []migration{
		{1, "add duration_ms to run_history", "ALTER TABLE run_history ADD COLUMN duration_ms BIGINT DEFAULT 0"},
		// A11-redo: shard metadata on task_assignments so a worker knows which
		// shard to execute.
		{2, "add shard_index to task_assignments", "ALTER TABLE task_assignments ADD COLUMN shard_index INT DEFAULT 0"},
		{3, "add shard_total to task_assignments", "ALTER TABLE task_assignments ADD COLUMN shard_total INT DEFAULT 0"},
		{4, "add record_hash to dead_letters", "ALTER TABLE dead_letters ADD COLUMN record_hash CHAR(64)"},
		{5, "add pipeline_version to dead_letters", "ALTER TABLE dead_letters ADD COLUMN pipeline_version INT DEFAULT 0"},
		{6, "add dag_node to dead_letters", "ALTER TABLE dead_letters ADD COLUMN dag_node VARCHAR(255)"},
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
		 VALUES (?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE spec_yaml=VALUES(spec_yaml), status=VALUES(status), updated_at=CURRENT_TIMESTAMP(3)`,
		row.Name, row.SpecYAML, row.Status,
	)
	if err != nil {
		return fmt.Errorf("save pipeline: %w", err)
	}
	return nil
}

func (s *Store) GetPipeline(ctx context.Context, name string) (*storage.PipelineRow, error) {
	row := &storage.PipelineRow{}
	err := s.db.QueryRowContext(ctx,
		`SELECT name, spec_yaml, status, created_at, updated_at FROM pipelines WHERE name=?`, name,
	).Scan(&row.Name, &row.SpecYAML, &row.Status, &row.CreatedAt, &row.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline: %w", err)
	}
	return row, nil
}

func (s *Store) ListPipelines(ctx context.Context) ([]*storage.PipelineRow, error) {
	rows, err := s.db.QueryContext(ctx,
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
	_, err := s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE name=?`, name)
	if err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	return nil
}

// ── Pipeline versions ────────────────────────────────────────────────

func (s *Store) UpdatePipelineStatus(ctx context.Context, name string, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE pipelines SET status=?, updated_at=NOW() WHERE name=?`, status, name)
	if err != nil {
		return fmt.Errorf("update pipeline status: %w", err)
	}
	return nil
}

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
	if err != nil {
		return 0, fmt.Errorf("save pipeline version: %w", err)
	}
	return version, nil
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
	if err != nil {
		return nil, fmt.Errorf("get pipeline version: %w", err)
	}
	return v, nil
}

func (s *Store) ListPipelineVersions(ctx context.Context, name string) ([]*storage.PipelineVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, pipeline, version, spec_yaml, created_at FROM pipeline_versions WHERE pipeline=? ORDER BY version DESC`,
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
	// Normalize a zero timestamp to now. SQLite accepts the Go zero time
	// (0001-01-01) but MySQL strict mode rejects it as '0000-00-00'; this
	// keeps the two modes behaviorally aligned (SPEC §6.1 dual-mode parity).
	ts := rec.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO checkpoints (job_name, source, position, timestamp, updated_at)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE source=VALUES(source), position=VALUES(position), timestamp=VALUES(timestamp), updated_at=CURRENT_TIMESTAMP(3)`,
		rec.JobName, rec.Source, json.RawMessage(rec.Position), ts,
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

func (s *Store) LoadCheckpoint(ctx context.Context, jobName string) (*storage.CheckpointRecord, error) {
	rec := &storage.CheckpointRecord{}
	var pos []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT job_name, source, position, timestamp, updated_at FROM checkpoints WHERE job_name=?`,
		jobName,
	).Scan(&rec.JobName, &rec.Source, &pos, &rec.Timestamp, &rec.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}
	rec.Position = append(json.RawMessage(nil), pos...)
	return rec, nil
}

func (s *Store) DeleteCheckpoint(ctx context.Context, jobName string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM checkpoints WHERE job_name=?`, jobName)
	if err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

func (s *Store) ListCheckpoints(ctx context.Context) ([]*storage.CheckpointRecord, error) {
	rows, err := s.db.QueryContext(ctx,
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
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO dead_letters (job_name, record_json, error, error_class, attempt, record_hash, pipeline_version, dag_node, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
	err := s.db.QueryRowContext(ctx,
		`SELECT id, job_name, record_json, error, error_class, attempt,
		        COALESCE(record_hash, ''), COALESCE(pipeline_version, 0), COALESCE(dag_node, ''), created_at
		 FROM dead_letters WHERE job_name=? AND id=?`,
		jobName, id,
	).Scan(&rec.ID, &rec.JobName, &recJSON, &rec.Error, &rec.ErrorClass, &rec.Attempt, &rec.RecordHash, &rec.PipelineVersion, &rec.DAGNode, &rec.CreatedAt)
	if err == sql.ErrNoRows {
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
	rows, err := s.db.QueryContext(ctx, qb.query, qb.args...)
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
	res, err := s.db.ExecContext(ctx, qb.query, qb.args...)
	if err != nil {
		return 0, fmt.Errorf("delete dlq by filter: %w", err)
	}
	return res.RowsAffected()
}

func (s *Store) DeleteDeadLetterByID(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dead_letters WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("delete dlq by id: %w", err)
	}
	return nil
}

func (s *Store) DeleteAllDeadLetters(ctx context.Context, jobName string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dead_letters WHERE job_name=?`, jobName)
	if err != nil {
		return fmt.Errorf("delete all dlq: %w", err)
	}
	return nil
}

// CountDeadLetters returns the total number of DLQ rows for a job via COUNT(*).
func (s *Store) CountDeadLetters(ctx context.Context, jobName string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters WHERE job_name=?`, jobName).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count dlq: %w", err)
	}
	return n, nil
}

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
	// LIMIT/OFFSET rendered as parameters (MySQL accepts placeholders here).
	q := fmt.Sprintf(
		`SELECT id, job_name, record_json, error, error_class, attempt,
		        COALESCE(record_hash, ''), COALESCE(pipeline_version, 0), COALESCE(dag_node, ''), created_at
		 FROM dead_letters WHERE %s ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		strings.Join(where, " AND "),
	)
	args = append(args, limit, f.Offset)
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
	if err != nil {
		return fmt.Errorf("write audit: %w", err)
	}
	return nil
}

func (s *Store) ListAudit(ctx context.Context, limit int) ([]*storage.AuditEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, action, method, path, target, remote, created_at
		 FROM audit_logs ORDER BY created_at DESC LIMIT ?`, limit)
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
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO run_history (job_name, status, started_at) VALUES (?, 'running', CURRENT_TIMESTAMP(3))`,
		jobName)
	if err != nil {
		return 0, fmt.Errorf("record run start: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("run start last insert id: %w", err)
	}
	return id, nil
}

func (s *Store) RecordRunEnd(ctx context.Context, runID int64, status string, read, written, failed, dlq, durationMs int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE run_history SET status=?, finished_at=CURRENT_TIMESTAMP(3), duration_ms=?, records_read=?, records_written=?, records_failed=?, records_dlq=? WHERE id=?`,
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
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, job_name, status, started_at, finished_at, duration_ms, records_read, records_written, records_failed, records_dlq
		 FROM run_history WHERE job_name=? ORDER BY started_at DESC LIMIT ?`, jobName, limit)
	if err != nil {
		return nil, fmt.Errorf("list run history: %w", err)
	}
	defer rows.Close()
	var result []*storage.RunRecord
	for rows.Next() {
		r := &storage.RunRecord{}
		var finishedAt sql.NullTime
		if err := rows.Scan(&r.ID, &r.JobName, &r.Status, &r.StartedAt, &finishedAt, &r.DurationMs, &r.RecordsRead, &r.RecordsWritten, &r.RecordsFailed, &r.RecordsDLQ); err != nil {
			return nil, fmt.Errorf("scan run history: %w", err)
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
	var labelsJSON any = "{}"
	if info.Labels != nil {
		b, err := json.Marshal(info.Labels)
		if err != nil {
			return fmt.Errorf("marshal worker labels: %w", err)
		}
		labelsJSON = string(b)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO workers (id, host, port, slots, status, labels, last_heartbeat, registered_at)
		 VALUES (?, ?, ?, ?, 'online', ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE host=VALUES(host), port=VALUES(port), slots=VALUES(slots), status='online', labels=VALUES(labels), last_heartbeat=CURRENT_TIMESTAMP(3)`,
		info.ID, info.Host, info.Port, info.Slots, labelsJSON)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context, workerID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workers SET last_heartbeat=CURRENT_TIMESTAMP(3), status='online' WHERE id=?`, workerID)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]*storage.WorkerInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, host, port, slots, status, labels, last_heartbeat, registered_at FROM workers ORDER BY registered_at`)
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
	_, err := s.db.ExecContext(ctx, `DELETE FROM workers WHERE id=?`, workerID)
	if err != nil {
		return fmt.Errorf("deregister worker: %w", err)
	}
	return nil
}

// ── Task assignments ─────────────────────────────────────────────────

func (s *Store) CreateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO task_assignments (task_id, pipeline, worker_id, status, assigned_at, shard_index, shard_total)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3), ?, ?)`,
		task.TaskID, task.Pipeline, task.WorkerID, task.Status, task.ShardIndex, task.ShardTotal)
	if err != nil {
		return fmt.Errorf("create task: %w", err)
	}
	return nil
}

func (s *Store) UpdateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE task_assignments SET status=?, worker_id=?, started_at=?, finished_at=? WHERE task_id=?`,
		task.Status, task.WorkerID, task.StartedAt, task.FinishedAt, task.TaskID)
	if err != nil {
		return fmt.Errorf("update task: %w", err)
	}
	return nil
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
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()
	var result []*storage.TaskAssignment
	for rows.Next() {
		t := &storage.TaskAssignment{}
		var workerID sql.NullString
		var assignedAt, startedAt, finishedAt sql.NullTime
		if err := rows.Scan(&t.ID, &t.TaskID, &t.Pipeline, &t.ShardIndex, &t.ShardTotal, &workerID, &t.Status, &assignedAt, &startedAt, &finishedAt); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
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
		 VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE kind=VALUES(kind), wasm_path=VALUES(wasm_path), version=VALUES(version), enabled=VALUES(enabled)`,
		p.Name, p.Kind, p.WASMPath, p.Version, enabled)
	if err != nil {
		return fmt.Errorf("save plugin: %w", err)
	}
	return nil
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
	if err != nil {
		return nil, fmt.Errorf("get plugin: %w", err)
	}
	p.Enabled = enabled == 1
	return p, nil
}

func (s *Store) ListPlugins(ctx context.Context) ([]*storage.PluginEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, kind, wasm_path, version, enabled, installed_at FROM plugins ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list plugins: %w", err)
	}
	defer rows.Close()
	var result []*storage.PluginEntry
	for rows.Next() {
		p := &storage.PluginEntry{}
		var enabled int
		if err := rows.Scan(&p.Name, &p.Kind, &p.WASMPath, &p.Version, &enabled, &p.InstalledAt); err != nil {
			return nil, fmt.Errorf("scan plugin: %w", err)
		}
		p.Enabled = enabled == 1
		result = append(result, p)
	}
	return result, rows.Err()
}

func (s *Store) DeletePlugin(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM plugins WHERE name=?`, name)
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
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO connections (name, kind, type, config_json, last_status, last_error, last_tested_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE kind=VALUES(kind), type=VALUES(type), config_json=VALUES(config_json), updated_at=CURRENT_TIMESTAMP(3)`,
		c.Name, c.Kind, c.Type, string(cfg), c.LastStatus, c.LastError, c.LastTestedAt)
	if err != nil {
		return fmt.Errorf("save connection: %w", err)
	}
	return nil
}

func (s *Store) GetConnection(ctx context.Context, name string) (*storage.ConnectionEntry, error) {
	c := &storage.ConnectionEntry{}
	var cfg string
	var lastTestedAt sql.NullTime
	err := s.db.QueryRowContext(ctx,
		`SELECT name, kind, type, config_json, COALESCE(last_status, ''), COALESCE(last_error, ''), last_tested_at, created_at, updated_at
		 FROM connections WHERE name=?`, name,
	).Scan(&c.Name, &c.Kind, &c.Type, &cfg, &c.LastStatus, &c.LastError, &lastTestedAt, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
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
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, kind, type, config_json, COALESCE(last_status, ''), COALESCE(last_error, ''), last_tested_at, created_at, updated_at
		 FROM connections ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	defer rows.Close()
	var result []*storage.ConnectionEntry
	for rows.Next() {
		c := &storage.ConnectionEntry{}
		var cfg string
		var lastTestedAt sql.NullTime
		if err := rows.Scan(&c.Name, &c.Kind, &c.Type, &cfg, &c.LastStatus, &c.LastError, &lastTestedAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan connection: %w", err)
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
	_, err := s.db.ExecContext(ctx, `DELETE FROM connections WHERE name=?`, name)
	if err != nil {
		return fmt.Errorf("delete connection: %w", err)
	}
	return nil
}

func (s *Store) UpdateConnectionHealth(ctx context.Context, name, status, lastError string, testedAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE connections SET last_status=?, last_error=?, last_tested_at=?, updated_at=CURRENT_TIMESTAMP(3) WHERE name=?`,
		status, lastError, testedAt, name)
	if err != nil {
		return fmt.Errorf("update connection health: %w", err)
	}
	return nil
}

// ── Settings ─────────────────────────────────────────────────────────

func (s *Store) GetSetting(ctx context.Context, key string) (string, error) {
	var val string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM settings WHERE `key`=?", key).Scan(&val)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get setting: %w", err)
	}
	return val, nil
}

func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO settings (`key`, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP(3)) "+
			"ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=CURRENT_TIMESTAMP(3)",
		key, value)
	if err != nil {
		return fmt.Errorf("set setting: %w", err)
	}
	return nil
}

func (s *Store) ListSettings(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT `key`, value FROM settings")
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

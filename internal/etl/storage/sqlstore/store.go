package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
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
	// SupportsDeleteLimit reports whether DELETE ... LIMIT N is legal.
	// PostgreSQL returns false and requires a ctid/subquery form.
	SupportsDeleteLimit() bool
}

// Store implements storage.Storage with one shared SQL code path.
type Store struct {
	db              *sql.DB
	readDB          *sql.DB // optional read-only connection pool (WAL concurrent reads); falls back to db when nil
	dialect         Dialect
	failureMu       sync.RWMutex
	failureInjector func(operation string) error
}

func New(db *sql.DB, dialect Dialect) *Store {
	return &Store{db: db, dialect: dialect}
}

// SetReadDB attaches a separate read connection pool for SELECT queries.
// SQLite WAL mode allows concurrent readers that do not block the single
// writer; routing reads to readDB keeps checkpoint saves (writes) from
// contending with API/metrics/list queries. When readDB is nil, all queries
// fall back to db.
func (s *Store) SetReadDB(readDB *sql.DB) {
	s.readDB = readDB
}

func (s *Store) DB() *sql.DB {
	return s.db
}

// BackendName reports the logical storage backend for backup manifests.
func (s *Store) BackendName() string {
	if s == nil || s.dialect == nil {
		return "unknown"
	}
	switch s.dialect.(type) {
	case MySQLDialect:
		return "mysql"
	case PostgresDialect:
		return "postgres"
	case SQLiteDialect:
		return "sqlite"
	default:
		return "sql"
	}
}

// SetFailureInjector installs a test/diagnostic hook for persistence
// operations. It is intentionally small and nil-safe; production callers do
// not set it. The hook lets recovery tests prove that a mid-transaction error
// rolls back both the current pipeline row and its version row.
func (s *Store) SetFailureInjector(fn func(operation string) error) {
	s.failureMu.Lock()
	s.failureInjector = fn
	s.failureMu.Unlock()
}

func (s *Store) injectFailure(operation string) error {
	s.failureMu.RLock()
	fn := s.failureInjector
	s.failureMu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(operation)
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.dialect.Bind(query), args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rdb := s.readDB
	if rdb == nil {
		rdb = s.db
	}
	return rdb.QueryContext(ctx, s.dialect.Bind(query), args...)
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	rdb := s.readDB
	if rdb == nil {
		rdb = s.db
	}
	return rdb.QueryRowContext(ctx, s.dialect.Bind(query), args...)
}

func (s *Store) Close() error {
	if s.readDB != nil && s.readDB != s.db {
		s.readDB.Close()
	}
	return s.db.Close()
}

func (s *Store) Ping() error {
	var v int
	return s.db.QueryRow("SELECT 1").Scan(&v)
}

func (s *Store) MigrateSQLite() error {
	return WithMigrationLock(context.Background(), s.db, s.dialect, s.migrateUnlocked)
}

// ── Migrations ───────────────────────────────────────────────────────

func (s *Store) migrate() error {
	return s.migrateUnlocked()
}

func (s *Store) migrateUnlocked() error {
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
// so it only runs once. Failures abort before recording the version so a
// half-applied step is not treated as complete on the next startup.
func (s *Store) runVersionedMigrations() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS _schema_version (
		version     INTEGER PRIMARY KEY,
		description TEXT,
		applied_at  DATETIME DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create _schema_version: %w", err)
	}

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
		// PR-D1.2: task ownership fencing (lease / generation / attempt history).
		{13, "add generation to task_assignments", "ALTER TABLE task_assignments ADD COLUMN generation INTEGER DEFAULT 0"},
		{14, "add attempt to task_assignments", "ALTER TABLE task_assignments ADD COLUMN attempt INTEGER DEFAULT 0"},
		{15, "add lease_expires_at to task_assignments", "ALTER TABLE task_assignments ADD COLUMN lease_expires_at DATETIME"},
		{16, "add last_error to task_assignments", "ALTER TABLE task_assignments ADD COLUMN last_error TEXT DEFAULT ''"},
	}

	for _, m := range migrations {
		var exists int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM _schema_version WHERE version = ?", m.version).Scan(&exists); err != nil {
			return fmt.Errorf("read schema version %d: %w", m.version, err)
		}
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
		if _, err := s.db.Exec("INSERT INTO _schema_version (version, description) VALUES (?, ?)", m.version, m.description); err != nil {
			return fmt.Errorf("record schema version %d: %w", m.version, err)
		}
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

// SavePipelineWithVersion persists the current row and its historical version
// in one SQL transaction. All built-in SQL backends embed sqlstore.Store, so
// PipelineSpecStore can discover this capability without expanding the public
// Storage interface used by external test doubles/plugins.
func (s *Store) SavePipelineWithVersion(ctx context.Context, row *storage.PipelineRow, specYAML string) error {
	return s.savePipelineWithVersion(ctx, row, specYAML, false)
}

// SavePipelineWithVersionAndCheckpointReset extends the atomic current/version
// commit with an optional checkpoint delete used by incompatible spec updates.
func (s *Store) SavePipelineWithVersionAndCheckpointReset(ctx context.Context, row *storage.PipelineRow, specYAML string, resetCheckpoint bool) error {
	return s.savePipelineWithVersion(ctx, row, specYAML, resetCheckpoint)
}

func (s *Store) savePipelineWithVersion(ctx context.Context, row *storage.PipelineRow, specYAML string, resetCheckpoint bool) error {
	if row == nil {
		return fmt.Errorf("pipeline row is required")
	}
	if row.ID == "" {
		if existing, err := s.GetPipeline(ctx, row.Name); err == nil && existing != nil {
			row.ID = existing.ID
		}
	}
	storage.EnsurePipelineID(row)

	// Retry allocation under the unique (pipeline, version) constraint so two
	// concurrent updaters cannot both observe the same MAX(version) and commit
	// a duplicate version number. The current/version/checkpoint boundary stays
	// one transaction per attempt.
	const maxAttempts = 8
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := s.savePipelineWithVersionOnce(ctx, row, specYAML, resetCheckpoint)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isUniqueVersionConflict(err) {
			return err
		}
	}
	return fmt.Errorf("allocate pipeline version after %d attempts: %w", maxAttempts, lastErr)
}

func (s *Store) savePipelineWithVersionOnce(ctx context.Context, row *storage.PipelineRow, specYAML string, resetCheckpoint bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pipeline/version transaction: %w", err)
	}
	defer tx.Rollback()

	if err := s.injectFailure("pipeline.current"); err != nil {
		return fmt.Errorf("save current pipeline: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.dialect.Bind(s.dialect.PipelineUpsert()), row.ID, row.Name, row.SpecYAML, row.Status); err != nil {
		return fmt.Errorf("save current pipeline: %w", err)
	}

	if err := s.injectFailure("pipeline.version"); err != nil {
		return fmt.Errorf("save pipeline version: %w", err)
	}
	var maxVer sql.NullInt64
	if err := tx.QueryRowContext(ctx, s.dialect.Bind(`SELECT MAX(version) FROM pipeline_versions WHERE pipeline=?`), row.ID).Scan(&maxVer); err != nil {
		return fmt.Errorf("read next pipeline version: %w", err)
	}
	version := 1
	if maxVer.Valid {
		version = int(maxVer.Int64) + 1
	}
	if _, err := tx.ExecContext(ctx,
		s.dialect.Bind(`INSERT INTO pipeline_versions (pipeline, version, spec_yaml) VALUES (?, ?, ?)`),
		row.ID, version, specYAML); err != nil {
		return fmt.Errorf("save pipeline version %d: %w", version, err)
	}
	if resetCheckpoint {
		if err := s.injectFailure("checkpoint.delete"); err != nil {
			return fmt.Errorf("reset pipeline checkpoint: %w", err)
		}
		if _, err := tx.ExecContext(ctx, s.dialect.Bind(`DELETE FROM checkpoints WHERE job_name=? OR job_name=?`), row.ID, row.Name); err != nil {
			return fmt.Errorf("reset pipeline checkpoint: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pipeline/version transaction: %w", err)
	}
	return nil
}

func isUniqueVersionConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique") ||
		strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "constraint")
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
	if err := s.injectFailure("pipeline.delete"); err != nil {
		return err
	}
	_, err := s.exec(ctx, `DELETE FROM pipelines WHERE id=? OR name=?`, ref, ref)
	return err
}

// DeletePipelineWithCheckpoint removes a pipeline, its historical versions,
// and its checkpoint in one transaction. It is the lifecycle counterpart to
// SavePipelineWithVersion and prevents a successful API delete from leaving
// orphaned versions or a checkpoint that can resurrect stale state.
func (s *Store) DeletePipelineWithCheckpoint(ctx context.Context, ref string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pipeline delete transaction: %w", err)
	}
	defer tx.Rollback()

	if err := s.injectFailure("pipeline.delete"); err != nil {
		return fmt.Errorf("delete pipeline: %w", err)
	}
	var id, name string
	err = tx.QueryRowContext(ctx,
		s.dialect.Bind(`SELECT id, name FROM pipelines WHERE id=? OR name=? ORDER BY created_at LIMIT 1`),
		ref, ref,
	).Scan(&id, &name)
	if err == sql.ErrNoRows {
		id, name = ref, ref
	} else if err != nil {
		return fmt.Errorf("resolve pipeline for delete: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.dialect.Bind(`DELETE FROM pipeline_versions WHERE pipeline=? OR pipeline=?`), id, name); err != nil {
		return fmt.Errorf("delete pipeline versions: %w", err)
	}
	if err := s.injectFailure("checkpoint.delete"); err != nil {
		return fmt.Errorf("delete pipeline checkpoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.dialect.Bind(`DELETE FROM checkpoints WHERE job_name=? OR job_name=?`), id, name); err != nil {
		return fmt.Errorf("delete pipeline checkpoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.dialect.Bind(`DELETE FROM pipelines WHERE id=? OR name=?`), ref, ref); err != nil {
		return fmt.Errorf("delete pipeline row: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pipeline delete transaction: %w", err)
	}
	return nil
}

func (s *Store) UpdatePipelineStatus(ctx context.Context, ref string, status string) error {
	_, err := s.exec(ctx,
		`UPDATE pipelines SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=? OR name=?`,
		status, ref, ref)
	return err
}

// ── Pipeline versions ────────────────────────────────────────────────

func (s *Store) SavePipelineVersion(ctx context.Context, name string, specYAML string) (int, error) {
	const maxAttempts = 8
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var maxVer sql.NullInt64
		_ = s.queryRow(ctx, `SELECT MAX(version) FROM pipeline_versions WHERE pipeline=?`, name).Scan(&maxVer)
		version := 1
		if maxVer.Valid {
			version = int(maxVer.Int64) + 1
		}
		_, err := s.exec(ctx,
			`INSERT INTO pipeline_versions (pipeline, version, spec_yaml) VALUES (?, ?, ?)`,
			name, version, specYAML)
		if err == nil {
			return version, nil
		}
		lastErr = err
		if !isUniqueVersionConflict(err) {
			return 0, err
		}
	}
	return 0, fmt.Errorf("allocate pipeline version after %d attempts: %w", maxAttempts, lastErr)
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
	if err := s.injectFailure("checkpoint.delete"); err != nil {
		return err
	}
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
	var errMsg, errClass sql.NullString
	err := s.queryRow(ctx,
		`SELECT id, job_name, record_json, error, error_class, attempt,
		        COALESCE(record_hash, ''), COALESCE(pipeline_version, 0), COALESCE(dag_node, ''), created_at
		 FROM dead_letters WHERE job_name=? AND id=?`,
		jobName, id,
	).Scan(&rec.ID, &rec.JobName, &recJSON, &errMsg, &errClass, &rec.Attempt, &rec.RecordHash, &rec.PipelineVersion, &rec.DAGNode, &rec.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec.Error = errMsg.String
	rec.ErrorClass = errClass.String
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
		var errMsg, errClass sql.NullString
		if err := rows.Scan(&rec.ID, &rec.JobName, &recJSON, &errMsg, &errClass, &rec.Attempt, &rec.RecordHash, &rec.PipelineVersion, &rec.DAGNode, &rec.CreatedAt); err != nil {
			return nil, err
		}
		rec.Error = errMsg.String
		rec.ErrorClass = errClass.String
		if err := json.Unmarshal([]byte(recJSON), &rec.Record); err != nil {
			continue
		}
		result = append(result, rec)
	}
	return result, rows.Err()
}

func (s *Store) DeleteDeadLettersByFilter(ctx context.Context, filter storage.DLQFilter) (int64, error) {
	qb := newDLQDeleteBuilder(filter)
	q := qb.query
	// modernc SQLite and PostgreSQL both reject bare DELETE ... LIMIT;
	// rewrite every limited delete into a portable subquery form.
	if filter.Limit > 0 {
		q = rewriteLimitedDelete(q, filter.Limit, s.dialect.SupportsDeleteLimit())
	}
	res, err := s.exec(ctx, q, qb.args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// rewriteLimitedDelete turns "DELETE FROM t WHERE pred LIMIT N" into a
// portable form. PostgreSQL uses ctid; SQLite/MySQL use id (or rowid fallback).
// supportsBareLimit is reserved for future dialects that accept DELETE LIMIT
// natively; today all three backends use the subquery form for consistency.
func rewriteLimitedDelete(query string, limit int, supportsBareLimit bool) string {
	_ = supportsBareLimit
	const marker = " LIMIT "
	idx := strings.LastIndex(strings.ToUpper(query), marker)
	if idx < 0 || limit <= 0 {
		return query
	}
	body := strings.TrimSpace(query[:idx])
	upper := strings.ToUpper(body)
	fromIdx := strings.Index(upper, " FROM ")
	whereIdx := strings.Index(upper, " WHERE ")
	if fromIdx < 0 || whereIdx < 0 || whereIdx <= fromIdx {
		return query
	}
	table := strings.TrimSpace(body[fromIdx+len(" FROM ") : whereIdx])
	pred := strings.TrimSpace(body[whereIdx+len(" WHERE "):])
	// Prefer id-based subquery (works on SQLite/MySQL/Postgres for tables with id).
	// For tables without id the caller should not pass limit, or use purgeByTime
	// which knows the PK column.
	return fmt.Sprintf(
		`DELETE FROM %s WHERE id IN (SELECT id FROM %s WHERE %s ORDER BY created_at ASC LIMIT %d)`,
		table, table, pred, limit,
	)
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
	var where []string
	var args []any
	// Empty JobName means cross-pipeline purge (janitor TTL / max-count paths).
	if f.JobName != "" {
		where = append(where, "job_name = ?")
		args = append(args, f.JobName)
	}
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
	if len(where) == 0 {
		// Refuse unscoped DELETE FROM dead_letters; callers must set a filter.
		return &dlqDeleteBuilder{query: `DELETE FROM dead_letters WHERE 1=0`, args: nil}
	}
	pred := strings.Join(where, " AND ")
	q := fmt.Sprintf(`DELETE FROM dead_letters WHERE %s`, pred)
	if f.Limit > 0 {
		// SQLite/MySQL accept LIMIT on DELETE. Postgres needs a ctid subquery;
		// DeleteDeadLettersByFilter rewrites when the dialect forbids LIMIT.
		q = fmt.Sprintf(`%s LIMIT %d`, q, f.Limit)
	}
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
		`INSERT INTO task_assignments (task_id, pipeline, worker_id, status, assigned_at, shard_index, shard_total, required_labels, generation, attempt, lease_expires_at, last_error)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?)`,
		task.TaskID, task.Pipeline, task.WorkerID, task.Status, task.ShardIndex, task.ShardTotal, labelsJSON,
		task.Generation, task.Attempt, task.LeaseExpiresAt, task.LastError)
	return err
}

func (s *Store) UpdateTask(ctx context.Context, task *storage.TaskAssignment) error {
	_, err := s.exec(ctx,
		`UPDATE task_assignments SET status=?, worker_id=?, started_at=?, finished_at=?, generation=?, attempt=?, lease_expires_at=?, last_error=? WHERE task_id=?`,
		task.Status, task.WorkerID, task.StartedAt, task.FinishedAt, task.Generation, task.Attempt, task.LeaseExpiresAt, task.LastError, task.TaskID)
	return err
}

// ClaimTask finds the oldest pending task whose RequiredLabels are satisfied by
// workerLabels and CAS-claims it under workerID. Generation and attempt are
// incremented; a lease is granted for leaseTTL (defaults to DefaultTaskLeaseTTL).
func (s *Store) ClaimTask(ctx context.Context, workerID string, workerLabels map[string]string, leaseTTL time.Duration) (*storage.TaskAssignment, error) {
	if workerID == "" {
		return nil, fmt.Errorf("claim task: worker id is required")
	}
	if leaseTTL <= 0 {
		leaseTTL = storage.DefaultTaskLeaseTTL
	}
	tasks, err := s.ListTasks(ctx, "")
	if err != nil {
		return nil, err
	}
	now := time.Now()
	leaseUntil := now.Add(leaseTTL)
	for _, t := range tasks {
		if t.Status != "pending" {
			continue
		}
		if !taskLabelsMatch(workerLabels, t.RequiredLabels) {
			continue
		}
		newGen := t.Generation + 1
		newAttempt := t.Attempt + 1
		res, err := s.exec(ctx,
			`UPDATE task_assignments
			 SET status='assigned', worker_id=?, generation=?, attempt=?, assigned_at=?, started_at=NULL, finished_at=NULL, lease_expires_at=?, last_error=''
			 WHERE task_id=? AND status='pending' AND generation=?`,
			workerID, newGen, newAttempt, now, leaseUntil, t.TaskID, t.Generation)
		if err != nil {
			return nil, err
		}
		affected, _ := res.RowsAffected()
		if affected == 0 {
			// Lost the race — another worker claimed it; try the next pending task.
			continue
		}
		t.WorkerID = workerID
		t.Status = "assigned"
		t.Generation = newGen
		t.Attempt = newAttempt
		t.AssignedAt = &now
		t.StartedAt = nil
		t.FinishedAt = nil
		t.LeaseExpiresAt = &leaseUntil
		t.LastError = ""
		return t, nil
	}
	return nil, nil
}

// CASUpdateTask updates a task only when the caller still owns the generation.
func (s *Store) CASUpdateTask(ctx context.Context, task *storage.TaskAssignment) error {
	if task == nil || task.TaskID == "" {
		return fmt.Errorf("cas update task: task_id is required")
	}
	res, err := s.exec(ctx,
		`UPDATE task_assignments
		 SET status=?, worker_id=?, started_at=?, finished_at=?, lease_expires_at=?, last_error=?, attempt=?
		 WHERE task_id=? AND worker_id=? AND generation=?`,
		task.Status, task.WorkerID, task.StartedAt, task.FinishedAt, task.LeaseExpiresAt, task.LastError, task.Attempt,
		task.TaskID, task.WorkerID, task.Generation)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return storage.ErrTaskFenced
	}
	return nil
}

// GetTask returns a single task by task_id (including terminal statuses).
func (s *Store) GetTask(ctx context.Context, taskID string) (*storage.TaskAssignment, error) {
	rows, err := s.query(ctx,
		`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at, required_labels, generation, attempt, lease_expires_at, last_error
		 FROM task_assignments WHERE task_id=? LIMIT 1`, taskID)
	if err != nil {
		if isMissingTaskFenceCols(err) {
			return s.getTaskLegacy(ctx, taskID)
		}
		return nil, err
	}
	defer rows.Close()
	tasks, err := scanTasks(rows)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	return tasks[0], nil
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
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at, required_labels, generation, attempt, lease_expires_at, last_error
			 FROM task_assignments
			 WHERE status IN ('pending','assigned','running')
			 ORDER BY assigned_at DESC LIMIT 1000`)
	} else {
		rows, err = s.query(ctx,
			`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at, required_labels, generation, attempt, lease_expires_at, last_error
			 FROM task_assignments WHERE pipeline=? ORDER BY assigned_at DESC LIMIT 1000`, pipeline)
	}
	if err != nil {
		// Fallback for DBs where the migration hasn't been applied yet:
		// retry without fencing columns / required_labels.
		if isMissingTaskFenceCols(err) {
			return s.listTasksNoFence(ctx, pipeline)
		}
		if strings.Contains(err.Error(), "no such column: required_labels") {
			return s.listTasksNoLabels(ctx, pipeline)
		}
		return nil, err
	}
	defer rows.Close()
	return scanTasks(rows)
}

func isMissingTaskFenceCols(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such column: generation") ||
		strings.Contains(msg, "no such column: attempt") ||
		strings.Contains(msg, "no such column: lease_expires_at") ||
		strings.Contains(msg, "no such column: last_error") ||
		strings.Contains(msg, "Unknown column 'generation'") ||
		strings.Contains(msg, "Unknown column 'attempt'") ||
		strings.Contains(msg, "Unknown column 'lease_expires_at'") ||
		strings.Contains(msg, "Unknown column 'last_error'")
}

func taskLabelsMatch(workerLabels, required map[string]string) bool {
	if len(required) == 0 {
		return true
	}
	for k, v := range required {
		if workerLabels == nil || workerLabels[k] != v {
			return false
		}
	}
	return true
}

func scanTasks(rows *sql.Rows) ([]*storage.TaskAssignment, error) {
	var result []*storage.TaskAssignment
	for rows.Next() {
		t := &storage.TaskAssignment{}
		var workerID sql.NullString
		var assignedAt, startedAt, finishedAt, leaseAt sql.NullTime
		var labelsStr, lastError string
		var generation sql.NullInt64
		var attempt sql.NullInt64
		if err := rows.Scan(
			&t.ID, &t.TaskID, &t.Pipeline, &t.ShardIndex, &t.ShardTotal, &workerID, &t.Status,
			&assignedAt, &startedAt, &finishedAt, &labelsStr,
			&generation, &attempt, &leaseAt, &lastError,
		); err != nil {
			return nil, err
		}
		t.WorkerID = workerID.String
		t.Generation = generation.Int64
		t.Attempt = int(attempt.Int64)
		t.LastError = lastError
		if assignedAt.Valid {
			t.AssignedAt = &assignedAt.Time
		}
		if startedAt.Valid {
			t.StartedAt = &startedAt.Time
		}
		if finishedAt.Valid {
			t.FinishedAt = &finishedAt.Time
		}
		if leaseAt.Valid {
			t.LeaseExpiresAt = &leaseAt.Time
		}
		if labelsStr != "" && labelsStr != "{}" {
			_ = json.Unmarshal([]byte(labelsStr), &t.RequiredLabels)
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (s *Store) getTaskLegacy(ctx context.Context, taskID string) (*storage.TaskAssignment, error) {
	rows, err := s.query(ctx,
		`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at, required_labels
		 FROM task_assignments WHERE task_id=? LIMIT 1`, taskID)
	if err != nil {
		if strings.Contains(err.Error(), "no such column: required_labels") {
			return s.getTaskNoLabels(ctx, taskID)
		}
		return nil, err
	}
	defer rows.Close()
	tasks, err := scanTasksLegacy(rows)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	return tasks[0], nil
}

func (s *Store) getTaskNoLabels(ctx context.Context, taskID string) (*storage.TaskAssignment, error) {
	rows, err := s.query(ctx,
		`SELECT id, task_id, pipeline, shard_index, shard_total, worker_id, status, assigned_at, started_at, finished_at
		 FROM task_assignments WHERE task_id=? LIMIT 1`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks, err := scanTasksNoLabels(rows)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, nil
	}
	return tasks[0], nil
}

func (s *Store) listTasksNoFence(ctx context.Context, pipeline string) ([]*storage.TaskAssignment, error) {
	var rows *sql.Rows
	var err error
	if pipeline == "" {
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
		if strings.Contains(err.Error(), "no such column: required_labels") {
			return s.listTasksNoLabels(ctx, pipeline)
		}
		return nil, err
	}
	defer rows.Close()
	return scanTasksLegacy(rows)
}

func scanTasksLegacy(rows *sql.Rows) ([]*storage.TaskAssignment, error) {
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

func scanTasksNoLabels(rows *sql.Rows) ([]*storage.TaskAssignment, error) {
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
	return scanTasksNoLabels(rows)
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

// ── Retention / inventory (PR-1.3) ───────────────────────────────────

// PurgeAuditBefore deletes audit_logs with created_at <= cutoff.
// limit caps rows deleted when > 0; 0 means unlimited.
func (s *Store) PurgeAuditBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	return s.purgeByTime(ctx, "audit_logs", "created_at <= ?", "created_at", cutoff, limit)
}

// PurgeRunHistoryBefore deletes finished run_history rows with started_at <= cutoff.
func (s *Store) PurgeRunHistoryBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	return s.purgeByTime(ctx, "run_history",
		"finished_at IS NOT NULL AND started_at <= ?", "started_at", cutoff, limit)
}

// PurgeFinishedTasksBefore deletes terminal task_assignments with finished_at <= cutoff.
func (s *Store) PurgeFinishedTasksBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	return s.purgeByTime(ctx, "task_assignments",
		"status IN ('completed','failed','cancelled') AND finished_at IS NOT NULL AND finished_at <= ?",
		"finished_at", cutoff, limit)
}

func (s *Store) purgeByTime(ctx context.Context, table, pred, orderCol string, cutoff time.Time, limit int) (int64, error) {
	var q string
	if limit > 0 {
		// Portable limited delete via id subquery (works on SQLite/MySQL/Postgres).
		q = fmt.Sprintf(
			`DELETE FROM %s WHERE id IN (SELECT id FROM %s WHERE %s ORDER BY %s ASC LIMIT %d)`,
			table, table, pred, orderCol, limit,
		)
	} else {
		q = fmt.Sprintf(`DELETE FROM %s WHERE %s`, table, pred)
	}
	res, err := s.exec(ctx, q, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// CountObjects returns row counts for every control-plane table covered by backup.
func (s *Store) CountObjects(ctx context.Context) (storage.ObjectCounts, error) {
	var c storage.ObjectCounts
	tables := []struct {
		name string
		dst  *int
	}{
		{"pipelines", &c.Pipelines},
		{"pipeline_versions", &c.PipelineVersions},
		{"checkpoints", &c.Checkpoints},
		{"dead_letters", &c.DeadLetters},
		{"audit_logs", &c.AuditLogs},
		{"run_history", &c.RunHistory},
		{"workers", &c.Workers},
		{"task_assignments", &c.Tasks},
		{"plugins", &c.Plugins},
		{"connections", &c.Connections},
		{"settings", &c.Settings},
	}
	for _, t := range tables {
		var n int
		if err := s.queryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, t.name)).Scan(&n); err != nil {
			// connections may be missing on very old DBs; treat as zero.
			if strings.Contains(err.Error(), "no such table") || strings.Contains(err.Error(), "doesn't exist") {
				*t.dst = 0
				continue
			}
			return c, fmt.Errorf("count %s: %w", t.name, err)
		}
		*t.dst = n
	}
	return c, nil
}

// SchemaVersions lists applied _schema_version rows ordered by version.
func (s *Store) SchemaVersions(ctx context.Context) ([]storage.SchemaVersionRow, error) {
	rows, err := s.query(ctx, `SELECT version, description, applied_at FROM _schema_version ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.SchemaVersionRow
	for rows.Next() {
		var r storage.SchemaVersionRow
		var applied sql.NullTime
		var desc sql.NullString
		if err := rows.Scan(&r.Version, &desc, &applied); err != nil {
			return nil, err
		}
		r.Description = desc.String
		if applied.Valid {
			r.AppliedAt = applied.Time
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

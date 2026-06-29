package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlstore"
)

const schemaVersionCode = "v1"

type Store struct {
	db   *sql.DB
	pool *pgxpool.Pool
	*sqlstore.Store
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
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("open postgres sql db: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	s := &Store{db: db, pool: pool}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		pool.Close()
		return nil, err
	}
	s.Store = sqlstore.New(db, sqlstore.PostgresDialect{})
	return s, nil
}

func (s *Store) Close() error {
	if s.db != nil {
		_ = s.db.Close()
	}
	s.pool.Close()
	return nil
}

func (s *Store) Ping() error { return s.db.Ping() }

// Pool exposes the underlying connection pool for tests / maintenance.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS schema_version (
			code       TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS pipelines (
			id          TEXT PRIMARY KEY,
			name        TEXT NOT NULL,
			spec_yaml   TEXT NOT NULL,
			status      TEXT NOT NULL DEFAULT 'stopped',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines (name)`,
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
		{7, "add uuid id to pipelines", "ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS id TEXT"},
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
		if m.version == 7 {
			if err := s.backfillPipelineIDs(ctx); err != nil {
				return err
			}
			if err := s.migratePipelinePrimaryKey(ctx); err != nil {
				return err
			}
			_, _ = s.pool.Exec(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_pipelines_id ON pipelines(id)`)
			_, _ = s.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name)`)
		}
		s.pool.Exec(ctx, "INSERT INTO _schema_version (version, description) VALUES ($1, $2)", m.version, m.description)
	}
	if err := s.backfillPipelineIDs(ctx); err != nil {
		return err
	}
	if err := s.migratePipelinePrimaryKey(ctx); err != nil {
		return err
	}
	_, _ = s.pool.Exec(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_pipelines_id ON pipelines(id)`)
	_, _ = s.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name)`)
	return nil
}

func (s *Store) backfillPipelineIDs(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, `SELECT name FROM pipelines WHERE id IS NULL OR id = ''`)
	if err != nil {
		if strings.Contains(err.Error(), "column \"id\" does not exist") {
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
		if _, err := s.pool.Exec(ctx, `UPDATE pipelines SET id=$1 WHERE name=$2 AND (id IS NULL OR id='')`, row.ID, name); err != nil {
			return fmt.Errorf("backfill pipeline id for %s: %w", name, err)
		}
	}
	return nil
}

func (s *Store) migratePipelinePrimaryKey(ctx context.Context) error {
	var column string
	err := s.pool.QueryRow(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = 'pipelines'::regclass
		  AND i.indisprimary
		LIMIT 1`).Scan(&column)
	if errors.Is(err, errNoRows) || column == "id" {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect pipelines primary key: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pipelines ALTER COLUMN id SET NOT NULL`); err != nil {
		return fmt.Errorf("make pipeline id not null: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pipelines DROP CONSTRAINT IF EXISTS pipelines_pkey`); err != nil {
		return fmt.Errorf("drop pipelines primary key: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE pipelines ADD PRIMARY KEY (id)`); err != nil {
		return fmt.Errorf("migrate pipelines primary key to id: %w", err)
	}
	_, _ = s.pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS idx_pipelines_name ON pipelines(name)`)
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

var errNoRows = pgx.ErrNoRows

package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlstore"
)

const schemaVersionCode = "v1"

type Store struct {
	db *sql.DB
	*sqlstore.Store
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
	s.Store = sqlstore.New(db, sqlstore.MySQLDialect{})
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
			id          CHAR(36) PRIMARY KEY,
			name        VARCHAR(255) NOT NULL,
			spec_yaml   LONGTEXT NOT NULL,
			status      VARCHAR(32) NOT NULL DEFAULT 'stopped',
			created_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			KEY idx_pipelines_name (name)
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
		{7, "add uuid id to pipelines", "ALTER TABLE pipelines ADD COLUMN id CHAR(36)"},
	}

	for _, m := range migrations {
		var exists int
		s.db.QueryRow("SELECT COUNT(*) FROM _schema_version WHERE version = ?", m.version).Scan(&exists)
		if exists > 0 {
			continue
		}
		if _, err := s.db.Exec(m.sql); err != nil {
			if !(m.version == 7 && strings.Contains(err.Error(), "Duplicate column")) {
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
			_, _ = s.db.Exec(`CREATE UNIQUE INDEX idx_pipelines_id ON pipelines(id)`)
			_, _ = s.db.Exec(`CREATE INDEX idx_pipelines_name ON pipelines(name)`)
		}
		s.db.Exec("INSERT INTO _schema_version (version, description) VALUES (?, ?)", m.version, m.description)
	}
	if err := s.backfillPipelineIDs(); err != nil {
		return err
	}
	if err := s.migratePipelinePrimaryKey(); err != nil {
		return err
	}
	_, _ = s.db.Exec(`CREATE UNIQUE INDEX idx_pipelines_id ON pipelines(id)`)
	_, _ = s.db.Exec(`CREATE INDEX idx_pipelines_name ON pipelines(name)`)
	return nil
}

func (s *Store) backfillPipelineIDs() error {
	rows, err := s.db.Query(`SELECT name FROM pipelines WHERE id IS NULL OR id = ''`)
	if err != nil {
		if strings.Contains(err.Error(), "Unknown column") {
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
	var column string
	err := s.db.QueryRow(`
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'pipelines'
		  AND CONSTRAINT_NAME = 'PRIMARY'
		LIMIT 1`).Scan(&column)
	if err == sql.ErrNoRows || column == "id" {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect pipelines primary key: %w", err)
	}
	if _, err := s.db.Exec(`ALTER TABLE pipelines MODIFY id CHAR(36) NOT NULL`); err != nil {
		return fmt.Errorf("make pipeline id not null: %w", err)
	}
	if _, err := s.db.Exec(`ALTER TABLE pipelines DROP PRIMARY KEY, ADD PRIMARY KEY (id)`); err != nil {
		return fmt.Errorf("migrate pipelines primary key to id: %w", err)
	}
	_, _ = s.db.Exec(`CREATE INDEX idx_pipelines_name ON pipelines(name)`)
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

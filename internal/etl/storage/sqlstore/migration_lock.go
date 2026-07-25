package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"
)

// migrationLockName is the advisory lock key shared by every process that may
// open the same metadata database. Keep it stable across releases.
const migrationLockName = "openetl_go_schema_migration"

// openETLMigrationLockKey is a stable signed 64-bit key for PostgreSQL
// pg_advisory_lock.
const openETLMigrationLockKey int64 = 0x4f70656e45544c4d // "OpenETLM"

// sqliteMigrationMu serializes migrations inside one process. Cross-process
// exclusivity uses a lease row because SQLite often runs with MaxOpenConns=1
// and cannot hold a write transaction while migrate() issues other statements.
var sqliteMigrationMu sync.Mutex

// WithMigrationLock serializes schema migrations across concurrent processes.
// Only one caller executes fn; others wait for the lock or fail if the wait
// budget is exhausted. A failed fn does not mark migrations applied.
func WithMigrationLock(ctx context.Context, db *sql.DB, dialect Dialect, fn func() error) error {
	if db == nil {
		return fmt.Errorf("migration lock: database is nil")
	}
	if fn == nil {
		return fmt.Errorf("migration lock: callback is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	switch dialect.(type) {
	case MySQLDialect:
		return withMySQLMigrationLock(ctx, db, fn)
	case PostgresDialect:
		return withPostgresMigrationLock(ctx, db, fn)
	default:
		return withSQLiteMigrationLock(ctx, db, fn)
	}
}

func withSQLiteMigrationLock(ctx context.Context, db *sql.DB, fn func() error) error {
	sqliteMigrationMu.Lock()
	defer sqliteMigrationMu.Unlock()

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _migration_lock (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		owner TEXT NOT NULL DEFAULT '',
		lease_until DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00'
	)`); err != nil {
		return fmt.Errorf("prepare sqlite migration lock: %w", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO _migration_lock (id, owner, lease_until)
		VALUES (1, '', '1970-01-01 00:00:00')
		ON CONFLICT(id) DO NOTHING`); err != nil {
		return fmt.Errorf("seed sqlite migration lock: %w", err)
	}

	owner := fmt.Sprintf("%s-%d-%d", hostnameBestEffort(), os.Getpid(), time.Now().UnixNano())
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		leaseUntil := time.Now().UTC().Add(2 * time.Minute).Format("2006-01-02 15:04:05")
		res, err := db.ExecContext(ctx, `
			UPDATE _migration_lock
			SET owner = ?, lease_until = ?
			WHERE id = 1 AND (
				owner = '' OR owner = ? OR datetime(lease_until) < datetime('now')
			)`, owner, leaseUntil, owner)
		if err != nil {
			return fmt.Errorf("claim sqlite migration lock: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 1 {
			defer func() {
				_, _ = db.Exec(`UPDATE _migration_lock SET owner='', lease_until='1970-01-01 00:00:00' WHERE id=1 AND owner=?`, owner)
			}()
			return fn()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("acquire sqlite migration lock: timed out waiting for another migrator")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func withMySQLMigrationLock(ctx context.Context, db *sql.DB, fn func() error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire mysql migration connection: %w", err)
	}
	defer conn.Close()

	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 30)`, migrationLockName).Scan(&got); err != nil {
		return fmt.Errorf("acquire mysql migration lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return fmt.Errorf("acquire mysql migration lock: lock not granted (timeout or error)")
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT RELEASE_LOCK(?)`, migrationLockName)
	}()
	return fn()
}

func withPostgresMigrationLock(ctx context.Context, db *sql.DB, fn func() error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire postgres migration connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, openETLMigrationLockKey); err != nil {
		return fmt.Errorf("acquire postgres migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, openETLMigrationLockKey)
	}()
	return fn()
}

func hostnameBestEffort() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "host"
	}
	return h
}

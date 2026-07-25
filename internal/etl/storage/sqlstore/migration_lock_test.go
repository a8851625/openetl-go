package sqlstore_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlstore"
)

func openLockTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "lock.db") + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLiteMigrationLockSerializesCallbacks(t *testing.T) {
	db := openLockTestDB(t)
	var concurrent int32
	var maxConcurrent int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := sqlstore.WithMigrationLock(context.Background(), db, sqlstore.SQLiteDialect{}, func() error {
				cur := atomic.AddInt32(&concurrent, 1)
				for {
					old := atomic.LoadInt32(&maxConcurrent)
					if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				atomic.AddInt32(&concurrent, -1)
				return nil
			})
			if err != nil {
				t.Errorf("lock: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if maxConcurrent != 1 {
		t.Fatalf("max concurrent migrators = %d, want 1", maxConcurrent)
	}
}

func TestSQLiteMigrationLockPropagatesFailure(t *testing.T) {
	db := openLockTestDB(t)
	err := sqlstore.WithMigrationLock(context.Background(), db, sqlstore.SQLiteDialect{}, func() error {
		return context.Canceled
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	// Lock must be released for the next caller.
	if err := sqlstore.WithMigrationLock(context.Background(), db, sqlstore.SQLiteDialect{}, func() error {
		return nil
	}); err != nil {
		t.Fatalf("second lock after failure: %v", err)
	}
}

func TestSQLiteMigrationFailureDoesNotRecordSchemaVersion(t *testing.T) {
	db := openLockTestDB(t)
	// Create the version table the same way production migrations do, then
	// simulate a failed step that must not mark the version applied.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS _schema_version (
		version INTEGER PRIMARY KEY,
		description TEXT,
		applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("create version table: %v", err)
	}

	err := sqlstore.WithMigrationLock(context.Background(), db, sqlstore.SQLiteDialect{}, func() error {
		// Pretend a versioned migration fails mid-step.
		if _, err := db.Exec(`ALTER TABLE missing_table ADD COLUMN x TEXT`); err != nil {
			return err
		}
		if _, err := db.Exec(`INSERT INTO _schema_version (version, description) VALUES (99, 'should-not-record')`); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected migration failure")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM _schema_version WHERE version=99`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("failed migration recorded version row, count=%d", n)
	}
}

func TestConcurrentSQLiteStoreOpenMigratesOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "etl.db")

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			store, err := sqlite.New(path)
			if err != nil {
				errs <- err
				return
			}
			_ = store.Close()
			errs <- nil
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent open: %v", err)
		}
	}

	// Re-open once and confirm schema_version table exists and is complete enough.
	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	var n int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM _schema_version`).Scan(&n); err != nil {
		t.Fatalf("count schema versions: %v", err)
	}
	if n < 16 {
		t.Fatalf("schema versions = %d, want at least 16", n)
	}
}

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

package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStorePersistenceSnapshotRestoreAndStats(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")

	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	if err := store.Set(ctx, "pipe", "lookup", "user:1", []byte(`{"tier":"vip"}`), 0); err != nil {
		t.Fatalf("Set user:1: %v", err)
	}
	if err := store.Set(ctx, "pipe", "lookup", "user:2", []byte(`{"tier":"basic"}`), 0); err != nil {
		t.Fatalf("Set user:2: %v", err)
	}
	stats, err := store.Stats(ctx, "pipe", "lookup")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Keys != 2 || stats.Bytes == 0 || stats.UpdatedAt.IsZero() {
		t.Fatalf("Stats = %#v, want 2 keys with bytes and updated_at", stats)
	}

	snap, err := store.Snapshot(ctx, "pipe", "lookup")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Entries) != 2 || snap.Version == "" {
		t.Fatalf("Snapshot = %#v", snap)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopened.Close()
	got, ok, err := reopened.Get(ctx, "pipe", "lookup", "user:1")
	if err != nil {
		t.Fatalf("Get reopened: %v", err)
	}
	if !ok || string(got) != `{"tier":"vip"}` {
		t.Fatalf("Get reopened = %q, %v", got, ok)
	}

	restored, err := NewSQLiteStore(filepath.Join(t.TempDir(), "restored.db"))
	if err != nil {
		t.Fatalf("New restored store: %v", err)
	}
	defer restored.Close()
	if err := restored.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, ok, err = restored.Get(ctx, "pipe", "lookup", "user:2")
	if err != nil {
		t.Fatalf("Get restored: %v", err)
	}
	if !ok || string(got) != `{"tier":"basic"}` {
		t.Fatalf("Get restored = %q, %v", got, ok)
	}
}

func TestSQLiteStoreTTLAndDelete(t *testing.T) {
	ctx := context.Background()
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer store.Close()

	if err := store.Set(ctx, "pipe", "window", "bucket", []byte("1"), time.Nanosecond); err != nil {
		t.Fatalf("Set expiring: %v", err)
	}
	time.Sleep(time.Millisecond)
	_, ok, err := store.Get(ctx, "pipe", "window", "bucket")
	if err != nil {
		t.Fatalf("Get expired: %v", err)
	}
	if ok {
		t.Fatal("expired key is still visible")
	}
	stats, err := store.Stats(ctx, "pipe", "window")
	if err != nil {
		t.Fatalf("Stats after expiry: %v", err)
	}
	if stats.Keys != 0 {
		t.Fatalf("Stats after expiry = %#v, want zero keys", stats)
	}

	if err := store.Set(ctx, "pipe", "window", "live", []byte("2"), 0); err != nil {
		t.Fatalf("Set live: %v", err)
	}
	if err := store.Delete(ctx, "pipe", "window", "live"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := store.Get(ctx, "pipe", "window", "live"); err != nil || ok {
		t.Fatalf("Get deleted ok=%v err=%v", ok, err)
	}
}

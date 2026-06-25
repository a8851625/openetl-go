package state

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreSetGetSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	if err := store.Set(ctx, "pipe", "lookup", "user:1", []byte(`{"tier":"vip"}`), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok, err := store.Get(ctx, "pipe", "lookup", "user:1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(got) != `{"tier":"vip"}` {
		t.Fatalf("Get = %q, %v", got, ok)
	}

	snap, err := store.Snapshot(ctx, "pipe", "lookup")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Entries) != 1 || snap.Version == "" {
		t.Fatalf("Snapshot = %#v", snap)
	}

	restored := NewMemoryStore()
	if err := restored.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, ok, err = restored.Get(ctx, "pipe", "lookup", "user:1")
	if err != nil {
		t.Fatalf("Get restored: %v", err)
	}
	if !ok || string(got) != `{"tier":"vip"}` {
		t.Fatalf("Get restored = %q, %v", got, ok)
	}
}

func TestMemoryStoreTTL(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	if err := store.Set(ctx, "pipe", "window", "bucket", []byte("1"), time.Nanosecond); err != nil {
		t.Fatalf("Set: %v", err)
	}
	time.Sleep(time.Millisecond)
	_, ok, err := store.Get(ctx, "pipe", "window", "bucket")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatal("expired key is still visible")
	}
}

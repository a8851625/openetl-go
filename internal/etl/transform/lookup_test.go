package transform

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/state"
)

func TestLookupNormalizesNumericJoinKeys(t *testing.T) {
	tr := &LookupTransform{
		joinKey: "user_id",
		fields:  []string{"tier"},
		cache: map[string]map[string]any{
			normalizeLookupKey(int64(1001)): {"tier": "vip"},
		},
	}

	rec, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{"user_id": float64(1001)},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Data["tier"] != "vip" {
		t.Fatalf("tier = %v, want vip", rec.Data["tier"])
	}
}

func TestNormalizeSQLValueConvertsBytesToString(t *testing.T) {
	if got := normalizeSQLValue([]byte("east")); got != "east" {
		t.Fatalf("normalizeSQLValue([]byte) = %#v, want east", got)
	}
}

func TestLookupStateStorePersistsAndRestoresCache(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	seed := &LookupTransform{
		joinKey: "user_id",
		dimKey:  "id",
		fields:  []string{"tier", "region"},
		cache: map[string]map[string]any{
			normalizeLookupKey(int64(1001)): {"id": int64(1001), "tier": "vip", "region": "east"},
		},
	}
	seed.WithStateStore(store, "wide-pipe", "lookup-users", time.Hour)
	if err := seed.persistCacheToStateLocked(ctx); err != nil {
		t.Fatalf("persistCacheToStateLocked: %v", err)
	}

	restarted := &LookupTransform{
		joinKey:   "user_id",
		dimKey:    "id",
		fields:    []string{"tier", "region"},
		cache:     map[string]map[string]any{},
		refreshIv: 0,
	}
	restarted.WithStateStore(store, "wide-pipe", "lookup-users", time.Hour)

	rec, err := restarted.Apply(ctx, core.Record{
		Data: map[string]any{"user_id": float64(1001)},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Data["tier"] != "vip" || rec.Data["region"] != "east" {
		t.Fatalf("restored lookup fields missing: %#v", rec.Data)
	}
}

func TestLookupStateStoreTTLExpiryLeavesRecordUnchanged(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	seed := &LookupTransform{
		joinKey: "user_id",
		dimKey:  "id",
		fields:  []string{"tier"},
		cache: map[string]map[string]any{
			normalizeLookupKey("u-ttl"): {"id": "u-ttl", "tier": "vip"},
		},
	}
	seed.WithStateStore(store, "wide-pipe", "lookup-users", 20*time.Millisecond)
	if err := seed.persistCacheToStateLocked(ctx); err != nil {
		t.Fatalf("persistCacheToStateLocked: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	restarted := &LookupTransform{
		joinKey:   "user_id",
		dimKey:    "id",
		fields:    []string{"tier"},
		cache:     map[string]map[string]any{},
		refreshIv: 0,
	}
	restarted.WithStateStore(store, "wide-pipe", "lookup-users", 20*time.Millisecond)

	rec, err := restarted.Apply(ctx, core.Record{
		Data: map[string]any{"user_id": "u-ttl"},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := rec.Data["tier"]; ok {
		t.Fatalf("tier restored after TTL expiry: %#v", rec.Data)
	}
}

func TestLookupTransformMetricsTracksHitMissAndMissingKey(t *testing.T) {
	tr := &LookupTransform{
		joinKey: "user_id",
		fields:  []string{"tier"},
		node:    "lookup-users",
		cache: map[string]map[string]any{
			normalizeLookupKey("u-1"): {"tier": "vip"},
		},
	}

	if _, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{"user_id": "u-1"},
	}); err != nil {
		t.Fatalf("Apply hit: %v", err)
	}
	if _, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{"user_id": "missing"},
	}); err != nil {
		t.Fatalf("Apply miss: %v", err)
	}
	if _, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{"other": "u-1"},
	}); err != nil {
		t.Fatalf("Apply missing key: %v", err)
	}

	metrics := tr.TransformMetrics()
	if metrics.Node != "lookup-users" || metrics.Transform != "lookup" {
		t.Fatalf("unexpected metric identity: %#v", metrics)
	}
	assertLookupCounter(t, metrics, "processed", 3)
	assertLookupCounter(t, metrics, "hit", 1)
	assertLookupCounter(t, metrics, "miss", 2)
	assertLookupCounter(t, metrics, "missing_key", 1)
}

func TestLookupOnMissNullFillsRequestedFields(t *testing.T) {
	tr := &LookupTransform{
		joinKey: "user_id",
		fields:  []string{"tier", "region"},
		onMiss:  "null",
		cache: map[string]map[string]any{
			normalizeLookupKey("known"): {"tier": "vip", "region": "east"},
		},
	}

	rec, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{"user_id": "missing"},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Data["tier"] != nil || rec.Data["region"] != nil {
		t.Fatalf("miss fields not explicitly nulled: %#v", rec.Data)
	}

	metrics := tr.TransformMetrics()
	assertLookupCounter(t, metrics, "miss", 1)
	assertLookupCounter(t, metrics, "miss_null", 1)
}

func TestLookupOnMissDLQReturnsError(t *testing.T) {
	tr := &LookupTransform{
		joinKey: "user_id",
		fields:  []string{"tier"},
		onMiss:  "dlq",
		cache: map[string]map[string]any{
			normalizeLookupKey("known"): {"tier": "vip"},
		},
	}

	_, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{"user_id": "missing"},
	})
	if err == nil || !strings.Contains(err.Error(), "no dimension match") {
		t.Fatalf("Apply err = %v, want dimension miss error", err)
	}

	metrics := tr.TransformMetrics()
	assertLookupCounter(t, metrics, "miss", 1)
	assertLookupCounter(t, metrics, "miss_dlq", 1)
}

func TestLookupOnRefreshErrorCanRouteToDLQ(t *testing.T) {
	tr := &LookupTransform{
		joinKey:   "user_id",
		fields:    []string{"tier"},
		onRefresh: "error",
		cache:     map[string]map[string]any{},
		db:        nil,
	}

	_, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{"user_id": "u-1"},
	})
	if err == nil || !strings.Contains(err.Error(), "refresh failed") {
		t.Fatalf("Apply err = %v, want refresh failed error", err)
	}

	metrics := tr.TransformMetrics()
	assertLookupCounter(t, metrics, "refresh_error", 1)
	assertLookupCounter(t, metrics, "refresh_error_dlq", 1)
}

func TestLookupStateRestoreMetricsTracksExternalFailureRecovery(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	seed := &LookupTransform{
		joinKey: "user_id",
		dimKey:  "id",
		fields:  []string{"tier"},
		cache: map[string]map[string]any{
			normalizeLookupKey("u-restore"): {"id": "u-restore", "tier": "vip"},
		},
	}
	seed.WithStateStore(store, "wide-pipe", "lookup-users", time.Hour)
	if err := seed.persistCacheToStateLocked(ctx); err != nil {
		t.Fatalf("persistCacheToStateLocked: %v", err)
	}

	restarted := &LookupTransform{
		joinKey: "user_id",
		dimKey:  "id",
		fields:  []string{"tier"},
		cache:   map[string]map[string]any{},
		db:      nil,
	}
	restarted.WithStateStore(store, "wide-pipe", "lookup-users", time.Hour)

	rec, err := restarted.Apply(ctx, core.Record{
		Data: map[string]any{"user_id": "u-restore"},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Data["tier"] != "vip" {
		t.Fatalf("restored lookup field missing: %#v", rec.Data)
	}

	metrics := restarted.TransformMetrics()
	assertLookupCounter(t, metrics, "processed", 1)
	assertLookupCounter(t, metrics, "hit", 1)
	assertLookupCounter(t, metrics, "restore_success", 1)
	assertLookupCounter(t, metrics, "refresh_error", 0)
}

func TestLookupStateRestoreRespectsMaxCacheEntries(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	seed := &LookupTransform{
		joinKey: "user_id",
		dimKey:  "id",
		fields:  []string{"tier"},
		cache: map[string]map[string]any{
			normalizeLookupKey("u-1"): {"id": "u-1", "tier": "vip"},
			normalizeLookupKey("u-2"): {"id": "u-2", "tier": "standard"},
		},
	}
	seed.WithStateStore(store, "wide-pipe", "lookup-users", time.Hour)
	if err := seed.persistCacheToStateLocked(ctx); err != nil {
		t.Fatalf("persistCacheToStateLocked: %v", err)
	}

	restarted := &LookupTransform{
		joinKey:  "user_id",
		dimKey:   "id",
		fields:   []string{"tier"},
		cache:    map[string]map[string]any{},
		maxCache: 1,
	}
	restarted.WithStateStore(store, "wide-pipe", "lookup-users", time.Hour)

	if _, err := restarted.restoreCacheFromStateLocked(ctx); err == nil {
		t.Fatal("restoreCacheFromStateLocked succeeded, want max cache error")
	}
	assertLookupCounter(t, restarted.TransformMetrics(), "cache_limit_exceeded", 1)
}

func assertLookupCounter(t *testing.T, metrics core.TransformMetrics, name string, want int64) {
	t.Helper()
	if got := metrics.Counters[name]; got != want {
		t.Fatalf("counter %s = %d, want %d (metrics=%#v)", name, got, want, metrics.Counters)
	}
}

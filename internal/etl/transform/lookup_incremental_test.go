package transform

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

var errLookupBoom = errors.New("dimension database unavailable")

func newIncrementalLookup(t *testing.T) (*LookupTransform, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	// Construct the transform directly: NewLookupTransform opens and pings a real
	// DSN, which a unit test must not require.
	tr := &LookupTransform{
		mode:              "cache",
		query:             "SELECT id, name, updated_at FROM dim_users",
		incrementalColumn: "updated_at",
		incrementalQuery:  "SELECT id, name, updated_at FROM dim_users WHERE updated_at > ?",
		incrementalDelete: "is_deleted",
		joinKey:           "user_id",
		dimKey:            "id",
		fields:            []string{"name"},
		refreshIv:         time.Second,
		onMiss:            "pass",
		onRefresh:         "pass",
		timeout:           5 * time.Second,
		cache:             make(map[string]map[string]any),
		pipeline:          "default",
		node:              "lookup",
		db:                db,
	}
	return tr, mock, db
}

func TestLookupIncrementalConfigValidation(t *testing.T) {
	// No DSN on purpose: NewLookupTransform opens and pings a real database, and
	// the config contract must be rejected before that.
	base := map[string]any{
		"query":  "SELECT id, name FROM dim_users",
		"fields": []any{"name"},
	}
	with := func(extra map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	if _, err := NewLookupTransform(with(map[string]any{"incremental_column": "updated_at"})); err == nil ||
		!strings.Contains(err.Error(), "incremental_query") {
		t.Fatalf("err = %v, want incremental_query requirement", err)
	}
	if _, err := NewLookupTransform(with(map[string]any{"incremental_query": "SELECT 1 WHERE updated_at > ?"})); err == nil ||
		!strings.Contains(err.Error(), "incremental_column") {
		t.Fatalf("err = %v, want incremental_column requirement", err)
	}
	if _, err := NewLookupTransform(with(map[string]any{"incremental_column": "updated_at", "incremental_delete_column": "is_deleted"})); err == nil ||
		!strings.Contains(err.Error(), "incremental_query") {
		t.Fatalf("err = %v, want incremental_query requirement for delete column", err)
	}
	// A fully valid incremental config passes the constructor's config gate; the
	// DSN/connection checks that follow are covered by the spec validator test
	// (internal/etl/pipeline) so this unit test needs no database.
	if _, err := NewLookupTransform(with(map[string]any{
		"incremental_column":        "updated_at",
		"incremental_query":         "SELECT id, name, updated_at FROM dim_users WHERE updated_at > ?",
		"incremental_delete_column": "is_deleted",
		"full_refresh_interval_sec": 3600,
	})); err == nil || strings.Contains(err.Error(), "incremental") {
		t.Fatalf("valid incremental config must not be rejected for its incremental fields: %v", err)
	}
}

func TestLookupIncrementalRefreshMergesOnlyNewRows(t *testing.T) {
	tr, mock, handle := newIncrementalLookup(t)
	defer handle.Close()
	ctx := context.Background()

	// First refresh is a full load.
	mock.ExpectQuery("SELECT id, name, updated_at FROM dim_users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at", "is_deleted"}).
			AddRow(int64(1), "alice", "2026-01-01 00:00:00", int64(0)).
			AddRow(int64(2), "bob", "2026-01-01 00:00:01", int64(0)))
	if err := tr.loadCache(ctx); err != nil {
		t.Fatalf("initial loadCache: %v", err)
	}
	if got := tr.watermark; got != "2026-01-01 00:00:01" {
		t.Fatalf("watermark = %#v, want the max updated_at", got)
	}

	// Second refresh must ask only for newer rows and merge them.
	mock.ExpectQuery("SELECT id, name, updated_at FROM dim_users WHERE updated_at > ?").
		WithArgs("2026-01-01 00:00:01").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at", "is_deleted"}).
			AddRow(int64(2), "bob-updated", "2026-01-01 00:00:05", int64(0)).
			AddRow(int64(3), "carol", "2026-01-01 00:00:06", int64(0)))
	tr.lastRefresh = time.Now().Add(-2 * time.Second)
	if err := tr.loadCache(ctx); err != nil {
		t.Fatalf("incremental loadCache: %v", err)
	}
	if len(tr.cache) != 3 {
		t.Fatalf("cache size = %d, want 3 (2 existing + 1 new)", len(tr.cache))
	}
	if got := tr.cache[normalizeLookupKey(int64(2))]["name"]; got != "bob-updated" {
		t.Fatalf("row 2 name = %v, want bob-updated (merged)", got)
	}
	if got := tr.watermark; got != "2026-01-01 00:00:06" {
		t.Fatalf("watermark = %#v, want the newest marker", got)
	}
	metrics := tr.TransformMetrics().Counters
	if metrics["incremental_merge"] != 1 || metrics["incremental_rows"] != 2 {
		t.Fatalf("incremental metrics = %v", metrics)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestLookupIncrementalSoftDeleteRemovesRow(t *testing.T) {
	tr, mock, handle := newIncrementalLookup(t)
	defer handle.Close()
	ctx := context.Background()
	mock.ExpectQuery("SELECT id, name, updated_at FROM dim_users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at", "is_deleted"}).
			AddRow(int64(1), "alice", "2026-01-01 00:00:00", int64(0)).
			AddRow(int64(2), "bob", "2026-01-01 00:00:00", int64(0)))
	if err := tr.loadCache(ctx); err != nil {
		t.Fatalf("initial loadCache: %v", err)
	}
	mock.ExpectQuery("WHERE updated_at > ?").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at", "is_deleted"}).
			AddRow(int64(2), "bob", "2026-01-01 00:00:09", int64(1)))
	tr.lastRefresh = time.Now().Add(-2 * time.Second)
	if err := tr.loadCache(ctx); err != nil {
		t.Fatalf("incremental loadCache: %v", err)
	}
	if _, ok := tr.cache[normalizeLookupKey(int64(2))]; ok {
		t.Fatal("soft-deleted dimension row must be removed from the cache")
	}
	if len(tr.cache) != 1 {
		t.Fatalf("cache size = %d, want 1", len(tr.cache))
	}
	if got := tr.TransformMetrics().Counters["incremental_deleted"]; got != 1 {
		t.Fatalf("incremental_deleted = %d, want 1", got)
	}
}

func TestLookupIncrementalFailureFallsBackToFullReload(t *testing.T) {
	tr, mock, handle := newIncrementalLookup(t)
	defer handle.Close()
	ctx := context.Background()
	mock.ExpectQuery("SELECT id, name, updated_at FROM dim_users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at", "is_deleted"}).
			AddRow(int64(1), "alice", "2026-01-01 00:00:00", int64(0)))
	if err := tr.loadCache(ctx); err != nil {
		t.Fatalf("initial loadCache: %v", err)
	}
	// Incremental query fails -> the cache must be rebuilt from the full query
	// instead of being left partially updated.
	mock.ExpectQuery("WHERE updated_at > ?").WillReturnError(errLookupBoom)
	mock.ExpectQuery("SELECT id, name, updated_at FROM dim_users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "updated_at", "is_deleted"}).
			AddRow(int64(1), "alice", "2026-01-01 00:00:00", int64(0)).
			AddRow(int64(9), "zoe", "2026-01-01 00:00:10", int64(0)))
	tr.lastRefresh = time.Now().Add(-2 * time.Second)
	if err := tr.loadCache(ctx); err != nil {
		t.Fatalf("fallback loadCache: %v", err)
	}
	if len(tr.cache) != 2 {
		t.Fatalf("cache size = %d, want 2 after the fallback full reload", len(tr.cache))
	}
	if got := tr.TransformMetrics().Counters["incremental_skips"]; got != 1 {
		t.Fatalf("incremental_skips = %d, want 1", got)
	}
}

func TestLookupIncrementalRequiresMarkerColumn(t *testing.T) {
	tr, mock, handle := newIncrementalLookup(t)
	defer handle.Close()
	ctx := context.Background()
	// The full query does not project updated_at -> the watermark cannot be
	// derived and the transform must fail loudly instead of never refreshing.
	mock.ExpectQuery("SELECT id, name, updated_at FROM dim_users").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "alice"))
	if err := tr.loadCache(ctx); err == nil {
		t.Fatal("loadCache must fail when the query does not select the incremental column")
	}
}

func TestLookupMonotonicGreater(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		a, b any
		want bool
	}{
		{base.Add(time.Second), base, true},
		{base, base, false},
		{base, base.Add(time.Second), false},
		{int64(5), int64(4), true},
		{int64(4), int64(5), false},
		{float64(5), int64(4), true},
		{"b", "a", true},
		{"a", "b", false},
		{nil, int64(1), false},
		{int64(1), nil, true},
	}
	for _, tc := range cases {
		if got := lookupMonotonicGreater(tc.a, tc.b); got != tc.want {
			t.Fatalf("lookupMonotonicGreater(%v,%v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestLookupTruthy(t *testing.T) {
	for _, v := range []any{true, int64(1), "1", "true", "YES", []byte("1")} {
		if !lookupTruthy(v) {
			t.Fatalf("lookupTruthy(%#v) = false, want true", v)
		}
	}
	for _, v := range []any{nil, false, int64(0), "0", "no", ""} {
		if lookupTruthy(v) {
			t.Fatalf("lookupTruthy(%#v) = true, want false", v)
		}
	}
}

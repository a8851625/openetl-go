package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/checkpoint"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func mustSnapshotPosition(t *testing.T, pos map[string]any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(pos)
	if err != nil {
		t.Fatalf("marshal position: %v", err)
	}
	return raw
}

func decodePosition(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	if env, wrapped, err := checkpoint.ParseEnvelope(raw); err == nil && wrapped {
		raw = env.Source
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode position: %v", err)
	}
	return out
}

func TestResyncSnapshotTablesClearsOnlyRequestedCursors(t *testing.T) {
	position := mustSnapshotPosition(t, map[string]any{
		"phase":   "cdc",
		"last_id": float64(500),
		"last_ids": map[string]any{
			"orders":    float64(500),
			"customers": float64(900),
		},
		"last_strs": map[string]any{
			"audit_log": "2026-01-01 00:00:00",
		},
		"file": "mysql-bin.000042",
		"pos":  float64(123456),
	})

	resynced, details, err := resyncSnapshotTables(position, []string{"orders"})
	if err != nil {
		t.Fatalf("resyncSnapshotTables: %v", err)
	}
	pos := decodePosition(t, resynced)
	if pos["phase"] != "snapshot" {
		t.Fatalf("phase = %v, want snapshot", pos["phase"])
	}
	ids, ok := pos["last_ids"].(map[string]any)
	if !ok {
		t.Fatalf("last_ids missing: %v", pos)
	}
	if _, stillThere := ids["orders"]; stillThere {
		t.Fatal("orders cursor must be cleared")
	}
	if ids["customers"] != float64(900) {
		t.Fatalf("customers cursor = %v, want 900 (untouched)", ids["customers"])
	}
	if strs, ok := pos["last_strs"].(map[string]any); !ok || strs["audit_log"] != "2026-01-01 00:00:00" {
		t.Fatalf("last_strs must be preserved: %v", pos["last_strs"])
	}
	if pos["file"] != "mysql-bin.000042" || pos["pos"] != float64(123456) {
		t.Fatalf("binlog anchor must be preserved: %v/%v", pos["file"], pos["pos"])
	}
	if pos["last_id"] != float64(0) {
		t.Fatalf("legacy last_id must be cleared so single-table specs re-snapshot: %v", pos["last_id"])
	}
	if details.Phase != "snapshot" || details.BinlogAnchor != "mysql-bin.000042:123456" {
		t.Fatalf("details = %+v", details)
	}
	if len(details.ClearedKeys) != 1 || details.ClearedKeys[0] != "last_ids.orders" {
		t.Fatalf("cleared keys = %v", details.ClearedKeys)
	}
}

func TestResyncSnapshotTablesDropsEmptyCursorMaps(t *testing.T) {
	position := mustSnapshotPosition(t, map[string]any{
		"phase":    "cdc",
		"last_ids": map[string]any{"orders": float64(10)},
		"file":     "mysql-bin.000001",
		"pos":      float64(4),
	})
	resynced, _, err := resyncSnapshotTables(position, []string{"orders"})
	if err != nil {
		t.Fatalf("resyncSnapshotTables: %v", err)
	}
	pos := decodePosition(t, resynced)
	if _, exists := pos["last_ids"]; exists {
		t.Fatalf("an emptied cursor map must be dropped, got %v", pos["last_ids"])
	}
}

func TestResyncSnapshotTablesPreservesEnvelope(t *testing.T) {
	inner := mustSnapshotPosition(t, map[string]any{
		"phase":    "cdc",
		"last_ids": map[string]any{"orders": float64(10), "items": float64(20)},
		"file":     "mysql-bin.000007",
		"pos":      float64(99),
	})
	enveloped, err := checkpoint.BuildEnvelope(inner, map[string]string{"window:agg": "3"}, map[string]any{"sink": "clickhouse", "acknowledged": true})
	if err != nil {
		t.Fatalf("BuildEnvelope: %v", err)
	}
	resynced, _, err := resyncSnapshotTables(enveloped, []string{"items"})
	if err != nil {
		t.Fatalf("resyncSnapshotTables: %v", err)
	}
	env, wrapped, err := checkpoint.ParseEnvelope(resynced)
	if err != nil || !wrapped {
		t.Fatalf("resynced position must stay enveloped: wrapped=%v err=%v", wrapped, err)
	}
	if env.State["window:agg"] != "3" {
		t.Fatalf("state versions must be preserved: %v", env.State)
	}
	if env.SinkCommit["sink"] != "clickhouse" {
		t.Fatalf("sink commit metadata must be preserved: %v", env.SinkCommit)
	}
	pos := decodePosition(t, resynced)
	ids := pos["last_ids"].(map[string]any)
	if ids["orders"] != float64(10) {
		t.Fatalf("orders must keep its cursor: %v", ids)
	}
	if _, ok := ids["items"]; ok {
		t.Fatal("items cursor must be cleared")
	}
}

func TestResyncSnapshotTablesRejectsUnusablePositions(t *testing.T) {
	cases := []struct {
		name     string
		position json.RawMessage
		tables   []string
		want     string
	}{
		{name: "empty position", position: nil, tables: []string{"orders"}, want: "no checkpoint"},
		{name: "not json", position: json.RawMessage("not-json"), tables: []string{"orders"}, want: "not valid JSON"},
		{name: "no tables", position: mustSnapshotPosition(t, map[string]any{"phase": "cdc", "file": "f", "pos": float64(1)}), want: "at least one table"},
		{
			name:     "missing anchor",
			position: mustSnapshotPosition(t, map[string]any{"phase": "snapshot", "file": "", "pos": float64(0)}),
			tables:   []string{"orders"},
			want:     "binlog anchor",
		},
		{
			name:     "not a snapshot position",
			position: mustSnapshotPosition(t, map[string]any{"topic": "orders", "partition": float64(0)}),
			tables:   []string{"orders"},
			want:     "mysql_snapshot_cdc",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := resyncSnapshotTables(tc.position, tc.tables)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestResyncConfiguredTables(t *testing.T) {
	spec := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_snapshot_cdc", Config: map[string]any{
			"database": "app",
			"tables":   []any{"orders", "customers"},
		}},
	}
	tables, err := resyncConfiguredTables(spec, resyncRequest{Table: "orders"})
	if err != nil || len(tables) != 1 || tables[0] != "orders" {
		t.Fatalf("tables = %v err = %v", tables, err)
	}
	// Duplicates collapse, and both body shapes combine.
	tables, err = resyncConfiguredTables(spec, resyncRequest{Table: "orders", Tables: []string{"orders", "customers", " "}})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %v", tables)
	}
	if _, err := resyncConfiguredTables(spec, resyncRequest{}); err == nil {
		t.Fatal("empty request must fail")
	}
	if _, err := resyncConfiguredTables(spec, resyncRequest{Table: "not_configured"}); err == nil ||
		!strings.Contains(err.Error(), "not part of this pipeline") {
		t.Fatalf("err = %v", err)
	}
}

func TestResyncHTTPSurface(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	snapshotSpec := &pipeline.Spec{
		Name:   "resync-target",
		Source: pipeline.SourceSpec{Type: "mysql_snapshot_cdc", Config: map[string]any{"database": "app", "tables": []any{"orders", "customers"}}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "orders", "batch_mode": "upsert"}},
	}
	cdcSpec := &pipeline.Spec{
		Name:   "resync-plain-cdc",
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"database": "app", "tables": []any{"orders"}}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "orders"}},
	}
	s.mu.Lock()
	s.specs["resync-target"] = snapshotSpec
	s.specs["resync-plain-cdc"] = cdcSpec
	s.mu.Unlock()

	// Wrong source type is rejected with an actionable message.
	resp, err := http.Post(ts.URL+"/api/v2/pipelines/resync-plain-cdc/resync", "application/json", strings.NewReader(`{"table":"orders"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("plain cdc status = %d, want 400", resp.StatusCode)
	}

	// Without a checkpoint the request explains what to do.
	resp2, err := http.Post(ts.URL+"/api/v2/pipelines/resync-target/resync", "application/json", strings.NewReader(`{"table":"orders"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("no-checkpoint status = %d, want 400", resp2.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg, _ := body["error"].(string); !strings.Contains(msg, "no checkpoint") {
		t.Fatalf("error = %q", msg)
	}

	// A GET is rejected: resync mutates the checkpoint.
	getResp, err := http.Get(ts.URL + "/api/v2/pipelines/resync-target/resync")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", getResp.StatusCode)
	}
}

func TestResyncRewritesPersistedCheckpoint(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := &pipeline.Spec{
		Name:   "resync-persist",
		Source: pipeline.SourceSpec{Type: "mysql_snapshot_cdc", Config: map[string]any{"database": "app", "tables": []any{"orders", "customers"}}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"table": "orders", "batch_mode": "upsert"}},
	}
	s.mu.Lock()
	s.specs["resync-persist"] = spec
	s.mu.Unlock()

	inner := mustSnapshotPosition(t, map[string]any{
		"phase": "cdc",
		"last_ids": map[string]any{
			"orders":    float64(120),
			"customers": float64(340),
		},
		"file": "mysql-bin.000010",
		"pos":  float64(4242),
	})
	enveloped, err := checkpoint.BuildEnvelope(inner, nil, nil)
	if err != nil {
		t.Fatalf("BuildEnvelope: %v", err)
	}
	if err := s.store.SavePipeline(t.Context(), &storage.PipelineRow{ID: "resync-persist", Name: "resync-persist", SpecYAML: "name: resync-persist", Status: "created", DesiredState: "stopped", ObservedState: "stopped"}); err != nil {
		t.Fatalf("seed pipeline: %v", err)
	}
	if _, err := s.lifecycleStore.SetPipelineCheckpoint(t.Context(), "resync-persist", &storage.CheckpointRecord{JobName: "resync-persist", Source: "mysql_snapshot_cdc", Position: enveloped, Timestamp: time.Now()}); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	resp, err := http.Post(ts.URL+"/api/v2/pipelines/resync-persist/resync", "application/json", strings.NewReader(`{"tables":["orders"]}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "resync_scheduled" {
		t.Fatalf("status = %v", body["status"])
	}
	semantics, ok := body["semantics"].(map[string]any)
	if !ok || semantics["sink_requirement"] == "" {
		t.Fatalf("semantics missing: %v", body["semantics"])
	}

	// The persisted checkpoint must now be a snapshot-phase position with only
	// the requested table's cursor cleared.
	stored, err := s.cpAdapter.Load(t.Context(), "resync-persist")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	pos := decodePosition(t, stored.Position)
	if pos["phase"] != "snapshot" {
		t.Fatalf("persisted phase = %v", pos["phase"])
	}
	ids := pos["last_ids"].(map[string]any)
	if _, ok := ids["orders"]; ok {
		t.Fatal("orders cursor must be cleared in the persisted checkpoint")
	}
	if ids["customers"] != float64(340) {
		t.Fatalf("customers cursor = %v", ids["customers"])
	}
}

package server

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// checkIDs collects the preflight check ids for assertions.
func preflightChecks(result *PreflightResult) map[string]PreflightIssue {
	byCheck := map[string]PreflightIssue{}
	for _, issue := range result.Issues {
		byCheck[issue.Check] = issue
	}
	return byCheck
}

func dorisSourceOrderSpec(sourceType string, sourceCfg map[string]any, sinkCfg map[string]any, transforms []pipeline.TransformSpec) *pipeline.Spec {
	return &pipeline.Spec{
		Name:       "doris-source-order-gate",
		Source:     pipeline.SourceSpec{Type: sourceType, Config: sourceCfg},
		Sink:       pipeline.SinkSpec{Type: "doris", Config: sinkCfg},
		Transforms: transforms,
	}
}

func baseDorisSourceOrderSink() map[string]any {
	return map[string]any{
		"host":         "127.0.0.1",
		"port":         9030,
		"http_port":    8030,
		"database":     "ods",
		"table":        "orders",
		"write_mode":   "stream_load",
		"batch_mode":   "upsert",
		"pk_columns":   []any{"order_id"},
		"version_mode": "source_order",
	}
}

func TestPreflightDorisSourceOrderGate(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	cdcSource := func() map[string]any {
		return map[string]any{
			"host":      "127.0.0.1",
			"port":      3306,
			"user":      "sync",
			"password":  "sync",
			"database":  "app",
			"server_id": 5001,
			"tables":    []any{"orders"},
		}
	}

	t.Run("cdc source is accepted", func(t *testing.T) {
		spec := dorisSourceOrderSpec("mysql_cdc", cdcSource(), baseDorisSourceOrderSink(), nil)
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		for id := range checks {
			if id == "doris-source-order-capability" || id == "doris-source-order-transform" ||
				id == "doris-sink-source-order-write-mode" || id == "doris-sink-source-order-batch-mode" ||
				id == "doris-sink-sequence-column" || id == "doris-sink-version-mode" {
				t.Fatalf("unexpected source_order issue %q: %+v", id, checks[id])
			}
		}
	})

	t.Run("source without durable order is rejected", func(t *testing.T) {
		spec := dorisSourceOrderSpec("file", map[string]any{"path": "/tmp/x.jsonl"}, baseDorisSourceOrderSink(), nil)
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		issue, ok := checks["doris-source-order-capability"]
		if !ok {
			t.Fatalf("missing doris-source-order-capability: %#v", checks)
		}
		if issue.Level != "error" {
			t.Fatalf("level = %q, want error", issue.Level)
		}
		if issue.Message == "" || issue.Remediation == "" {
			t.Fatalf("issue must explain itself: %+v", issue)
		}
	})

	t.Run("window transform is rejected", func(t *testing.T) {
		spec := dorisSourceOrderSpec("mysql_cdc", cdcSource(), baseDorisSourceOrderSink(), []pipeline.TransformSpec{{
			Type:   "window",
			Config: map[string]any{"window_type": "tumbling", "size_sec": 60, "aggregates": []any{}},
		}})
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		if _, ok := checks["doris-source-order-transform"]; !ok {
			t.Fatalf("missing doris-source-order-transform: %#v", checks)
		}
	})

	t.Run("insert write mode is rejected", func(t *testing.T) {
		cfg := baseDorisSourceOrderSink()
		cfg["write_mode"] = "insert"
		spec := dorisSourceOrderSpec("mysql_cdc", cdcSource(), cfg, nil)
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		if _, ok := checks["doris-sink-source-order-write-mode"]; !ok {
			t.Fatalf("missing doris-sink-source-order-write-mode: %#v", checks)
		}
	})

	t.Run("insert batch mode is rejected", func(t *testing.T) {
		cfg := baseDorisSourceOrderSink()
		cfg["batch_mode"] = "insert"
		spec := dorisSourceOrderSpec("mysql_cdc", cdcSource(), cfg, nil)
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		if _, ok := checks["doris-sink-source-order-batch-mode"]; !ok {
			t.Fatalf("missing doris-sink-source-order-batch-mode: %#v", checks)
		}
	})

	t.Run("sequence column colliding with the key is rejected", func(t *testing.T) {
		cfg := baseDorisSourceOrderSink()
		cfg["sequence_column"] = "order_id"
		spec := dorisSourceOrderSpec("mysql_cdc", cdcSource(), cfg, nil)
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		if _, ok := checks["doris-sink-sequence-column"]; !ok {
			t.Fatalf("missing doris-sink-sequence-column: %#v", checks)
		}
	})

	t.Run("reserved delete-sign column is rejected", func(t *testing.T) {
		cfg := baseDorisSourceOrderSink()
		cfg["sequence_column"] = "__DORIS_DELETE_SIGN"
		spec := dorisSourceOrderSpec("mysql_cdc", cdcSource(), cfg, nil)
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		if _, ok := checks["doris-sink-sequence-column"]; !ok {
			t.Fatalf("missing doris-sink-sequence-column: %#v", checks)
		}
	})

	t.Run("unsupported version mode is rejected", func(t *testing.T) {
		cfg := baseDorisSourceOrderSink()
		cfg["version_mode"] = "sequence"
		spec := dorisSourceOrderSpec("mysql_cdc", cdcSource(), cfg, nil)
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		if _, ok := checks["doris-sink-version-mode"]; !ok {
			t.Fatalf("missing doris-sink-version-mode: %#v", checks)
		}
	})

	t.Run("clickhouse gate still fires", func(t *testing.T) {
		spec := &pipeline.Spec{
			Name:   "ch-source-order-gate",
			Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "/tmp/x.jsonl"}},
			Sink: pipeline.SinkSpec{Type: "clickhouse", Config: map[string]any{
				"host": "127.0.0.1", "database": "ods", "table": "orders",
				"pk_columns": []any{"order_id"}, "version_mode": "source_order",
			}},
		}
		pipeline.ApplyDefaults(spec)
		checks := preflightChecks(s.RunPreflight(context.Background(), spec))
		if _, ok := checks["clickhouse-source-order-capability"]; !ok {
			t.Fatalf("missing clickhouse-source-order-capability: %#v", checks)
		}
	})
}

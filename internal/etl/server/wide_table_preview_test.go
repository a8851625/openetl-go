package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestWideTablePreviewEndpoint(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "wide-preview",
		Source: pipeline.SourceSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers": []string{"localhost:9092"},
				"topic":   "orders.cdc",
			},
		},
		Transforms: []pipeline.TransformSpec{
			{Type: "normalize_envelope", Config: map[string]any{"keep_metadata": true}},
			{Type: "lookup", Config: map[string]any{
				"dsn":      "root:root@tcp(mysql:3306)/app",
				"query":    "SELECT id, tier, region FROM dim_users",
				"join_key": "user_id",
				"dim_key":  "id",
				"fields":   []string{"tier", "region"},
				"on_miss":  "null",
			}},
			{Type: "window", Config: map[string]any{
				"window_type":         "tumbling",
				"window_size_seconds": 60,
				"group_by":            []string{"region", "tier"},
				"aggregates": map[string]any{
					"order_count":  map[string]any{"func": "count"},
					"total_amount": map[string]any{"func": "sum", "field": "amount"},
				},
			}},
		},
		Sink: pipeline.SinkSpec{
			Type: "clickhouse",
			Config: map[string]any{
				"database":       "wide",
				"table":          "order_minute_aggregate",
				"pk_columns":     []string{"window_start", "region", "tier"},
				"version_column": "_version",
				"auto_create":    true,
				"schema_drift":   "add_columns",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	body, err := json.Marshal(map[string]any{
		"spec": spec,
		"samples": []map[string]any{
			{
				"payload": map[string]any{
					"op":    "c",
					"ts_ms": float64(1710000000123),
					"source": map[string]any{
						"table": "orders",
					},
					"after": map[string]any{
						"id":      float64(10001),
						"user_id": float64(1001),
						"amount":  float64(12.5),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	resp, err := http.Post(ts.URL+"/api/v2/wide-table/preview", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST preview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var got wideTablePreviewResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Valid {
		t.Fatalf("preview valid = false, errors=%v", got.Errors)
	}
	if got.Envelope["present"] != true {
		t.Fatalf("envelope preview = %#v", got.Envelope)
	}
	if len(got.Lookups) != 1 {
		t.Fatalf("lookups = %#v", got.Lookups)
	}
	if got.Lookups[0]["on_miss"] != "null" {
		t.Fatalf("lookup on_miss = %#v, want null", got.Lookups[0]["on_miss"])
	}
	if got.Window == nil || got.Window["type"] != "tumbling" {
		t.Fatalf("window = %#v", got.Window)
	}
	if !strings.Contains(got.ProposedDDL, "ReplacingMergeTree") || !strings.Contains(got.ProposedDDL, "order_minute_aggregate") {
		t.Fatalf("ProposedDDL = %s", got.ProposedDDL)
	}
	if got.FieldTypes["order_count"] != "UInt64" {
		t.Fatalf("order_count type = %q, want UInt64", got.FieldTypes["order_count"])
	}
	if len(got.Sample) != 1 {
		t.Fatalf("sample = %#v", got.Sample)
	}
	data, _ := got.Sample[0]["data"].(map[string]any)
	if data["_source_table"] != "orders" {
		t.Fatalf("normalized sample data = %#v", data)
	}
}

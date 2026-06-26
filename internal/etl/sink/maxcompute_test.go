package sink

import (
	"context"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestNewMaxComputeSinkValidatesRequiredConfig(t *testing.T) {
	_, err := NewMaxComputeSink(map[string]any{
		"endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
		"project":  "warehouse",
		"table":    "ods_events",
	})
	if err == nil {
		t.Fatal("NewMaxComputeSink() = nil error, want missing credential error")
	}
	if !strings.Contains(err.Error(), "access_key_id") || !strings.Contains(err.Error(), "access_key_secret") {
		t.Fatalf("error = %v, want missing access key fields", err)
	}
}

func TestMaxComputeSinkParsesPartitionAndValidatesSchema(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"columns": map[string]any{
			"id":         "BIGINT",
			"amount":     "DOUBLE",
			"event_time": "TIMESTAMP",
			"payload":    "STRING",
		},
		"partition":        map[string]any{},
		"partition_fields": []any{"dt"},
	}))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	schema := core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "bigint"},
		{Name: "amount", DataType: "decimal(10,2)"},
		{Name: "event_time", DataType: "datetime"},
		{Name: "payload", DataType: "json"},
		{Name: "dt", DataType: "varchar(10)"},
	}}
	if err := s.ValidateSchema(context.Background(), schema); err != nil {
		t.Fatalf("ValidateSchema() = %v", err)
	}
}

func TestMaxComputeSinkRejectsMissingDynamicPartitionField(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"columns":          map[string]string{"id": "BIGINT"},
		"partition":        map[string]any{},
		"partition_fields": []string{"dt"},
	}))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	err = s.ValidateSchema(context.Background(), core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "bigint"},
	}})
	if err == nil {
		t.Fatal("ValidateSchema() = nil, want partition field error")
	}
	if !strings.Contains(err.Error(), "partition field(s) [dt] missing") {
		t.Fatalf("error = %v, want missing dt partition field", err)
	}
}

func TestMaxComputeSinkRejectsUnsupportedColumnType(t *testing.T) {
	_, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"columns": map[string]string{"payload": "ARRAY<STRING>"},
	}))
	if err == nil {
		t.Fatal("NewMaxComputeSink() = nil, want unsupported type error")
	}
	if !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("error = %v, want unsupported type", err)
	}
}

func TestMaxComputeSinkOpenIsExplicitlyExperimental(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(nil))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	err = s.Open(context.Background())
	if err == nil {
		t.Fatal("Open() = nil, want explicit experimental error")
	}
	if !strings.Contains(err.Error(), "experimental") || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("Open() error = %v, want explicit experimental/not enabled message", err)
	}
}

func TestODPSSinkAliasIsRegistered(t *testing.T) {
	built, err := registry.BuildSink("odps", validMaxComputeConfig(nil))
	if err != nil {
		t.Fatalf("BuildSink(odps) = %v", err)
	}
	if got := built.Name(); got != "odps" {
		t.Fatalf("built.Name() = %q, want odps", got)
	}
}

func validMaxComputeConfig(extra map[string]any) map[string]any {
	cfg := map[string]any{
		"endpoint":          "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
		"project":           "warehouse",
		"table":             "ods_events",
		"access_key_id":     "ak",
		"access_key_secret": "secret",
		"partition":         map[string]any{"dt": "2026-06-26"},
	}
	for k, v := range extra {
		cfg[k] = v
	}
	return cfg
}

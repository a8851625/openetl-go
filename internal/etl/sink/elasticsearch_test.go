package sink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestElasticsearchWriteReturnsMarshalErrors(t *testing.T) {
	s, err := NewElasticsearchSink(map[string]any{
		"hosts": []interface{}{"http://127.0.0.1:1"},
		"index": "orders",
	})
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}

	err = s.Write(context.Background(), []core.Record{
		{
			Data: map[string]any{
				"id":  "order-1",
				"bad": make(chan int),
			},
		},
	})
	if err == nil {
		t.Fatal("Write() = nil error, want document marshal error")
	}
	if !strings.Contains(err.Error(), "elasticsearch marshal document") {
		t.Fatalf("Write() error = %v, want elasticsearch marshal document", err)
	}
}

func TestNewElasticsearchSinkValidatesConfig(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
		want   string
	}{
		{
			name:   "missing hosts",
			config: map[string]any{"index": "orders"},
			want:   "hosts are required",
		},
		{
			name:   "missing index",
			config: map[string]any{"hosts": []string{"http://127.0.0.1:9200"}},
			want:   "index is required",
		},
		{
			name: "invalid chunk size",
			config: map[string]any{
				"hosts":      []string{"http://127.0.0.1:9200"},
				"index":      "orders",
				"chunk_size": 0,
			},
			want: "chunk_size must be > 0",
		},
		{
			name: "invalid max retries",
			config: map[string]any{
				"hosts":       []string{"http://127.0.0.1:9200"},
				"index":       "orders",
				"max_retries": -1,
			},
			want: "max_retries must be >= 0",
		},
		{
			name: "invalid retry base",
			config: map[string]any{
				"hosts":         []string{"http://127.0.0.1:9200"},
				"index":         "orders",
				"retry_base_ms": -1,
			},
			want: "retry_base_ms must be >= 0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewElasticsearchSink(tc.config)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("NewElasticsearchSink() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestElasticsearchBulkItemErrorsExposeFailedRecordIndices(t *testing.T) {
	var bulkCalls int32
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_bulk":
			call := atomic.AddInt32(&bulkCalls, 1)
			w.Header().Set("Content-Type", "application/json")
			if call == 1 {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"_id":"1","status":201}}]}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"errors":true,"items":[{"index":{"_id":"2","status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field age"}}}]}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"green"}`))
		}
	}))
	defer es.Close()

	s, err := NewElasticsearchSink(map[string]any{
		"host":        es.URL,
		"index":       "orders",
		"chunk_size":  1,
		"max_retries": 0,
	})
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}

	err = s.Write(context.Background(), []core.Record{
		{Data: map[string]any{"id": 1, "age": 30}},
		{Data: map[string]any{"id": 2, "age": "bad"}},
	})
	if err == nil {
		t.Fatal("Write() = nil error, want partial batch error")
	}
	var partial core.PartialBatchError
	if !errors.As(err, &partial) {
		t.Fatalf("Write() error = %T %v, want core.PartialBatchError", err, err)
	}
	indices := partial.FailedRecordIndices()
	if len(indices) != 1 || indices[0] != 1 {
		t.Fatalf("FailedRecordIndices() = %#v, want [1]", indices)
	}
	recordErr := partial.ErrorForRecord(1)
	if recordErr == nil {
		t.Fatal("ErrorForRecord(1) = nil")
	}
	if got := core.ClassifyError(recordErr); got != core.ErrorClassSchema {
		t.Fatalf("ClassifyError(recordErr) = %s, want %s; err=%v", got, core.ErrorClassSchema, recordErr)
	}
	if !strings.Contains(recordErr.Error(), "id=2") || !strings.Contains(recordErr.Error(), "mapper_parsing_exception") {
		t.Fatalf("record error = %v, want id and mapper type", recordErr)
	}
}

func TestElasticsearchValidateSchemaUsesConfiguredMapping(t *testing.T) {
	s, err := NewElasticsearchSink(map[string]any{
		"hosts": []interface{}{"http://127.0.0.1:1"},
		"index": "orders",
		"mappings": map[string]any{
			"properties": map[string]any{
				"id":    map[string]any{"type": "long"},
				"phone": map[string]any{"type": "long"},
				"name":  map[string]any{"type": "keyword"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}

	err = s.ValidateSchema(context.Background(), core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "BIGINT"},
		{Name: "phone", DataType: "VARCHAR(32)"},
		{Name: "name", DataType: "TEXT"},
	}})
	if err == nil {
		t.Fatal("ValidateSchema() = nil, want phone type mismatch")
	}
	if !strings.Contains(err.Error(), "phone source=VARCHAR(32) target=long") {
		t.Fatalf("ValidateSchema() error = %v, want phone mismatch", err)
	}
}

func TestElasticsearchValidateSchemaFetchesRemoteMapping(t *testing.T) {
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orders/_mapping":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"orders":{"mappings":{"properties":{"id":{"type":"long"},"amount":{"type":"double"},"status":{"type":"keyword"}}}}}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"green"}`))
		}
	}))
	defer es.Close()

	s, err := NewElasticsearchSink(map[string]any{
		"host":  es.URL,
		"index": "orders",
	})
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	err = s.ValidateSchema(context.Background(), core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "BIGINT"},
		{Name: "amount", DataType: "DOUBLE"},
		{Name: "status", DataType: "VARCHAR(16)"},
	}})
	if err != nil {
		t.Fatalf("ValidateSchema() = %v, want compatible mapping", err)
	}
}

func TestSummarizeBulkErrorsIncludesItemIndex(t *testing.T) {
	summary := summarizeBulkErrors(map[string]any{
		"items": []any{
			map[string]any{"index": map[string]any{"_id": "ok", "status": float64(201)}},
			map[string]any{"index": map[string]any{
				"_id":    "bad",
				"status": float64(400),
				"error": map[string]any{
					"type":   "mapper_parsing_exception",
					"reason": "failed to parse",
				},
			}},
		},
	})
	for _, want := range []string{"item=1", "id=bad", "status=400", "mapper_parsing_exception"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary = %q, want %q", summary, want)
		}
	}
}

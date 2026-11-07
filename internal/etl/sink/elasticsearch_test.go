package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func newESTestSink(t *testing.T, baseURL string, extra map[string]any) *ElasticsearchSink {
	t.Helper()
	cfg := map[string]any{
		"hosts":         []any{baseURL},
		"index":         "orders",
		"chunk_size":    100,
		"retry_base_ms": 1,
	}
	for k, v := range extra {
		cfg[k] = v
	}
	s, err := NewElasticsearchSink(cfg)
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}
	return s
}

func TestElasticsearchBulkIndexesWithIDColumn(t *testing.T) {
	var bodies []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"status":201}}]}`))
	}))
	defer ts.Close()

	s := newESTestSink(t, ts.URL, nil)
	rec := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 7, "amount": 3}}
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(bodies) != 1 {
		t.Fatalf("bulk calls = %d, want 1", len(bodies))
	}
	lines := strings.Split(strings.TrimSpace(bodies[0]), "\n")
	if len(lines) != 2 {
		t.Fatalf("bulk body lines = %d, want 2 (action + document): %q", len(lines), bodies[0])
	}
	var action map[string]map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &action); err != nil {
		t.Fatalf("decode action: %v", err)
	}
	if got := action["index"]["_id"]; got != "7" {
		t.Fatalf("_id = %v, want 7 (from id_column)", got)
	}
	if got := action["index"]["_index"]; got != "orders" {
		t.Fatalf("_index = %v, want orders", got)
	}
}

func TestElasticsearchDeleteUsesDeleteAction(t *testing.T) {
	var bodies []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		_, _ = w.Write([]byte(`{"errors":false,"items":[{"delete":{"status":200}}]}`))
	}))
	defer ts.Close()

	s := newESTestSink(t, ts.URL, nil)
	rec := core.Record{Operation: core.OpDelete, Data: map[string]any{"id": 9}}
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(bodies[0], `"delete"`) {
		t.Fatalf("expected a delete action, got %q", bodies[0])
	}
	if strings.Count(strings.TrimSpace(bodies[0]), "\n") != 0 {
		t.Fatalf("a delete action must not carry a document body: %q", bodies[0])
	}
}

func TestElasticsearchRetriesOn429AndHonoursRetryAfter(t *testing.T) {
	var calls int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if atomic.AddInt64(&calls, 1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"too many requests"}`))
			return
		}
		_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"status":201}}]}`))
	}))
	defer ts.Close()

	s := newESTestSink(t, ts.URL, map[string]any{"max_retries": 2})
	rec := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}}
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n := atomic.LoadInt64(&calls); n != 2 {
		t.Fatalf("bulk calls = %d, want 2 (429 must be retried)", n)
	}
}

func TestElasticsearchSurfacesPartialBulkFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		// One item rejected by the mapping: the sink must fail the batch instead
		// of silently dropping the row.
		_, _ = w.Write([]byte(`{"errors":true,"items":[{"index":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [amount]"}}}]}`))
	}))
	defer ts.Close()

	s := newESTestSink(t, ts.URL, map[string]any{"max_retries": 0})
	rec := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1, "amount": "abc"}}
	err := s.Write(context.Background(), []core.Record{rec})
	if err == nil {
		t.Fatal("expected a per-item bulk failure to surface")
	}
	if !strings.Contains(err.Error(), "mapper_parsing_exception") && !strings.Contains(err.Error(), "failed to parse") {
		t.Fatalf("err = %v, want the ES reason", err)
	}
}

func TestElasticsearchIndexTemplateRouting(t *testing.T) {
	var indexes []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// A bulk body alternates action/document lines; collect every action.
		for i, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if i%2 != 0 || strings.TrimSpace(line) == "" {
				continue
			}
			var action map[string]map[string]any
			if err := json.Unmarshal([]byte(line), &action); err != nil {
				continue
			}
			if idx, ok := action["index"]["_index"].(string); ok {
				indexes = append(indexes, idx)
			}
		}
		_, _ = w.Write([]byte(`{"errors":false,"items":[{"index":{"status":201}}]}`))
	}))
	defer ts.Close()

	s := newESTestSink(t, ts.URL, map[string]any{"index": "", "index_template": "ods_{table}"})
	records := []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{Table: "orders"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 2}, Metadata: core.Metadata{Table: "items"}},
	}
	if err := s.Write(context.Background(), records); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(indexes) != 2 || indexes[0] != "ods_orders" || indexes[1] != "ods_items" {
		t.Fatalf("indexes = %v, want [ods_orders ods_items]", indexes)
	}
}

func TestElasticsearchFailsClosedWithoutIDForDelete(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"errors":false,"items":[]}`))
	}))
	defer ts.Close()

	s := newESTestSink(t, ts.URL, map[string]any{"id_column": "missing_col"})
	// A delete with neither the id column nor any source position has no stable
	// _id; ES must not delete an arbitrary document.
	rec := core.Record{Operation: core.OpDelete, Data: map[string]any{"other": 1}}
	if err := s.Write(context.Background(), []core.Record{rec}); err == nil {
		t.Fatal("expected a hard failure for a delete without a resolvable id")
	}
}

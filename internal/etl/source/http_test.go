package source

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"openetl-go/internal/etl/core"
)

// TestHTTPSourceResumesFromCheckpoint verifies that Open() restores the page
// number from a saved checkpoint.
func TestHTTPSourceResumesFromCheckpoint(t *testing.T) {
	var fetched int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fetched, 1)
		page := r.URL.Query().Get("page")
		// Return empty result to terminate quickly.
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
		_ = page
	}))
	defer srv.Close()

	src, err := NewHTTPSource(map[string]any{
		"url":        srv.URL,
		"page_param": "page",
		"size_param": "size",
		"page_size":  10,
	})
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}

	// Pretend we already committed page 5.
	cpPos, _ := json.Marshal(httpPosition{Page: 5})
	cp := &core.Checkpoint{Position: cpPos}

	reader, err := src.Open(context.Background(), cp)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	hr := reader.(*httpReader)
	if hr.page != 5 {
		t.Errorf("after resume page = %d, want 5 (next fetch should be page 6)", hr.page)
	}

	// Trigger a fetch; we expect the page counter to advance to 6.
	_, _ = reader.ReadBatch(context.Background(), 10)
	if hr.page != 6 {
		t.Errorf("post-fetch page = %d, want 6", hr.page)
	}
}

// TestHTTPSourceRetryOn5xx verifies exponential backoff retries on 5xx.
func TestHTTPSourceRetryOn5xx(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{map[string]any{"k": "v"}}})
	}))
	defer srv.Close()

	src, err := NewHTTPSource(map[string]any{
		"url":           srv.URL,
		"page_param":    "page",
		"size_param":    "size",
		"page_size":     10,
		"max_retries":   3,
		"retry_base_ms": 5,
	})
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	recs, err := reader.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 1 {
		t.Errorf("records = %d, want 1", len(recs))
	}
	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("attempts = %d, want 3 (2 retries + success)", attempts)
	}
}

// TestHTTPSourceNoRetryOn4xx verifies non-retryable errors propagate.
func TestHTTPSourceNoRetryOn4xx(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	src, err := NewHTTPSource(map[string]any{
		"url":           srv.URL,
		"page_param":    "page",
		"size_param":    "size",
		"max_retries":   3,
		"retry_base_ms": 5,
	})
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	_, err = reader.ReadBatch(context.Background(), 10)
	if err == nil {
		t.Fatal("expected error on 400, got nil")
	}
	if atomic.LoadInt32(&attempts) != 1 {
		t.Errorf("attempts = %d, want 1 (no retries on 4xx)", attempts)
	}
}

// TestHTTPExtractItems verifies dynamic result-key detection.
func TestHTTPExtractItems(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		resultKey string
		wantLen   int
	}{
		{"array_root", `[{"a":1},{"a":2}]`, "", 2},
		{"explicit_key", `{"data":[{"a":1}]}`, "data", 1},
		{"fallback_data", `{"data":[{"a":1}]}`, "", 1},
		{"fallback_items", `{"items":[{"a":1}]}`, "", 1},
		{"fallback_results", `{"results":[{"a":1}]}`, "", 1},
		{"fallback_records", `{"records":[{"a":1}]}`, "", 1},
		{"fallback_list", `{"list":[{"a":1}]}`, "", 1},
		{"missing_key", `{"foo":[]}`, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items, err := extractItems([]byte(tc.body), tc.resultKey)
			if err != nil {
				t.Fatalf("extractItems: %v", err)
			}
			if len(items) != tc.wantLen {
				t.Errorf("len = %d, want %d", len(items), tc.wantLen)
			}
		})
	}
}

// TestHTTPCheckpointForRecord verifies the checkpoint records current page.
func TestHTTPCheckpointForRecord(t *testing.T) {
	src := &HTTPSource{name: "http"}
	reader := &httpReader{source: src, page: 7, committedPage: 7}
	cp, err := reader.CheckpointForRecord(context.Background(), core.Record{})
	if err != nil {
		t.Fatalf("CheckpointForRecord: %v", err)
	}
	var pos httpPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if pos.Page != 7 {
		t.Errorf("page = %d, want 7", pos.Page)
	}
}

// TestHTTPBasicAuth verifies basic auth is applied.
func TestHTTPBasicAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
	}))
	defer srv.Close()

	src, err := NewHTTPSource(map[string]any{
		"url":        srv.URL,
		"page_param": "page",
		"size_param": "size",
		"auth_type":  "basic",
		"auth_user":  "alice",
		"auth_pass":  "secret",
	})
	if err != nil {
		t.Fatalf("NewHTTPSource: %v", err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Allow some time for the request.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = reader.ReadBatch(ctx, 10)
	if gotAuth != "Basic YWxpY2U6c2VjcmV0" {
		t.Errorf("basic auth = %q, want base64 of alice:secret", gotAuth)
	}
}

// silence unused import if io is not referenced directly elsewhere.
var _ = io.EOF

package transform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// newHTTPEnricher builds an enricher against the given test server with the
// provided config overrides. The helper centralizes boilerplate so each test
// focuses on the behavior under inspection.
func newHTTPEnricher(t *testing.T, url string, overrides map[string]any) *EnricherTransform {
	t.Helper()
	cfg := map[string]any{
		"mode":            "http",
		"url":             url,
		"timeout_seconds": 2,
		"target_field":    "enriched",
		"on_error":        "error",
		"retry_base_ms":   10,
		"max_retries":     0,
		"concurrency":     1,
		"max_in_flight":   100,
	}
	for k, v := range overrides {
		cfg[k] = v
	}
	tr, err := NewEnricherTransform(cfg)
	if err != nil {
		t.Fatalf("NewEnricherTransform: %v", err)
	}
	return tr
}

func TestEnricherHTTPSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"tier":"vip"}`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", nil)

	rec, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"user_id": "1001"}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got, _ := rec.Data["enriched"].(map[string]any)
	if got["tier"] != "vip" {
		t.Fatalf("enriched.tier = %v, want vip", got["tier"])
	}
	m := tr.TransformMetrics().Counters
	if m["processed"] != 1 || m["succeeded"] != 1 || m["errors"] != 0 {
		t.Fatalf("metrics = %+v, want processed=1 succeeded=1 errors=0", m)
	}
}

func TestEnricherHTTPErrorIsClassifiedAndRetried(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `rate limited`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", map[string]any{
		"max_retries":   1,
		"retry_base_ms": 2000, // larger than Retry-After=1s so Retry-After wins
	})

	_, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"user_id": "1001"}})
	if err == nil {
		t.Fatalf("Apply: expected error, got nil")
	}
	// 1 initial + 1 retry.
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
	m := tr.TransformMetrics().Counters
	if m["retries"] != 1 {
		t.Fatalf("metrics retries = %d, want 1", m["retries"])
	}
	// Error must be classified as transient (429).
	class := core.ClassifyError(err)
	if class != core.ErrorClassTransient {
		t.Fatalf("error class = %s, want %s", class, core.ErrorClassTransient)
	}
}

func TestEnricherHTTP4xxNotRetried(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `not found`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", map[string]any{
		"max_retries": 3,
	})

	_, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"user_id": "1001"}})
	if err == nil {
		t.Fatalf("Apply: expected error, got nil")
	}
	// 404 is data-class, not retried despite max_retries=3.
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("calls = %d, want 1 (no retries for 4xx data errors)", got)
	}
	class := core.ClassifyError(err)
	if class != core.ErrorClassData {
		t.Fatalf("error class = %s, want %s", class, core.ErrorClassData)
	}
}

func TestEnricherTimeoutRecorded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		fmt.Fprint(w, `{"tier":"vip"}`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", map[string]any{
		"timeout_seconds": 1,
	})

	_, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"user_id": "1001"}})
	if err == nil {
		t.Fatalf("Apply: expected timeout error, got nil")
	}
	m := tr.TransformMetrics().Counters
	if m["timeouts"] < 1 {
		t.Fatalf("metrics timeouts = %d, want >= 1", m["timeouts"])
	}
}

func TestEnricherBatchTransformConcurrent(t *testing.T) {
	var inFlight, maxInFlight int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxInFlight)
			if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"tier":"vip"}`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", map[string]any{
		"concurrency": 4,
	})

	recs := make([]core.Record, 8)
	for i := range recs {
		recs[i] = core.Record{Data: map[string]any{"user_id": fmt.Sprintf("u%d", i)}}
	}
	out, err := tr.ApplyBatch(context.Background(), recs)
	if err != nil {
		t.Fatalf("ApplyBatch err: %v", err)
	}
	if len(out) != 8 {
		t.Fatalf("out len = %d, want 8", len(out))
	}
	// With concurrency=4 and 8 records of ~50ms each, the server should have
	// seen at least 2 (and at most 4) concurrent requests.
	if got := atomic.LoadInt32(&maxInFlight); got < 2 {
		t.Fatalf("maxInFlight = %d, want >= 2 (concurrency should have allowed parallelism)", got)
	}
	if got := atomic.LoadInt32(&maxInFlight); got > 4 {
		t.Fatalf("maxInFlight = %d, want <= 4 (bounded by concurrency)", got)
	}
}

func TestEnricherBatchPartialFailureRoutesOnlyFailedToDLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/users/fail" {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `boom`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"tier":"vip"}`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", map[string]any{
		"on_error":    "error",
		"max_retries": 0,
	})

	recs := []core.Record{
		{Data: map[string]any{"user_id": "ok1"}},
		{Data: map[string]any{"user_id": "fail"}},
		{Data: map[string]any{"user_id": "ok2"}},
	}
	_, err := tr.ApplyBatch(context.Background(), recs)
	if err == nil {
		t.Fatalf("expected PartialTransformError, got nil")
	}
	var pte core.PartialTransformError
	if !errors.As(err, &pte) {
		t.Fatalf("expected PartialTransformError, got %T: %v", err, err)
	}
	if len(pte.FailedRecords()) != 1 {
		t.Fatalf("failed records = %d, want 1", len(pte.FailedRecords()))
	}
	m := tr.TransformMetrics().Counters
	if m["errors"] != 1 || m["succeeded"] != 2 {
		t.Fatalf("metrics = %+v, want errors=1 succeeded=2", m)
	}
}

func TestEnricherOnPassSwallowsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, `bad gateway`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", map[string]any{
		"on_error": "pass",
	})

	rec, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"user_id": "1001"}})
	if err != nil {
		t.Fatalf("Apply with on_error=pass should not surface error, got: %v", err)
	}
	if _, ok := rec.Data["enriched"]; ok {
		t.Fatalf("enriched field should be absent on failure with on_error=pass")
	}
	m := tr.TransformMetrics().Counters
	if m["errors"] != 1 {
		t.Fatalf("metrics errors = %d, want 1", m["errors"])
	}
}

func TestEnricherJSONDecodeFallsBackToString(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `plain text response`)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", nil)

	rec, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"user_id": "1001"}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got, _ := rec.Data["enriched"].(string); got != "plain text response" {
		t.Fatalf("enriched = %v, want plain text response", rec.Data["enriched"])
	}
}

func TestEnricherTransformMetricsShape(t *testing.T) {
	tr := &EnricherTransform{node: "n1"}
	m := tr.TransformMetrics()
	if m.Node != "n1" || m.Transform != "enricher" {
		t.Fatalf("metrics identity = %+v, want node=n1 transform=enricher", m)
	}
	required := []string{
		"processed", "hits", "misses", "cache_hits", "cache_misses",
		"timeouts", "retries", "errors", "succeeded", "in_flight",
	}
	for _, k := range required {
		if _, ok := m.Counters[k]; !ok {
			t.Fatalf("metrics missing counter %q in %+v", k, m.Counters)
		}
	}
}

func TestEnricherHTTPErrorClassification(t *testing.T) {
	cases := []struct {
		status int
		class  core.ErrorClass
	}{
		{429, core.ErrorClassTransient},
		{500, core.ErrorClassTransient},
		{502, core.ErrorClassTransient},
		{401, core.ErrorClassAuth},
		{403, core.ErrorClassAuth},
		{400, core.ErrorClassData},
		{404, core.ErrorClassData},
		{422, core.ErrorClassData},
	}
	for _, c := range cases {
		err := classifyHTTPError(&enricherHTTPError{statusCode: c.status, body: "x"})
		if got := core.ClassifyError(err); got != c.class {
			t.Fatalf("status %d classified as %s, want %s", c.status, got, c.class)
		}
	}
}

// Ensure JSON payload round-trips through the HTTP path unchanged.
func TestEnricherJSONPayloadRoundTrip(t *testing.T) {
	payload := map[string]any{"id": 42.0, "name": "alice", "tags": []any{"a", "b"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer srv.Close()

	tr := newHTTPEnricher(t, srv.URL+"/users/{{.user_id}}", nil)
	rec, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"user_id": "1001"}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got, ok := rec.Data["enriched"].(map[string]any)
	if !ok {
		t.Fatalf("enriched = %#v, want map", rec.Data["enriched"])
	}
	if got["name"] != "alice" {
		t.Fatalf("enriched.name = %v, want alice", got["name"])
	}
}

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	"github.com/a8851625/openetl-go/internal/etl/storage/factory"
)

func newTestHTTPServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()

	dir := t.TempDir()
	store, err := factory.NewStore(context.Background(), "sqlite", filepath.Join(dir, "cp"), filepath.Join(dir, "dlq"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	s, err := NewServer(store, dir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	return s, httptest.NewServer(mux)
}

func mustPipelineJSON(t *testing.T, spec pipeline.Spec) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return body
}

func mustPipelineUpdateJSON(t *testing.T, spec pipeline.Spec) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"spec": spec, "reset_checkpoint": false})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return body
}

func TestCreatePipelineRejectsPreflightErrors(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "p5-14-create",
		Source: pipeline.SourceSpec{
			Type: "mysql_cdc",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     1,
				"user":     "root",
				"database": "db",
				"tables":   []string{"customers"},
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     3306,
				"user":     "root",
				"database": "target",
				"table":    "customers",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	resp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(mustPipelineJSON(t, spec)))
	if err != nil {
		t.Fatalf("POST /api/v2/pipelines: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, _ := body["preflight_valid"].(bool); got {
		t.Fatalf("preflight_valid = %v, want false", got)
	}
	if _, exists := s.pipelines[spec.Name]; exists {
		t.Fatalf("pipeline %q should not be created when preflight fails", spec.Name)
	}
}

func TestUpdatePipelineRejectsPreflightErrorsWithoutReplacingRunner(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	original := pipeline.Spec{
		Name: "p5-14-update",
		Source: pipeline.SourceSpec{
			Type:   "file",
			Config: map[string]any{"path": filepath.Join(t.TempDir(), "missing.jsonl"), "format": "json"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&original)

	runner, err := s.newRunner(&original)
	if err != nil {
		t.Fatalf("newRunner(original): %v", err)
	}
	s.pipelines[original.Name] = runner
	s.specs[original.Name] = &original

	badUpdate := pipeline.Spec{
		Name: "p5-14-update",
		Source: pipeline.SourceSpec{
			Type: "mysql_cdc",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     1,
				"user":     "root",
				"database": "db",
				"tables":   []string{"customers"},
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     3306,
				"user":     "root",
				"database": "target",
				"table":    "customers",
			},
		},
	}
	pipeline.ApplyDefaults(&badUpdate)

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/pipelines", bytes.NewReader(mustPipelineUpdateJSON(t, badUpdate)))
	if err != nil {
		t.Fatalf("new PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /api/v2/pipelines: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	if got := s.pipelines[original.Name]; got != runner {
		t.Fatalf("runner replaced on failed update")
	}
	if got := s.specs[original.Name]; got != &original {
		t.Fatalf("spec replaced on failed update")
	}
}

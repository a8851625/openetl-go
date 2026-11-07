package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/sink"
)

func TestPipelineOutputCompactMergesDayObjects(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	outDir := t.TempDir()
	spec := &pipeline.Spec{
		Name:   "compact-pipe",
		Source: pipeline.SourceSpec{Type: "demo", Config: map[string]any{}},
		Sink: pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{
			"output_dir": outDir,
			"format":     "jsonl",
			"manifest":   true,
		}},
	}
	pipeline.ApplyDefaults(spec)
	s.mu.Lock()
	s.specs["compact-pipe"] = spec
	s.mu.Unlock()

	// Seed five committed batches through the real sink.
	fileSink, err := sink.NewFileSink(spec.Sink.Config)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := fileSink.Write(context.Background(), []core.Record{{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": i},
		}}); err != nil {
			t.Fatalf("seed write %d: %v", i, err)
		}
	}

	// Dry run first: nothing may change.
	resp, err := http.Post(ts.URL+"/api/v2/pipelines/compact-pipe/output/compact", "application/json",
		strings.NewReader(`{"dry_run":true,"min_objects":2}`))
	if err != nil {
		t.Fatalf("POST dry run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dry run status = %d", resp.StatusCode)
	}
	var dry sink.CompactResult
	if err := json.NewDecoder(resp.Body).Decode(&dry); err != nil {
		t.Fatalf("decode dry run: %v", err)
	}
	if !dry.DryRun || len(dry.Groups) != 1 {
		t.Fatalf("dry run = %+v", dry)
	}

	// Real run.
	resp2, err := http.Post(ts.URL+"/api/v2/pipelines/compact-pipe/output/compact", "application/json",
		strings.NewReader(`{"min_objects":2}`))
	if err != nil {
		t.Fatalf("POST compact: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("compact status = %d", resp2.StatusCode)
	}
	var result sink.CompactResult
	if err := json.NewDecoder(resp2.Body).Decode(&result); err != nil {
		t.Fatalf("decode compact: %v", err)
	}
	if len(result.Groups) != 1 || len(result.Groups[0].ReplacedKeys) != 5 {
		t.Fatalf("result = %+v", result)
	}
	if _, err := os.Stat(filepath.Join(outDir, filepath.FromSlash(result.Groups[0].MergedKey))); err != nil {
		t.Fatalf("merged object missing: %v", err)
	}
}

func TestPipelineOutputCompactRejectsNonManifestSink(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	spec := &pipeline.Spec{
		Name:   "no-manifest",
		Source: pipeline.SourceSpec{Type: "demo", Config: map[string]any{}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": t.TempDir(), "format": "jsonl"}},
	}
	pipeline.ApplyDefaults(spec)
	s.mu.Lock()
	s.specs["no-manifest"] = spec
	s.mu.Unlock()

	resp, err := http.Post(ts.URL+"/api/v2/pipelines/no-manifest/output/compact", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if msg, _ := body["error"].(string); !strings.Contains(msg, "manifest") {
		t.Fatalf("error = %v, want a manifest requirement", body)
	}
}

func TestPipelineOutputCompactRejectsOtherSinks(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	spec := &pipeline.Spec{
		Name:   "mysql-sink",
		Source: pipeline.SourceSpec{Type: "demo", Config: map[string]any{}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"host": "127.0.0.1", "database": "d", "table": "t"}},
	}
	pipeline.ApplyDefaults(spec)
	s.mu.Lock()
	s.specs["mysql-sink"] = spec
	s.mu.Unlock()

	resp, err := http.Post(ts.URL+"/api/v2/pipelines/mysql-sink/output/compact", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

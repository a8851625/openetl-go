package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

const dagSpecYAML = `name: dag-file-demo
dag:
  nodes:
    - id: source1
      kind: source
      plugin: file
      config:
        path: source1.jsonl
        format: json
    - id: sink1
      kind: sink
      plugin: file_sink
      config:
        output_dir: ./out
        format: jsonl
  edges:
    - from: source1
      to: sink1
`

const linearSpecYAML = `name: linear-file-demo
source:
  type: file
  config:
    path: linear.jsonl
    format: json
sink:
  type: file_sink
  config:
    output_dir: ./out
    format: jsonl
`

func writeSpecFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestLoadSpecsDAGFile(t *testing.T) {
	s := newSchedulerTestServer(t)
	writeSpecFile(t, s.specsDir, "dag.yaml", dagSpecYAML)

	res, err := s.loadSpecs(context.Background(), false)
	if err != nil {
		t.Fatalf("loadSpecs: %v", err)
	}
	if len(res.Loaded) != 1 {
		t.Fatalf("expected 1 loaded, got %d (errors=%v skipped=%v)", len(res.Loaded), res.Errors, res.Skipped)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.dagSpecs) != 1 {
		t.Fatalf("expected 1 dagSpec registered, got %d", len(s.dagSpecs))
	}
}

func TestLoadSpecsLinearFile(t *testing.T) {
	s := newSchedulerTestServer(t)
	writeSpecFile(t, s.specsDir, "linear.yaml", linearSpecYAML)

	res, err := s.loadSpecs(context.Background(), false)
	if err != nil {
		t.Fatalf("loadSpecs: %v", err)
	}
	if len(res.Loaded) != 1 {
		t.Fatalf("expected 1 loaded, got %d (errors=%v)", len(res.Loaded), res.Errors)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.specs) != 1 {
		t.Fatalf("expected 1 linear spec registered, got %d", len(s.specs))
	}
}

func TestLoadSpecsDAGAndLinearCoexist(t *testing.T) {
	s := newSchedulerTestServer(t)
	writeSpecFile(t, s.specsDir, "dag.yaml", dagSpecYAML)
	writeSpecFile(t, s.specsDir, "linear.yaml", linearSpecYAML)

	res, err := s.loadSpecs(context.Background(), false)
	if err != nil {
		t.Fatalf("loadSpecs: %v", err)
	}
	if len(res.Loaded) != 2 {
		t.Fatalf("expected 2 loaded, got %d (errors=%v skipped=%v)", len(res.Loaded), res.Errors, res.Skipped)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.dagSpecs) != 1 {
		t.Fatalf("expected 1 dagSpec, got %d", len(s.dagSpecs))
	}
	if len(s.specs) != 1 {
		t.Fatalf("expected 1 linear spec, got %d", len(s.specs))
	}
}

func TestLoadSpecsDuplicateDAGSkipped(t *testing.T) {
	s := newSchedulerTestServer(t)
	writeSpecFile(t, s.specsDir, "dag1.yaml", dagSpecYAML)
	writeSpecFile(t, s.specsDir, "dag2.yaml", dagSpecYAML)

	res, err := s.loadSpecs(context.Background(), false)
	if err != nil {
		t.Fatalf("loadSpecs: %v", err)
	}
	if len(res.Loaded) != 1 {
		t.Fatalf("expected 1 loaded, got %d", len(res.Loaded))
	}
	if len(res.Skipped) != 1 {
		t.Fatalf("expected 1 skipped duplicate, got %v", res.Skipped)
	}
}

func TestReloadSpecsPicksUpNewDAGFile(t *testing.T) {
	s := newSchedulerTestServer(t)
	writeSpecFile(t, s.specsDir, "dag.yaml", dagSpecYAML)

	if _, err := s.loadSpecs(context.Background(), false); err != nil {
		t.Fatalf("initial loadSpecs: %v", err)
	}

	// Add a second DAG file and reload with skipExisting=true
	writeSpecFile(t, s.specsDir, "dag3.yaml", "name: dag-file-3\ndag:\n  nodes:\n    - id: s3\n      kind: source\n      plugin: file\n      config:\n        path: x.jsonl\n        format: json\n    - id: k3\n      kind: sink\n      plugin: file_sink\n      config:\n        output_dir: ./out\n        format: jsonl\n  edges:\n    - from: s3\n      to: k3\n")

	res, err := s.ReloadSpecs(context.Background())
	if err != nil {
		t.Fatalf("ReloadSpecs: %v", err)
	}
	if len(res.Loaded) != 1 {
		t.Fatalf("expected 1 newly loaded on reload, got %d (skipped=%v)", len(res.Loaded), res.Skipped)
	}
}

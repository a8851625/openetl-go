package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"   // register sinks (file_sink) for registry.BuildSink
	_ "github.com/a8851625/openetl-go/internal/etl/source" // register sources (file) for registry.BuildSource
	"github.com/a8851625/openetl-go/internal/etl/storage/factory"
)

// TestNewRunnerNotRecursive (P5-1) guards against the infinite-self-recursion
// regression in Server.newRunner's non-distributed branch (server.go): it
// previously read `return s.newRunner(spec)` and stack-overflowed every
// standalone-role and single-shard pipeline at start (committed in f5faef0,
// missed because the A11 tests only exercised the distributed branch).
//
// With the bug present this test process fatals with a
// "goroutine stack exceeds 1000000000-byte limit" runtime error; with the fix
// it returns a non-nil inline runner built via pipeline.NewPipeline.
func TestNewRunnerNotRecursive(t *testing.T) {
	dir := t.TempDir()

	// Minimal zero-external-dependency spec: file(json) -> file_sink.
	srcPath := filepath.Join(dir, "in.jsonl")
	if err := os.WriteFile(srcPath, []byte(`{"id":1}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	spec := &pipeline.Spec{
		Name:   "p5-1-regression",
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": srcPath, "format": "json"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": filepath.Join(dir, "out")}},
	}

	store, err := factory.NewStore(context.Background(), "sqlite", filepath.Join(dir, "cp"), filepath.Join(dir, "dlq"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	s, err := NewServer(store, dir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if s.distributed {
		t.Fatal("test precondition: default Server must be non-distributed (standalone)")
	}

	// Default role is standalone -> s.distributed is false -> newRunner must take
	// the inline pipeline.NewPipeline branch, NOT recurse into itself.
	runner, err := s.newRunner(spec)
	if err != nil {
		t.Fatalf("newRunner returned error: %v", err)
	}
	if runner == nil {
		t.Fatal("newRunner returned nil runner")
	}
}

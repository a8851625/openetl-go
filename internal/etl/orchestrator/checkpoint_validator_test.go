package orchestrator

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

type dagCheckpointValidatingSource struct {
	err       error
	openCalls int32
}

func (s *dagCheckpointValidatingSource) Name() string { return "dag-checkpoint-validating-source" }
func (s *dagCheckpointValidatingSource) ValidateCheckpoint(context.Context, *core.Checkpoint) error {
	return s.err
}
func (s *dagCheckpointValidatingSource) Open(context.Context, *core.Checkpoint) (core.RecordReader, error) {
	atomic.AddInt32(&s.openCalls, 1)
	return dagCheckpointEmptyReader{}, nil
}

type dagCheckpointEmptyReader struct{}

func (dagCheckpointEmptyReader) Read(context.Context) (core.Record, error) {
	return core.Record{}, io.EOF
}
func (dagCheckpointEmptyReader) ReadBatch(context.Context, int) ([]core.Record, error) {
	return nil, io.EOF
}
func (dagCheckpointEmptyReader) Snapshot(context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}
func (dagCheckpointEmptyReader) Close() error { return nil }

func TestDAGSourceCheckpointValidatorFailsBeforeOpenAndSurfacesStats(t *testing.T) {
	adapter, cleanup := newDAGCheckpointAdapter(t)
	defer cleanup()
	if err := adapter.Save(context.Background(), core.Checkpoint{
		JobName:  "dag-checkpoint-validator-src",
		Source:   "src",
		Position: json.RawMessage(`{"offset":-1}`),
	}); err != nil {
		t.Fatal(err)
	}
	am := alert.NewManager()
	defer am.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sourceErr := core.NewCheckpointValidationError(
		"checkpoint.dag.invalid",
		"DAG source cursor is invalid",
		"Repair the source position, then retry the DAG.",
	)
	src := &dagCheckpointValidatingSource{err: sourceErr}
	exec := &DAGExecutor{
		spec:      &PipelineSpec{Name: "dag-checkpoint-validator"},
		cpAdapter: adapter,
		alertMgr:  am,
		cancel:    cancel,
		status:    "running",
	}
	records := make(chan recordMsg, 1)
	exec.readSource(ctx, src, "src", records)

	if got := atomic.LoadInt32(&src.openCalls); got != 0 {
		t.Fatalf("source Open calls = %d, want 0", got)
	}
	if exec.Status() != "failed" {
		t.Fatalf("status = %q, want failed", exec.Status())
	}
	stats := exec.Stats()
	if stats.LastErrorCode != "checkpoint.dag.invalid" {
		t.Fatalf("LastErrorCode = %q", stats.LastErrorCode)
	}
	if stats.LastErrorRemediation != "Repair the source position, then retry the DAG." {
		t.Fatalf("LastErrorRemediation = %q", stats.LastErrorRemediation)
	}
	if !strings.Contains(stats.LastError, "DAG source cursor is invalid") {
		t.Fatalf("LastError = %q", stats.LastError)
	}
	if ctx.Err() == nil {
		t.Fatal("DAG context was not cancelled after source validation failure")
	}
}

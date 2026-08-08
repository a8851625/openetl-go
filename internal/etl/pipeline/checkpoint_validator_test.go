package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

type startupCheckpointSource struct {
	validateErr  error
	validateCall int32
	openCall     int32
	order        []string
}

func (s *startupCheckpointSource) Name() string { return "startup-checkpoint-source" }

func (s *startupCheckpointSource) ValidateCheckpoint(_ context.Context, _ *core.Checkpoint) error {
	atomic.AddInt32(&s.validateCall, 1)
	s.order = append(s.order, "validate")
	return s.validateErr
}

func (s *startupCheckpointSource) Open(_ context.Context, _ *core.Checkpoint) (core.RecordReader, error) {
	atomic.AddInt32(&s.openCall, 1)
	s.order = append(s.order, "open")
	return startupCheckpointReader{}, nil
}

type startupCheckpointReader struct{}

func (startupCheckpointReader) Read(context.Context) (core.Record, error) {
	return core.Record{}, io.EOF
}
func (startupCheckpointReader) ReadBatch(context.Context, int) ([]core.Record, error) {
	return nil, io.EOF
}
func (startupCheckpointReader) Snapshot(context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}
func (startupCheckpointReader) Close() error { return nil }

func TestRunnerValidatesCheckpointBeforeSourceOpenAndExposesRemediation(t *testing.T) {
	store := newMemoryCPStore()
	if err := store.Save(context.Background(), core.Checkpoint{
		JobName:  "checkpoint-validator-startup",
		Source:   "startup-checkpoint-source",
		Position: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatal(err)
	}
	sourceErr := core.NewCheckpointValidationError(
		"checkpoint.test.invalid",
		"test source position is missing offset",
		"Repair the source position, then retry the pipeline.",
	)
	src := &startupCheckpointSource{validateErr: sourceErr}
	sink := &schemaValidatingSink{}
	am := alert.NewManager()
	defer am.Close()
	r := &Runner{
		spec:            &Spec{Name: "checkpoint-validator-startup"},
		source:          src,
		sink:            sink,
		checkpointStore: store,
		alertManager:    am,
	}

	err := r.Start(context.Background())
	if err == nil || !errors.Is(err, sourceErr) {
		t.Fatalf("Start() = %v, want wrapped source checkpoint error", err)
	}
	if got := atomic.LoadInt32(&src.validateCall); got != 1 {
		t.Fatalf("ValidateCheckpoint calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&src.openCall); got != 0 {
		t.Fatalf("source Open calls = %d, want 0", got)
	}
	if r.Status() != StatusFailed {
		t.Fatalf("status = %s, want failed", r.Status())
	}
	stats := r.Stats()
	if stats.LastErrorCode != "checkpoint.test.invalid" {
		t.Fatalf("LastErrorCode = %q", stats.LastErrorCode)
	}
	if stats.LastErrorRemediation != "Repair the source position, then retry the pipeline." {
		t.Fatalf("LastErrorRemediation = %q", stats.LastErrorRemediation)
	}
	if atomic.LoadInt32(&sink.closeCalls) != 1 {
		t.Fatalf("sink Close calls = %d, want 1", sink.closeCalls)
	}
}

func TestRunnerAllowsValidLegacyCheckpointThroughValidator(t *testing.T) {
	store := newMemoryCPStore()
	if err := store.Save(context.Background(), core.Checkpoint{
		JobName:  "checkpoint-validator-legacy",
		Source:   "startup-checkpoint-source",
		Position: json.RawMessage(`{"offset":7}`),
	}); err != nil {
		t.Fatal(err)
	}
	src := &startupCheckpointSource{}
	am := alert.NewManager()
	defer am.Close()
	r := &Runner{
		spec:            &Spec{Name: "checkpoint-validator-legacy"},
		source:          src,
		sink:            &recordingSink{},
		checkpointStore: store,
		alertManager:    am,
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want valid legacy position to start", err)
	}
	r.Wait()
	if got := atomic.LoadInt32(&src.openCall); got != 1 {
		t.Fatalf("source Open calls = %d, want 1", got)
	}
	if len(src.order) != 2 || src.order[0] != "validate" || src.order[1] != "open" {
		t.Fatalf("startup order = %v, want validate then open", src.order)
	}
}

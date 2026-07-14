package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/checkpoint"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/retry"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestDAGValidation(t *testing.T) {
	// Valid DAG
	valid := &DAG{
		Nodes: []*Node{
			{ID: "src", Kind: KindSource, Plugin: "file"},
			{ID: "tfm", Kind: KindTransform, Plugin: "identity"},
			{ID: "snk", Kind: KindSink, Plugin: "file_sink"},
		},
		Edges: []*Edge{
			{From: "src", To: "tfm"},
			{From: "tfm", To: "snk"},
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid dag failed: %v", err)
	}

	// Missing source
	noSrc := &DAG{
		Nodes: []*Node{
			{ID: "snk", Kind: KindSink, Plugin: "file_sink"},
		},
	}
	if err := noSrc.Validate(); err == nil {
		t.Error("expected error for dag with no source")
	}

	// Cycle
	cyclic := &DAG{
		Nodes: []*Node{
			{ID: "src", Kind: KindSource, Plugin: "file"},
			{ID: "a", Kind: KindTransform, Plugin: "identity"},
			{ID: "b", Kind: KindTransform, Plugin: "identity"},
			{ID: "snk", Kind: KindSink, Plugin: "file_sink"},
		},
		Edges: []*Edge{
			{From: "src", To: "a"},
			{From: "a", To: "b"},
			{From: "b", To: "a"}, // cycle!
			{From: "b", To: "snk"},
		},
	}
	if err := cyclic.Validate(); err == nil {
		t.Error("expected cycle error")
	}

	// Duplicate node IDs
	dupes := &DAG{
		Nodes: []*Node{
			{ID: "src", Kind: KindSource, Plugin: "file"},
			{ID: "src", Kind: KindSource, Plugin: "file"},
		},
	}
	if err := dupes.Validate(); err == nil {
		t.Error("expected duplicate ID error")
	}
}

func TestTopoSort(t *testing.T) {
	dag := &DAG{
		Nodes: []*Node{
			{ID: "src", Kind: KindSource, Plugin: "file"},
			{ID: "a", Kind: KindTransform, Plugin: "identity"},
			{ID: "b", Kind: KindTransform, Plugin: "identity"},
			{ID: "snk", Kind: KindSink, Plugin: "file_sink"},
		},
		Edges: []*Edge{
			{From: "src", To: "a"},
			{From: "a", To: "b"},
			{From: "b", To: "snk"},
		},
	}
	order, err := dag.TopoSort()
	if err != nil {
		t.Fatalf("toposort: %v", err)
	}
	if len(order) != 4 {
		t.Fatalf("order length = %d, want 4", len(order))
	}
	if order[0] != "src" {
		t.Errorf("first node = %s, want src", order[0])
	}
	if order[3] != "snk" {
		t.Errorf("last node = %s, want snk", order[3])
	}
}

func TestConvertLinearSpec(t *testing.T) {
	linear := &pipeline.Spec{
		Name: "test-linear",
		Source: pipeline.SourceSpec{
			Type:   "file",
			Config: map[string]any{"path": "/tmp/input.json"},
		},
		Transforms: []pipeline.TransformSpec{
			{Type: "identity", Config: map[string]any{}},
			{Type: "add_field", Config: map[string]any{"field": "x", "value": "1"}},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": "/tmp/out"},
		},
		BatchSize: 500,
	}

	dagSpec, err := ConvertLinearSpec(linear)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	if dagSpec.Name != "test-linear" {
		t.Errorf("name = %s", dagSpec.Name)
	}

	// Should have 4 nodes: src, tfm-0, tfm-1, snk
	if len(dagSpec.DAG.Nodes) != 4 {
		t.Fatalf("nodes = %d, want 4", len(dagSpec.DAG.Nodes))
	}

	// 3 edges: src→tfm0, tfm0→tfm1, tfm1→snk
	if len(dagSpec.DAG.Edges) != 3 {
		t.Fatalf("edges = %d, want 3", len(dagSpec.DAG.Edges))
	}

	// Validate the DAG is well-formed
	if err := dagSpec.DAG.Validate(); err != nil {
		t.Fatalf("converted dag invalid: %v", err)
	}

	// Check batch size propagated
	if dagSpec.Execution.BatchSize != 500 {
		t.Errorf("batch_size = %d, want 500", dagSpec.Execution.BatchSize)
	}

	// Check node kinds
	sources := dagSpec.DAG.Sources()
	if len(sources) != 1 {
		t.Fatalf("sources = %d, want 1", len(sources))
	}
	sinks := dagSpec.DAG.Sinks()
	if len(sinks) != 1 {
		t.Fatalf("sinks = %d, want 1", len(sinks))
	}
}

func TestMultiSinkDAG(t *testing.T) {
	// A DAG with fan-out: src → tfm → snk1, src → tfm → snk2
	dag := &DAG{
		Nodes: []*Node{
			{ID: "src", Kind: KindSource, Plugin: "file"},
			{ID: "tfm", Kind: KindTransform, Plugin: "identity"},
			{ID: "snk1", Kind: KindSink, Plugin: "file_sink"},
			{ID: "snk2", Kind: KindSink, Plugin: "mysql"},
		},
		Edges: []*Edge{
			{From: "src", To: "tfm"},
			{From: "tfm", To: "snk1"},
			{From: "tfm", To: "snk2"},
		},
	}
	if err := dag.Validate(); err != nil {
		t.Fatalf("multi-sink dag invalid: %v", err)
	}
	if len(dag.Sinks()) != 2 {
		t.Errorf("sinks = %d, want 2", len(dag.Sinks()))
	}
	downstream := dag.Downstream("tfm")
	if len(downstream) != 2 {
		t.Errorf("downstream of tfm = %d, want 2", len(downstream))
	}
}

func TestDAGExecutorInjectsStateDefaultsBeforeBuildingTransforms(t *testing.T) {
	var captured map[string]any
	registry.RegisterSource("state-defaults-source", func(config map[string]any) (core.Source, error) {
		return dagNoopSource{}, nil
	})
	registry.RegisterSink("state-defaults-sink", func(config map[string]any) (core.Sink, error) {
		return dagNoopSink{}, nil
	})
	registry.RegisterTransform("state-defaults-dag-probe", func(config map[string]any) (core.Transform, error) {
		captured = config
		return dagNoopTransform{}, nil
	})

	spec := &PipelineSpec{
		Name: "dag-state-defaults",
		DAG: DAG{
			Nodes: []*Node{
				{ID: "src", Kind: KindSource, Plugin: "state-defaults-source"},
				{ID: "window-node", Kind: KindTransform, Plugin: "state-defaults-dag-probe", Config: map[string]any{"state_backend": "sqlite"}},
				{ID: "sink", Kind: KindSink, Plugin: "state-defaults-sink"},
			},
			Edges: []*Edge{
				{From: "src", To: "window-node"},
				{From: "window-node", To: "sink"},
			},
		},
	}

	if _, err := NewDAGExecutor(spec, nil, nil, nil); err != nil {
		t.Fatalf("NewDAGExecutor: %v", err)
	}
	if captured["state_pipeline"] != "dag-state-defaults" || captured["state_node"] != "window-node" {
		t.Fatalf("state defaults captured = %#v", captured)
	}
	if _, ok := spec.DAG.Nodes[1].Config["state_pipeline"]; ok {
		t.Fatalf("NewDAGExecutor mutated original transform config: %#v", spec.DAG.Nodes[1].Config)
	}
}

func TestDAGExecutorCheckpointIncludesStateSnapshotVersions(t *testing.T) {
	adapter, cleanup := newDAGCheckpointAdapter(t)
	defer cleanup()
	am := alert.NewManager()
	defer am.Close()

	exec := &DAGExecutor{
		spec: &PipelineSpec{Name: "dag-checkpoint"},
		transforms: map[string]core.Transform{
			"window-node": dagStateSnapshotTransform{node: "window-node", version: "state-v1"},
		},
		sinks: map[string]core.Sink{
			"sink": dagNoopSink{},
		},
		readers: map[string]core.RecordReader{
			"src": dagCheckpointReader{},
		},
		cpAdapter:   adapter,
		alertMgr:    am,
		retryConfig: retry.DefaultConfig(),
		breakers:    map[string]*pipeline.CircuitBreaker{},
	}

	exec.writeToSink(context.Background(), "sink", []core.Record{
		{Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 42}},
	}, map[string]core.Record{
		"src": {Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 42}},
	})

	cp, err := adapter.Load(context.Background(), "dag-checkpoint-src")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if cp == nil {
		t.Fatal("checkpoint not saved")
	}
	env, ok, err := checkpoint.ParseEnvelope(cp.Position)
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	if !ok {
		t.Fatalf("checkpoint position not wrapped in envelope: %s", cp.Position)
	}
	if env.State["window-node"] != "state-v1" {
		t.Fatalf("state versions = %#v", env.State)
	}
	if string(env.Source) != `{"offset":42}` {
		t.Fatalf("source position = %s, want offset 42", env.Source)
	}
	if env.SinkCommit["acknowledged"] != true || env.SinkCommit["sink"] != "dag-noop-sink" || env.SinkCommit["node"] != "sink" {
		t.Fatalf("sink commit metadata = %#v", env.SinkCommit)
	}
}

func TestDAGExecutorDoesNotCheckpointWhenStateSnapshotFails(t *testing.T) {
	adapter, cleanup := newDAGCheckpointAdapter(t)
	defer cleanup()
	am := alert.NewManager()
	defer am.Close()

	exec := &DAGExecutor{
		spec: &PipelineSpec{Name: "dag-checkpoint-fail"},
		transforms: map[string]core.Transform{
			"window-node": dagFailingStateSnapshotTransform{},
		},
		sinks: map[string]core.Sink{
			"sink": dagNoopSink{},
		},
		readers: map[string]core.RecordReader{
			"src": dagCheckpointReader{},
		},
		cpAdapter:   adapter,
		alertMgr:    am,
		retryConfig: retry.DefaultConfig(),
		breakers:    map[string]*pipeline.CircuitBreaker{},
	}

	exec.writeToSink(context.Background(), "sink", []core.Record{
		{Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 42}},
	}, map[string]core.Record{
		"src": {Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 42}},
	})

	cp, err := adapter.Load(context.Background(), "dag-checkpoint-fail-src")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if cp != nil {
		t.Fatalf("checkpoint advanced after state snapshot failed: %s", cp.Position)
	}
}

func TestDAGExecutorDoesNotCheckpointWhenSinkCommitMetadataFails(t *testing.T) {
	adapter, cleanup := newDAGCheckpointAdapter(t)
	defer cleanup()
	am := alert.NewManager()
	defer am.Close()

	exec := &DAGExecutor{
		spec:       &PipelineSpec{Name: "dag-commit-metadata-fail"},
		transforms: map[string]core.Transform{},
		sinks: map[string]core.Sink{
			"sink": dagCommitMetadataFailSink{},
		},
		readers: map[string]core.RecordReader{
			"src": dagCheckpointReader{},
		},
		cpAdapter:        adapter,
		alertMgr:         am,
		retryConfig:      retry.DefaultConfig(),
		breakers:         map[string]*pipeline.CircuitBreaker{},
		checkpointBlocks: map[string]string{},
	}

	exec.writeToSink(context.Background(), "sink", []core.Record{
		{Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 42}},
	}, map[string]core.Record{
		"src": {Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 42}},
	})

	cp, err := adapter.Load(context.Background(), "dag-commit-metadata-fail-src")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if cp != nil {
		t.Fatalf("checkpoint advanced after commit metadata failed: %s", cp.Position)
	}
}

func TestDAGExecutorDoesNotCheckpointPastFailedDLQPersistence(t *testing.T) {
	adapter, cleanup := newDAGCheckpointAdapter(t)
	defer cleanup()
	am := alert.NewManager()
	defer am.Close()

	exec := &DAGExecutor{
		spec:             &PipelineSpec{Name: "dag-dlq-boundary"},
		transforms:       map[string]core.Transform{},
		sinks:            map[string]core.Sink{"sink": dagNoopSink{}},
		readers:          map[string]core.RecordReader{"src": dagCheckpointReader{}},
		cpAdapter:        adapter,
		dlqWriter:        dagFailingDLQ{},
		alertMgr:         am,
		retryConfig:      retry.DefaultConfig(),
		breakers:         map[string]*pipeline.CircuitBreaker{},
		checkpointBlocks: map[string]string{},
	}

	if exec.handleFailed(context.Background(), core.Record{Metadata: core.Metadata{Source: "dag-dlq-boundary"}}, errors.New("transform failed"), "transform") {
		t.Fatal("failed DLQ persistence reported a durable boundary")
	}
	exec.blockSourceCheckpoint("src", "record failure could not be persisted to DLQ")
	exec.writeToSink(context.Background(), "sink", []core.Record{
		{Data: map[string]any{"id": 2}, Metadata: core.Metadata{Offset: 2}},
	}, map[string]core.Record{
		"src": {Data: map[string]any{"id": 2}, Metadata: core.Metadata{Offset: 2}},
	})

	cp, err := adapter.Load(context.Background(), "dag-dlq-boundary-src")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if cp != nil {
		t.Fatalf("later successful sink write advanced checkpoint past failed DLQ persistence: %s", cp.Position)
	}
}

func TestDAGExecutorLoadSourceCheckpointUnwrapsEnvelope(t *testing.T) {
	adapter, cleanup := newDAGCheckpointAdapter(t)
	defer cleanup()
	raw, err := checkpoint.BuildEnvelope(json.RawMessage(`{"offset":99}`), map[string]string{"window-node": "state-v1"}, nil)
	if err != nil {
		t.Fatalf("BuildEnvelope: %v", err)
	}
	if err := adapter.Save(context.Background(), core.Checkpoint{
		JobName:  "dag-load-src",
		Source:   "src",
		Position: raw,
	}); err != nil {
		t.Fatalf("Save checkpoint: %v", err)
	}

	exec := &DAGExecutor{spec: &PipelineSpec{Name: "dag-load"}, cpAdapter: adapter}
	got := exec.loadSourceCheckpoint(context.Background(), "src")

	if got == nil {
		t.Fatal("checkpoint not loaded")
	}
	if string(got.Position) != `{"offset":99}` {
		t.Fatalf("unwrapped position = %s, want offset 99", got.Position)
	}
}

func TestDAGExecutorExecutionWorkersParallelizeRouting(t *testing.T) {
	const (
		sourceName    = "dag-workers-source"
		transformName = "dag-workers-transform"
		sinkName      = "dag-workers-sink"
	)
	probe := &dagConcurrencyProbe{}
	sinkProbe := &dagCountingSink{}
	registry.RegisterSource(sourceName, func(config map[string]any) (core.Source, error) {
		return dagCountingSource{count: 8}, nil
	})
	registry.RegisterTransform(transformName, func(config map[string]any) (core.Transform, error) {
		return probe, nil
	})
	registry.RegisterSink(sinkName, func(config map[string]any) (core.Sink, error) {
		return sinkProbe, nil
	})

	spec := &PipelineSpec{
		Name:      "dag-workers",
		Execution: &ExecutionConfig{Workers: 4, BatchSize: 20, BackpressureBuf: 20},
		DAG: DAG{
			Nodes: []*Node{
				{ID: "src", Kind: KindSource, Plugin: sourceName},
				{ID: "tfm", Kind: KindTransform, Plugin: transformName},
				{ID: "sink", Kind: KindSink, Plugin: sinkName},
			},
			Edges: []*Edge{
				{From: "src", To: "tfm"},
				{From: "tfm", To: "sink"},
			},
		},
	}
	am := alert.NewManager()
	defer am.Close()
	exec, err := NewDAGExecutor(spec, nil, nil, am)
	if err != nil {
		t.Fatalf("NewDAGExecutor: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	exec.Wait()

	if got := atomic.LoadInt64(&probe.maxInFlight); got < 2 {
		t.Fatalf("execution.workers did not parallelize transform routing: max in-flight = %d", got)
	}
	if got := atomic.LoadInt64(&sinkProbe.records); got != 8 {
		t.Fatalf("sink records = %d, want 8", got)
	}
}

func newDAGCheckpointAdapter(t *testing.T) (*storage.CheckpointStoreAdapter, func()) {
	t.Helper()
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	return storage.NewCheckpointStoreAdapter(store), func() { _ = store.Close() }
}

type dagNoopSource struct{}

func (dagNoopSource) Name() string { return "dag-noop-source" }
func (dagNoopSource) Open(context.Context, *core.Checkpoint) (core.RecordReader, error) {
	return nil, nil
}

type dagNoopSink struct{}

func (dagNoopSink) Name() string                               { return "dag-noop-sink" }
func (dagNoopSink) Open(context.Context) error                 { return nil }
func (dagNoopSink) Write(context.Context, []core.Record) error { return nil }
func (dagNoopSink) Close() error                               { return nil }

type dagCommitMetadataFailSink struct{}

func (dagCommitMetadataFailSink) Name() string                               { return "dag-commit-fail" }
func (dagCommitMetadataFailSink) Open(context.Context) error                 { return nil }
func (dagCommitMetadataFailSink) Write(context.Context, []core.Record) error { return nil }
func (dagCommitMetadataFailSink) Close() error                               { return nil }
func (dagCommitMetadataFailSink) SinkCommitMetadata(context.Context) (map[string]any, error) {
	return nil, errors.New("commit token unavailable")
}

type dagFailingDLQ struct{}

func (dagFailingDLQ) WriteDLQ(context.Context, pipeline.DLQEntry) error {
	return errors.New("DLQ store unavailable")
}

type dagNoopTransform struct{}

func (dagNoopTransform) Name() string { return "dag-noop-transform" }
func (dagNoopTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}

type dagCountingSource struct {
	count int
}

func (s dagCountingSource) Name() string { return "dag-counting-source" }
func (s dagCountingSource) Open(context.Context, *core.Checkpoint) (core.RecordReader, error) {
	return &dagCountingReader{count: s.count}, nil
}

type dagCountingReader struct {
	count int
	next  int
}

func (r *dagCountingReader) Read(context.Context) (core.Record, error) {
	if r.next >= r.count {
		return core.Record{}, io.EOF
	}
	r.next++
	return core.Record{
		Data:     map[string]any{"id": r.next},
		Metadata: core.Metadata{Offset: int64(r.next)},
	}, nil
}

func (r *dagCountingReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	out := make([]core.Record, 0, n)
	for len(out) < n {
		rec, err := r.Read(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return out, err
		}
		out = append(out, rec)
	}
	if len(out) == 0 {
		return nil, io.EOF
	}
	return out, nil
}

func (r *dagCountingReader) Snapshot(context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}
func (r *dagCountingReader) Close() error { return nil }
func (r *dagCountingReader) CheckpointForRecord(_ context.Context, rec core.Record) (core.Checkpoint, error) {
	pos, _ := json.Marshal(map[string]any{"offset": rec.Metadata.Offset})
	return core.Checkpoint{Source: "dag-counting-reader", Position: pos}, nil
}

type dagConcurrencyProbe struct {
	inFlight    int64
	maxInFlight int64
}

func (p *dagConcurrencyProbe) Name() string { return "dag-concurrency-probe" }
func (p *dagConcurrencyProbe) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	cur := atomic.AddInt64(&p.inFlight, 1)
	for {
		max := atomic.LoadInt64(&p.maxInFlight)
		if cur <= max || atomic.CompareAndSwapInt64(&p.maxInFlight, max, cur) {
			break
		}
	}
	select {
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		atomic.AddInt64(&p.inFlight, -1)
		return rec, ctx.Err()
	}
	atomic.AddInt64(&p.inFlight, -1)
	return rec, nil
}

type dagCountingSink struct {
	records int64
}

func (s *dagCountingSink) Name() string               { return "dag-counting-sink" }
func (s *dagCountingSink) Open(context.Context) error { return nil }
func (s *dagCountingSink) Write(_ context.Context, records []core.Record) error {
	atomic.AddInt64(&s.records, int64(len(records)))
	return nil
}
func (s *dagCountingSink) Close() error { return nil }

type dagCheckpointReader struct{}

func (dagCheckpointReader) Read(context.Context) (core.Record, error) {
	return core.Record{}, errors.New("unused")
}
func (dagCheckpointReader) ReadBatch(context.Context, int) ([]core.Record, error) {
	return nil, errors.New("unused")
}
func (dagCheckpointReader) Snapshot(context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}
func (dagCheckpointReader) Close() error { return nil }
func (dagCheckpointReader) CheckpointForRecord(_ context.Context, rec core.Record) (core.Checkpoint, error) {
	pos, _ := json.Marshal(map[string]any{"offset": rec.Metadata.Offset})
	return core.Checkpoint{Source: "dag-checkpoint-reader", Position: pos}, nil
}

type dagStateSnapshotTransform struct {
	node    string
	version string
}

func (t dagStateSnapshotTransform) Name() string { return "dag-state-snapshot" }
func (t dagStateSnapshotTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}
func (t dagStateSnapshotTransform) SnapshotState(context.Context) (string, string, bool, error) {
	return t.node, t.version, true, nil
}

type dagFailingStateSnapshotTransform struct{}

func (t dagFailingStateSnapshotTransform) Name() string { return "dag-failing-state-snapshot" }
func (t dagFailingStateSnapshotTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}
func (t dagFailingStateSnapshotTransform) SnapshotState(context.Context) (string, string, bool, error) {
	return "window-node", "", false, errors.New("state snapshot failed")
}

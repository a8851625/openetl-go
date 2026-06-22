package orchestrator

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
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

package pipeline

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/alert"
)

func TestShardRecoveryContextRetainsConnectionsAndPipelineIdentity(t *testing.T) {
	spec := &Spec{
		Name:        "69fc3b18-871f-4e08-9928-05096b2a57e9",
		Source:      SourceSpec{Type: "demo", Connection: "source-fixture", Config: map[string]any{"interval_ms": 10}},
		Sink:        SinkSpec{Type: "file_sink", ConnectionRef: "sink-fixture", Config: map[string]any{"path": t.TempDir() + "/out.jsonl"}},
		Transforms:  []TransformSpec{{Type: "identity", ConnectionRef: "shape-fixture", Config: map[string]any{"nested": map[string]any{"key": "original"}}}},
		Parallelism: &ParallelismConfig{Count: 2},
	}
	r, err := BuildShardRunner(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if r.spec.Name != spec.Name || r.spec.Source.Connection != "source-fixture" || r.spec.Sink.ConnectionRef != "sink-fixture" {
		t.Fatal("shard changed runtime owner identity")
	}
	for endpoint, connection := range map[string]string{"source": "source-fixture", "sink": "sink-fixture", "identity": "sink-fixture", "transform": "shape-fixture"} {
		captured := r.recoveryCaptureContext(endpoint)
		if !captured.ConnectionKnown || captured.ConnectionName != connection || captured.Endpoint != endpoint {
			t.Fatalf("%s context=%+v", endpoint, captured)
		}
	}
	r.spec.Transforms[0].Config["new-key"] = "mutated"
	if _, exists := spec.Transforms[0].Config["new-key"]; exists {
		t.Fatal("shard shares mutable transform config")
	}
	r.spec.Transforms = append(r.spec.Transforms, TransformSpec{Type: "identity", Connection: "other"})
	if context := r.recoveryCaptureContext("transform"); context.ConnectionKnown || context.ConnectionName != "" {
		t.Fatalf("multi-transform failure guessed a connection: %+v", context)
	}
}

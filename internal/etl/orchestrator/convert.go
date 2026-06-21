package orchestrator

import (
	"fmt"

	"openetl-go/internal/etl/pipeline"
)

// ConvertLinearSpec converts the legacy linear pipeline.Spec (1 source → N transforms → 1 sink)
// into a DAG PipelineSpec with a chain of nodes.
//
// The resulting DAG has the following shape:
//
//	src-0 → tfm-0 → tfm-1 → ... → tfm-N → snk-0
//
// This ensures full backward compatibility: all existing YAML pipeline specs
// can be loaded by the new DAG executor without modification.
func ConvertLinearSpec(spec *pipeline.Spec) (*PipelineSpec, error) {
	if spec == nil {
		return nil, fmt.Errorf("nil spec")
	}

	dag := DAG{}
	nodeIDCounter := 0

	// Source node
	srcID := fmt.Sprintf("src-%d", nodeIDCounter)
	nodeIDCounter++
	dag.Nodes = append(dag.Nodes, &Node{
		ID:     srcID,
		Kind:   KindSource,
		Plugin: spec.Source.Type,
		Config: spec.Source.Config,
	})

	prevID := srcID

	// Transform nodes
	for i := range spec.Transforms {
		tfmID := fmt.Sprintf("tfm-%d-%d", i, nodeIDCounter)
		nodeIDCounter++
		dag.Nodes = append(dag.Nodes, &Node{
			ID:     tfmID,
			Kind:   KindTransform,
			Plugin: spec.Transforms[i].Type,
			Config: spec.Transforms[i].Config,
		})
		dag.Edges = append(dag.Edges, &Edge{
			ID:   fmt.Sprintf("e-%s-%s", prevID, tfmID),
			From: prevID,
			To:   tfmID,
		})
		prevID = tfmID
	}

	// Sink node
	sinkID := fmt.Sprintf("snk-%d", nodeIDCounter)
	dag.Nodes = append(dag.Nodes, &Node{
		ID:     sinkID,
		Kind:   KindSink,
		Plugin: spec.Sink.Type,
		Config: spec.Sink.Config,
	})
	dag.Edges = append(dag.Edges, &Edge{
		ID:   fmt.Sprintf("e-%s-%s", prevID, sinkID),
		From: prevID,
		To:   sinkID,
	})

	dagSpec := &PipelineSpec{
		Name: spec.Name,
		DAG:  dag,
		Execution: &ExecutionConfig{
			BatchSize:        spec.BatchSize,
			BackpressureBuf:  spec.BackpressureBuffer,
			CheckpointEveryS: spec.CheckpointIntervalSec,
		},
	}

	if spec.Retry != nil {
		dagSpec.Retry = &RetryConfig{
			MaxAttempts:       spec.Retry.MaxAttempts,
			InitialIntervalMs: spec.Retry.InitialIntervalMs,
			MaxIntervalMs:     spec.Retry.MaxIntervalMs,
		}
	}

	if spec.DLQ != nil {
		dagSpec.DLQ = &DLQConfig{Enable: spec.DLQ.Enable}
	}

	if spec.Schedule != nil {
		dagSpec.Schedule = &ScheduleConfig{
			Type: ScheduleType(spec.Schedule.Type),
			Cron: spec.Schedule.Cron,
		}
	}

	return dagSpec, nil
}

package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/retry"
)

// DAGReplayer replays a dead-letter record from the node that originally failed.
// It intentionally mirrors DAGExecutor's routing semantics: source/control nodes
// pass records downstream, KindTransform applies one transform, and KindSink
// writes the recovered record.
type DAGReplayer struct {
	spec        *PipelineSpec
	transforms  map[string]core.Transform
	sinks       map[string]core.Sink
	openedSinks map[string]bool
	retryConfig retry.Config
}

func NewDAGReplayer(spec *PipelineSpec) (*DAGReplayer, error) {
	if spec == nil {
		return nil, fmt.Errorf("dag spec is nil")
	}
	if err := spec.DAG.Validate(); err != nil {
		return nil, fmt.Errorf("validate dag: %w", err)
	}

	replayer := &DAGReplayer{
		spec:        spec,
		transforms:  map[string]core.Transform{},
		sinks:       map[string]core.Sink{},
		openedSinks: map[string]bool{},
	}
	replayer.retryConfig = retry.DefaultConfig()
	if spec.Retry != nil {
		replayer.retryConfig.MaxAttempts = spec.Retry.MaxAttempts
		replayer.retryConfig.InitialInterval = time.Duration(spec.Retry.InitialIntervalMs) * time.Millisecond
		replayer.retryConfig.MaxInterval = time.Duration(spec.Retry.MaxIntervalMs) * time.Millisecond
	}

	for _, node := range spec.DAG.Nodes {
		switch node.Kind {
		case KindSource:
			// Replay starts from an already-materialized DLQ record, so sources
			// are not opened.
		case KindTransform:
			config := pipeline.InjectStateDefaults(spec.Name, node.ID, node.Config)
			t, err := registry.BuildTransform(node.Plugin, config)
			if err != nil {
				return nil, fmt.Errorf("build transform %s (%s): %w", node.ID, node.Plugin, err)
			}
			replayer.transforms[node.ID] = t
		case KindSink:
			sink, err := registry.BuildSink(node.Plugin, node.Config)
			if err != nil {
				return nil, fmt.Errorf("build sink %s (%s): %w", node.ID, node.Plugin, err)
			}
			replayer.sinks[node.ID] = sink
		case KindFanout, KindRouter, KindTap, KindRateLimiter, KindEnricher, KindLookup:
			// These node kinds are topology/control markers in the current DAG
			// executor path unless expressed as KindTransform with a plugin.
		default:
			return nil, fmt.Errorf("node %s has unsupported kind %q", node.ID, node.Kind)
		}
	}
	return replayer, nil
}

func (r *DAGReplayer) Open(ctx context.Context) error {
	return nil
}

func (r *DAGReplayer) Close() error {
	var first error
	for id, sink := range r.sinks {
		if !r.openedSinks[id] {
			continue
		}
		if err := sink.Close(); err != nil && first == nil {
			first = fmt.Errorf("close sink %s: %w", id, err)
		}
	}
	return first
}

func (r *DAGReplayer) Replay(ctx context.Context, nodeID string, rec core.Record) error {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return fmt.Errorf("dag replay requires dag_node on the DLQ record")
	}
	if r.spec.DAG.GetNode(nodeID) == nil {
		return fmt.Errorf("dag replay node %q not found", nodeID)
	}

	batchBySink := map[string][]core.Record{}
	if err := r.route(ctx, nodeID, cloneRecord(rec), batchBySink); err != nil {
		return err
	}
	for sinkID, batch := range batchBySink {
		if len(batch) == 0 {
			continue
		}
		sink := r.sinks[sinkID]
		if sink == nil {
			return fmt.Errorf("sink node %s is not built", sinkID)
		}
		if err := r.openSink(ctx, sinkID, sink); err != nil {
			return err
		}
		if err := retry.Do(ctx, r.retryConfig, core.IsRetryableError, func() error {
			return sink.Write(ctx, batch)
		}); err != nil {
			return fmt.Errorf("write sink %s: %w", sinkID, err)
		}
	}
	return nil
}

func (r *DAGReplayer) openSink(ctx context.Context, sinkID string, sink core.Sink) error {
	if r.openedSinks[sinkID] {
		return nil
	}
	if err := sink.Open(ctx); err != nil {
		return fmt.Errorf("open sink %s: %w", sinkID, err)
	}
	r.openedSinks[sinkID] = true
	return nil
}

func (r *DAGReplayer) route(ctx context.Context, nodeID string, rec core.Record, batchBySink map[string][]core.Record) error {
	node := r.spec.DAG.GetNode(nodeID)
	if node == nil {
		return fmt.Errorf("dag node %q not found", nodeID)
	}

	if node.Kind == KindTransform {
		transform := r.transforms[nodeID]
		if transform == nil {
			return fmt.Errorf("transform node %s is not built", nodeID)
		}
		transformed, err := applyTransformSafely(ctx, transform, rec)
		if err != nil {
			if err == core.ErrRecordFiltered {
				return nil
			}
			return fmt.Errorf("transform node %s: %w", nodeID, err)
		}
		rec = transformed
	}

	if node.Kind == KindSink {
		batchBySink[nodeID] = append(batchBySink[nodeID], rec)
		return nil
	}

	for _, edge := range r.spec.DAG.Edges {
		if edge.From != nodeID {
			continue
		}
		if edge.Condition != nil && !evalCondition(*edge.Condition, rec) {
			continue
		}
		if err := r.route(ctx, edge.To, cloneRecord(rec), batchBySink); err != nil {
			return err
		}
	}
	return nil
}

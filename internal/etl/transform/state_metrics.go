package transform

import (
	"context"
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/state"
)

func stateMetrics(ctx context.Context, store state.Store, pipeline, node, owner string) (core.StateMetrics, bool, error) {
	if store == nil {
		return core.StateMetrics{}, false, nil
	}
	stats, err := store.Stats(ctx, pipeline, node)
	if err != nil {
		return core.StateMetrics{}, false, fmt.Errorf("%s: state metrics: %w", owner, err)
	}
	return core.StateMetrics{
		Pipeline:  pipeline,
		Node:      node,
		Keys:      stats.Keys,
		Bytes:     stats.Bytes,
		UpdatedAt: stats.UpdatedAt,
	}, true, nil
}

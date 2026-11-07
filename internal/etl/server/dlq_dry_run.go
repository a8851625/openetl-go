package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// isDryRunRequest reports whether a replay request is a preview only.
// The UI's "preview impact" action previously invented its numbers client-side;
// a real dry run answers with the identity gate's actual verdict per row.
func isDryRunRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	q := r.URL.Query()
	switch q.Get("dry_run") {
	case "1", "true", "yes":
		return true
	}
	switch q.Get("dry-run") {
	case "1", "true", "yes":
		return true
	}
	return false
}

// DLQDryRunItem is the per-row verdict of a replay dry run.
type DLQDryRunItem struct {
	ID         int64  `json:"id"`
	DAGNode    string `json:"dag_node,omitempty"`
	Replayable bool   `json:"replayable"`
	State      string `json:"state,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// DLQDryRunResult summarises what a real replay would do.
type DLQDryRunResult struct {
	DryRun     bool            `json:"dry_run"`
	Replayable int             `json:"replayable"`
	Blocked    int             `json:"blocked"`
	Items      []DLQDryRunItem `json:"items"`
}

// dryRunDLQReplay evaluates the replay identity gate for every selected row
// without writing to the sink or mutating DLQ state.
//
// Scope note: the preview covers the identity gate (the only replay decision that
// is provable without executing transforms). Transform output validation and sink
// idempotency are still only proven by a real replay.
func (s *Server) dryRunDLQReplay(ctx context.Context, name string, filter dlqFilter) (*DLQDryRunResult, error) {
	filter.Ascending = true
	items, err := s.readFilteredDLQ(ctx, name, filter)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()
	if spec == nil && dagSpec == nil {
		return nil, fmt.Errorf("pipeline %s not found", name)
	}
	if spec != nil && pipelineUnsafeForReplay(spec) {
		return nil, fmt.Errorf("pipeline %s uses a CDC source with a non-idempotent sink and does not set allow_unsafe; replaying would duplicate records, so it is refused until the spec accepts that boundary", name)
	}

	result := &DLQDryRunResult{DryRun: true, Items: make([]DLQDryRunItem, 0, len(items))}
	for _, item := range items {
		verdict := DLQDryRunItem{ID: item.ID, DAGNode: item.DAGNode, Replayable: true}
		if item.IdentityContext.ReplayState == core.DLQReplayStateSinkAcked {
			// A sink-acknowledged row only needs cleanup; nothing is re-written.
			verdict.State = string(core.DLQReplayStateSinkAcked)
			result.Replayable++
			result.Items = append(result.Items, verdict)
			continue
		}
		var gateErr error
		if dagSpec != nil {
			nodeID := item.DAGNode
			if nodeID == "" {
				verdict.Replayable = false
				verdict.Reason = "dag dlq record has no dag_node; replay requires node context"
				result.Blocked++
				result.Items = append(result.Items, verdict)
				continue
			}
			prepared := item
			for _, identitySpec := range dagReplayIdentitySpecs(dagSpec, nodeID) {
				prepared, gateErr = s.prepareDLQReplayIdentity(ctx, identitySpec, prepared, false)
				if gateErr != nil {
					break
				}
			}
		} else {
			_, gateErr = s.prepareDLQReplayIdentity(ctx, spec, item, false)
		}
		if gateErr != nil {
			verdict.Replayable = false
			verdict.Reason = gateErr.Error()
			var blockErr *dlqReplayGateError
			if errors.As(gateErr, &blockErr) {
				verdict.State = string(blockErr.State)
				verdict.Reason = blockErr.Reason
			}
			result.Blocked++
			result.Items = append(result.Items, verdict)
			continue
		}
		result.Replayable++
		result.Items = append(result.Items, verdict)
	}
	return result, nil
}

// pipelineUnsafeForReplay reports whether replay would cross the documented
// duplicate boundary the pipeline creation path refuses. Replay writes straight
// to the sink, so it must apply the same allow_unsafe gate.
func pipelineUnsafeForReplay(spec *pipeline.Spec) bool {
	return pipeline.UnsafeDuplicateBoundary(spec) && !spec.AllowUnsafe
}

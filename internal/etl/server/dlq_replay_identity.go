package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

type dlqReplayGateError struct {
	ID     int64
	State  core.DLQReplayState
	Reason string
}

type dlqIdentityRepairRequest struct {
	PrimaryKeyColumns []string `json:"primary_key_columns"`
}

func (e *dlqReplayGateError) Error() string {
	return fmt.Sprintf("dlq id %d replay blocked: state=%s reason=%s", e.ID, e.State, e.Reason)
}

// prepareDLQReplayIdentity enforces the replay identity contract for one row
// against the sink that will receive it. spec only needs Source/Sink for the
// linear path; the DAG path synthesises it from the target sink node, so both
// replay paths share exactly one gate instead of DAG replay bypassing it.
// persist=false is used by the replay dry-run: the gate still proves (or
// rejects) identity, but nothing is written to the DLQ row.
func (s *Server) prepareDLQReplayIdentity(ctx context.Context, spec *pipeline.Spec, item storage.DeadLetter, persist bool) (storage.DeadLetter, error) {
	if !pipeline.MetadataIdentityRequired(spec) {
		return item, nil
	}
	if item.IdentityContext.ReplayProvenance == core.DLQReplayProvenanceLegacyVerified {
		staticPK := configuredStringSlice(spec.Sink.Config["pk_columns"])
		if len(staticPK) == 0 || !sameStringSet(staticPK, item.IdentityContext.PrimaryKeyColumns) {
			return item, s.blockDLQReplay(ctx, item, core.DLQReplayStateQuarantined,
				"verified legacy provenance requires original primary_key_columns to exactly match sink.config.pk_columns", persist)
		}
	}

	identity := core.RecordIdentity(item.Record)
	if identity.Complete {
		changed := false
		if len(item.IdentityContext.PrimaryKeyColumns) == 0 {
			item.IdentityContext.PrimaryKeyColumns = append([]string(nil), item.Record.Metadata.PrimaryKeyColumns...)
			changed = true
		}
		if item.IdentityContext.FormatContractID == "" && item.Record.Metadata.FormatContractID != "" {
			item.IdentityContext.FormatContractID = item.Record.Metadata.FormatContractID
			changed = true
		}
		if item.IdentityContext.ReplayProvenance == core.DLQReplayProvenanceLegacyUnknown {
			// A complete declared key plus a complete UPDATE before image is
			// self-proving; no static fallback or current config inference occurs.
			item.IdentityContext.ReplayProvenance = core.DLQReplayProvenanceReconstructed
			item.IdentityContext.ReplayState = core.DLQReplayStatePending
			changed = true
		}
		item.Record.Metadata.ReplayProvenance = item.IdentityContext.ReplayProvenance
		if changed && persist {
			if err := s.dlqWriter.Update(ctx, item); err != nil {
				return item, fmt.Errorf("persist proven dlq identity: %w", err)
			}
		}
		return item, nil
	}

	if item.IdentityContext.ReplayProvenance == core.DLQReplayProvenanceLegacyUnknown {
		return item, s.blockDLQReplay(ctx, item, core.DLQReplayStateRepairRequired,
			"legacy row has no frozen identity provenance; repair it explicitly before replay", persist)
	}
	rebuilt, result := core.ReconstructDLQRecordIdentity(item.Record, item.IdentityContext)
	if !result.Complete {
		state := core.DLQReplayStateRepairRequired
		switch result.Reason {
		case core.RecordIdentityReasonFormatContractUnknown,
			core.RecordIdentityReasonBeforeStateInvalid,
			core.RecordIdentityReasonLegacyProvenanceInvalid,
			core.RecordIdentityReasonLegacyKeyChangeUnsupported:
			state = core.DLQReplayStateQuarantined
		}
		reason := string(result.Reason)
		if len(result.MissingColumns) > 0 {
			reason += " missing_columns=" + strings.Join(result.MissingColumns, ",")
		}
		return item, s.blockDLQReplay(ctx, item, state, reason, persist)
	}

	item.Record = rebuilt
	if item.IdentityContext.ReplayProvenance != core.DLQReplayProvenanceLegacyVerified {
		item.IdentityContext.ReplayProvenance = core.DLQReplayProvenanceReconstructed
	}
	item.IdentityContext.ReplayState = core.DLQReplayStatePending
	item.IdentityContext.ReplayError = ""
	item.Record.Metadata.ReplayProvenance = item.IdentityContext.ReplayProvenance
	if persist {
		if err := s.dlqWriter.Update(ctx, item); err != nil {
			return item, fmt.Errorf("persist reconstructed dlq identity: %w", err)
		}
	}
	return item, nil
}

// validateDLQReplayIdentity re-checks the identity contract after transforms
// ran, for both linear and DAG replay.
func (s *Server) validateDLQReplayIdentity(ctx context.Context, spec *pipeline.Spec, item storage.DeadLetter, record core.Record, persist bool) error {
	if !pipeline.MetadataIdentityRequired(spec) {
		return nil
	}
	identity := core.RecordIdentity(record)
	if identity.Complete {
		return nil
	}
	reason := string(identity.Reason)
	if len(identity.MissingColumns) > 0 {
		reason += " missing_columns=" + strings.Join(identity.MissingColumns, ",")
	}
	return s.blockDLQReplay(ctx, item, core.DLQReplayStateQuarantined,
		"transform output violates replay identity contract: "+reason, persist)
}

func (s *Server) blockDLQReplay(ctx context.Context, item storage.DeadLetter, state core.DLQReplayState, reason string, persist bool) error {
	item.IdentityContext.ReplayState = state
	item.IdentityContext.ReplayAttempt++
	item.IdentityContext.ReplayError = reason
	if persist {
		if err := s.dlqWriter.Update(ctx, item); err != nil {
			return fmt.Errorf("persist dlq replay block (%s): %w", reason, err)
		}
	}
	return &dlqReplayGateError{ID: item.ID, State: state, Reason: reason}
}

func (s *Server) markDLQReplayFailure(ctx context.Context, item storage.DeadLetter, replayErr error) {
	if item.ID == 0 || replayErr == nil {
		return
	}
	item.IdentityContext.ReplayAttempt++
	item.IdentityContext.ReplayError = replayErr.Error()
	// A persisted sink acknowledgement is a replay checkpoint. Preserve it on
	// cleanup failure so a retry cannot write the same item to the sink again.
	_ = s.dlqWriter.Update(ctx, item)
}

// checkpointDLQReplay durably records sink acknowledgement before the DLQ row
// can be deleted. If deletion then fails or the process crashes, the next
// replay request observes sink_acked and only completes cleanup; it does not
// write the record to the sink a second time.
func (s *Server) checkpointDLQReplay(ctx context.Context, item *storage.DeadLetter) error {
	if item == nil || item.ID == 0 {
		return nil
	}
	candidate := *item
	now := time.Now().UTC()
	candidate.IdentityContext.ReplayState = core.DLQReplayStateSinkAcked
	candidate.IdentityContext.ReplayAttempt++
	candidate.IdentityContext.ReplayError = ""
	candidate.IdentityContext.SinkAcknowledgedAt = &now
	if err := s.dlqWriter.Update(ctx, candidate); err != nil {
		return fmt.Errorf("persist dlq replay checkpoint after sink acknowledgement: %w", err)
	}
	*item = candidate
	return nil
}

// repairLegacyDLQIdentity promotes one historical row from legacy_unknown to
// legacy_verified. The caller must confirm the exact declaration already
// frozen in the historical Record; this endpoint cannot invent pkNames from
// today's pipeline configuration.
func (s *Server) repairLegacyDLQIdentity(ctx context.Context, name string, id int64, request dlqIdentityRepairRequest) (*storage.DeadLetter, error) {
	s.mu.RLock()
	spec := s.specs[name]
	dagSpec := s.dagSpecs[name]
	s.mu.RUnlock()
	if spec == nil && dagSpec == nil {
		return nil, fmt.Errorf("pipeline %s not found", name)
	}

	item, err := s.dlqWriter.ReadByID(ctx, name, id)
	if err != nil || item == nil {
		return item, err
	}

	// Resolve the sink(s) that will receive this row. Linear pipelines have one
	// sink; DAG pipelines resolve every metadata-PK sink reachable from the
	// failing node. Repair is therefore available to DAG rows too, instead of
	// the previous blanket refusal that left them permanently unreplayable.
	var identitySpecs []*pipeline.Spec
	if dagSpec != nil {
		identitySpecs = dagReplayIdentitySpecs(dagSpec, item.DAGNode)
		if len(identitySpecs) == 0 {
			return nil, fmt.Errorf("pipeline %s has no sink reachable from dag_node %q that enables sink.config.pk_columns_from_metadata", name, item.DAGNode)
		}
	} else {
		if !pipeline.MetadataIdentityRequired(spec) {
			return nil, fmt.Errorf("pipeline %s does not enable sink.config.pk_columns_from_metadata", name)
		}
		identitySpecs = []*pipeline.Spec{spec}
	}

	if item.IdentityContext.ReplayState == core.DLQReplayStateSinkAcked {
		return nil, &dlqReplayGateError{ID: item.ID, State: item.IdentityContext.ReplayState, Reason: "sink acknowledgement is already checkpointed; retry replay to complete DLQ cleanup"}
	}
	if item.IdentityContext.ReplayProvenance != core.DLQReplayProvenanceLegacyUnknown &&
		item.IdentityContext.ReplayProvenance != core.DLQReplayProvenanceLegacyVerified {
		return nil, &dlqReplayGateError{ID: item.ID, State: item.IdentityContext.ReplayState, Reason: "only legacy_unknown or legacy_verified rows accept controlled identity repair"}
	}

	originalPK := append([]string(nil), item.Record.Metadata.PrimaryKeyColumns...)
	if len(item.IdentityContext.PrimaryKeyColumns) > 0 {
		if len(originalPK) > 0 && !sameStringSet(originalPK, item.IdentityContext.PrimaryKeyColumns) {
			return nil, s.blockDLQReplay(ctx, *item, core.DLQReplayStateQuarantined,
				"historical record primary_key_columns conflict with frozen DLQ identity context", true)
		}
		originalPK = append(originalPK[:0], item.IdentityContext.PrimaryKeyColumns...)
	}
	confirmedPK := configuredStringSlice(request.PrimaryKeyColumns)
	if len(originalPK) == 0 {
		return nil, s.blockDLQReplay(ctx, *item, core.DLQReplayStateRepairRequired,
			"historical row has no original primary_key_columns declaration; current configuration cannot supply it", true)
	}
	if len(confirmedPK) == 0 || !sameStringSet(confirmedPK, originalPK) {
		return nil, s.blockDLQReplay(ctx, *item, core.DLQReplayStateQuarantined,
			"confirmed primary_key_columns must exactly match the declaration frozen in the historical record", true)
	}
	for _, identitySpec := range identitySpecs {
		staticPK := configuredStringSlice(identitySpec.Sink.Config["pk_columns"])
		if len(staticPK) == 0 || !sameStringSet(staticPK, originalPK) {
			return nil, s.blockDLQReplay(ctx, *item, core.DLQReplayStateQuarantined,
				"historical primary_key_columns must exactly match sink.config.pk_columns", true)
		}
	}

	candidate := *item
	candidate.IdentityContext.PrimaryKeyColumns = append([]string(nil), originalPK...)
	if candidate.IdentityContext.FormatContractID == "" {
		candidate.IdentityContext.FormatContractID = candidate.Record.Metadata.FormatContractID
	}
	if candidate.IdentityContext.BeforeImageState == "" {
		candidate.IdentityContext.BeforeImageState = candidate.Record.Metadata.BeforeImageState
	}
	candidate.IdentityContext.ReplayProvenance = core.DLQReplayProvenanceLegacyVerified
	candidate.IdentityContext.ReplayState = core.DLQReplayStatePending
	candidate.IdentityContext.ReplayError = ""
	candidate.IdentityContext.SinkAcknowledgedAt = nil
	rebuilt, result := core.ReconstructDLQRecordIdentity(candidate.Record, candidate.IdentityContext)
	if !result.Complete {
		reason := string(result.Reason)
		if len(result.MissingColumns) > 0 {
			reason += " missing_columns=" + strings.Join(result.MissingColumns, ",")
		}
		return nil, s.blockDLQReplay(ctx, *item, core.DLQReplayStateQuarantined,
			"legacy identity cannot be proven: "+reason, true)
	}
	candidate.Record = rebuilt
	candidate.Record.Metadata.ReplayProvenance = core.DLQReplayProvenanceLegacyVerified
	if err := s.dlqWriter.Update(ctx, candidate); err != nil {
		return nil, fmt.Errorf("persist repaired dlq identity: %w", err)
	}
	return &candidate, nil
}

func configuredStringSlice(value any) []string {
	var result []string
	switch typed := value.(type) {
	case []string:
		result = append(result, typed...)
	case []any:
		for _, item := range typed {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
	case string:
		for _, item := range strings.Split(typed, ",") {
			result = append(result, item)
		}
	}
	clean := result[:0]
	for _, item := range result {
		if item = strings.TrimSpace(item); item != "" {
			clean = append(clean, item)
		}
	}
	return clean
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	a := append([]string(nil), left...)
	b := append([]string(nil), right...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// dagReplayIdentitySpecs returns one synthetic linear spec per sink node
// reachable from nodeID whose config enables pk_columns_from_metadata. DAG
// replay must enforce the same identity contract as linear replay; before this
// helper it validated nothing at all, so a legacy_unknown or key-incomplete DAG
// row could be written to a metadata-PK sink without any proof, while the
// repair endpoint refused to help ("not supported for DAG pipelines").
func dagReplayIdentitySpecs(dagSpec *orchestrator.PipelineSpec, nodeID string) []*pipeline.Spec {
	if dagSpec == nil {
		return nil
	}
	start := strings.TrimSpace(nodeID)
	if start == "" || dagSpec.DAG.GetNode(start) == nil {
		return nil
	}
	// BFS over DAG edges to collect reachable nodes (the graph is small).
	reachable := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, edge := range dagSpec.DAG.Edges {
			if edge == nil || edge.From != current || reachable[edge.To] {
				continue
			}
			reachable[edge.To] = true
			queue = append(queue, edge.To)
		}
	}
	specs := make([]*pipeline.Spec, 0, 1)
	for _, node := range dagSpec.DAG.Nodes {
		if node == nil || !reachable[node.ID] {
			continue
		}
		if node.Kind != orchestrator.KindSink {
			continue
		}
		spec := &pipeline.Spec{Name: dagSpec.Name}
		spec.Sink.Type = node.Plugin
		spec.Sink.Config = node.Config
		if pipeline.MetadataIdentityRequired(spec) {
			specs = append(specs, spec)
		}
	}
	return specs
}

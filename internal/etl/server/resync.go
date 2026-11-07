package server

// Targeted table re-synchronisation for snapshot+CDC pipelines.
//
// A mysql_snapshot_cdc pipeline snapshots every configured table once and then
// follows the binlog. Until now the only way to recover a single drifted or
// truncated table was checkpoint/reset, which re-snapshots *every* table and
// re-delivers the whole database — an expensive and risky operation on a wide
// pipeline.
//
// The snapshot progress is already persisted per table (last_ids / last_strs /
// last_composite_cursors), so a targeted resync is a checkpoint rewrite:
//
//  1. keep the existing binlog anchor (file/pos) as the new snapshot handoff;
//  2. drop the cursor entries of the requested tables only;
//  3. set phase=snapshot so the next start re-enters the snapshot phase.
//
// Tables that keep their cursors are already past their last key and therefore
// emit nothing, so the cost is proportional to the re-synced tables only. Reusing
// the older binlog position as the handoff is deliberately conservative: the
// re-read rows are ordered at or before live CDC rows, so a versioned sink can
// never let re-snapshot data overwrite newer state.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/checkpoint"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

type resyncRequest struct {
	Table  string   `json:"table"`
	Tables []string `json:"tables"`
}

type resyncDetails struct {
	Tables       []string `json:"tables"`
	Phase        string   `json:"phase"`
	BinlogAnchor string   `json:"binlog_anchor"`
	ClearedKeys  []string `json:"cleared_cursor_keys,omitempty"`
	KeptTables   []string `json:"kept_tables,omitempty"`
}

// resyncSnapshotTables rewrites a mysql_snapshot_cdc checkpoint position so the
// named tables re-run their snapshot phase on the next start while every other
// table keeps its cursor. It preserves the surrounding checkpoint envelope
// (state versions and sink commit metadata) because only the source position
// changes.
func resyncSnapshotTables(position json.RawMessage, tables []string) (json.RawMessage, resyncDetails, error) {
	var details resyncDetails
	if len(strings.TrimSpace(string(position))) == 0 {
		return nil, details, fmt.Errorf("pipeline has no checkpoint yet; start it once so the snapshot phase persists its progress")
	}
	if !json.Valid(position) {
		return nil, details, fmt.Errorf("checkpoint position is not valid JSON")
	}
	if len(tables) == 0 {
		return nil, details, fmt.Errorf("at least one table is required")
	}

	inner := json.RawMessage(position)
	env, wrapped, err := checkpoint.ParseEnvelope(position)
	if err != nil {
		return nil, details, err
	}
	if wrapped {
		inner = env.Source
	}
	var pos map[string]any
	if err := json.Unmarshal(inner, &pos); err != nil {
		return nil, details, fmt.Errorf("decode mysql_snapshot_cdc position: %w", err)
	}
	if _, ok := pos["phase"]; !ok {
		return nil, details, fmt.Errorf("checkpoint is not a mysql_snapshot_cdc position (missing phase); use checkpoint/reset for a full re-snapshot")
	}
	file, _ := pos["file"].(string)
	posValue, _ := pos["pos"].(float64)
	if file == "" || posValue == 0 {
		return nil, details, fmt.Errorf("checkpoint has no binlog anchor (file/pos); a targeted resync needs one. Use checkpoint/reset for a full re-snapshot")
	}

	wanted := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		if t = strings.TrimSpace(t); t != "" {
			wanted[t] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return nil, details, fmt.Errorf("at least one non-empty table name is required")
	}

	for _, field := range []string{"last_ids", "last_strs", "last_composite_cursors"} {
		raw, ok := pos[field]
		if !ok || raw == nil {
			continue
		}
		entries, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		for table := range wanted {
			if _, exists := entries[table]; exists {
				delete(entries, table)
				details.ClearedKeys = append(details.ClearedKeys, field+"."+table)
			}
		}
		if len(entries) == 0 {
			delete(pos, field)
		} else {
			pos[field] = entries
		}
	}

	// Legacy single-table cursor: a mysql_snapshot_cdc spec with `table:` (no
	// `tables:`) restores last_id into last_ids[table] on load, which would
	// silently keep the old cursor. Clearing it lets the re-snapshot restart.
	if _, ok := pos["last_id"]; ok {
		pos["last_id"] = 0
	}

	for _, field := range []string{"last_ids", "last_strs", "last_composite_cursors"} {
		if entries, ok := pos[field].(map[string]any); ok {
			for table := range entries {
				details.KeptTables = append(details.KeptTables, table)
			}
		}
	}

	pos["phase"] = "snapshot"
	encoded, err := json.Marshal(pos)
	if err != nil {
		return nil, details, fmt.Errorf("marshal resynced position: %w", err)
	}
	out := json.RawMessage(encoded)
	if wrapped {
		out, err = checkpoint.BuildEnvelope(encoded, env.State, env.SinkCommit)
		if err != nil {
			return nil, details, err
		}
	}
	details.Tables = append([]string(nil), tables...)
	details.Phase = "snapshot"
	details.BinlogAnchor = fmt.Sprintf("%s:%d", file, int64(posValue))
	return out, details, nil
}

// resyncConfiguredTables validates the requested tables against the pipeline's
// configured table list.
func resyncConfiguredTables(spec *pipeline.Spec, req resyncRequest) ([]string, error) {
	requested := req.Tables
	if strings.TrimSpace(req.Table) != "" {
		requested = append(append([]string(nil), requested...), strings.TrimSpace(req.Table))
	}
	var deduped []string
	seen := map[string]struct{}{}
	for _, t := range requested {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		deduped = append(deduped, t)
	}
	if len(deduped) == 0 {
		return nil, fmt.Errorf("body must name at least one table: {\"table\":\"orders\"} or {\"tables\":[\"orders\",\"items\"]}")
	}
	configured := map[string]struct{}{}
	for _, t := range stringSlice(spec.Source.Config["tables"]) {
		configured[strings.TrimSpace(t)] = struct{}{}
	}
	if t := strings.TrimSpace(str(spec.Source.Config, "table")); t != "" {
		configured[t] = struct{}{}
	}
	for _, t := range deduped {
		if _, ok := configured[t]; !ok {
			return nil, fmt.Errorf("table %q is not part of this pipeline's source config", t)
		}
	}
	return deduped, nil
}

// handlePipelineResync serves POST /api/v2/pipelines/{id}/resync.
func (s *Server) handlePipelineResync(w http.ResponseWriter, r *http.Request, id string, spec *pipeline.Spec, runner pipeline.RunnerInterface) {
	if spec == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "resync requires POST with {\"table\":\"orders\"} or {\"tables\":[...]}"})
		return
	}
	if strings.ToLower(strings.TrimSpace(spec.Source.Type)) != "mysql_snapshot_cdc" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{
			"error": fmt.Sprintf("targeted resync is only supported for mysql_snapshot_cdc sources (this pipeline uses %q); other sources have no per-table snapshot cursor", spec.Source.Type),
		})
		return
	}
	var req resyncRequest
	if err := decodeOptionalJSONBody(r, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid body: " + err.Error()})
		return
	}
	tables, err := resyncConfiguredTables(spec, req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	cp, err := s.cpAdapter.Load(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("load checkpoint: %v", err)})
		return
	}
	if cp == nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "pipeline has no checkpoint yet; start it once so the snapshot phase persists per-table progress",
		})
		return
	}
	resynced, details, err := resyncSnapshotTables(cp.Position, tables)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	record := &storage.CheckpointRecord{
		JobName:   cp.JobName,
		Source:    cp.Source,
		Position:  resynced,
		Timestamp: time.Now(),
	}
	generation, err := s.setManagedCheckpoint(r.Context(), id, runner, record)
	if err != nil {
		if errors.Is(err, storage.ErrPipelineNotQuiescent) {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{
				"error":       fmt.Sprintf("resync: %v", err),
				"code":        "pipeline_not_quiescent",
				"remediation": "Stop or pause the pipeline and wait for it to quiesce before scheduling a table resync.",
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": fmt.Sprintf("resync: %v", err)})
		return
	}
	s.audit(r, "pipeline.resync_table", id)
	json.NewEncoder(w).Encode(map[string]any{
		"status":     "resync_scheduled",
		"generation": generation,
		"tables":     tables,
		"details":    details,
		"semantics": map[string]any{
			"effect":           "The named tables re-run the snapshot phase on the next start. Tables not named keep their cursor and emit nothing.",
			"sink_requirement": "The sink must absorb re-delivered rows (upsert / business key / version column). Plain-insert sinks will duplicate the re-synced rows.",
			"cdc_anchor":       "The existing binlog anchor is reused as the snapshot handoff, so re-read rows are never ordered after live CDC rows.",
			"checkpoint":       "The checkpoint phase is set to snapshot; the persisted progress of other tables is preserved.",
			"scope":            "Targeted per-table resync only applies to mysql_snapshot_cdc sources; use checkpoint/reset for a full re-snapshot.",
		},
	})
}

// Ensure the snapshot source's position contract stays in sync with this file.
var _ = core.SourcePhaseSnapshot

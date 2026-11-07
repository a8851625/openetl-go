package server

// Output compaction for manifest-enabled file / object-store sinks.
//
// A CDC pipeline with manifest:true commits one data object per batch, which
// quickly becomes thousands of small objects per day. POST
// /api/v2/pipelines/{id}/output/compact folds the committed objects of a day
// into one object and publishes a compaction manifest that retires the replaced
// keys — a metadata rewrite, not an atomic multi-object commit, and nothing is
// deleted, so the operation is reversible.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/sink"
)

// outputCompactTimeout bounds one compaction pass: it walks every committed
// object of the selected day, so it is slower than a reconcile.
const outputCompactTimeout = 10 * time.Minute

func writeOutputCompactError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": message})
}

type outputCompactRequest struct {
	Day            string `json:"day"`
	MinObjects     int    `json:"min_objects"`
	MaxObjects     int    `json:"max_objects"`
	MaxObjectBytes int64  `json:"max_object_bytes"`
	DryRun         bool   `json:"dry_run"`
}

func (s *Server) handlePipelineOutputCompact(w http.ResponseWriter, r *http.Request, id string, spec *pipeline.Spec) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "output/compact accepts POST"})
		return
	}
	if spec == nil {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}
	sinkType := strings.ToLower(strings.TrimSpace(spec.Sink.Type))
	if sinkType != "s3" && sinkType != "file_sink" {
		writeOutputCompactError(w, http.StatusBadRequest, fmt.Sprintf("output/compact applies to the s3 and file_sink sinks, got %q", spec.Sink.Type))
		return
	}
	if enabled, _ := spec.Sink.Config["manifest"].(bool); !enabled {
		writeOutputCompactError(w, http.StatusBadRequest, "sink.config.manifest must be true: without a commit boundary the compactor cannot distinguish committed objects from crash-window leftovers")
		return
	}

	var req outputCompactRequest
	if err := decodeOptionalJSONBody(r, &req); err != nil {
		writeOutputCompactError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	newSink := sink.NewSinkForCompact
	compactor, err := newSink(sinkType, spec.Sink.Config)
	if err != nil {
		writeOutputCompactError(w, http.StatusBadRequest, fmt.Sprintf("build %s sink for compaction: %v", sinkType, err))
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), outputCompactTimeout)
	defer cancel()
	result, err := compactor.CompactOutput(ctx, sink.CompactOptions{
		Day:            req.Day,
		MinObjects:     req.MinObjects,
		MaxObjects:     req.MaxObjects,
		MaxObjectBytes: req.MaxObjectBytes,
		DryRun:         req.DryRun,
	})
	if err != nil {
		writeOutputCompactError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.audit(r, "pipeline.output.compact", id)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

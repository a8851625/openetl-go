package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/google/uuid"
)

func validRecoveryNodeID(value string) bool {
	if value == "" || !utf8.ValidString(value) || utf8.RuneCountInString(value) > 255 || strings.ContainsAny(value, "/?#&=") {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 92 {
			return false
		}
	}
	return true
}

func (s *Server) handleRecoveryEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	recovery, ok := s.recoveryStore()
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": "recovery tracking unavailable"})
		return
	}
	if rest := strings.TrimPrefix(r.URL.Path, "/api/v2/recovery-events/"); r.URL.Path != "/api/v2/recovery-events" && rest != "" {
		id, err := uuid.Parse(rest)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid recovery id"})
			return
		}
		event, err := recovery.GetRecoveryEvent(r.Context(), id.String())
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if event == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "recovery record not found"})
			return
		}
		operations, err := recovery.ListRecoveryOperations(r.Context(), event.ID)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"event": event, "operations": operations})
		return
	}
	q := r.URL.Query()
	for key, values := range q {
		if len(values) != 1 || values[0] == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid " + key})
			return
		}
	}
	f := storage.RecoveryFilter{PipelineID: q.Get("pipeline_id"), State: q.Get("state"), Limit: 100}
	if f.PipelineID != "" {
		if id, err := uuid.Parse(f.PipelineID); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid pipeline id"})
			return
		} else {
			f.PipelineID = id.String()
		}
	}
	switch f.State {
	case "", "open", "replaying", "resolved", "abandoned", "unknown":
	default:
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid recovery state"})
		return
	}
	for key, target := range map[string]*int{"limit": &f.Limit, "offset": &f.Offset} {
		if value := q.Get(key); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 || (key == "limit" && (n == 0 || n > 500)) {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]any{"error": "invalid " + key})
				return
			}
			*target = n
		}
	}
	events, err := recovery.ListRecoveryEvents(r.Context(), f)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	unassociated, err := recovery.ListUnassociatedRecoveryOperations(r.Context(), f.PipelineID)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"events": events, "unassociated_operations": unassociated, "limit": f.Limit, "offset": f.Offset})
}

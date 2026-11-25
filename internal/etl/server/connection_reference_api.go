package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func (s *Server) connectionReferences(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	guard, ok := s.store.(storage.ConnectionReferenceStore)
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": "connection reference protection unavailable"})
		return
	}
	report, err := guard.ConnectionReferences(r.Context(), name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(report)
}

func writeConnectionReferenceConflict(w http.ResponseWriter, err error) bool {
	var conflict *storage.ConnectionReferenceConflict
	if !errors.As(err, &conflict) {
		return false
	}
	w.WriteHeader(http.StatusConflict)
	json.NewEncoder(w).Encode(map[string]any{
		"error": conflict.Message, "code": conflict.Code,
		"connection_name": conflict.Report.ConnectionName,
		"known":           conflict.Report.Known, "references": conflict.Report.References,
		"unreadable_specs": conflict.Report.Unreadable,
	})
	return true
}

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

type connectionTestRequest struct {
	Kind   string         `json:"kind"`
	Type   string         `json:"type"`
	Config map[string]any `json:"config"`
	Open   *bool          `json:"open,omitempty"`
}

type connectionSaveRequest struct {
	Name   string         `json:"name"`
	Kind   string         `json:"kind"`
	Type   string         `json:"type"`
	Config map[string]any `json:"config"`
	Test   bool           `json:"test,omitempty"`
	Open   *bool          `json:"open,omitempty"`
}

var connectionNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		conns, err := s.store.ListConnections(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		out := make([]*storage.ConnectionEntry, 0, len(conns))
		for _, c := range conns {
			out = append(out, maskConnection(c))
		}
		json.NewEncoder(w).Encode(map[string]any{"connections": out})
	case http.MethodPost:
		s.saveConnection(w, r, "")
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
	}
}

func (s *Server) handleConnectionAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	rest := strings.TrimPrefix(r.URL.Path, "/api/v2/connections/")
	if rest == "" || rest == "test" {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "connection not found"})
		return
	}
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	name := parts[0]
	if !connectionNamePattern.MatchString(name) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid connection name"})
		return
	}
	if len(parts) == 2 && parts[1] == "test" {
		s.testSavedConnection(w, r, name)
		return
	}
	if len(parts) == 2 && (parts[1] == "context" || parts[1] == "introspect") {
		s.connectionContext(w, r, name)
		return
	}
	if len(parts) != 1 {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "unknown connection action"})
		return
	}

	switch r.Method {
	case http.MethodGet:
		c, err := s.store.GetConnection(r.Context(), name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		if c == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "connection not found"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"connection": maskConnection(c)})
	case http.MethodPut:
		s.saveConnection(w, r, name)
	case http.MethodDelete:
		if err := s.store.DeleteConnection(r.Context(), name); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		s.audit(r, "connection.delete", name)
		json.NewEncoder(w).Encode(map[string]any{"ok": true})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
	}
}

func (s *Server) saveConnection(w http.ResponseWriter, r *http.Request, pathName string) {
	var req connectionSaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": "invalid body"})
		return
	}
	if pathName != "" {
		req.Name = pathName
	}
	if req.Config == nil {
		req.Config = map[string]any{}
	}
	if err := validateConnectionRequest(req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}

	// Preserve previously stored secrets when the UI resubmits GET-masked values.
	if existing, err := s.store.GetConnection(r.Context(), req.Name); err == nil && existing != nil {
		req.Config = preserveSecretConfig(req.Config, existing.Config)
	} else {
		req.Config = scrubSecretPlaceholders(req.Config)
	}

	conn := &storage.ConnectionEntry{
		Name:   req.Name,
		Kind:   req.Kind,
		Type:   req.Type,
		Config: req.Config,
	}
	if err := s.store.SaveConnection(r.Context(), conn); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	s.audit(r, "connection.save", req.Name)

	var testResult map[string]any
	if req.Test {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		result, status := s.runConnectionTest(ctx, connectionTestRequest{
			Kind: req.Kind, Type: req.Type, Config: req.Config, Open: req.Open,
		})
		testResult = result
		lastErr := ""
		health := "ok"
		if result["ok"] != true {
			health = "error"
			if msg, ok := result["error"].(string); ok {
				lastErr = msg
			}
		}
		_ = s.store.UpdateConnectionHealth(r.Context(), req.Name, health, lastErr, time.Now().UTC())
		if status != http.StatusOK {
			w.WriteHeader(status)
		}
	}

	saved, _ := s.store.GetConnection(r.Context(), req.Name)
	json.NewEncoder(w).Encode(map[string]any{
		"ok":         true,
		"connection": maskConnection(saved),
		"test":       testResult,
	})
}

func (s *Server) testSavedConnection(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	conn, err := s.store.GetConnection(r.Context(), name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if conn == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "connection not found"})
		return
	}
	var req struct {
		Open *bool `json:"open,omitempty"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, status := s.runConnectionTest(ctx, connectionTestRequest{
		Kind: conn.Kind, Type: conn.Type, Config: conn.Config, Open: req.Open,
	})
	health := "ok"
	lastErr := ""
	if result["ok"] != true {
		health = "error"
		if msg, ok := result["error"].(string); ok {
			lastErr = msg
		}
	}
	testedAt := time.Now().UTC()
	_ = s.store.UpdateConnectionHealth(r.Context(), name, health, lastErr, testedAt)
	s.audit(r, "connection.test", name)
	if status != http.StatusOK {
		w.WriteHeader(status)
	}
	result["connection"] = map[string]any{
		"name":           name,
		"last_status":    health,
		"last_error":     lastErr,
		"last_tested_at": testedAt,
	}
	json.NewEncoder(w).Encode(result)
}

func (s *Server) runConnectionTest(ctx context.Context, req connectionTestRequest) (map[string]any, int) {
	if req.Config == nil {
		req.Config = map[string]any{}
	}
	openPlugin := true
	if req.Open != nil {
		openPlugin = *req.Open
	}
	switch req.Kind {
	case "source":
		source, err := registry.BuildSource(req.Type, req.Config)
		if err != nil {
			return map[string]any{"ok": false, "stage": "build", "error": err.Error()}, http.StatusBadRequest
		}
		if openPlugin {
			reader, err := source.Open(ctx, nil)
			if err != nil {
				return map[string]any{"ok": false, "stage": "open", "error": err.Error()}, http.StatusBadRequest
			}
			var samples []map[string]any
			for i := 0; i < 5; i++ {
				rec, readErr := reader.Read(ctx)
				if readErr != nil {
					break
				}
				samples = append(samples, map[string]any{
					"operation": string(rec.Operation),
					"table":     rec.Metadata.Table,
					"key":       rec.Metadata.Key,
					"data":      rec.Data,
				})
			}
			_ = reader.Close()
			return map[string]any{"ok": true, "kind": req.Kind, "type": req.Type, "opened": openPlugin, "sample": samples, "count": len(samples)}, http.StatusOK
		}
	case "sink":
		sink, err := registry.BuildSink(req.Type, req.Config)
		if err != nil {
			return map[string]any{"ok": false, "stage": "build", "error": err.Error()}, http.StatusBadRequest
		}
		if openPlugin {
			if err := sink.Open(ctx); err != nil {
				return map[string]any{"ok": false, "stage": "open", "error": err.Error()}, http.StatusBadRequest
			}
			_ = sink.Close()
		}
	case "transform":
		if _, err := registry.BuildTransform(req.Type, req.Config); err != nil {
			return map[string]any{"ok": false, "stage": "build", "error": err.Error()}, http.StatusBadRequest
		}
	default:
		return map[string]any{"ok": false, "error": "kind must be source, sink, or transform"}, http.StatusBadRequest
	}
	return map[string]any{"ok": true, "kind": req.Kind, "type": req.Type, "opened": openPlugin}, http.StatusOK
}

func validateConnectionRequest(req connectionSaveRequest) error {
	if !connectionNamePattern.MatchString(req.Name) {
		return fmt.Errorf("invalid connection name")
	}
	if req.Kind != "source" && req.Kind != "sink" && req.Kind != "transform" {
		return fmt.Errorf("kind must be source, sink, or transform")
	}
	if strings.TrimSpace(req.Type) == "" {
		return fmt.Errorf("type is required")
	}
	return nil
}

func maskConnection(c *storage.ConnectionEntry) *storage.ConnectionEntry {
	if c == nil {
		return nil
	}
	out := *c
	out.Config = maskSecretMap(c.Config)
	return &out
}

package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SchemaEntry defines a registered data schema (table structure, field types, constraints).
type SchemaEntry struct {
	Name        string        `json:"name"`
	Description string        `json:"description,omitempty"`
	Fields      []SchemaField `json:"fields"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

// SchemaField describes a single column in a registered schema.
type SchemaField struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Nullable    bool   `json:"nullable"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// SchemaRegistry manages named schemas with file-based persistence and an
// in-memory cache for fast reads.
type SchemaRegistry struct {
	mu    sync.RWMutex
	dir   string
	cache map[string]*SchemaEntry
}

func NewSchemaRegistry(dir string) *SchemaRegistry {
	os.MkdirAll(dir, 0755)
	r := &SchemaRegistry{dir: dir, cache: make(map[string]*SchemaEntry)}
	r.loadAll()
	return r
}

func (sr *SchemaRegistry) loadAll() {
	entries, _ := os.ReadDir(sr.dir)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			name := e.Name()[:len(e.Name())-5]
			if entry, err := sr.load(name); err == nil {
				sr.cache[name] = entry
			}
		}
	}
}

func (sr *SchemaRegistry) load(name string) (*SchemaEntry, error) {
	path := filepath.Join(sr.dir, name+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entry SchemaEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (sr *SchemaRegistry) save(name string, entry *SchemaEntry) error {
	path := filepath.Join(sr.dir, name+".json")
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (sr *SchemaRegistry) Get(name string) (*SchemaEntry, bool) {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	if entry, ok := sr.cache[name]; ok {
		return entry, true
	}
	return nil, false
}

func (sr *SchemaRegistry) Put(name string, entry *SchemaEntry) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	now := time.Now()
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = now
	}
	entry.UpdatedAt = now
	entry.Name = name
	if err := sr.save(name, entry); err != nil {
		return err
	}
	sr.cache[name] = entry
	return nil
}

func (sr *SchemaRegistry) Delete(name string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	os.Remove(filepath.Join(sr.dir, name+".json"))
	delete(sr.cache, name)
}

func (sr *SchemaRegistry) List() []SchemaEntry {
	sr.mu.RLock()
	defer sr.mu.RUnlock()
	result := make([]SchemaEntry, 0, len(sr.cache))
	for _, entry := range sr.cache {
		result = append(result, *entry)
	}
	return result
}

// ── HTTP Handlers ───────────────────────────────────────────────────────

// handleSchemas lists all registered schemas.
func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		json.NewEncoder(w).Encode(map[string]any{
			"schemas": s.schemaRegistry.List(),
		})
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// handleSchemaAction handles GET/PUT/DELETE for a single schema.
func (s *Server) handleSchemaAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	name := strings.TrimPrefix(r.URL.Path, "/api/v2/schemas/")
	name = strings.TrimRight(name, "/")
	if name == "" {
		s.handleSchemas(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		entry, ok := s.schemaRegistry.Get(name)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "schema not found"})
			return
		}
		json.NewEncoder(w).Encode(entry)
	case http.MethodPut, http.MethodPost:
		var entry SchemaEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid schema: " + err.Error()})
			return
		}
		if err := s.schemaRegistry.Put(name, &entry); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "save schema: " + err.Error()})
			return
		}
		s.audit(r, "schema.create", name)
		json.NewEncoder(w).Encode(map[string]any{"status": "saved", "name": name})
	case http.MethodDelete:
		s.schemaRegistry.Delete(name)
		s.audit(r, "schema.delete", name)
		json.NewEncoder(w).Encode(map[string]any{"status": "deleted"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

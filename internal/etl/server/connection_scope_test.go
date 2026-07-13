package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func TestConnectionBehaviorFieldDeprecation(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	if err := s.store.SaveConnection(context.Background(), &storage.ConnectionEntry{
		Name:   "warehouse-mysql",
		Kind:   "sink",
		Type:   "mysql",
		Config: map[string]any{"host": "mysql", "user": "u", "database": "d", "batch_mode": "upsert", "pk_columns": []string{"id"}},
	}); err != nil {
		t.Fatalf("SaveConnection: %v", err)
	}

	spec := pipeline.Spec{
		Name: "uses-conn-behavior",
		Source: pipeline.SourceSpec{Type: testPlainPreflightSource, Config: map[string]any{}},
		Sink: pipeline.SinkSpec{
			Connection: "warehouse-mysql",
			Config:     map[string]any{"table": "orders"},
		},
	}
	body := mustPipelineJSON(t, spec)
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Valid    bool     `json:"valid"`
		Warnings []string `json:"warnings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Valid {
		t.Fatalf("expected valid spec")
	}
	foundBatch := false
	foundPK := false
	for _, w := range out.Warnings {
		if containsStr(w, "batch_mode") {
			foundBatch = true
		}
		if containsStr(w, "pk_columns") {
			foundPK = true
		}
	}
	if !foundBatch {
		t.Fatalf("expected deprecation warning for batch_mode, warnings=%v", out.Warnings)
	}
	if !foundPK {
		t.Fatalf("expected deprecation warning for pk_columns, warnings=%v", out.Warnings)
	}
}

func TestConnectionPureScopeNoDeprecation(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	if err := s.store.SaveConnection(context.Background(), &storage.ConnectionEntry{
		Name:   "pure-mysql",
		Kind:   "sink",
		Type:   "mysql",
		Config: map[string]any{"host": "mysql", "user": "u", "database": "d"},
	}); err != nil {
		t.Fatalf("SaveConnection: %v", err)
	}

	spec := pipeline.Spec{
		Name: "uses-pure-conn",
		Source: pipeline.SourceSpec{Type: testPlainPreflightSource, Config: map[string]any{}},
		Sink: pipeline.SinkSpec{
			Connection: "pure-mysql",
			Config:     map[string]any{"table": "orders", "batch_mode": "upsert", "pk_columns": []string{"id"}},
		},
	}
	body := mustPipelineJSON(t, spec)
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Valid    bool     `json:"valid"`
		Warnings []string `json:"warnings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !out.Valid {
		t.Fatalf("expected valid spec")
	}
	for _, w := range out.Warnings {
		if containsStr(w, "carries behavior field") {
			t.Fatalf("did not expect behavior-field deprecation, got: %s", w)
		}
	}
}

func TestConnectionBackwardCompatMergeStillWorks(t *testing.T) {
	s := newSchedulerTestServer(t)
	if err := s.store.SaveConnection(context.Background(), &storage.ConnectionEntry{
		Name:   "legacy-mysql",
		Kind:   "sink",
		Type:   "mysql",
		Config: map[string]any{"host": "mysql", "user": "u", "database": "d", "batch_mode": "upsert", "pk_columns": []string{"id"}},
	}); err != nil {
		t.Fatalf("SaveConnection: %v", err)
	}
	spec := &pipeline.Spec{
		Name:   "legacy",
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "x.jsonl", "format": "json"}},
		Sink: pipeline.SinkSpec{Connection: "legacy-mysql", Config: map[string]any{"table": "orders"}},
	}
	if err := s.resolvePipelineConnections(context.Background(), spec); err != nil {
		t.Fatalf("resolvePipelineConnections: %v", err)
	}
	// Behavior fields should still be merged for backward compat.
	if spec.Sink.Config["batch_mode"] != "upsert" {
		t.Fatalf("batch_mode not merged: %#v", spec.Sink.Config)
	}
	if _, ok := spec.Sink.Config["pk_columns"]; !ok {
		t.Fatalf("pk_columns not merged: %#v", spec.Sink.Config)
	}
	// Deprecation warnings should have been recorded.
	warns := s.connDeprecations.drain()
	if len(warns) == 0 {
		t.Fatalf("expected deprecation warnings")
	}
}

func TestFieldScopeAnnotationOnDescriptors(t *testing.T) {
	var mysqlSink *ConnectorDescriptor
	for i := range connectorDescriptors() {
		d := connectorDescriptors()[i]
		if d.Kind == "sink" && d.Type == "mysql" {
			mysqlSink = &d
			break
		}
	}
	if mysqlSink == nil {
		t.Fatal("mysql sink descriptor missing")
	}
	var hostScope, batchScope string
	for _, f := range mysqlSink.Fields {
		switch f.Name {
		case "host":
			hostScope = f.Scope
		case "batch_mode":
			batchScope = f.Scope
		}
	}
	if hostScope != FieldScopeConnection {
		t.Fatalf("host scope = %q, want connection", hostScope)
	}
	if batchScope != FieldScopeBehavior {
		t.Fatalf("batch_mode scope = %q, want behavior", batchScope)
	}
}

// containsStr is a small helper to avoid importing strings just for one call.
func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
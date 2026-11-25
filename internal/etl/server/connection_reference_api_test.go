package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func TestConnectionReferenceAPIUsesCurrentPersistedSpecs(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	ctx := context.Background()
	if err := s.store.SaveConnection(ctx, &storage.ConnectionEntry{
		Name: "protected", Kind: "sink", Type: "mysql", Config: map[string]any{"password": "never-in-reference-response"},
	}); err != nil {
		t.Fatal(err)
	}
	row := &storage.PipelineRow{ID: "persisted-only", Name: "saved display name", Status: "stopped",
		SpecYAML: "dag:\n  nodes:\n    - {id: target, kind: sink, plugin: mysql, connection: protected}\n"}
	if err := s.store.SavePipeline(ctx, row); err != nil {
		t.Fatal(err)
	}
	// Deliberately do not register a runtime: the database is authoritative.
	resp, err := http.Get(ts.URL + "/api/v2/connections/protected/references")
	if err != nil {
		t.Fatal(err)
	}
	var report storage.ConnectionReferenceReport
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !report.Known || len(report.References) != 1 || report.References[0].NodeID != "target" {
		t.Fatalf("report = %+v, status=%d", report, resp.StatusCode)
	}
	data, _ := json.Marshal(report)
	if strings.Contains(string(data), "never-in-reference-response") {
		t.Fatal("reference report exposed a secret")
	}
	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/api/v2/connections/protected", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var conflict struct {
		Code       string                        `json:"code"`
		References []storage.ConnectionReference `json:"references"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&conflict); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict || conflict.Code != "connection_in_use" || len(conflict.References) != 1 {
		t.Fatalf("delete status=%d body=%+v", resp.StatusCode, conflict)
	}
	row.SpecYAML = "name: repaired\n"
	if err := s.store.SavePipeline(ctx, row); err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dereferenced delete status=%d", resp.StatusCode)
	}
}

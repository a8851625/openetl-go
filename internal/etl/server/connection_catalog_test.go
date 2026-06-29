package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestConnectionCatalogCRUDAndHealth(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	body, err := json.Marshal(map[string]any{
		"name": "identity-dev",
		"kind": "transform",
		"type": "identity",
		"config": map[string]any{
			"api_token": "secret-value",
			"nested": map[string]any{
				"password": "pw",
			},
		},
		"test": true,
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(ts.URL+"/api/v2/connections", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST connection: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d, want 200", resp.StatusCode)
	}
	var created map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	conn := created["connection"].(map[string]any)
	cfg := conn["config"].(map[string]any)
	if cfg["api_token"] != "******" {
		t.Fatalf("secret not masked: %#v", cfg)
	}
	nested := cfg["nested"].(map[string]any)
	if nested["password"] != "******" {
		t.Fatalf("nested secret not masked: %#v", nested)
	}

	testResp, err := http.Post(ts.URL+"/api/v2/connections/identity-dev/test", "application/json", bytes.NewReader([]byte(`{"open":false}`)))
	if err != nil {
		t.Fatalf("POST saved test: %v", err)
	}
	defer testResp.Body.Close()
	if testResp.StatusCode != http.StatusOK {
		t.Fatalf("test status = %d, want 200", testResp.StatusCode)
	}
	var tested map[string]any
	if err := json.NewDecoder(testResp.Body).Decode(&tested); err != nil {
		t.Fatalf("decode test: %v", err)
	}
	if tested["ok"] != true {
		t.Fatalf("test result = %#v", tested)
	}

	listResp, err := http.Get(ts.URL + "/api/v2/connections")
	if err != nil {
		t.Fatalf("GET connections: %v", err)
	}
	defer listResp.Body.Close()
	var listed struct {
		Connections []map[string]any `json:"connections"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Connections) != 1 || listed.Connections[0]["last_status"] != "ok" {
		t.Fatalf("unexpected list: %#v", listed.Connections)
	}

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/api/v2/connections/identity-dev", nil)
	if err != nil {
		t.Fatalf("delete request: %v", err)
	}
	delResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE connection: %v", err)
	}
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", delResp.StatusCode)
	}
}

func TestPipelineUsesSavedConnections(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "demo-source",
		"kind": "source",
		"type": "demo",
		"config": map[string]any{
			"interval_ms": 0,
			"count":       1,
			"fields": []map[string]any{
				{"name": "id", "type": "counter"},
			},
		},
	})
	outDir := t.TempDir()
	saveTestConnection(t, ts.URL, map[string]any{
		"name":   "file-target",
		"kind":   "sink",
		"type":   "file_sink",
		"config": map[string]any{"output_dir": outDir, "format": "json"},
	})

	spec := pipeline.Spec{
		Name:   "connection-ref-linear",
		Source: pipeline.SourceSpec{Connection: "demo-source", Config: map[string]any{"count": 2}},
		Sink:   pipeline.SinkSpec{ConnectionRef: "file-target"},
	}
	body := mustPipelineJSON(t, spec)
	resp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST pipeline: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var got map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&got)
		t.Fatalf("create status = %d, body=%#v", resp.StatusCode, got)
	}
	var created map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("create response missing id: %#v", created)
	}

	s.mu.RLock()
	resolved := s.specs[id]
	s.mu.RUnlock()
	if resolved == nil {
		t.Fatal("pipeline was not created")
	}
	if resolved.Source.Type != "demo" || resolved.Sink.Type != "file_sink" {
		t.Fatalf("connection types not resolved: source=%q sink=%q", resolved.Source.Type, resolved.Sink.Type)
	}
	if resolved.Source.Config["interval_ms"] != float64(0) || resolved.Source.Config["count"] != float64(2) {
		t.Fatalf("source config not merged with override: %#v", resolved.Source.Config)
	}
	if resolved.Sink.Config["output_dir"] != outDir || resolved.Sink.Config["format"] != "json" {
		t.Fatalf("sink config not merged: %#v", resolved.Sink.Config)
	}
}

func TestDAGValidateResolvesSavedConnections(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "dag-demo-source",
		"kind": "source",
		"type": "demo",
		"config": map[string]any{
			"interval_ms": 0,
			"count":       1,
		},
	})
	saveTestConnection(t, ts.URL, map[string]any{
		"name":   "dag-file-target",
		"kind":   "sink",
		"type":   "file_sink",
		"config": map[string]any{"output_dir": t.TempDir()},
	})

	spec := map[string]any{
		"name": "connection-ref-dag",
		"dag": map[string]any{
			"nodes": []map[string]any{
				{"id": "src", "kind": "source", "connection": "dag-demo-source", "config": map[string]any{"count": 3}},
				{"id": "snk", "kind": "sink", "connection_ref": "dag-file-target"},
			},
			"edges": []map[string]any{{"from": "src", "to": "snk"}},
		},
	}
	body, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal validate request: %v", err)
	}
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST validate: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("validate status = %d", resp.StatusCode)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode validate: %v", err)
	}
	if got["valid"] != true {
		t.Fatalf("validate response = %#v", got)
	}
	resolved := got["spec"].(map[string]any)
	dag := resolved["dag"].(map[string]any)
	nodes := dag["nodes"].([]any)
	src := nodes[0].(map[string]any)
	if src["plugin"] != "demo" {
		t.Fatalf("source plugin not resolved: %#v", src)
	}
	srcCfg := src["config"].(map[string]any)
	if srcCfg["interval_ms"] != float64(0) || srcCfg["count"] != float64(3) {
		t.Fatalf("source config not merged with override: %#v", srcCfg)
	}
	snk := nodes[1].(map[string]any)
	if snk["plugin"] != "file_sink" {
		t.Fatalf("sink plugin not resolved: %#v", snk)
	}
}

func TestConnectionContextIncludesRecommendationsAndDescriptor(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	saveTestConnection(t, ts.URL, map[string]any{
		"name": "demo-context",
		"kind": "source",
		"type": "demo",
		"config": map[string]any{
			"fields": []map[string]any{
				{"name": "id", "type": "counter"},
				{"name": "name", "type": "string"},
			},
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/demo-context/context")
	if err != nil {
		t.Fatalf("GET connection context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Descriptor *struct {
			Kind string `json:"kind"`
			Type string `json:"type"`
		} `json:"descriptor"`
		Recommendations []map[string]any `json:"recommendations"`
		Introspection   struct {
			OK     bool             `json:"ok"`
			Schema []map[string]any `json:"schema"`
			Sample []map[string]any `json:"sample"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode context: %v", err)
	}
	if got.Descriptor == nil || got.Descriptor.Kind != "source" || got.Descriptor.Type != "demo" {
		t.Fatalf("descriptor not returned: %#v", got.Descriptor)
	}
	if !got.Introspection.OK || len(got.Introspection.Schema) != 2 || len(got.Introspection.Sample) != 1 {
		t.Fatalf("unexpected demo introspection: %#v", got.Introspection)
	}
	if len(got.Recommendations) == 0 {
		t.Fatalf("expected recommendations, got none")
	}
}

func TestConnectionContextIntrospectsFileSource(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "orders.csv")
	if err := os.WriteFile(path, []byte("id,name\n1,Alice\n2,Bob\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	saveTestConnection(t, ts.URL, map[string]any{
		"name": "file-context",
		"kind": "source",
		"type": "file",
		"config": map[string]any{
			"path":       path,
			"format":     "csv",
			"has_header": true,
		},
	})

	resp, err := http.Get(ts.URL + "/api/v2/connections/file-context/context")
	if err != nil {
		t.Fatalf("GET file context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("context status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Introspection struct {
			OK     bool `json:"ok"`
			Schema []struct {
				Name string `json:"name"`
			} `json:"schema"`
			Sample []struct {
				Data map[string]any `json:"data"`
			} `json:"sample"`
		} `json:"introspection"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode file context: %v", err)
	}
	if !got.Introspection.OK {
		t.Fatalf("file introspection failed: %#v", got.Introspection)
	}
	if len(got.Introspection.Schema) != 2 || got.Introspection.Schema[0].Name != "id" || got.Introspection.Schema[1].Name != "name" {
		t.Fatalf("unexpected schema: %#v", got.Introspection.Schema)
	}
	if len(got.Introspection.Sample) != 2 || got.Introspection.Sample[0].Data["name"] != "Alice" {
		t.Fatalf("unexpected sample: %#v", got.Introspection.Sample)
	}
}

func saveTestConnection(t *testing.T, baseURL string, body map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal connection: %v", err)
	}
	resp, err := http.Post(baseURL+"/api/v2/connections", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST connection: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var got map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&got)
		t.Fatalf("save connection status = %d, body=%#v", resp.StatusCode, got)
	}
}

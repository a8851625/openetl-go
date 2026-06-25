package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

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

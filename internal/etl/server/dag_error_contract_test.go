package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

func invalidDAGConnectorSpec() map[string]any {
	return map[string]any{
		"name": "invalid-dag-connector",
		"dag": map[string]any{
			"nodes": []any{
				map[string]any{
					"id":     "src",
					"kind":   "source",
					"plugin": "does_not_exist",
					"config": map[string]any{},
				},
				map[string]any{
					"id":     "sink",
					"kind":   "sink",
					"plugin": "file_sink",
					"config": map[string]any{"output_dir": "/tmp/openetl-dag-test"},
				},
			},
			"edges": []any{map[string]any{"from": "src", "to": "sink"}},
		},
	}
}

func postDAGContractRequest(t *testing.T, tsURL, method, path string, spec map[string]any) (*http.Response, map[string]any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req, err := http.NewRequest(method, tsURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response (status=%d): %v", resp.StatusCode, err)
	}
	return resp, result
}

func TestDAGValidationRejectsConnectorBuilderErrors(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	resp, result := postDAGContractRequest(t, ts.URL, http.MethodPost, "/api/v2/specs/validate", invalidDAGConnectorSpec())
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("validate status = %d, want 400 (body=%#v)", resp.StatusCode, result)
	}
	if valid, ok := result["valid"].(bool); !ok || valid {
		t.Fatalf("validate valid = %#v, want false", result["valid"])
	}
	if len(result["errors"].([]any)) == 0 {
		t.Fatalf("validate errors = %#v, want connector error", result["errors"])
	}
	fieldIssues, ok := result["field_issues"].([]any)
	if !ok || len(fieldIssues) == 0 {
		t.Fatalf("validate field_issues = %#v, want node field issue", result["field_issues"])
	}
	issue, ok := fieldIssues[0].(map[string]any)
	if !ok || issue["field"] != "dag.nodes[src].plugin" || issue["node"] != "src" {
		t.Fatalf("first field issue = %#v, want src plugin context", fieldIssues[0])
	}
}

func TestDAGCreateAndUpdateRejectConnectorBuilderErrorsWithNon2xx(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	createResp, createResult := postDAGContractRequest(t, ts.URL, http.MethodPost, "/api/v2/pipelines", invalidDAGConnectorSpec())
	if createResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("create status = %d, want 400 (body=%#v)", createResp.StatusCode, createResult)
	}

	updateResp, updateResult := postDAGContractRequest(t, ts.URL, http.MethodPut, "/api/v2/pipelines", invalidDAGConnectorSpec())
	if updateResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("update status = %d, want 400 (body=%#v)", updateResp.StatusCode, updateResult)
	}

	listResp, err := http.Get(ts.URL + "/api/v2/pipelines")
	if err != nil {
		t.Fatalf("list pipelines: %v", err)
	}
	defer listResp.Body.Close()
	var listed struct {
		Pipelines []struct {
			Name string `json:"name"`
		} `json:"pipelines"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode pipelines: %v", err)
	}
	for _, p := range listed.Pipelines {
		if p.Name == "invalid-dag-connector" {
			t.Fatalf("invalid DAG was persisted despite failed create/update")
		}
	}
}

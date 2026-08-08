package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func pipelineContractRequest(t *testing.T, tsURL, method, path string, payload any) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(payload)
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
		t.Fatalf("request %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode response status=%d body=%s: %v", resp.StatusCode, raw, err)
	}
	return resp.StatusCode, decoded
}

func pipelineContractIssues(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, ok := body["issues"].([]any)
	if !ok {
		t.Fatalf("response has no structured issues: %#v", body)
	}
	issues := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		issue, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("issue has unexpected shape: %#v", item)
		}
		issues = append(issues, issue)
	}
	return issues
}

func pipelineContractIssue(t *testing.T, issues []map[string]any, code string) map[string]any {
	t.Helper()
	for _, issue := range issues {
		if issue["code"] == code {
			return issue
		}
	}
	t.Fatalf("structured issue %q not found in %#v", code, issues)
	return nil
}

func invalidDAGContractSpec() map[string]any {
	return map[string]any{
		"name": "invalid-dag-contract",
		"dag": map[string]any{
			"nodes": []map[string]any{
				{"id": "source-orders", "kind": "source", "plugin": "missing_source_connector", "config": map[string]any{}},
				{"id": "sink-file", "kind": "sink", "plugin": "file_sink", "config": map[string]any{"output_dir": "/tmp/openetl-contract", "format": "jsonl"}},
			},
			"edges": []map[string]any{{"from": "source-orders", "to": "sink-file"}},
		},
	}
}

func TestDAGValidationContractRejectsUnknownConnector(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	status, body := pipelineContractRequest(t, ts.URL, http.MethodPost, "/api/v2/specs/validate", map[string]any{"spec": invalidDAGContractSpec()})
	if status < http.StatusBadRequest {
		t.Fatalf("DAG validate status = %d, want non-2xx; body=%#v", status, body)
	}
	if valid, _ := body["valid"].(bool); valid {
		t.Fatalf("DAG validate valid=true: %#v", body)
	}
	issue := pipelineContractIssue(t, pipelineContractIssues(t, body), "unknown_connector")
	if issue["node_id"] != "source-orders" || issue["plugin"] != "missing_source_connector" {
		t.Fatalf("unknown connector issue lost node/plugin context: %#v", issue)
	}
	if issue["field"] != "dag.nodes.source-orders.plugin" || issue["remediation"] == "" {
		t.Fatalf("unknown connector issue lacks field/remediation: %#v", issue)
	}
}

func TestDAGCreateAndUpdateContractDoNotPersistInvalidPipeline(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	status, body := pipelineContractRequest(t, ts.URL, http.MethodPost, "/api/v2/pipelines", map[string]any{"spec": invalidDAGContractSpec()})
	if status < http.StatusBadRequest {
		t.Fatalf("DAG create status = %d, want non-2xx; body=%#v", status, body)
	}
	pipelineContractIssue(t, pipelineContractIssues(t, body), "unknown_connector")

	status, body = pipelineContractRequest(t, ts.URL, http.MethodPut, "/api/v2/pipelines", map[string]any{
		"id":   "missing-pipeline",
		"spec": invalidDAGContractSpec(),
	})
	if status < http.StatusBadRequest {
		t.Fatalf("DAG update status = %d, want non-2xx; body=%#v", status, body)
	}
	pipelineContractIssue(t, pipelineContractIssues(t, body), "unknown_connector")

	rows, err := s.store.ListPipelines(context.Background())
	if err != nil {
		t.Fatalf("list pipelines: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("invalid DAG operation persisted %d pipeline rows: %#v", len(rows), rows)
	}
}

func TestLinearValidationContractProvidesFieldIssue(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	_ = s

	spec := map[string]any{
		"name":   "invalid-linear-contract",
		"source": map[string]any{"type": "missing_source_connector", "config": map[string]any{}},
		"sink":   map[string]any{"type": "file_sink", "config": map[string]any{"output_dir": "/tmp/openetl-contract", "format": "jsonl"}},
	}
	status, body := pipelineContractRequest(t, ts.URL, http.MethodPost, "/api/v2/specs/validate", map[string]any{"spec": spec})
	if status < http.StatusBadRequest {
		t.Fatalf("linear validate status = %d, want non-2xx; body=%#v", status, body)
	}
	issue := pipelineContractIssue(t, pipelineContractIssues(t, body), "unknown_connector")
	if issue["field"] != "source.type" || issue["scope"] != "source" {
		t.Fatalf("linear issue lacks source field context: %#v", issue)
	}
}

func TestValidateContractRetainsPreflightFieldRemediation(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := map[string]any{
		"name": "preflight-contract",
		"source": map[string]any{
			"type": "postgres_cdc",
			"config": map[string]any{
				"port": 0, "slot_name": "bad slot", "sslmode": "tls",
				"enable_snapshot": true, "tables": []string{"", ".orders"},
			},
		},
		"sink": map[string]any{"type": testSchemaPreflightSink, "config": map[string]any{}},
	}
	status, body := pipelineContractRequest(t, ts.URL, http.MethodPost, "/api/v2/specs/validate", map[string]any{"spec": spec})
	if status < http.StatusBadRequest {
		t.Fatalf("preflight validate status = %d, want non-2xx; body=%#v", status, body)
	}
	issues := pipelineContractIssues(t, body)
	fieldFound := false
	for _, issue := range issues {
		field, _ := issue["field"].(string)
		if strings.HasPrefix(field, "source.config.") {
			fieldFound = true
			if issue["remediation"] == "" {
				t.Fatalf("preflight field issue lost remediation: %#v", issue)
			}
		}
	}
	if !fieldFound {
		t.Fatalf("preflight response has no source field issue: %#v", issues)
	}
}

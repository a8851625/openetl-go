//go:build !nolua

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestTransformDryRunSupportsFlatMapOutputs(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	body := map[string]any{
		"transforms": []map[string]any{{
			"type": "flat_map",
			"config": map[string]any{
				"script": `return {
  { data = { id = record.data.id, idx = 1 } },
  { data = { id = record.data.id, idx = 2 } },
}`,
			},
		}},
		"record": core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": "order-1"},
			Metadata:  core.Metadata{Source: "ui", Table: "sample"},
		},
	}
	resp := postJSON(t, ts.URL+"/api/v2/transforms/dry-run", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var got struct {
		Filtered    bool          `json:"filtered"`
		OutputCount int           `json:"output_count"`
		Record      core.Record   `json:"record"`
		Records     []core.Record `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Filtered || got.OutputCount != 2 || len(got.Records) != 2 {
		t.Fatalf("response = %#v, want 2 unfiltered outputs", got)
	}
	if got.Record.Data["idx"] != float64(1) || got.Records[1].Data["idx"] != float64(2) {
		t.Fatalf("records = %#v, want idx 1 and 2", got.Records)
	}
}

func TestTransformDryRunReportsFlatMapPartialErrors(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	body := map[string]any{
		"transforms": []map[string]any{{
			"type": "flat_map",
			"config": map[string]any{
				"script": `error("bad payload")`,
			},
		}},
		"record": core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": "bad-1"},
			Metadata:  core.Metadata{Source: "ui", Table: "sample"},
		},
	}
	resp := postJSON(t, ts.URL+"/api/v2/transforms/dry-run", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var got struct {
		Filtered     bool `json:"filtered"`
		OutputCount  int  `json:"output_count"`
		PartialError bool `json:"partial_error"`
		Errors       []struct {
			Error      string      `json:"error"`
			ErrorClass string      `json:"error_class"`
			Record     core.Record `json:"record"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !got.Filtered || got.OutputCount != 0 || !got.PartialError || len(got.Errors) != 1 {
		t.Fatalf("response = %#v, want filtered partial error", got)
	}
	if got.Errors[0].ErrorClass != string(core.ErrorClassData) || got.Errors[0].Record.Data["id"] != "bad-1" {
		t.Fatalf("errors = %#v", got.Errors)
	}
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

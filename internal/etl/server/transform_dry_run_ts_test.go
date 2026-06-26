//go:build cgo

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestTransformDryRunSupportsJavaScriptArrayOutputs(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	body := map[string]any{
		"transforms": []map[string]any{{
			"type": "javascript",
			"config": map[string]any{
				"script": `
function transform(record) {
  return [
    { data: { id: record.data.id, idx: 1 } },
    { data: { id: record.data.id, idx: 2 } }
  ];
}
`,
			},
		}},
		"record": core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": "order-1"},
			Metadata:  core.Metadata{Source: "ui", Table: "sample"},
		},
	}
	resp := postJSONForTS(t, ts.URL+"/api/v2/transforms/dry-run", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var got struct {
		Filtered    bool          `json:"filtered"`
		OutputCount int           `json:"output_count"`
		Records     []core.Record `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Filtered || got.OutputCount != 2 || len(got.Records) != 2 {
		t.Fatalf("response = %#v, want 2 unfiltered outputs", got)
	}
	if got.Records[0].Data["idx"] != float64(1) || got.Records[1].Data["idx"] != float64(2) {
		t.Fatalf("records = %#v, want idx 1 and 2", got.Records)
	}
}

func postJSONForTS(t *testing.T, url string, body any) *http.Response {
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

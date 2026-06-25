package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

func TestPipelineScheduleAPI(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "input.jsonl")
	if err := os.WriteFile(srcPath, []byte(`{"id":1}`+"\n"), 0644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	spec := pipeline.Spec{
		Name:   "schedule-api-pipe",
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": srcPath, "format": "json"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": filepath.Join(dir, "out")}},
	}
	resp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(mustPipelineJSON(t, spec)))
	if err != nil {
		t.Fatalf("create pipeline: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d, want 200", resp.StatusCode)
	}

	body := bytes.NewReader([]byte(`{"type":"cron","cron":"*/5 * * * *"}`))
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/pipelines/schedule-api-pipe/schedule", body)
	if err != nil {
		t.Fatalf("new put: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put schedule: %v", err)
	}
	defer putResp.Body.Close()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("put status = %d, want 200", putResp.StatusCode)
	}

	getResp, err := http.Get(ts.URL + "/api/v2/pipelines/schedule-api-pipe/schedule")
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	defer getResp.Body.Close()
	var got struct {
		Enabled  bool `json:"enabled"`
		Schedule struct {
			Type string `json:"type"`
			Cron string `json:"cron"`
		} `json:"schedule"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode schedule: %v", err)
	}
	if !got.Enabled || got.Schedule.Type != "cron" || got.Schedule.Cron != "*/5 * * * *" {
		t.Fatalf("unexpected schedule: %+v", got)
	}

	delReq, err := http.NewRequest(http.MethodDelete, ts.URL+"/api/v2/pipelines/schedule-api-pipe/schedule", nil)
	if err != nil {
		t.Fatalf("new delete: %v", err)
	}
	delResp, err := http.DefaultClient.Do(delReq)
	if err != nil {
		t.Fatalf("delete schedule: %v", err)
	}
	defer delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", delResp.StatusCode)
	}
}

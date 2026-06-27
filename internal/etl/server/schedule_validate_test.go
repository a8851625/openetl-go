package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

func TestSpecValidateRejectsUnsupportedSourceSchedule(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name:                  "bad-source-schedule",
		Source:                pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{"host": "localhost", "user": "root", "database": "test"}},
		Sink:                  pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"host": "localhost", "user": "root", "database": "test", "table": "orders"}},
		Schedule:              &pipeline.ScheduleConfig{Type: "cron", Cron: "0 * * * * *"},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		BackpressureBuffer:    1,
		Retry:                 &pipeline.RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
	}
	body, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST validate: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var got struct {
		Valid  bool     `json:"valid"`
		Errors []string `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Valid || len(got.Errors) == 0 || !strings.Contains(got.Errors[0], "does not support schedule.type") {
		t.Fatalf("response = %#v, want unsupported schedule error", got)
	}
}

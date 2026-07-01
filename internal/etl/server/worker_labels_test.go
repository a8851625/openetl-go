package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

func TestReadWorkerLabels(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		env  string
		want map[string]string
	}{
		{"unset", "", nil},
		{"empty", "  ", nil},
		{"single kv", "gpu=true", map[string]string{"gpu": "true"}},
		{"multi kv", "gpu=true,zone=us-east-1", map[string]string{"gpu": "true", "zone": "us-east-1"}},
		{"json object", `{"gpu":"true","zone":"x"}`, map[string]string{"gpu": "true", "zone": "x"}},
		{"spaces trimmed", " gpu = true , zone = x ", map[string]string{"gpu": "true", "zone": "x"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("ETL_WORKER_LABELS", c.env)
			got := readWorkerLabels(ctx)
			if len(got) != len(c.want) {
				t.Fatalf("readWorkerLabels(%q) = %v (len %d), want %v (len %d)", c.env, got, len(got), c.want, len(c.want))
			}
			for k, v := range c.want {
				if got[k] != v {
					t.Errorf("readWorkerLabels(%q)[%q] = %q, want %q", c.env, k, got[k], v)
				}
			}
		})
	}
}

func TestReadWorkerLabelsInvalidJSONFallsBackToNil(t *testing.T) {
	t.Setenv("ETL_WORKER_LABELS", "{not-json")
	if got := readWorkerLabels(context.Background()); got != nil {
		t.Errorf("readWorkerLabels invalid JSON = %v, want nil", got)
	}
}

func TestReadWorkerLabelsIgnoresBadPairs(t *testing.T) {
	t.Setenv("ETL_WORKER_LABELS", "gpu=true,badpair,=empty,zone=x")
	got := readWorkerLabels(context.Background())
	// gpu and zone should survive; "badpair" (no =) and "=empty" (empty key) dropped.
	if got["gpu"] != "true" || got["zone"] != "x" {
		t.Errorf("readWorkerLabels = %v, want gpu=true and zone=x present", got)
	}
	if _, ok := got["badpair"]; ok {
		t.Errorf("badpair should have been dropped: %v", got)
	}
}

func TestReadWorkerLabelsEmptyValueAllowed(t *testing.T) {
	t.Setenv("ETL_WORKER_LABELS", "key=")
	got := readWorkerLabels(context.Background())
	if got["key"] != "" {
		t.Errorf("readWorkerLabels key= should give empty value, got %q", got["key"])
	}
}

// Ensure os import is used even if future cases drop the only reference.
var _ = os.Getenv

// TestSpecValidateWarnsOnWorkerSelector proves that when a linear pipeline
// carries worker_selector.match_labels, spec validate surfaces a warning so
// users know to register matching workers (rather than shards silently sitting
// pending forever with no eligible claimant).
func TestSpecValidateWarnsOnWorkerSelector(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name:     "selector-pipe",
		Source:   pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "x", "format": "json"}},
		Sink:     pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"path": "out", "format": "json"}},
		WorkerSelector: &pipeline.WorkerSelector{
			MatchLabels: map[string]string{"gpu": "true"},
		},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		BackpressureBuffer:    1,
		Retry:                 &pipeline.RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
	}
	body, _ := json.Marshal(map[string]any{"spec": spec})
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST validate: %v", err)
	}
	defer resp.Body.Close()
	var got struct {
		Valid    bool     `json:"valid"`
		Warnings []string `json:"warnings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	joined := strings.Join(got.Warnings, "\n")
	if !strings.Contains(joined, "worker_selector.match_labels") || !strings.Contains(joined, "gpu") {
		t.Fatalf("expected worker_selector warning mentioning gpu, got warnings=%v", got.Warnings)
	}
}

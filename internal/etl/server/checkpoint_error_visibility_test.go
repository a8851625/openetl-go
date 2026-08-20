package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

type checkpointVisibilityRunner struct {
	status   pipeline.Status
	stats    pipeline.Stats
	logs     *pipeline.LogBuffer
	done     chan struct{}
	startErr error
}

func newCheckpointVisibilityRunner() *checkpointVisibilityRunner {
	return &checkpointVisibilityRunner{
		status: pipeline.StatusFailed,
		stats: pipeline.Stats{
			LastError:            "validate source checkpoint: file checkpoint is missing offset",
			LastErrorCode:        "checkpoint.file.invalid",
			LastErrorRemediation: "Repair the source position, then retry.",
		},
		logs: pipeline.NewLogBuffer(4),
		done: make(chan struct{}),
	}
}

func (r *checkpointVisibilityRunner) Start(context.Context) error  { return r.startErr }
func (r *checkpointVisibilityRunner) Stop() error                  { return nil }
func (r *checkpointVisibilityRunner) Pause() error                 { return nil }
func (r *checkpointVisibilityRunner) Resume(context.Context) error { return nil }
func (r *checkpointVisibilityRunner) Wait()                        { <-r.done }
func (r *checkpointVisibilityRunner) Done() <-chan struct{}        { return r.done }
func (r *checkpointVisibilityRunner) Status() pipeline.Status      { return r.status }
func (r *checkpointVisibilityRunner) Stats() pipeline.Stats        { return r.stats }
func (r *checkpointVisibilityRunner) Duration() time.Duration      { return 0 }
func (r *checkpointVisibilityRunner) MetricsSnapshot() pipeline.MetricsSnapshot {
	return pipeline.MetricsSnapshot{}
}
func (r *checkpointVisibilityRunner) LogBuffer() *pipeline.LogBuffer  { return r.logs }
func (r *checkpointVisibilityRunner) Shards() []pipeline.ShardInfo    { return nil }
func (r *checkpointVisibilityRunner) IncrementDLQReplay(int64)        {}
func (r *checkpointVisibilityRunner) IncrementDLQDelete(int64)        {}
func (r *checkpointVisibilityRunner) CircuitBreakerState() int        { return 0 }
func (r *checkpointVisibilityRunner) SinkMetrics() []core.SinkMetrics { return nil }
func (r *checkpointVisibilityRunner) StateMetrics() []core.StateMetrics {
	return nil
}
func (r *checkpointVisibilityRunner) TransformMetrics() []core.TransformMetrics {
	return nil
}

func TestPipelineAPIAndHealthExposeCheckpointRemediation(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	runner := newCheckpointVisibilityRunner()
	s.mu.Lock()
	s.registerPipelineLocked("checkpoint-visibility", "checkpoint-visibility", runner, nil, nil)
	s.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/api/v2/pipelines/checkpoint-visibility", nil)
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("pipeline GET status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Status string         `json:"status"`
		Stats  pipeline.Stats `json:"stats"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("pipeline GET JSON: %v", err)
	}
	if body.Status != string(pipeline.StatusFailed) || body.Stats.LastErrorCode != "checkpoint.file.invalid" {
		t.Fatalf("pipeline response = %#v", body)
	}
	if body.Stats.LastErrorRemediation == "" {
		t.Fatal("pipeline response omitted checkpoint remediation")
	}

	health := s.getHealthStatus()
	if health["status"] != "unhealthy" {
		t.Fatalf("health status=%q, want unhealthy", health["status"])
	}
	var issues map[string]map[string]string
	if err := json.Unmarshal([]byte(health["pipeline_issues"]), &issues); err != nil {
		t.Fatalf("health pipeline_issues JSON: %v (%q)", err, health["pipeline_issues"])
	}
	issue := issues["checkpoint-visibility"]
	if issue["error_code"] != "checkpoint.file.invalid" || issue["remediation"] == "" {
		t.Fatalf("health issue=%#v", issue)
	}
}

func TestPipelineStartReturnsConflictWhilePreviousRunStops(t *testing.T) {
	s, _ := newTestHTTPServer(t)
	runner := newCheckpointVisibilityRunner()
	runner.status = pipeline.StatusStopped
	runner.startErr = pipeline.ErrRunnerStopping
	s.mu.Lock()
	s.registerPipelineLocked("pipeline-stopping", "pipeline-stopping", runner, nil, nil)
	s.mu.Unlock()

	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/api/v2/pipelines/pipeline-stopping/start", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("start status=%d body=%s, want 409", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("start JSON: %v", err)
	}
	if body["code"] != "pipeline_stopping" || body["remediation"] == "" {
		t.Fatalf("start response=%#v", body)
	}
}

func TestPipelineStartReturnsNon2xxWithCheckpointRemediation(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	runner := newCheckpointVisibilityRunner()
	runner.startErr = core.NewCheckpointValidationError(
		"checkpoint.file.invalid",
		"checkpoint file offset is missing",
		"Repair the source position, then retry.",
	)
	s.mu.Lock()
	s.registerPipelineLocked("checkpoint-start-error", "checkpoint-start-error", runner, nil, nil)
	s.mu.Unlock()

	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	req := httptest.NewRequest(http.MethodPost, "/api/v2/pipelines/checkpoint-start-error/start", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("start status=%d body=%s, want 422", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("start JSON: %v", err)
	}
	if body["code"] != "checkpoint.file.invalid" || body["remediation"] != "Repair the source position, then retry." {
		t.Fatalf("start response=%#v", body)
	}
}

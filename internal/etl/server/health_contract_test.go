package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/telemetry"
)

type healthCircuitRunner struct {
	pipeline.RunnerInterface
	circuit int
}

func (r *healthCircuitRunner) CircuitBreakerState() int { return r.circuit }

type healthCountFaultStore struct{ storage.Storage }

func (s healthCountFaultStore) CountDeadLetters(context.Context, string) (int64, error) {
	return 0, errors.New("injected DLQ count failure")
}

func TestMetricsAndHealthExposeCurrentFailureSignals(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      pipeline.Status
		circuit     int
		countFailed bool
		want        string
	}{
		{"open", pipeline.StatusRunning, 1, false, "degraded"},
		{"unknown", pipeline.StatusRunning, 0, true, "unknown"},
		{"restore", pipeline.Status("restore_failed"), 0, false, "restore_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configureRestoreTestProfile(t, RuntimeProfileDevelopment, "false")
			t.Setenv("ETL_STATE_REDIS_ADDR", "")
			t.Setenv("ETL_HEALTH_CHECKPOINT_STALE_SEC", "30")
			t.Setenv("ETL_HEALTH_CDC_LAG_MS", "120000")
			s, ts := newTestHTTPServer(t)
			defer ts.Close()
			base := newCheckpointVisibilityRunner()
			base.status = tc.status
			base.stats = pipeline.Stats{}
			runner := &healthCircuitRunner{RunnerInterface: base, circuit: tc.circuit}
			s.mu.Lock()
			s.registerPipelineLocked(tc.name, tc.name, runner, nil, nil)
			s.mu.Unlock()
			saveLifecycleTestPipeline(t, s, tc.name, tc.name, string(tc.status))
			if tc.countFailed {
				s.dlqWriter = storage.NewDLQCompatWriter(healthCountFaultStore{s.store})
			}
			response := restoreRequest(t, s, http.MethodGet, "/api/v2/metrics")
			var body struct {
				Pipelines []telemetry.PipelineMetrics `json:"pipelines"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Pipelines) != 1 || body.Pipelines[0].Health != tc.want {
				t.Fatalf("metrics=%s, want health=%s", response.Body.String(), tc.want)
			}
			if body.Pipelines[0].HealthThresholds.StaleCheckpointSec != 30 ||
				body.Pipelines[0].HealthThresholds.HighCDCLagMs != 120000 {
				t.Fatalf("thresholds=%+v", body.Pipelines[0].HealthThresholds)
			}
			if tc.countFailed && body.Pipelines[0].DLQFileCount != -1 {
				t.Fatalf("unreadable backlog=%d, want -1", body.Pipelines[0].DLQFileCount)
			}
			health := restoreRequest(t, s, http.MethodGet, "/api/v2/health")
			if health.Code != http.StatusServiceUnavailable {
				t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
			}
		})
	}
}

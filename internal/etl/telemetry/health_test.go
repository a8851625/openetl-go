package telemetry

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDerivePipelineHealth(t *testing.T) {
	th := DefaultHealthThresholds()
	cases := []struct {
		name string
		in   PipelineHealthInput
		want PipelineHealth
	}{
		{"failed", PipelineHealthInput{Status: "failed"}, PipelineFailed},
		{"paused not unhealthy", PipelineHealthInput{Status: "paused", RecordsDLQ: 5}, PipelinePaused},
		{"completed ok", PipelineHealthInput{Status: "completed"}, PipelineCompleted},
		{"scheduled ok", PipelineHealthInput{Status: "scheduled"}, PipelineScheduled},
		{"running healthy", PipelineHealthInput{Status: "running"}, PipelineHealthy},
		{"running dlq degraded", PipelineHealthInput{Status: "running", RecordsDLQ: 1}, PipelineDegraded},
		{"running lag degraded", PipelineHealthInput{Status: "running", CDCLagMs: th.HighCDCLagMs + 1}, PipelineDegraded},
		{"running checkpoint stale", PipelineHealthInput{Status: "running", CheckpointAgeSeconds: th.StaleCheckpointSec + 1}, PipelineDegraded},
		{"running cb open", PipelineHealthInput{Status: "running", CircuitBreakerState: 1}, PipelineDegraded},
		{"running last error", PipelineHealthInput{Status: "running", LastError: "boom"}, PipelineDegraded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DerivePipelineHealth(tc.in, th); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestAggregateHealth(t *testing.T) {
	overall, reasons := AggregateHealth(
		[]ComponentHealth{
			{Name: "storage", Status: HealthOK, Level: HealthOK},
			{Name: "redis_state", Status: HealthUnhealthy, Level: HealthUnhealthy, Detail: "connection refused"},
		},
		map[string]PipelineHealth{"orders": PipelineDegraded},
	)
	if overall != HealthUnhealthy {
		t.Fatalf("overall=%s want unhealthy", overall)
	}
	joined := strings.Join(reasons, " ")
	if !strings.Contains(joined, "redis_state") || !strings.Contains(joined, "degraded") {
		t.Fatalf("reasons=%v", reasons)
	}

	overall, _ = AggregateHealth(
		[]ComponentHealth{{Name: "storage", Status: HealthOK, Level: HealthOK}},
		map[string]PipelineHealth{"a": PipelineHealthy, "b": PipelineDegraded},
	)
	if overall != HealthDegraded {
		t.Fatalf("overall=%s want degraded", overall)
	}

	overall, _ = AggregateHealth(
		[]ComponentHealth{{Name: "storage", Status: HealthOK, Level: HealthOK}},
		map[string]PipelineHealth{"a": PipelineFailed},
	)
	if overall != HealthUnhealthy {
		t.Fatalf("failed pipeline should make overall unhealthy, got %s", overall)
	}
}

func TestEscapePrometheusLabel(t *testing.T) {
	in := `pipe"name\with
newline`
	got := EscapePrometheusLabel(in)
	want := `pipe\"name\\with\nnewline`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestPrometheusHandlerEscapesPipelineName(t *testing.T) {
	handler := PrometheusHandler(func() []PipelineMetrics {
		return []PipelineMetrics{{
			Name:   `orders"prod`,
			Status: "running",
		}}
	})
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, `pipeline="orders\"prod"`) {
		t.Fatalf("escaped label missing:\n%s", body)
	}
	if strings.Contains(body, `pipeline="orders"prod"`) {
		t.Fatalf("raw quote leaked into exposition:\n%s", body)
	}
	if !strings.Contains(body, "etl_alert_dropped_total") {
		t.Fatalf("missing alert dropped metric:\n%s", body)
	}
	if !strings.Contains(body, `etl_pipeline_health{pipeline="orders\"prod",health="healthy"} 1`) {
		t.Fatalf("missing pipeline health gauge:\n%s", body)
	}
}

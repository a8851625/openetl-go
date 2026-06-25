package telemetry

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPrometheusHandlerEmitsStateMetrics(t *testing.T) {
	updatedAt := time.Unix(1700000000, 0)
	handler := PrometheusHandler(func() []PipelineMetrics {
		return []PipelineMetrics{{
			Name: "orders-wide",
			StateMetrics: []StateMetric{{
				Node:      "window-orders",
				Keys:      7,
				Bytes:     512,
				UpdatedAt: updatedAt,
			}},
			TransformMetrics: []TransformMetric{{
				Node:      "join-orders",
				Transform: "join",
				Counters:  map[string]int64{"hit": 3},
			}},
		}}
	})

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	body := rec.Body.String()

	for _, want := range []string{
		`etl_state_keys{pipeline="orders-wide",node="window-orders"} 7`,
		`etl_state_bytes{pipeline="orders-wide",node="window-orders"} 512`,
		`etl_state_updated_timestamp_seconds{pipeline="orders-wide",node="window-orders"} 1700000000`,
		`etl_transform_metric_total{pipeline="orders-wide",node="join-orders",transform="join",metric="hit"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q\n%s", want, body)
		}
	}
}

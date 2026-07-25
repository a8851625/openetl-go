package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/a8851625/openetl-go/internal/etl/telemetry"
)

func TestHealthEndpointIncludesBusinessComponents(t *testing.T) {
	t.Setenv("ETL_PROFILE", "development")
	t.Setenv("ETL_INSECURE_DEV", "false")
	t.Setenv("ETL_API_TOKEN", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")
	t.Setenv("ETL_STATE_REDIS_ADDR", "")
	t.Setenv("ETL_STATE_REDIS_PASSWORD", "")

	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	s, err := NewServer(store, t.TempDir())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	req := httptest.NewRequest(http.MethodGet, "/api/v2/health", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json: %v", err)
	}
	for _, key := range []string{"status", "storage", "redis_state", "scheduler", "workers"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("health missing %q: %#v", key, body)
		}
	}
	if body["status"] != telemetry.HealthOK {
		t.Fatalf("status=%q want ok (%#v)", body["status"], body)
	}
	if body["storage"] != telemetry.HealthOK && body["storage"] != "ok" {
		t.Fatalf("storage=%q", body["storage"])
	}
	if body["redis_state"] == "" || body["redis_state"] == telemetry.HealthUnhealthy {
		t.Fatalf("redis_state unexpected: %q", body["redis_state"])
	}
}

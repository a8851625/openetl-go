package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// TestHealthSurfacesPlaintextSecrets verifies the IT-3/T3.2 contract: a legacy
// plaintext secret row makes the secret_encryption health component degraded
// with an actionable detail, and remediation returns it to ok without data
// loss.
func TestHealthSurfacesPlaintextSecrets(t *testing.T) {
	t.Setenv("ETL_PROFILE", "development")
	t.Setenv("ETL_INSECURE_DEV", "false")
	t.Setenv("ETL_API_TOKEN", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", testHealthSecretKey)
	t.Setenv("ETL_STATE_REDIS_ADDR", "")
	t.Setenv("ETL_STATE_REDIS_PASSWORD", "")

	raw, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	// Seed a legacy plaintext connection directly in the raw backend, exactly
	// like rows written before dsn was declared secret.
	legacy := &storage.ConnectionEntry{
		Name: "legacy-jdbc",
		Kind: "sink",
		Type: "jdbc",
		Config: map[string]any{
			"dsn": "mysql://root:hunter2@tcp(prod:3306)/db",
		},
	}
	if err := raw.SaveConnection(context.Background(), legacy); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}

	s, err := NewServer(raw, t.TempDir())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)

	getHealth := func(t *testing.T) map[string]string {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v2/health", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		// A degraded (but functional) runtime still answers 503 by design; the
		// test accepts both 200 and 503 and asserts on the component fields.
		if rec.Code != http.StatusOK && rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("json: %v", err)
		}
		return body
	}

	body := getHealth(t)
	if !strings.HasPrefix(body["secret_encryption"], "degraded") {
		t.Fatalf("secret_encryption=%q want degraded prefix: %#v", body["secret_encryption"], body)
	}

	// Remediate through the storage API and confirm health returns to ok.
	sfs, ok := s.store.(*storage.SecretFieldStore)
	if !ok {
		t.Fatalf("store is %T, want *SecretFieldStore", s.store)
	}
	if _, err := sfs.RemediatePlaintextSecrets(context.Background()); err != nil {
		t.Fatalf("remediate: %v", err)
	}
	body = getHealth(t)
	if !strings.HasPrefix(body["secret_encryption"], "ok") {
		t.Fatalf("secret_encryption=%q want ok after remediation: %#v", body["secret_encryption"], body)
	}

	// The remediated row must still round-trip its value.
	got, err := sfs.GetConnection(context.Background(), "legacy-jdbc")
	if err != nil || got == nil {
		t.Fatalf("get after remediation: %v %v", got, err)
	}
	if got.Config["dsn"] != "mysql://root:hunter2@tcp(prod:3306)/db" {
		t.Fatalf("dsn lost after remediation: %v", got.Config["dsn"])
	}
}

const testHealthSecretKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" // 32 zero bytes

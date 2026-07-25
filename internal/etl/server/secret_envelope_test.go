package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func fixedSecretKey(n byte) string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = n
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func withSpecEncryptionEnv(t *testing.T, keyID, key, previous string) {
	t.Helper()
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", key)
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", keyID)
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", previous)
}

func TestConnectionAndSettingsSecretEnvelopeAPI(t *testing.T) {
	const password = "conn-secret-pr11-value"
	const apiKey = "sk-pr11-settings-secret"
	key := fixedSecretKey(11)
	withSpecEncryptionEnv(t, "primary", key, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "etl.db")
	store, err := sqlite.New(dbPath)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	s, err := NewServer(store, dir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{
		"name": "db-src",
		"kind": "source",
		"type": "identity",
		"config": map[string]any{
			"host":     "db.example",
			"password": password,
		},
	})
	resp, err := http.Post(ts.URL+"/api/v2/connections", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST connection: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d", resp.StatusCode)
	}
	var created map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	cfg := created["connection"].(map[string]any)["config"].(map[string]any)
	if cfg["password"] != "******" {
		t.Fatalf("API password mask = %#v", cfg["password"])
	}

	setBody, _ := json.Marshal(map[string]string{
		"llm_base_url": "https://llm.example",
		"llm_model":    "demo",
		"llm_api_key":  apiKey,
	})
	setResp, err := http.Post(ts.URL+"/api/v2/settings", "application/json", bytes.NewReader(setBody))
	if err != nil {
		t.Fatalf("POST settings: %v", err)
	}
	defer setResp.Body.Close()
	if setResp.StatusCode != http.StatusOK {
		t.Fatalf("settings status=%d", setResp.StatusCode)
	}

	getSettings, err := http.Get(ts.URL + "/api/v2/settings")
	if err != nil {
		t.Fatalf("GET settings: %v", err)
	}
	defer getSettings.Body.Close()
	var settings map[string]string
	if err := json.NewDecoder(getSettings.Body).Decode(&settings); err != nil {
		t.Fatalf("decode settings: %v", err)
	}
	if settings["llm_api_key"] == apiKey || !strings.HasSuffix(settings["llm_api_key"], "****") {
		t.Fatalf("settings api key not masked: %#v", settings["llm_api_key"])
	}
	if settings["llm_model"] != "demo" {
		t.Fatalf("llm_model = %q", settings["llm_model"])
	}

	var rawCfg, rawKey string
	if err := store.DB().QueryRow(`SELECT config_json FROM connections WHERE name=?`, "db-src").Scan(&rawCfg); err != nil {
		t.Fatalf("raw connection: %v", err)
	}
	if err := store.DB().QueryRow(`SELECT value FROM settings WHERE key=?`, "llm_api_key").Scan(&rawKey); err != nil {
		t.Fatalf("raw setting: %v", err)
	}
	if strings.Contains(rawCfg, password) || strings.Contains(rawKey, apiKey) {
		t.Fatalf("plaintext secret found in SQL rows cfg=%s key=%s", rawCfg, rawKey)
	}
	if !strings.Contains(rawCfg, "enc:v1:primary:") || !strings.HasPrefix(rawKey, "enc:v1:primary:") {
		t.Fatalf("expected encrypted envelopes cfg=%s key=%s", rawCfg, rawKey)
	}

	updateBody, _ := json.Marshal(map[string]any{
		"name": "db-src",
		"kind": "source",
		"type": "identity",
		"config": map[string]any{
			"host":     "db.example",
			"password": "******",
		},
	})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/connections/db-src", bytes.NewReader(updateBody))
	req.Header.Set("Content-Type", "application/json")
	updResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT connection: %v", err)
	}
	defer updResp.Body.Close()
	if updResp.StatusCode != http.StatusOK {
		t.Fatalf("update status=%d", updResp.StatusCode)
	}

	maskUpdate, _ := json.Marshal(map[string]string{
		"llm_api_key": settings["llm_api_key"],
		"llm_model":   "demo-2",
	})
	maskResp, err := http.Post(ts.URL+"/api/v2/settings", "application/json", bytes.NewReader(maskUpdate))
	if err != nil {
		t.Fatalf("POST masked settings: %v", err)
	}
	defer maskResp.Body.Close()
	if maskResp.StatusCode != http.StatusOK {
		t.Fatalf("masked settings status=%d", maskResp.StatusCode)
	}

	_ = store.Close()
	store2, err := sqlite.New(dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })
	s2, err := NewServer(store2, dir)
	if err != nil {
		t.Fatalf("restart NewServer: %v", err)
	}
	conn, err := s2.store.GetConnection(t.Context(), "db-src")
	if err != nil || conn == nil {
		t.Fatalf("restart GetConnection: err=%v conn=%v", err, conn)
	}
	if conn.Config["password"] != password {
		t.Fatalf("restart password = %#v", conn.Config["password"])
	}
	gotKey, err := s2.store.GetSetting(t.Context(), "llm_api_key")
	if err != nil || gotKey != apiKey {
		t.Fatalf("restart llm_api_key = %q err=%v", gotKey, err)
	}
	gotModel, _ := s2.store.GetSetting(t.Context(), "llm_model")
	if gotModel != "demo-2" {
		t.Fatalf("restart llm_model = %q", gotModel)
	}
}

func TestSecretEnvelopeWrongKeyFailsClosed(t *testing.T) {
	const password = "wrong-key-secret"
	key := fixedSecretKey(21)
	withSpecEncryptionEnv(t, "primary", key, "")

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "etl.db")
	store, err := sqlite.New(dbPath)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	s, err := NewServer(store, dir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := s.store.SaveConnection(t.Context(), &storage.ConnectionEntry{
		Name:   "c1",
		Kind:   "source",
		Type:   "identity",
		Config: map[string]any{"password": password},
	}); err != nil {
		t.Fatalf("SaveConnection: %v", err)
	}
	_ = store.Close()

	withSpecEncryptionEnv(t, "primary", fixedSecretKey(22), "")
	store2, err := sqlite.New(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })
	s2, err := NewServer(store2, dir)
	if err != nil {
		t.Fatalf("NewServer with wrong key: %v", err)
	}
	_, err = s2.store.GetConnection(t.Context(), "c1")
	if err == nil {
		t.Fatalf("expected wrong-key failure")
	}
	if strings.Contains(err.Error(), password) {
		t.Fatalf("error leaked secret: %v", err)
	}
}

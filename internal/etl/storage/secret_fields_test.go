package storage_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func testSecretKey(n byte) string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = n
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func openSecretFieldStore(t *testing.T, keyID, keyB64, previous string) (storage.Storage, *storage.SecretFieldStore, *sqlite.Store) {
	t.Helper()
	raw, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	cipher, err := storage.NewSpecCipher(keyID, keyB64, previous)
	if err != nil {
		t.Fatalf("NewSpecCipher: %v", err)
	}
	wrapped := storage.NewSecretFieldStore(raw, cipher)
	sf, ok := wrapped.(*storage.SecretFieldStore)
	if !ok {
		t.Fatalf("expected *SecretFieldStore, got %T", wrapped)
	}
	return wrapped, sf, raw
}

func rawConnectionConfigJSON(t *testing.T, raw *sqlite.Store, name string) string {
	t.Helper()
	var cfg string
	err := raw.DB().QueryRow(`SELECT config_json FROM connections WHERE name=?`, name).Scan(&cfg)
	if err != nil {
		t.Fatalf("raw connection query: %v", err)
	}
	return cfg
}

func rawSettingValue(t *testing.T, raw *sqlite.Store, key string) string {
	t.Helper()
	var val string
	err := raw.DB().QueryRow(`SELECT value FROM settings WHERE key=?`, key).Scan(&val)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("raw setting query: %v", err)
	}
	return val
}

func TestSecretFieldStoreEncryptsConnectionAndSettings(t *testing.T) {
	const secret = "super-secret-password-pr11"
	const apiKey = "sk-live-pr11-test-key"
	store, _, raw := openSecretFieldStore(t, "primary", testSecretKey(1), "")

	ctx := context.Background()
	if err := store.SaveConnection(ctx, &storage.ConnectionEntry{
		Name: "mysql-src",
		Kind: "source",
		Type: "mysql_batch",
		Config: map[string]any{
			"host":     "db.example",
			"password": secret,
			"nested": map[string]any{
				"api_token": "nested-token-value",
			},
		},
	}); err != nil {
		t.Fatalf("SaveConnection: %v", err)
	}
	if err := store.SetSetting(ctx, "llm_api_key", apiKey); err != nil {
		t.Fatalf("SetSetting: %v", err)
	}
	if err := store.SetSetting(ctx, "llm_model", "gpt-test"); err != nil {
		t.Fatalf("SetSetting non-secret: %v", err)
	}

	got, err := store.GetConnection(ctx, "mysql-src")
	if err != nil || got == nil {
		t.Fatalf("GetConnection: err=%v got=%v", err, got)
	}
	if got.Config["password"] != secret {
		t.Fatalf("password = %#v, want decrypted secret", got.Config["password"])
	}
	nested := got.Config["nested"].(map[string]any)
	if nested["api_token"] != "nested-token-value" {
		t.Fatalf("nested token = %#v", nested["api_token"])
	}
	if v, _ := store.GetSetting(ctx, "llm_api_key"); v != apiKey {
		t.Fatalf("llm_api_key = %q", v)
	}
	if v, _ := store.GetSetting(ctx, "llm_model"); v != "gpt-test" {
		t.Fatalf("llm_model = %q", v)
	}

	rawCfg := rawConnectionConfigJSON(t, raw, "mysql-src")
	if strings.Contains(rawCfg, secret) || strings.Contains(rawCfg, "nested-token-value") {
		t.Fatalf("raw connection config leaked plaintext secret: %s", rawCfg)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(rawCfg), &parsed); err != nil {
		t.Fatalf("parse raw config: %v", err)
	}
	pw, _ := parsed["password"].(string)
	if !strings.HasPrefix(pw, "enc:v1:primary:") {
		t.Fatalf("password envelope = %q", pw)
	}
	rawKey := rawSettingValue(t, raw, "llm_api_key")
	if strings.Contains(rawKey, apiKey) || !strings.HasPrefix(rawKey, "enc:v1:primary:") {
		t.Fatalf("raw llm_api_key = %q", rawKey)
	}
	if rawSettingValue(t, raw, "llm_model") != "gpt-test" {
		t.Fatalf("non-secret setting should remain plaintext")
	}
}

func TestSecretFieldStoreLegacyPlaintextReadableAndReencrypted(t *testing.T) {
	const secret = "legacy-plain-secret"
	store, sf, raw := openSecretFieldStore(t, "primary", testSecretKey(2), "")
	ctx := context.Background()

	if err := raw.SaveConnection(ctx, &storage.ConnectionEntry{
		Name: "legacy-conn",
		Kind: "source",
		Type: "mysql",
		Config: map[string]any{
			"password": secret,
			"host":     "h",
		},
	}); err != nil {
		t.Fatalf("seed legacy connection: %v", err)
	}
	if err := raw.SetSetting(ctx, "llm_api_key", "legacy-api-key"); err != nil {
		t.Fatalf("seed legacy setting: %v", err)
	}

	got, err := store.GetConnection(ctx, "legacy-conn")
	if err != nil || got == nil || got.Config["password"] != secret {
		t.Fatalf("legacy connection read: err=%v got=%+v", err, got)
	}
	if v, _ := store.GetSetting(ctx, "llm_api_key"); v != "legacy-api-key" {
		t.Fatalf("legacy setting = %q", v)
	}

	if err := sf.ReencryptSecrets(ctx); err != nil {
		t.Fatalf("ReencryptSecrets: %v", err)
	}
	rawCfg := rawConnectionConfigJSON(t, raw, "legacy-conn")
	if strings.Contains(rawCfg, secret) {
		t.Fatalf("re-encrypt left plaintext: %s", rawCfg)
	}
	rawKey := rawSettingValue(t, raw, "llm_api_key")
	if strings.Contains(rawKey, "legacy-api-key") || !strings.HasPrefix(rawKey, "enc:v1:primary:") {
		t.Fatalf("re-encrypt setting failed: %q", rawKey)
	}
}

func TestSecretFieldStoreRotationAndWrongKey(t *testing.T) {
	oldKey := testSecretKey(3)
	newKey := testSecretKey(4)
	const secret = "rotate-me-secret"
	const apiKey = "rotate-me-api-key"

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "rotate.db")
	raw, err := sqlite.New(dbPath)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	oldCipher, err := storage.NewSpecCipher("old", oldKey, "")
	if err != nil {
		t.Fatalf("old cipher: %v", err)
	}
	storeOld := storage.NewSecretFieldStore(raw, oldCipher)
	if err := storeOld.SaveConnection(ctx, &storage.ConnectionEntry{
		Name:   "rot",
		Kind:   "source",
		Type:   "mysql",
		Config: map[string]any{"password": secret},
	}); err != nil {
		t.Fatalf("save old: %v", err)
	}
	if err := storeOld.SetSetting(ctx, "llm_api_key", apiKey); err != nil {
		t.Fatalf("set old setting: %v", err)
	}

	newOnly, err := storage.NewSpecCipher("new", newKey, "")
	if err != nil {
		t.Fatalf("new-only cipher: %v", err)
	}
	storeNewOnly := storage.NewSecretFieldStore(raw, newOnly)
	_, err = storeNewOnly.GetConnection(ctx, "rot")
	if err == nil {
		t.Fatalf("expected decrypt failure with wrong key")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), apiKey) {
		t.Fatalf("error leaked secret: %v", err)
	}

	rotCipher, err := storage.NewSpecCipher("new", newKey, `{"old":"`+oldKey+`"}`)
	if err != nil {
		t.Fatalf("rotation cipher: %v", err)
	}
	storeRot := storage.NewSecretFieldStore(raw, rotCipher).(*storage.SecretFieldStore)
	got, err := storeRot.GetConnection(ctx, "rot")
	if err != nil || got.Config["password"] != secret {
		t.Fatalf("rotation read connection: err=%v got=%+v", err, got)
	}
	if v, err := storeRot.GetSetting(ctx, "llm_api_key"); err != nil || v != apiKey {
		t.Fatalf("rotation read setting: err=%v v=%q", err, v)
	}
	if err := storeRot.ReencryptSecrets(ctx); err != nil {
		t.Fatalf("reencrypt: %v", err)
	}
	rawCfg := rawConnectionConfigJSON(t, raw, "rot")
	if !strings.Contains(rawCfg, "enc:v1:new:") || strings.Contains(rawCfg, secret) {
		t.Fatalf("expected new-key envelope without plaintext, got %s", rawCfg)
	}

	oldOnly := storage.NewSecretFieldStore(raw, oldCipher)
	if _, err := oldOnly.GetConnection(ctx, "rot"); err == nil {
		t.Fatalf("expected failure reading re-encrypted row with old key only")
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked secret: %v", err)
	}
}

func TestSecretFieldStoreMalformedCiphertext(t *testing.T) {
	store, _, raw := openSecretFieldStore(t, "primary", testSecretKey(5), "")
	ctx := context.Background()
	if err := raw.SaveConnection(ctx, &storage.ConnectionEntry{
		Name:   "bad",
		Kind:   "source",
		Type:   "mysql",
		Config: map[string]any{"password": "enc:v1:primary:%%%not-base64%%%"},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_, err := store.GetConnection(ctx, "bad")
	if err == nil {
		t.Fatalf("expected malformed ciphertext error")
	}
	if !errors.Is(err, storage.ErrSpecEncryptionMalformed) && !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEncryptConfigSecretsDisabledCipherKeepsPlaintext(t *testing.T) {
	cipher, err := storage.NewSpecCipher("default", "", "")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	cfg, err := storage.EncryptConfigSecrets(cipher, map[string]any{"password": "plain"})
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if cfg["password"] != "plain" {
		t.Fatalf("disabled cipher should keep plaintext, got %#v", cfg["password"])
	}
}

func TestConfigContainsPlaintextSecret(t *testing.T) {
	cfg := map[string]any{
		"host":     "h",
		"password": "secret",
		"nested":   map[string]any{"api_token": "tok"},
	}
	if !storage.ConfigContainsPlaintextSecret(cfg, "secret") {
		t.Fatalf("expected password hit")
	}
	if !storage.ConfigContainsPlaintextSecret(cfg, "tok") {
		t.Fatalf("expected nested hit")
	}
	if storage.ConfigContainsPlaintextSecret(cfg, "h") {
		t.Fatalf("non-secret host must not match")
	}
}

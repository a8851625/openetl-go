package server

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func specCryptoTestKey(fill byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 32))
}

func openSpecCryptoTestStore(t *testing.T, path string) *sqlite.Store {
	t.Helper()
	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	return store
}

func newSpecCryptoTestServer(t *testing.T, store storage.Storage, specsDir string) *Server {
	t.Helper()
	s, err := NewServer(store, specsDir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s
}

func specCryptoRequest(t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func specCryptoRawRequest(t *testing.T, s *Server, method, path, contentType string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func decodeSpecCryptoResponse(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode response %d %q: %v", w.Code, w.Body.String(), err)
	}
	return result
}

func TestNewServerRejectsInvalidSpecEncryptionKey(t *testing.T) {
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "not-base64")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "primary")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")

	store := openSpecCryptoTestStore(t, filepath.Join(t.TempDir(), "etl.db"))
	defer store.Close()
	if _, err := NewServer(store, t.TempDir()); err == nil || !strings.Contains(err.Error(), "base64-encoded 32-byte key") {
		t.Fatalf("NewServer error = %v, want invalid key diagnostic", err)
	}
}

func TestRestoreFromDBFailsClosedOnSpecCryptoErrors(t *testing.T) {
	tests := []struct {
		name        string
		prepare     func(t *testing.T, store storage.Storage)
		key         string
		keyID       string
		want        error
		remediation string
	}{
		{
			name: "missing key",
			prepare: func(t *testing.T, store storage.Storage) {
				cipher, err := storage.NewSpecCipher("primary", specCryptoTestKey(1), "")
				if err != nil {
					t.Fatal(err)
				}
				if err := storage.NewPipelineSpecStore(store, cipher).SaveWithID(context.Background(), "p1", "p1", linearSpecYAML, "created"); err != nil {
					t.Fatal(err)
				}
			},
			want:        storage.ErrSpecEncryptionKeyUnavailable,
			remediation: "ETL_SPEC_ENCRYPTION_KEY",
		},
		{
			name: "wrong key",
			prepare: func(t *testing.T, store storage.Storage) {
				cipher, err := storage.NewSpecCipher("primary", specCryptoTestKey(2), "")
				if err != nil {
					t.Fatal(err)
				}
				if err := storage.NewPipelineSpecStore(store, cipher).SaveWithID(context.Background(), "p2", "p2", linearSpecYAML, "created"); err != nil {
					t.Fatal(err)
				}
			},
			key:         specCryptoTestKey(3),
			keyID:       "primary",
			want:        storage.ErrSpecEncryptionAuthFailed,
			remediation: "verify the configured key",
		},
		{
			name: "corrupt ciphertext",
			prepare: func(t *testing.T, store storage.Storage) {
				cipher, err := storage.NewSpecCipher("primary", specCryptoTestKey(4), "")
				if err != nil {
					t.Fatal(err)
				}
				if err := storage.NewPipelineSpecStore(store, cipher).SaveWithID(context.Background(), "p3", "p3", linearSpecYAML, "created"); err != nil {
					t.Fatal(err)
				}
				row, err := store.GetPipeline(context.Background(), "p3")
				if err != nil {
					t.Fatal(err)
				}
				row.SpecYAML = row.SpecYAML[:len(row.SpecYAML)-1] + "!"
				if err := store.SavePipeline(context.Background(), row); err != nil {
					t.Fatal(err)
				}
			},
			key:         specCryptoTestKey(4),
			keyID:       "primary",
			want:        storage.ErrSpecEncryptionMalformed,
			remediation: "restore an intact metadata backup",
		},
		{
			name: "unsupported envelope version",
			prepare: func(t *testing.T, store storage.Storage) {
				if err := store.SavePipeline(context.Background(), &storage.PipelineRow{ID: "p4", Name: "p4", SpecYAML: "enc:v99:primary:AAAA", Status: "created"}); err != nil {
					t.Fatal(err)
				}
			},
			key:         specCryptoTestKey(5),
			keyID:       "primary",
			want:        storage.ErrSpecEncryptionVersion,
			remediation: "upgrade OpenETL-Go",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			store := openSpecCryptoTestStore(t, filepath.Join(dir, "etl.db"))
			defer store.Close()
			tt.prepare(t, store)
			t.Setenv("ETL_SPEC_ENCRYPTION_KEY", tt.key)
			t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", tt.keyID)
			t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")

			s, err := NewServer(store, filepath.Join(dir, "pipes"))
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			err = s.RestoreFromDB(context.Background())
			if !errors.Is(err, tt.want) {
				t.Fatalf("RestoreFromDB error = %v, want %v", err, tt.want)
			}
			if !strings.Contains(err.Error(), tt.remediation) {
				t.Fatalf("RestoreFromDB error = %q, want remediation %q", err, tt.remediation)
			}
			if strings.Contains(err.Error(), "enc:v") {
				t.Fatalf("crypto error leaked ciphertext: %v", err)
			}
		})
	}
}

func TestSpecEncryptionRotationKeepsOldVersionsReadable(t *testing.T) {
	dir := t.TempDir()
	store := openSpecCryptoTestStore(t, filepath.Join(dir, "etl.db"))
	defer store.Close()

	oldKey := specCryptoTestKey(6)
	newKey := specCryptoTestKey(7)
	oldCipher, err := storage.NewSpecCipher("old", oldKey, "")
	if err != nil {
		t.Fatal(err)
	}
	oldStore := storage.NewPipelineSpecStore(store, oldCipher)
	if err := oldStore.SaveWithID(context.Background(), "rotate-id", "rotate", linearSpecYAML, "created"); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", newKey)
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "new")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "old="+oldKey)
	s := newSpecCryptoTestServer(t, store, filepath.Join(dir, "pipes"))
	if err := s.RestoreFromDB(context.Background()); err != nil {
		t.Fatalf("RestoreFromDB with previous key: %v", err)
	}
	if err := s.specStore.SaveWithID(context.Background(), "rotate-id", "rotate", strings.Replace(linearSpecYAML, "linear-file-demo", "rotate", 1), "updated"); err != nil {
		t.Fatalf("save with new key: %v", err)
	}

	raw, err := store.GetPipeline(context.Background(), "rotate-id")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(raw.SpecYAML, "enc:v1:new:") {
		t.Fatalf("current row = %q, want new key envelope", raw.SpecYAML)
	}
	versions, err := s.specStore.Versions(context.Background(), "rotate-id")
	if err != nil {
		t.Fatalf("Versions after rotation: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("versions = %d, want 2", len(versions))
	}
}

func TestLegacySpecCiphertextRemainsReadable(t *testing.T) {
	dir := t.TempDir()
	store := openSpecCryptoTestStore(t, filepath.Join(dir, "etl.db"))
	defer store.Close()
	key := bytes.Repeat([]byte{8}, 32)
	legacy := legacyEncryptedSpec(t, key, linearSpecYAML)
	ctx := context.Background()
	if err := store.SavePipeline(ctx, &storage.PipelineRow{ID: "legacy-id", Name: "linear-file-demo", SpecYAML: legacy, Status: "created"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SavePipelineVersion(ctx, "legacy-id", legacy); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(key))
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "primary")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")

	s := newSpecCryptoTestServer(t, store, filepath.Join(dir, "pipes"))
	if err := s.RestoreFromDB(ctx); err != nil {
		t.Fatalf("RestoreFromDB legacy ciphertext: %v", err)
	}
	version, err := s.specStore.GetVersion(ctx, "legacy-id", 1)
	if err != nil {
		t.Fatalf("GetVersion legacy ciphertext: %v", err)
	}
	if version == nil || !strings.Contains(version.SpecYAML, "linear-file-demo") {
		t.Fatalf("legacy version not decrypted: %#v", version)
	}
}

func TestLegacyPlaintextSpecRecoveryFlow(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "etl.db")
	ctx := context.Background()
	store := openSpecCryptoTestStore(t, dbPath)
	if err := store.SavePipeline(ctx, &storage.PipelineRow{ID: "legacy-plain-id", Name: "legacy-plain", SpecYAML: linearSpecYAML, Status: "created"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SavePipelineVersion(ctx, "legacy-plain-id", linearSpecYAML); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")

	s := newSpecCryptoTestServer(t, store, filepath.Join(dir, "pipes"))
	if err := s.RestoreFromDB(ctx); err != nil {
		t.Fatalf("RestoreFromDB plaintext legacy: %v", err)
	}
	for _, endpoint := range []string{
		"/api/v2/pipelines/legacy-plain-id/versions",
		"/api/v2/pipelines/legacy-plain-id/versions/1/diff",
	} {
		response := specCryptoRequest(t, s, http.MethodGet, endpoint, nil)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", endpoint, response.Code, response.Body.String())
		}
	}
	rollback := specCryptoRequest(t, s, http.MethodPost, "/api/v2/pipelines/legacy-plain-id/versions/1/rollback", nil)
	if rollback.Code != http.StatusOK {
		t.Fatalf("plaintext rollback status=%d body=%s", rollback.Code, rollback.Body.String())
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openSpecCryptoTestStore(t, dbPath)
	defer store.Close()
	s = newSpecCryptoTestServer(t, store, filepath.Join(dir, "pipes"))
	if err := s.RestoreFromDB(ctx); err != nil {
		t.Fatalf("RestoreFromDB plaintext after rollback: %v", err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.specs["legacy-plain-id"] == nil {
		t.Fatal("plaintext legacy spec missing after restart")
	}
}

func legacyEncryptedSpec(t *testing.T, key []byte, plaintext string) string {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	nonce := bytes.Repeat([]byte{9}, gcm.NonceSize())
	payload := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return "enc:" + base64.StdEncoding.EncodeToString(payload)
}

func TestEncryptedSpecRecoveryFlowLinearAndDAG(t *testing.T) {
	for _, dag := range []bool{false, true} {
		name := "linear"
		if dag {
			name = "dag"
		}
		t.Run(name, func(t *testing.T) {
			runEncryptedSpecRecoveryFlow(t, dag)
		})
	}
}

func runEncryptedSpecRecoveryFlow(t *testing.T, dag bool) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "etl.db")
	sourcePath := filepath.Join(dir, "input.jsonl")
	if err := os.WriteFile(sourcePath, []byte("{\"id\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", specCryptoTestKey(10))
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "primary")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")

	store := openSpecCryptoTestStore(t, dbPath)
	s := newSpecCryptoTestServer(t, store, filepath.Join(dir, "pipes"))
	pipelineName := "crypto-linear"
	secret := "linear-secret-value"
	if dag {
		pipelineName = "crypto-dag"
		secret = "dag-secret-value"
	}
	first := recoverySpec(dag, pipelineName, sourcePath, filepath.Join(dir, "out-one"), secret, "revision-one")
	created := specCryptoRequest(t, s, http.MethodPost, "/api/v2/pipelines", map[string]any{"spec": first})
	if created.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	id, _ := decodeSpecCryptoResponse(t, created)["id"].(string)
	if id == "" {
		t.Fatal("create response missing id")
	}
	second := recoverySpec(dag, pipelineName, sourcePath, filepath.Join(dir, "out-two"), secret, "revision-two")
	updated := specCryptoRequest(t, s, http.MethodPut, "/api/v2/pipelines", map[string]any{"id": id, "spec": second})
	if updated.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", updated.Code, updated.Body.String())
	}
	raw, err := store.GetPipeline(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil || !strings.HasPrefix(raw.SpecYAML, "enc:v1:primary:") {
		t.Fatalf("raw current spec = %#v, want encrypted envelope", raw)
	}
	if strings.Contains(raw.SpecYAML, pipelineName) || strings.Contains(raw.SpecYAML, secret) {
		t.Fatal("raw encrypted spec contains plaintext marker")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openSpecCryptoTestStore(t, dbPath)
	s = newSpecCryptoTestServer(t, store, filepath.Join(dir, "pipes"))
	if err := s.RestoreFromDB(context.Background()); err != nil {
		t.Fatalf("first restart RestoreFromDB: %v", err)
	}
	assertRecoveredRevision(t, s, id, dag, "revision-two")

	for _, endpoint := range []string{
		"/api/v2/pipelines/" + id + "/spec",
		"/api/v2/pipelines/" + id + "/versions",
		"/api/v2/pipelines/" + id + "/versions/1",
		"/api/v2/pipelines/" + id + "/versions/1/diff",
		"/api/v2/pipelines/" + id + "/export",
	} {
		response := specCryptoRequest(t, s, http.MethodGet, endpoint, nil)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%s", endpoint, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), secret) || strings.Contains(response.Body.String(), "enc:v1:") {
			t.Fatalf("GET %s leaked secret/ciphertext: %s", endpoint, response.Body.String())
		}
	}
	if dag {
		importBody, err := json.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		imported := specCryptoRawRequest(t, s, http.MethodPost, "/api/v2/specs/import", "application/x-yaml", importBody)
		if imported.Code != http.StatusOK || !strings.Contains(imported.Body.String(), `"action":"updated"`) {
			t.Fatalf("DAG import status=%d body=%s", imported.Code, imported.Body.String())
		}
	}

	rollback := specCryptoRequest(t, s, http.MethodPost, "/api/v2/pipelines/"+id+"/versions/1/rollback", nil)
	if rollback.Code != http.StatusOK {
		t.Fatalf("rollback status=%d body=%s", rollback.Code, rollback.Body.String())
	}
	assertRecoveredRevision(t, s, id, dag, "revision-one")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openSpecCryptoTestStore(t, dbPath)
	defer store.Close()
	s = newSpecCryptoTestServer(t, store, filepath.Join(dir, "pipes"))
	if err := s.RestoreFromDB(context.Background()); err != nil {
		t.Fatalf("second restart RestoreFromDB: %v", err)
	}
	assertRecoveredRevision(t, s, id, dag, "revision-one")
}

func recoverySpec(dag bool, name, sourcePath, outputDir, secret, revision string) map[string]any {
	if dag {
		return map[string]any{
			"name": name,
			"tags": []string{revision},
			"dag": map[string]any{
				"nodes": []any{
					map[string]any{"id": "source", "kind": "source", "plugin": "file", "config": map[string]any{"path": sourcePath, "format": "json", "api_token": secret}},
					map[string]any{"id": "sink", "kind": "sink", "plugin": "file_sink", "config": map[string]any{"output_dir": outputDir, "format": "jsonl"}},
				},
				"edges": []any{map[string]any{"from": "source", "to": "sink"}},
			},
		}
	}
	return map[string]any{
		"name": name,
		"tags": []string{revision},
		"source": map[string]any{
			"type":   "file",
			"config": map[string]any{"path": sourcePath, "format": "json", "api_token": secret},
		},
		"sink": map[string]any{
			"type":   "file_sink",
			"config": map[string]any{"output_dir": outputDir, "format": "jsonl"},
		},
	}
}

func assertRecoveredRevision(t *testing.T, s *Server, id string, dag bool, want string) {
	t.Helper()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var tags []string
	if dag {
		if s.dagSpecs[id] == nil {
			t.Fatalf("DAG spec %s not restored", id)
		}
		tags = s.dagSpecs[id].Tags
	} else {
		if s.specs[id] == nil {
			t.Fatalf("linear spec %s not restored", id)
		}
		tags = s.specs[id].Tags
	}
	if len(tags) != 1 || tags[0] != want {
		t.Fatalf("tags = %v, want [%s]", tags, want)
	}
}

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestEveryDescriptorSecretIsEncryptedMaskedAndPreserved(t *testing.T) {
	withSpecEncryptionEnv(t, "descriptor-test", fixedSecretKey(13), "")
	raw, err := sqlite.New(filepath.Join(t.TempDir(), "secrets.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	s, err := NewServer(raw, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	checked := 0
	for i, descriptor := range connectorDescriptors() {
		if len(descriptor.SecretFields) == 0 {
			continue
		}
		t.Run(descriptor.Kind+"/"+descriptor.Type, func(t *testing.T) {
			name := fmt.Sprintf("secret-%d", i)
			config := map[string]any{}
			for _, field := range descriptor.SecretFields {
				config[field] = "descriptor-secret-" + name + "-" + field
			}
			request := map[string]any{"name": name, "kind": descriptor.Kind, "type": descriptor.Type, "config": config}
			post := func() *storage.ConnectionEntry {
				payload, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v2/connections", bytes.NewReader(payload)))
				if rec.Code != http.StatusOK {
					t.Fatalf("save connection status=%d", rec.Code)
				}
				var response struct {
					Connection *storage.ConnectionEntry `json:"connection"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || response.Connection == nil {
					t.Fatalf("response: %v", err)
				}
				return response.Connection
			}
			masked := post()
			stored, err := raw.GetConnection(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			for field := range config {
				checked++
				if value, ok := stored.Config[field].(string); !ok || !strings.HasPrefix(value, "enc:v1:descriptor-test:") {
					t.Errorf("%s not sealed in raw storage", field)
				}
				if masked.Config[field] != "******" {
					t.Errorf("%s was not masked by the API", field)
				}
				masked.Config[field] = "******"
			}
			request["config"] = masked.Config
			post()
			got, err := s.store.GetConnection(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			for field, expected := range config {
				if got.Config[field] != expected {
					t.Errorf("resubmitting %s's mask destroyed the real credential", field)
				}
			}
		})
	}
	if checked == 0 {
		t.Fatal("no descriptor secrets exercised")
	}
	t.Logf("encrypted, masked and preserved %d descriptor secret fields", checked)
}

func TestDescriptorDSNMaskedInLinearAndDAGSpecs(t *testing.T) {
	const dsn = "postgres://test:spec-secret-marker@localhost/db"
	linear := &pipeline.Spec{Source: pipeline.SourceSpec{Type: "file"}, Sink: pipeline.SinkSpec{Type: "jdbc", Config: map[string]any{"dsn": dsn}}, Transforms: []pipeline.TransformSpec{{Type: "lookup", Config: map[string]any{"dsn": dsn}}}}
	masked := maskSpecSecrets(linear)
	if masked.Sink.Config["dsn"] == dsn || masked.Transforms[0].Config["dsn"] == dsn {
		t.Fatal("linear spec exposed descriptor DSN")
	}
	preserveLinearSpecSecrets(masked, linear)
	if masked.Sink.Config["dsn"] != dsn || masked.Transforms[0].Config["dsn"] != dsn {
		t.Fatal("linear DSN placeholder did not preserve credential")
	}
	dag := &orchestrator.PipelineSpec{DAG: orchestrator.DAG{Nodes: []*orchestrator.Node{{ID: "lookup", Kind: orchestrator.KindTransform, Plugin: "lookup", Config: map[string]any{"dsn": dsn}}}}}
	maskedDAG := maskDAGSpecSecrets(dag)
	if maskedDAG.DAG.Nodes[0].Config["dsn"] == dsn {
		t.Fatal("DAG spec exposed descriptor DSN")
	}
	preserveDAGSpecSecrets(maskedDAG, dag)
	if maskedDAG.DAG.Nodes[0].Config["dsn"] != dsn {
		t.Fatal("DAG DSN placeholder did not preserve credential")
	}
}

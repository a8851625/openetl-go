package server

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/plugin/pluginsystem"
)

func TestPluginsEndpointExposesABIContract(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v2/plugins")
	if err != nil {
		t.Fatalf("GET plugins: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		PluginABI map[string]any `json:"plugin_abi"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.PluginABI["version"] != pluginsystem.ABIVersionV1 {
		t.Fatalf("plugin_abi.version = %#v", got.PluginABI["version"])
	}
	if got.PluginABI["min_runtime_version"] != pluginsystem.MinRuntimeVersionV1 {
		t.Fatalf("plugin_abi.min_runtime_version = %#v", got.PluginABI["min_runtime_version"])
	}
	if got.PluginABI["manifest_required_for_certified"] != true {
		t.Fatalf("manifest_required_for_certified = %#v", got.PluginABI["manifest_required_for_certified"])
	}
}

func TestPluginInstallRejectsInvalidManifestBeforeWASMLoad(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("name", "bad-manifest"); err != nil {
		t.Fatalf("write name: %v", err)
	}
	if err := writer.WriteField("kind", "transform"); err != nil {
		t.Fatalf("write kind: %v", err)
	}
	if err := writer.WriteField("version", "1.0.0"); err != nil {
		t.Fatalf("write version: %v", err)
	}
	if err := writer.WriteField("manifest", `{
		"name": "bad-manifest",
		"kind": "transform",
		"version": "1.0.0",
		"abi": "openetl.plugin.abi/v1",
		"entrypoints": ["transform"]
	}`); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	part, err := writer.CreateFormFile("wasm", "bad-manifest.wasm")
	if err != nil {
		t.Fatalf("create wasm field: %v", err)
	}
	if _, err := part.Write([]byte("not-a-real-wasm")); err != nil {
		t.Fatalf("write wasm: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v2/plugins/install", &body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST install: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if errText, _ := got["error"].(string); !strings.Contains(errText, "min_runtime_version is required") {
		t.Fatalf("error = %q", errText)
	}
}

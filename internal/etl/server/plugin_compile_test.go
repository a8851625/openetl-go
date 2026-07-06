package server

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
)

func TestPluginCompileRejectsSourceAndSinkKinds(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	for _, kind := range []string{"source", "sink"} {
		t.Run(kind, func(t *testing.T) {
			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			if err := writer.WriteField("source", "export function transform() {}"); err != nil {
				t.Fatalf("write source field: %v", err)
			}
			if err := writer.WriteField("name", "compiled-"+kind); err != nil {
				t.Fatalf("write name field: %v", err)
			}
			if err := writer.WriteField("kind", kind); err != nil {
				t.Fatalf("write kind field: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("close multipart writer: %v", err)
			}

			req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/v2/plugins/compile", &body)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", writer.FormDataContentType())
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST compile: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
			var got map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got["kind"] != kind {
				t.Fatalf("kind = %#v, want %q", got["kind"], kind)
			}
			errText, _ := got["error"].(string)
			if !strings.Contains(errText, "supports transform plugins only") || !strings.Contains(errText, "/api/v2/plugins/install") {
				t.Fatalf("error = %q, want transform-only offline upload guidance", errText)
			}
		})
	}
}

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestTapSchemaMarksLegacyAlertFieldsUnimplemented(t *testing.T) {
	schema := configSchema()
	transforms := schema["transforms"].(map[string][]ConfigField)
	fields := transforms["tap"]
	for _, name := range tapUnimplementedConfigFields {
		field, ok := configFieldByName(fields, name)
		if !ok {
			t.Fatalf("tap schema missing field %s", name)
		}
		if !field.Unimplemented {
			t.Fatalf("tap field %s Unimplemented=false, want true", name)
		}
		if !strings.Contains(strings.ToLower(field.Description), "unimplemented") {
			t.Fatalf("tap field %s description = %q, want unimplemented marker", name, field.Description)
		}
	}
}

func TestTapDescriptorDoesNotAdvertiseUnimplementedAlertsOrMetrics(t *testing.T) {
	desc := findDescriptor(connectorDescriptors(), "transform", "tap")
	if desc == nil {
		t.Fatal("missing tap descriptor")
	}
	if contains(desc.Capabilities, "alerts") || contains(desc.Capabilities, "metrics") {
		t.Fatalf("tap capabilities = %#v, should not advertise alerts/metrics", desc.Capabilities)
	}
	for _, name := range tapUnimplementedConfigFields {
		field, ok := configFieldByName(desc.Fields, name)
		if !ok || !field.Unimplemented {
			t.Fatalf("tap descriptor field %s = %#v, want unimplemented field", name, field)
		}
	}
}

func TestSpecValidateWarnsForTapUnimplementedAlertFields(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "tap-warning-linear",
		Source: pipeline.SourceSpec{
			Type:   "demo",
			Config: map[string]any{"count": 1},
		},
		Transforms: []pipeline.TransformSpec{{
			Type: "tap",
			Config: map[string]any{
				"alert_on":  "field_match",
				"threshold": 1,
				"field":     "status",
				"value":     "bad",
				"webhook":   "https://example.invalid/hook",
			},
		}},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir(), "format": "jsonl"},
		},
		DLQ: &pipeline.DLQSpec{Enable: true},
	}

	raw, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST validate: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Valid    bool     `json:"valid"`
		Warnings []string `json:"warnings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !got.Valid {
		t.Fatalf("valid = false, warnings=%v", got.Warnings)
	}
	joined := strings.Join(got.Warnings, "\n")
	for _, field := range tapUnimplementedConfigFields {
		if !strings.Contains(joined, "transforms[0].config."+field) || !strings.Contains(joined, "unimplemented") {
			t.Fatalf("warnings = %v, want unimplemented warning for %s", got.Warnings, field)
		}
	}
}

func TestSpecValidateWarnsForDAGTapUnimplementedAlertFields(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := map[string]any{
		"name": "tap-warning-dag",
		"dag": map[string]any{
			"nodes": []map[string]any{
				{"id": "src", "kind": "source", "plugin": "demo", "config": map[string]any{"count": 1}},
				{"id": "tap1", "kind": "tap", "plugin": "tap", "config": map[string]any{"alert_on": "field_match", "field": "status"}},
				{"id": "sink", "kind": "sink", "plugin": "file_sink", "config": map[string]any{"output_dir": t.TempDir(), "format": "jsonl"}},
			},
			"edges": []map[string]any{
				{"from": "src", "to": "tap1"},
				{"from": "tap1", "to": "sink"},
			},
		},
	}
	raw, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	resp, err := http.Post(ts.URL+"/api/v2/specs/validate", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST validate: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var got struct {
		Warnings []string `json:"warnings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	joined := strings.Join(got.Warnings, "\n")
	if !strings.Contains(joined, `dag.nodes["tap1"].config.alert_on`) || !strings.Contains(joined, `dag.nodes["tap1"].config.field`) {
		t.Fatalf("warnings = %v, want DAG tap unimplemented field warnings", got.Warnings)
	}
}

func configFieldByName(fields []ConfigField, name string) (ConfigField, bool) {
	for _, field := range fields {
		if field.Name == name {
			return field, true
		}
	}
	return ConfigField{}, false
}

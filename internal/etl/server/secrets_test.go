package server

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func TestPreserveSecretConfigKeepsMaskedPassword(t *testing.T) {
	existing := map[string]any{
		"host":     "db.example",
		"password": "super-secret",
		"nested": map[string]any{
			"api_token": "tok-12345",
		},
	}
	// Simulates a full UI resubmit after GET masking.
	incoming := map[string]any{
		"host":     "db.example",
		"password": maskString("super-secret"), // s****t
		"nested": map[string]any{
			"api_token": "******",
		},
		"user": "sync",
	}
	got := preserveSecretConfig(incoming, existing)
	if got["password"] != "super-secret" {
		t.Fatalf("password = %#v, want real secret preserved", got["password"])
	}
	nested, _ := got["nested"].(map[string]any)
	if nested["api_token"] != "tok-12345" {
		t.Fatalf("nested api_token = %#v, want real secret preserved", nested["api_token"])
	}
	if got["user"] != "sync" {
		t.Fatalf("user = %#v, want sync", got["user"])
	}
}

func TestPreserveSecretConfigAllowsExplicitChange(t *testing.T) {
	existing := map[string]any{"password": "old-pass"}
	incoming := map[string]any{"password": "new-pass"}
	got := preserveSecretConfig(incoming, existing)
	if got["password"] != "new-pass" {
		t.Fatalf("password = %#v, want new-pass", got["password"])
	}
}

func TestPreserveSecretConfigDropsPlaceholderWithoutExisting(t *testing.T) {
	incoming := map[string]any{
		"host":     "db",
		"password": "******",
	}
	got := preserveSecretConfig(incoming, nil)
	if _, ok := got["password"]; ok {
		t.Fatalf("password placeholder should be scrubbed, got %#v", got)
	}
	if got["host"] != "db" {
		t.Fatalf("host = %#v", got["host"])
	}
}

func TestPreserveLinearSpecSecrets(t *testing.T) {
	old := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_batch", Config: map[string]any{"password": "src-secret"}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"password": "sink-secret"}},
	}
	in := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_batch", Config: map[string]any{"password": "s****t", "host": "h"}},
		Sink:   pipeline.SinkSpec{Type: "mysql", Config: map[string]any{"password": "****"}},
	}
	preserveLinearSpecSecrets(in, old)
	if in.Source.Config["password"] != "src-secret" {
		t.Fatalf("source password = %#v", in.Source.Config["password"])
	}
	if in.Sink.Config["password"] != "sink-secret" {
		t.Fatalf("sink password = %#v", in.Sink.Config["password"])
	}
	if in.Source.Config["host"] != "h" {
		t.Fatalf("source host lost: %#v", in.Source.Config)
	}
}

func TestPreserveDAGSpecSecretsByNodeID(t *testing.T) {
	old := &orchestrator.PipelineSpec{
		DAG: orchestrator.DAG{Nodes: []*orchestrator.Node{
			{ID: "src", Kind: orchestrator.KindSource, Plugin: "mysql_batch", Config: map[string]any{"password": "real"}},
			{ID: "snk", Kind: orchestrator.KindSink, Plugin: "mysql", Config: map[string]any{"password": "sink-real"}},
		}},
	}
	in := &orchestrator.PipelineSpec{
		DAG: orchestrator.DAG{Nodes: []*orchestrator.Node{
			// Reordered: sink first, then source — must still match by ID.
			{ID: "snk", Kind: orchestrator.KindSink, Plugin: "mysql", Config: map[string]any{"password": "******"}},
			{ID: "src", Kind: orchestrator.KindSource, Plugin: "mysql_batch", Config: map[string]any{"password": "r****l"}},
		}},
	}
	preserveDAGSpecSecrets(in, old)
	byID := map[string]map[string]any{}
	for _, n := range in.DAG.Nodes {
		byID[n.ID] = n.Config
	}
	if byID["src"]["password"] != "real" {
		t.Fatalf("src password = %#v", byID["src"]["password"])
	}
	if byID["snk"]["password"] != "sink-real" {
		t.Fatalf("snk password = %#v", byID["snk"]["password"])
	}
}

func TestIsSecretPlaceholder(t *testing.T) {
	cases := []struct {
		v    any
		want bool
	}{
		{"", true},
		{"****", true},
		{"******", true},
		{maskString("password"), true},
		{"new-password", false},
		{123, false},
		{nil, false},
	}
	for _, tc := range cases {
		if got := isSecretPlaceholder(tc.v); got != tc.want {
			t.Fatalf("isSecretPlaceholder(%#v)=%v, want %v", tc.v, got, tc.want)
		}
	}
}

func TestMergeConnectionConfigPreservesMaskedSecrets(t *testing.T) {
	conn := &storage.ConnectionEntry{
		Name: "db",
		Kind: "source",
		Type: "mysql_batch",
		Config: map[string]any{
			"host":     "db",
			"password": "conn-secret",
			"user":     "u",
		},
	}
	merged := mergeConnectionConfig(conn, map[string]any{
		"password": "******",
		"user":     "u2",
	})
	if merged["password"] != "conn-secret" {
		t.Fatalf("password = %#v", merged["password"])
	}
	if merged["user"] != "u2" {
		t.Fatalf("user = %#v", merged["user"])
	}
	if merged["host"] != "db" {
		t.Fatalf("host = %#v", merged["host"])
	}
}

func TestMaskConfigSecretsNested(t *testing.T) {
	cfg := map[string]any{
		"password": "abc",
		"nested": map[string]any{
			"token": "xyz",
		},
	}
	got := maskConfigSecrets(cfg)
	if got["password"] != maskString("abc") {
		t.Fatalf("password mask = %#v", got["password"])
	}
	nested := got["nested"].(map[string]any)
	if nested["token"] != maskString("xyz") {
		t.Fatalf("token mask = %#v", nested["token"])
	}
	// Original must not be mutated.
	if cfg["password"] != "abc" {
		t.Fatalf("original mutated: %#v", cfg)
	}
}

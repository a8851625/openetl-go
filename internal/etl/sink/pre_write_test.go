package sink

import (
	"testing"
)

func TestParsePreWriteConfigAbsent(t *testing.T) {
	cfg, err := ParsePreWriteConfig(map[string]any{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Enabled() {
		t.Fatalf("expected disabled when absent")
	}
}

func TestParsePreWriteConfigDelete(t *testing.T) {
	cfg, err := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{
			"action":    "delete",
			"condition": "dt = '${PROCESSING_DATE}'",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.Enabled() {
		t.Fatalf("expected enabled")
	}
	if cfg.Action != PreWriteDelete {
		t.Fatalf("action=%s want delete", cfg.Action)
	}
	if cfg.Condition == "" {
		t.Fatalf("condition should be set")
	}
}

func TestParsePreWriteConfigDeleteRequiresCondition(t *testing.T) {
	_, err := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{"action": "delete"},
	})
	if err == nil {
		t.Fatalf("expected error for delete without condition")
	}
}

func TestParsePreWriteConfigTruncateOK(t *testing.T) {
	cfg, err := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{"action": "truncate"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Action != PreWriteTruncate {
		t.Fatalf("action=%s want truncate", cfg.Action)
	}
}

func TestParsePreWriteConfigInvalidAction(t *testing.T) {
	_, err := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{"action": "drop_table"},
	})
	if err == nil {
		t.Fatalf("expected error for invalid action")
	}
}

func TestPreWriteIsDangerousForStreaming(t *testing.T) {
	trunc, _ := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{"action": "truncate"},
	})
	if !trunc.IsDangerousForStreaming() {
		t.Fatalf("truncate should be dangerous for streaming")
	}
	del, _ := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{"action": "delete", "condition": "dt='x'"},
	})
	if del.IsDangerousForStreaming() {
		t.Fatalf("delete should not be flagged dangerous for streaming")
	}
}

func TestPreWriteDescribeForWarning(t *testing.T) {
	cfg, _ := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{"action": "truncate"},
	})
	desc := cfg.DescribeForWarning("dws.t")
	if desc == "" {
		t.Fatalf("expected non-empty warning description")
	}
}

func TestPreWriteExpandParams(t *testing.T) {
	cfg, _ := ParsePreWriteConfig(map[string]any{
		"pre_write": map[string]any{
			"action":    "delete",
			"condition": "dt = '${params.dt}'",
			"params":    map[string]any{"dt": "2026-07-01"},
		},
	})
	expanded := cfg.expandParams()
	if expanded != "dt = '2026-07-01'" {
		t.Fatalf("expanded=%q", expanded)
	}
}

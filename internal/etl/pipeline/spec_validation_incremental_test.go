package pipeline

import (
	"strings"
	"testing"
)

func TestValidateLookupIncrementalConfig(t *testing.T) {
	base := func(extra map[string]any) *Spec {
		cfg := map[string]any{
			"query":  "SELECT id, name FROM dim_users",
			"fields": []any{"name"},
		}
		for k, v := range extra {
			cfg[k] = v
		}
		return &Spec{
			Name:       "lookup-incremental",
			Source:     SourceSpec{Type: "file", Config: map[string]any{"path": "x.jsonl"}},
			Sink:       SinkSpec{Type: "file_sink", Config: map[string]any{}},
			Transforms: []TransformSpec{{Type: "lookup", Config: cfg}},
			BatchSize:  10, CheckpointIntervalSec: 1, BackpressureBuffer: 10,
		}
	}

	t.Run("valid incremental config", func(t *testing.T) {
		problems := ValidateTransformConfigRequirements(base(map[string]any{
			"incremental_column":        "updated_at",
			"incremental_query":         "SELECT id, name, updated_at FROM dim_users WHERE updated_at > ?",
			"incremental_delete_column": "is_deleted",
			"full_refresh_interval_sec": 3600,
		}))
		for _, p := range problems {
			if strings.Contains(p, "incremental") {
				t.Fatalf("valid incremental config rejected: %s", p)
			}
		}
	})

	t.Run("column without query", func(t *testing.T) {
		problems := ValidateTransformConfigRequirements(base(map[string]any{"incremental_column": "updated_at"}))
		if !containsProblem(problems, "incremental_column requires incremental_query") {
			t.Fatalf("problems = %v", problems)
		}
	})

	t.Run("query without column", func(t *testing.T) {
		problems := ValidateTransformConfigRequirements(base(map[string]any{"incremental_query": "SELECT 1 WHERE x > ?"}))
		if !containsProblem(problems, "require incremental_column") {
			t.Fatalf("problems = %v", problems)
		}
	})

	t.Run("query without placeholder", func(t *testing.T) {
		problems := ValidateTransformConfigRequirements(base(map[string]any{
			"incremental_column": "updated_at",
			"incremental_query":  "SELECT id FROM dim_users",
		}))
		if !containsProblem(problems, "watermark placeholder") {
			t.Fatalf("problems = %v", problems)
		}
	})

	t.Run("query mode is rejected", func(t *testing.T) {
		problems := ValidateTransformConfigRequirements(base(map[string]any{
			"mode":               "query",
			"query":              "SELECT id, name FROM dim_users WHERE id = {{.user_id}}",
			"incremental_column": "updated_at",
			"incremental_query":  "SELECT id FROM dim_users WHERE updated_at > ?",
		}))
		if !containsProblem(problems, "only applies to lookup mode=cache") {
			t.Fatalf("problems = %v", problems)
		}
	})

	t.Run("negative full refresh interval", func(t *testing.T) {
		problems := ValidateTransformConfigRequirements(base(map[string]any{
			"incremental_column":        "updated_at",
			"incremental_query":         "SELECT id FROM dim_users WHERE updated_at > ?",
			"full_refresh_interval_sec": -1,
		}))
		if !containsProblem(problems, "full_refresh_interval_sec must be >= 0") {
			t.Fatalf("problems = %v", problems)
		}
	})

	t.Run("absent config is a no-op", func(t *testing.T) {
		problems := ValidateTransformConfigRequirements(base(nil))
		for _, p := range problems {
			if strings.Contains(p, "incremental") {
				t.Fatalf("unexpected incremental problem: %s", p)
			}
		}
	})
}

func containsProblem(problems []string, want string) bool {
	for _, p := range problems {
		if strings.Contains(p, want) {
			return true
		}
	}
	return false
}

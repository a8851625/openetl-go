package pipeline

import (
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
)

func TestInjectStateDefaultsUsesPipelineAndNodeWhenBackendEnabled(t *testing.T) {
	original := map[string]any{"state_backend": "sqlite", "state_path": "state.db"}

	got := InjectStateDefaults("orders-wide", "window-2", original)

	if got["state_pipeline"] != "orders-wide" || got["state_node"] != "window-2" {
		t.Fatalf("state defaults not injected: %#v", got)
	}
	if _, ok := original["state_pipeline"]; ok {
		t.Fatalf("InjectStateDefaults mutated original config: %#v", original)
	}
}

func TestInjectStateDefaultsPreservesExplicitNamespace(t *testing.T) {
	got := InjectStateDefaults("orders-wide", "window-2", map[string]any{
		"state_backend":  "sqlite",
		"state_pipeline": "custom-pipe",
		"state_node":     "custom-node",
	})

	if got["state_pipeline"] != "custom-pipe" || got["state_node"] != "custom-node" {
		t.Fatalf("explicit state namespace overwritten: %#v", got)
	}
}

func TestNewRunnerInjectsStateDefaultsBeforeBuildingTransforms(t *testing.T) {
	var captured map[string]any
	registry.RegisterTransform("state_defaults_probe", func(config map[string]any) (core.Transform, error) {
		captured = config
		return filterAllTransform{}, nil
	})

	tmpDir := t.TempDir()
	spec := &Spec{
		Name:                  "runner-state-defaults",
		Source:                SourceSpec{Type: "demo", Config: map[string]any{"count": 1}},
		Transforms:            []TransformSpec{{Type: "state_defaults_probe", Config: map[string]any{"state_backend": "sqlite"}}},
		Sink:                  SinkSpec{Type: "file_sink", Config: map[string]any{"path": filepath.Join(tmpDir, "out.jsonl"), "format": "json"}},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		BackpressureBuffer:    1,
	}

	am := alert.NewManager()
	t.Cleanup(am.Close)
	if _, err := NewRunner(spec, newMemoryCPStore(), noopDLQ{}, am); err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	if captured["state_pipeline"] != "runner-state-defaults" || captured["state_node"] != "state_defaults_probe-0" {
		t.Fatalf("state defaults captured = %#v", captured)
	}
	if _, ok := spec.Transforms[0].Config["state_pipeline"]; ok {
		t.Fatalf("NewRunner mutated original transform config: %#v", spec.Transforms[0].Config)
	}
}

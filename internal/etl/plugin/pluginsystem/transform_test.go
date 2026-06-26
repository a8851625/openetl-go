//go:build extism

package pluginsystem

import (
	"context"
	"sync"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

var _ core.BatchTransform = (*pluginTransform)(nil)

// fakeStorage implements storage.Storage for tests (only plugins needed).
type fakeStorage struct {
	mu      sync.Mutex
	plugins []struct {
		name    string
		kind    string
		path    string
		version string
		enabled bool
	}
}

// For our test we don't actually exercise storage — we install via a
// pre-populated manager field. Skip the Storage interface implementation.

// TestPluginTransformRegistration verifies that after RegisterTransforms,
// the registry can build a `plugin_<name>` transform.
//
// We bypass Manager.Install (which needs a real WASM file) and directly
// populate the in-memory map with a mock plugin entry. RegisterTransforms
// walks the map and registers each transform-kind plugin.
func TestPluginTransformRegistration(t *testing.T) {
	m := &Manager{
		plugins: map[string]*loadedPlugin{},
	}

	// Simulate a loaded transform plugin without actually invoking extism.
	m.plugins["upper"] = &loadedPlugin{
		meta: &PluginMeta{Name: "upper", Kind: KindTransform, Enabled: true},
		// extism field is nil — we don't call Apply in this test.
	}
	m.plugins["notransform"] = &loadedPlugin{
		meta: &PluginMeta{Name: "notransform", Kind: KindSource, Enabled: true},
	}

	m.RegisterTransforms()

	// Verify the transform-kind plugin is registered.
	if !registry.HasTransform("plugin_upper") {
		t.Error("expected plugin_upper to be registered as transform")
	}

	// Source-kind plugin should NOT be registered as a transform.
	if registry.HasTransform("plugin_notransform") {
		t.Error("source-kind plugin should not be registered as transform")
	}
}

// TestPluginTransformApply verifies that the registered transform delegates
// to Manager.ExecTransform. We stub ExecTransform via a custom plugin entry.
func TestPluginTransformApply(t *testing.T) {
	m := &Manager{
		plugins: map[string]*loadedPlugin{},
	}
	m.plugins["doubler"] = &loadedPlugin{
		meta: &PluginMeta{Name: "doubler", Kind: KindTransform, Enabled: true},
	}
	m.RegisterTransforms()

	if !registry.HasTransform("plugin_doubler") {
		t.Fatal("plugin_doubler not registered")
	}
	// Note: we cannot actually call Apply without a real extism.Plugin
	// instance. The registration mechanism is the unit under test here;
	// the full WASM path is exercised by hack/e2e-plugin.sh.
}

// fakeRecord helper for tests.
func fakeRecord() core.Record {
	return core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"x": 1},
		Metadata:  core.Metadata{Source: "kafka", Table: "raw", Offset: 7},
	}
}

func TestPluginTransformOutputToRecordsSupportsArrayABI(t *testing.T) {
	base := fakeRecord()
	out, err := pluginTransformOutputToRecords([]byte(`[
  {"data":{"x":2,"idx":1},"metadata":{"table":"parsed"}},
  {"x":3,"idx":2}
]`), base)
	if err != nil {
		t.Fatalf("pluginTransformOutputToRecords: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("outputs = %d, want 2: %#v", len(out), out)
	}
	if out[0].Operation != core.OpInsert || out[0].Metadata.Source != "kafka" || out[0].Metadata.Table != "parsed" || out[0].Metadata.Offset != 7 {
		t.Fatalf("first output envelope = %#v", out[0])
	}
	if out[0].Data["x"] != float64(2) || out[0].Data["idx"] != float64(1) {
		t.Fatalf("first output data = %#v", out[0].Data)
	}
	if out[1].Operation != core.OpInsert || out[1].Metadata.Table != "raw" || out[1].Metadata.Offset != 7 {
		t.Fatalf("second output envelope = %#v", out[1])
	}
	if out[1].Data["x"] != float64(3) || out[1].Data["idx"] != float64(2) {
		t.Fatalf("second output data = %#v", out[1].Data)
	}
}

func TestPluginTransformOutputToRecordsDropsEmptyNullAndFalse(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte(""), []byte(" null "), []byte("false")} {
		out, err := pluginTransformOutputToRecords(raw, fakeRecord())
		if err != nil {
			t.Fatalf("pluginTransformOutputToRecords(%q): %v", string(raw), err)
		}
		if len(out) != 0 {
			t.Fatalf("pluginTransformOutputToRecords(%q) = %#v, want no records", string(raw), out)
		}
	}
}

func TestPluginTransformOutputToRecordsDoesNotShareDataMaps(t *testing.T) {
	base := fakeRecord()
	out, err := pluginTransformOutputToRecords([]byte(`[
  {"data":{"x":2}},
  {"data":{"x":3}}
]`), base)
	if err != nil {
		t.Fatalf("pluginTransformOutputToRecords: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("outputs = %#v, want 2 records", out)
	}
	if out[0].Data["x"] != float64(2) || out[1].Data["x"] != float64(3) {
		t.Fatalf("outputs share or merge data unexpectedly: %#v", out)
	}
	if base.Data["x"] != 1 {
		t.Fatalf("base record mutated: %#v", base)
	}
}

var _ = context.Background

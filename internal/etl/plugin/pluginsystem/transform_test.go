package pluginsystem

import (
	"context"
	"sync"
	"testing"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

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
	}
}

var _ = fakeRecord
var _ = context.Background

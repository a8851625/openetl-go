//go:build !extism

package pluginsystem

import (
	"context"
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// Manager is a no-op manager used when the binary is built without WASM support
// (no extism build tag). All operations return descriptive errors so callers
// can detect the missing capability at runtime (P4-25, TF-4).
type Manager struct {
	store      storage.Storage
	pluginsDir string
}

// NewManager creates a no-op plugin manager. WASM plugins are not available
// in this build; rebuild with -tags=extism to enable them.
func NewManager(store storage.Storage, pluginsDir string) (*Manager, error) {
	return &Manager{store: store, pluginsDir: pluginsDir}, nil
}

func (m *Manager) Install(ctx context.Context, name string, kind PluginKind, version string, wasmBytes []byte) error {
	return fmt.Errorf("WASM plugins are not available in this build (rebuild with -tags=extism)")
}

func (m *Manager) Uninstall(ctx context.Context, name string) error {
	return m.store.DeletePlugin(ctx, name)
}

func (m *Manager) List() []*PluginMeta {
	return nil
}

func (m *Manager) Get(name string) (*PluginMeta, error) {
	return nil, fmt.Errorf("plugin %s not found (WASM support not compiled in)", name)
}

func (m *Manager) ExecTransform(ctx context.Context, pluginName string, rec core.Record) (core.Record, error) {
	return rec, fmt.Errorf("WASM plugins are not available in this build (rebuild with -tags=extism)")
}

func (m *Manager) ExecTransformWithConfig(ctx context.Context, pluginName string, rec core.Record, config map[string]any) (core.Record, error) {
	return rec, fmt.Errorf("WASM plugins are not available in this build (rebuild with -tags=extism)")
}

func (m *Manager) ExecTransformRecords(ctx context.Context, pluginName string, rec core.Record) ([]core.Record, error) {
	return nil, fmt.Errorf("WASM plugins are not available in this build (rebuild with -tags=extism)")
}

func (m *Manager) ExecTransformRecordsWithConfig(ctx context.Context, pluginName string, rec core.Record, config map[string]any) ([]core.Record, error) {
	return nil, fmt.Errorf("WASM plugins are not available in this build (rebuild with -tags=extism)")
}

func (m *Manager) Close(ctx context.Context) {}

func (m *Manager) RegisterTransforms() {}

func (m *Manager) RegisterSources() {}

func (m *Manager) RegisterSinks() {}

//go:build extism

package pluginsystem

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	extism "github.com/extism/go-sdk"
	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/storage"
)

// Manager manages the lifecycle of extism WASM plugins.
type Manager struct {
	store      storage.Storage
	pluginsDir string
	mu         sync.RWMutex
	plugins    map[string]*loadedPlugin
}

type loadedPlugin struct {
	meta   *PluginMeta
	extism *extism.Plugin
	hctx   *HostFunctionContext
}

// NewManager creates a new plugin manager.
func NewManager(store storage.Storage, pluginsDir string) (*Manager, error) {
	if err := os.MkdirAll(pluginsDir, 0755); err != nil {
		return nil, fmt.Errorf("create plugins dir: %w", err)
	}
	m := &Manager{
		store:      store,
		pluginsDir: pluginsDir,
		plugins:    map[string]*loadedPlugin{},
	}
	m.loadFromStorage(context.Background())
	return m, nil
}

// Install installs a WASM plugin from bytes.
func (m *Manager) Install(ctx context.Context, name string, kind PluginKind, version string, wasmBytes []byte) error {
	if name == "" {
		return fmt.Errorf("plugin name is required")
	}
	wasmPath := filepath.Join(m.pluginsDir, name+".wasm")
	if err := os.WriteFile(wasmPath, wasmBytes, 0644); err != nil {
		return fmt.Errorf("write wasm file: %w", err)
	}

	entry := &storage.PluginEntry{
		Name:     name,
		Kind:     string(kind),
		WASMPath: wasmPath,
		Version:  version,
		Enabled:  true,
	}
	if err := m.store.SavePlugin(ctx, entry); err != nil {
		return fmt.Errorf("save plugin to storage: %w", err)
	}

	if err := m.loadPlugin(ctx, entry); err != nil {
		return fmt.Errorf("load plugin: %w", err)
	}

	g.Log().Infof(ctx, "Plugin installed: %s (%s v%s)", name, kind, version)
	return nil
}

// loadPlugin instantiates an extism plugin from a storage entry.
func (m *Manager) loadPlugin(ctx context.Context, entry *storage.PluginEntry) error {
	wasmBytes, err := os.ReadFile(entry.WASMPath)
	if err != nil {
		return fmt.Errorf("read wasm: %w", err)
	}

	manifest := extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmData{Data: wasmBytes, Name: entry.Name},
		},
		Timeout:      30000,
		AllowedHosts: []string{},
	}

	config := extism.PluginConfig{
		EnableWasi: false,
	}

	// Build host functions for this plugin.
	hctx := NewHostFunctionContext("", entry.Name, nil)
	hostFuncs := buildHostFunctions(hctx)

	extismPlugin, err := extism.NewPlugin(ctx, manifest, config, hostFuncs)
	if err != nil {
		return fmt.Errorf("create extism plugin: %w", err)
	}

	m.mu.Lock()
	m.plugins[entry.Name] = &loadedPlugin{
		meta: &PluginMeta{
			Name:    entry.Name,
			Kind:    PluginKind(entry.Kind),
			Version: entry.Version,
			Enabled: entry.Enabled,
			Path:    entry.WASMPath,
		},
		extism: extismPlugin,
		hctx:   hctx,
	}
	m.mu.Unlock()
	return nil
}

// loadFromStorage loads all enabled plugins from storage at startup.
func (m *Manager) loadFromStorage(ctx context.Context) {
	entries, err := m.store.ListPlugins(ctx)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.Enabled {
			continue
		}
		if err := m.loadPlugin(ctx, entry); err != nil {
			g.Log().Warningf(ctx, "Failed to load plugin %s: %v", entry.Name, err)
		} else {
			g.Log().Infof(ctx, "Loaded plugin: %s", entry.Name)
		}
	}
}

// Uninstall removes a plugin from storage and memory.
func (m *Manager) Uninstall(ctx context.Context, name string) error {
	m.mu.Lock()
	if lp, ok := m.plugins[name]; ok {
		lp.extism.Close(ctx)
		delete(m.plugins, name)
	}
	m.mu.Unlock()

	if err := m.store.DeletePlugin(ctx, name); err != nil {
		return fmt.Errorf("delete plugin from storage: %w", err)
	}

	wasmPath := filepath.Join(m.pluginsDir, name+".wasm")
	os.Remove(wasmPath)

	g.Log().Infof(ctx, "Plugin uninstalled: %s", name)
	return nil
}

// List returns metadata for all installed plugins.
func (m *Manager) List() []*PluginMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*PluginMeta, 0, len(m.plugins))
	for _, lp := range m.plugins {
		result = append(result, lp.meta)
	}
	return result
}

// Get returns metadata for a specific plugin.
func (m *Manager) Get(name string) (*PluginMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lp, ok := m.plugins[name]
	if !ok {
		return nil, fmt.Errorf("plugin %s not found", name)
	}
	return lp.meta, nil
}

// ExecTransform runs a transform plugin on a single record.
// The WASM plugin must export a function named "transform" that accepts
// and returns a JSON-encoded Record.
func (m *Manager) ExecTransform(ctx context.Context, pluginName string, rec core.Record) (core.Record, error) {
	m.mu.RLock()
	lp, ok := m.plugins[pluginName]
	m.mu.RUnlock()
	if !ok {
		return rec, fmt.Errorf("plugin %s not found", pluginName)
	}

	input, err := json.Marshal(rec)
	if err != nil {
		return rec, fmt.Errorf("marshal record: %w", err)
	}

	_, output, err := lp.extism.CallWithContext(ctx, "transform", input)
	if err != nil {
		return rec, fmt.Errorf("plugin %s call failed: %w", pluginName, err)
	}

	var result core.Record
	if err := json.Unmarshal(output, &result); err != nil {
		return rec, fmt.Errorf("unmarshal plugin output: %w", err)
	}
	return result, nil
}

// ExecTransformWithConfig runs a transform plugin with an updated config context.
// The config map is merged into the plugin's HostFunctionContext so that
// host_config_get() calls inside the WASM plugin can read the values.
func (m *Manager) ExecTransformWithConfig(ctx context.Context, pluginName string, rec core.Record, config map[string]any) (core.Record, error) {
	m.mu.RLock()
	lp, ok := m.plugins[pluginName]
	m.mu.RUnlock()
	if !ok {
		return rec, fmt.Errorf("plugin %s not found", pluginName)
	}

	// Update config on the host function context.
	if config != nil && lp.hctx != nil {
		lp.hctx.mu.Lock()
		for k, v := range config {
			lp.hctx.Config[k] = v
		}
		lp.hctx.mu.Unlock()
	}

	return m.ExecTransform(ctx, pluginName, rec)
}

// Close unloads all plugins.
func (m *Manager) Close(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, lp := range m.plugins {
		lp.extism.Close(ctx)
		delete(m.plugins, name)
	}
}

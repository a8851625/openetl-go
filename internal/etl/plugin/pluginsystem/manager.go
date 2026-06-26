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

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
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
// a JSON-encoded Record and returns either an empty output, one JSON-encoded
// Record/data object, or an array of Record/data objects.
func (m *Manager) ExecTransform(ctx context.Context, pluginName string, rec core.Record) (core.Record, error) {
	records, err := m.ExecTransformRecords(ctx, pluginName, rec)
	if err != nil {
		return rec, err
	}
	switch len(records) {
	case 0:
		return rec, core.ErrRecordFiltered
	case 1:
		return records[0], nil
	default:
		return rec, fmt.Errorf("plugin %s produced %d records; use batch execution for multi-output transform results", pluginName, len(records))
	}
}

// ExecTransformRecords runs a transform plugin on a single record and returns
// zero, one, or many output records according to Plugin ABI v1.
func (m *Manager) ExecTransformRecords(ctx context.Context, pluginName string, rec core.Record) ([]core.Record, error) {
	m.mu.RLock()
	lp, ok := m.plugins[pluginName]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("plugin %s not found", pluginName)
	}

	input, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal record: %w", err)
	}

	_, output, err := lp.extism.CallWithContext(ctx, "transform", input)
	if err != nil {
		return nil, fmt.Errorf("plugin %s call failed: %w", pluginName, err)
	}

	records, err := pluginTransformOutputToRecords(output, rec)
	if err != nil {
		return nil, fmt.Errorf("parse plugin %s transform output: %w", pluginName, err)
	}
	return records, nil
}

// ExecTransformWithConfig runs a transform plugin with an updated config context.
// The config map is merged into the plugin's HostFunctionContext so that
// host_config_get() calls inside the WASM plugin can read the values.
func (m *Manager) ExecTransformWithConfig(ctx context.Context, pluginName string, rec core.Record, config map[string]any) (core.Record, error) {
	records, err := m.ExecTransformRecordsWithConfig(ctx, pluginName, rec, config)
	if err != nil {
		return rec, err
	}
	switch len(records) {
	case 0:
		return rec, core.ErrRecordFiltered
	case 1:
		return records[0], nil
	default:
		return rec, fmt.Errorf("plugin %s produced %d records; use batch execution for multi-output transform results", pluginName, len(records))
	}
}

// ExecTransformRecordsWithConfig runs a transform plugin with an updated config
// context and returns all output records.
func (m *Manager) ExecTransformRecordsWithConfig(ctx context.Context, pluginName string, rec core.Record, config map[string]any) ([]core.Record, error) {
	m.mu.RLock()
	lp, ok := m.plugins[pluginName]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("plugin %s not found", pluginName)
	}

	// Update config on the host function context.
	if config != nil && lp.hctx != nil {
		lp.hctx.mu.Lock()
		for k, v := range config {
			lp.hctx.Config[k] = v
		}
		lp.hctx.mu.Unlock()
	}

	return m.ExecTransformRecords(ctx, pluginName, rec)
}

func pluginTransformOutputToRecords(output []byte, base core.Record) ([]core.Record, error) {
	trimmed := string(bytesTrimSpace(output))
	switch {
	case trimmed == "", trimmed == "null", trimmed == "undefined", trimmed == "false":
		return nil, nil
	case len(trimmed) > 0 && trimmed[0] == '[':
		var items []json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &items); err != nil {
			return nil, fmt.Errorf("array unmarshal: %w", err)
		}
		records := make([]core.Record, 0, len(items))
		for i, item := range items {
			rec, err := pluginTransformRecordFromJSON(item, base)
			if err != nil {
				return nil, fmt.Errorf("result[%d]: %w", i, err)
			}
			records = append(records, rec)
		}
		return records, nil
	case len(trimmed) > 0 && trimmed[0] == '{':
		rec, err := pluginTransformRecordFromJSON(json.RawMessage(trimmed), base)
		if err != nil {
			return nil, err
		}
		return []core.Record{rec}, nil
	default:
		return nil, fmt.Errorf("result must be empty/null/false, a record or data object, or an array of records/data objects")
	}
}

func pluginTransformRecordFromJSON(raw json.RawMessage, base core.Record) (core.Record, error) {
	trimmed := string(bytesTrimSpace(raw))
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return base, fmt.Errorf("record output must be an object")
	}

	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return base, fmt.Errorf("record output unmarshal: %w", err)
	}

	if _, ok := probe["data"]; ok || pluginJSONHasAnyKey(probe, "before", "metadata", "operation") {
		out := base
		out.Data = clonePluginDataMap(base.Data)
		out.Before = clonePluginDataMap(base.Before)
		if _, hasData := probe["data"]; hasData {
			out.Data = nil
		}
		if _, hasBefore := probe["before"]; hasBefore {
			out.Before = nil
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return base, fmt.Errorf("record output unmarshal: %w", err)
		}
		return out, nil
	}

	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return base, fmt.Errorf("data output unmarshal: %w", err)
	}
	out := base
	out.Data = data
	return out, nil
}

func pluginJSONHasAnyKey(m map[string]json.RawMessage, keys ...string) bool {
	for _, key := range keys {
		if _, ok := m[key]; ok {
			return true
		}
	}
	return false
}

func clonePluginDataMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func bytesTrimSpace(in []byte) []byte {
	start := 0
	for start < len(in) {
		switch in[start] {
		case ' ', '\n', '\r', '\t':
			start++
		default:
			goto foundStart
		}
	}
	return nil
foundStart:
	end := len(in)
	for end > start {
		switch in[end-1] {
		case ' ', '\n', '\r', '\t':
			end--
		default:
			return in[start:end]
		}
	}
	return in[start:end]
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

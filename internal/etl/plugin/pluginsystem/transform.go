//go:build extism

package pluginsystem

import (
	"context"
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

// init registers a dynamic transform factory for each installed plugin kind=transform.
// The factory returns a core.Transform whose Apply delegates to Manager.ExecTransform.
// Registration happens at server startup after Manager is constructed.
func init() {
	// No auto-registration here; RegisterTransforms is called explicitly by the
	// server once the Manager has loaded plugins from storage.
}

// pluginTransform adapts a WASM plugin to the core.Transform interface.
// It captures the YAML config and injects it into the plugin's HostFunctionContext.
type pluginTransform struct {
	name    string
	manager *Manager
	config  map[string]any
}

func (t *pluginTransform) Name() string { return t.name }

func (t *pluginTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	out, err := t.manager.ExecTransformWithConfig(ctx, t.name, rec, t.config)
	if err != nil {
		return rec, fmt.Errorf("plugin transform %s: %w", t.name, err)
	}
	return out, nil
}

// RegisterTransforms registers every loaded transform-kind plugin as a
// core.Transform in the registry, using the type prefix "plugin_<name>".
// Pipeline specs reference them via:
//
//	transforms:
//	  - type: plugin_<name>
//
// This must be called after Manager has loaded plugins (e.g. from storage).
func (m *Manager) RegisterTransforms() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for name, lp := range m.plugins {
		if lp.meta.Kind != KindTransform {
			continue
		}
		pluginName := name
		manager := m
		registry.RegisterTransform("plugin_"+pluginName, func(config map[string]any) (core.Transform, error) {
			return &pluginTransform{name: pluginName, manager: manager, config: config}, nil
		})
	}
}

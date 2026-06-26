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
	out, err := t.applyOne(ctx, rec)
	if err != nil {
		if err == core.ErrRecordFiltered {
			return rec, err
		}
		return rec, fmt.Errorf("plugin transform %s: %w", t.name, err)
	}
	switch len(out) {
	case 0:
		return rec, core.ErrRecordFiltered
	case 1:
		return out[0], nil
	default:
		return rec, fmt.Errorf("plugin transform %s produced %d records; use batch execution for multi-output plugin results", t.name, len(out))
	}
}

func (t *pluginTransform) ApplyBatch(ctx context.Context, recs []core.Record) ([]core.Record, error) {
	out := make([]core.Record, 0, len(recs))
	var failures []core.TransformRecordFailure

	for _, rec := range recs {
		produced, err := t.applyOne(ctx, rec)
		if err != nil {
			failures = append(failures, core.TransformRecordFailure{Record: rec, Err: err})
			continue
		}
		out = append(out, produced...)
	}
	if len(failures) > 0 {
		return out, core.NewPartialTransformError("plugin transform "+t.name+" partial transform failed", failures)
	}
	return out, nil
}

func (t *pluginTransform) applyOne(ctx context.Context, rec core.Record) ([]core.Record, error) {
	out, err := t.manager.ExecTransformRecordsWithConfig(ctx, t.name, rec, t.config)
	if err != nil {
		return nil, err
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

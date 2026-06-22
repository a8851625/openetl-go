//go:build extism

package pluginsystem

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

// ── Source Plugin Support ───────────────────────────────────────────────

// RegisterSources registers all loaded source-kind plugins as source builders
// in the registry, so pipeline specs can reference them as `type: plugin_<name>`.
func (m *Manager) RegisterSources() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for name, lp := range m.plugins {
		if lp.meta.Kind != KindSource {
			continue
		}
		pluginName := name
		manager := m
		registry.RegisterSource("plugin_"+pluginName, func(config map[string]any) (core.Source, error) {
			return &pluginSource{name: pluginName, manager: manager, config: config}, nil
		})
	}
}

// ExecSource opens a source plugin and returns a RecordReader.
// The WASM plugin must export a `read` function that returns a JSON record
// or an empty string (EOF). The plugin receives the config via host functions.
func (m *Manager) ExecSource(ctx context.Context, pluginName string, config map[string]any) (core.RecordReader, error) {
	m.mu.RLock()
	lp, ok := m.plugins[pluginName]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("source plugin %q not found", pluginName)
	}
	if lp.meta.Kind != KindSource {
		return nil, fmt.Errorf("plugin %q is not a source (kind=%s)", pluginName, lp.meta.Kind)
	}

	for k, v := range config {
		lp.hctx.Config[k] = v
	}

	reader := &pluginSourceReader{
		manager:    m,
		pluginName: pluginName,
		lp:         lp,
		records:    make(chan core.Record, 1024),
		errors:     make(chan error, 16),
		done:       make(chan struct{}),
	}

	go reader.run(ctx)
	return reader, nil
}

type pluginSource struct {
	name    string
	manager *Manager
	config  map[string]any
}

func (s *pluginSource) Name() string { return "plugin_" + s.name }

func (s *pluginSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	return s.manager.ExecSource(ctx, s.name, s.config)
}

// pluginSourceReader implements core.RecordReader by calling the WASM `read` export.
type pluginSourceReader struct {
	manager    *Manager
	pluginName string
	lp         *loadedPlugin
	records    chan core.Record
	errors     chan error
	done       chan struct{}
}

func (r *pluginSourceReader) run(ctx context.Context) {
	defer close(r.records)
	defer close(r.errors)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rec, err := r.callRead(ctx)
		if err != nil {
			if err == io.EOF {
				return
			}
			select {
			case r.errors <- err:
			case <-r.done:
				return
			}
			return
		}
		if rec == nil {
			// EOF from plugin
			return
		}
		select {
		case r.records <- *rec:
		case <-r.done:
			return
		}
	}
}

func (r *pluginSourceReader) callRead(ctx context.Context) (*core.Record, error) {
	r.manager.mu.RLock()
	lp := r.lp
	r.manager.mu.RUnlock()

	_, output, err := lp.extism.CallWithContext(ctx, "read", nil)
	if err != nil {
		return nil, fmt.Errorf("wasm source read: %w", err)
	}
	if len(output) == 0 {
		return nil, io.EOF
	}
	var rec core.Record
	if err := json.Unmarshal(output, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal source record: %w", err)
	}
	return &rec, nil
}

func (r *pluginSourceReader) Read(ctx context.Context) (core.Record, error) {
	select {
	case rec, ok := <-r.records:
		if !ok {
			return core.Record{}, io.EOF
		}
		return rec, nil
	case err := <-r.errors:
		return core.Record{}, err
	case <-ctx.Done():
		return core.Record{}, ctx.Err()
	}
}

func (r *pluginSourceReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	batch := make([]core.Record, 0, n)
	for i := 0; i < n; i++ {
		rec, err := r.Read(ctx)
		if err != nil {
			if len(batch) > 0 {
				return batch, nil
			}
			return nil, err
		}
		batch = append(batch, rec)
	}
	return batch, nil
}

func (r *pluginSourceReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{Source: "plugin_source", Timestamp: time.Now()}, nil
}

func (r *pluginSourceReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	return r.Snapshot(ctx)
}

func (r *pluginSourceReader) Close() error {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
	return nil
}

// ── Sink Plugin Support ─────────────────────────────────────────────────

// RegisterSinks registers all loaded sink-kind plugins as sink builders.
func (m *Manager) RegisterSinks() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for name, lp := range m.plugins {
		if lp.meta.Kind != KindSink {
			continue
		}
		pluginName := name
		manager := m
		registry.RegisterSink("plugin_"+pluginName, func(config map[string]any) (core.Sink, error) {
			return &pluginSink{name: pluginName, manager: manager, config: config}, nil
		})
	}
}

// ExecSink writes records via a sink plugin's `write` WASM export.
func (m *Manager) ExecSink(ctx context.Context, pluginName string, records []core.Record, config map[string]any) error {
	m.mu.RLock()
	lp, ok := m.plugins[pluginName]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("sink plugin %q not found", pluginName)
	}
	if lp.meta.Kind != KindSink {
		return fmt.Errorf("plugin %q is not a sink (kind=%s)", pluginName, lp.meta.Kind)
	}

	for k, v := range config {
		lp.hctx.Config[k] = v
	}

	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("marshal records for sink: %w", err)
	}

	_, _, err = lp.extism.CallWithContext(ctx, "write", data)
	if err != nil {
		return fmt.Errorf("wasm sink write: %w", err)
	}
	return nil
}

type pluginSink struct {
	name    string
	manager *Manager
	config  map[string]any
}

func (s *pluginSink) Name() string { return "plugin_" + s.name }

func (s *pluginSink) Open(ctx context.Context) error {
	m := s.manager
	m.mu.RLock()
	lp, ok := m.plugins[s.name]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("sink plugin %q not found", s.name)
	}
	for k, v := range s.config {
		lp.hctx.Config[k] = v
	}
	return nil
}

func (s *pluginSink) Write(ctx context.Context, records []core.Record) error {
	return s.manager.ExecSink(ctx, s.name, records, s.config)
}

func (s *pluginSink) Close() error { return nil }

//go:build cgo

package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/buke/quickjs-go"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("ts", func(config map[string]any) (core.Transform, error) {
		return NewTSTransform(config)
	})
	registry.RegisterTransform("javascript", func(config map[string]any) (core.Transform, error) {
		return NewTSTransform(config)
	})
	registry.RegisterTransform("js", func(config map[string]any) (core.Transform, error) {
		return NewTSTransform(config)
	})
}

// TSTransform executes inline TypeScript/JavaScript code for each record.
// Uses QuickJS (requires CGO). When built with CGO_ENABLED=0, this transform
// is unavailable and the stub in ts_nocgo.go is used instead.
type TSTransform struct {
	code       string
	fnName     string
	mu         sync.Mutex
	runtime    *quickjs.Runtime
	ctx        *quickjs.Context
	timeoutSec uint64 // per-record execution budget in seconds (TF-2)
}

func NewTSTransform(config map[string]any) (*TSTransform, error) {
	code, _ := config["script"].(string)
	if code == "" {
		code, _ = config["code"].(string)
	}
	if code == "" {
		return nil, fmt.Errorf("ts: script or code is required")
	}

	// Per-record execution budget in seconds (TF-2). QuickJS's
	// SetExecuteTimeout (a wall-clock deadline set before each eval) aborts a
	// runaway script (while(true){}) — converting a hang into a per-record
	// error → DLQ. quickjs-go's timeout is integer-seconds (time_t), min 1.
	timeoutSec := uint64(5)
	if v, ok := config["timeout_ms"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			s := uint64(n) / 1000
			if s == 0 {
				s = 1
			}
			timeoutSec = s
		}
	}

	t := &TSTransform{code: code, timeoutSec: timeoutSec}

	for _, name := range []string{"transform", "apply", "filter", "process"} {
		if strings.Contains(code, "function "+name) || strings.Contains(code, name+"(record") {
			t.fnName = name
			break
		}
	}
	if t.fnName == "" {
		t.fnName = "__etl_transform"
	}

	if err := t.compileCheck(); err != nil {
		return nil, err
	}

	if err := t.initRuntime(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *TSTransform) compileCheck() error {
	runtime := quickjs.NewRuntime()
	defer runtime.Close()
	ctx := runtime.NewContext()
	defer ctx.Close()
	val := ctx.Eval(t.wrappedCode())
	defer val.Free()
	if val.IsException() {
		if err := val.Error(); err != nil {
			return fmt.Errorf("ts compile error: %w", err)
		}
		return fmt.Errorf("ts compile error: unknown exception")
	}
	return nil
}

func (t *TSTransform) wrappedCode() string {
	if t.fnName == "__etl_transform" {
		return fmt.Sprintf("var __etl_transform = %s;", t.code)
	}
	return t.code
}

func (t *TSTransform) Name() string { return "ts" }

func (t *TSTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	out, err := t.applyOne(ctx, rec)
	if err != nil {
		return rec, err
	}
	switch len(out) {
	case 0:
		return rec, core.ErrRecordFiltered
	case 1:
		return out[0], nil
	default:
		return rec, fmt.Errorf("ts produced %d records; use batch execution for multi-output script results", len(out))
	}
}

func (t *TSTransform) ApplyBatch(ctx context.Context, recs []core.Record) ([]core.Record, error) {
	out := make([]core.Record, 0, len(recs))
	var failures []core.TransformRecordFailure

	for _, rec := range recs {
		produced, err := t.applyOne(ctx, rec)
		if err != nil {
			failures = append(failures, core.TransformRecordFailure{
				Record: rec,
				Err:    core.ClassifiedError{Class: core.ErrorClassData, Err: err},
			})
			continue
		}
		out = append(out, produced...)
	}
	if len(failures) > 0 {
		return out, core.NewPartialTransformError("ts partial transform failed", failures)
	}
	return out, nil
}

func (t *TSTransform) initRuntime() error {
	// Create a persistent runtime+context reused across Apply calls. The
	// caller serializes access with t.mu after construction.
	runtime := quickjs.NewRuntime()
	runtime.SetMemoryLimit(32 * 1024 * 1024) // 32MB cap; prevents unbounded memory
	ctx := runtime.NewContext()

	setupVal := ctx.Eval(t.wrappedCode())
	if setupVal.IsException() {
		var setupErr error
		if err := setupVal.Error(); err != nil {
			setupErr = fmt.Errorf("ts setup: %w", err)
		} else {
			setupErr = fmt.Errorf("ts setup: unknown exception")
		}
		setupVal.Free()
		ctx.Close()
		runtime.Close()
		return setupErr
	}
	setupVal.Free()

	t.runtime = runtime
	t.ctx = ctx
	return nil
}

func (t *TSTransform) applyOne(ctx context.Context, rec core.Record) ([]core.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.runtime == nil || t.ctx == nil {
		if err := t.initRuntime(); err != nil {
			return nil, err
		}
	}

	// Per-record wall-clock budget (TF-2): set a fresh deadline before each
	// eval so a runaway script (while(true){}) is aborted → exception → DLQ.
	// (SetExecuteTimeout sets now+timeout; must be refreshed per Apply.)
	t.runtime.SetExecuteTimeout(t.timeoutSec)

	// The script body is evaluated once in the constructor; the transform
	// function is already defined on the persistent context.

	recJSON, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("ts marshal record: %w", err)
	}

	callCode := fmt.Sprintf(`%s(JSON.parse(%s))`, t.fnName, quoteJSON(string(recJSON)))
	result := t.ctx.Eval(callCode)
	defer result.Free()
	if result.IsException() {
		if err := result.Error(); err != nil {
			return nil, fmt.Errorf("ts execute: %w", err)
		}
		return nil, fmt.Errorf("ts execute: unknown exception")
	}

	if result.IsNull() || result.IsUndefined() {
		return nil, nil
	}

	return tsRecordsFromJSON(result.JSONStringify(), rec)
}

// Close releases the persistent QuickJS runtime/context.
func (t *TSTransform) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx != nil {
		t.ctx.Close()
		t.ctx = nil
	}
	if t.runtime != nil {
		t.runtime.Close()
		t.runtime = nil
	}
	return nil
}

func quoteJSON(s string) string {
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}

func tsRecordsFromJSON(raw string, base core.Record) ([]core.Record, error) {
	trimmed := strings.TrimSpace(raw)
	switch {
	case trimmed == "", trimmed == "undefined", trimmed == "null", trimmed == "false":
		return nil, nil
	case strings.HasPrefix(trimmed, "["):
		var items []json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &items); err != nil {
			return nil, fmt.Errorf("ts result array unmarshal: %w", err)
		}
		out := make([]core.Record, 0, len(items))
		for i, item := range items {
			rec, err := tsRecordFromJSON(item, base)
			if err != nil {
				return nil, fmt.Errorf("ts result[%d]: %w", i, err)
			}
			out = append(out, rec)
		}
		return out, nil
	case strings.HasPrefix(trimmed, "{"):
		rec, err := tsRecordFromJSON(json.RawMessage(trimmed), base)
		if err != nil {
			return nil, err
		}
		return []core.Record{rec}, nil
	default:
		return nil, fmt.Errorf("ts result must be null/undefined/false, a record or data object, or an array of records/data objects; got %s", trimmed)
	}
}

func tsRecordFromJSON(raw json.RawMessage, base core.Record) (core.Record, error) {
	trimmed := strings.TrimSpace(string(raw))
	if !strings.HasPrefix(trimmed, "{") {
		return base, fmt.Errorf("record output must be an object")
	}

	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return base, fmt.Errorf("record output unmarshal: %w", err)
	}

	if _, ok := probe["data"]; ok || hasAnyJSONKey(probe, "before", "metadata", "operation") {
		out := base
		out.Data = cloneTSMap(base.Data)
		out.Before = cloneTSMap(base.Before)
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

func hasAnyJSONKey(m map[string]json.RawMessage, keys ...string) bool {
	for _, key := range keys {
		if _, ok := m[key]; ok {
			return true
		}
	}
	return false
}

func cloneTSMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

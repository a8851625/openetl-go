//go:build cgo

package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/buke/quickjs-go"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
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
	code    string
	fnName  string
	mu      sync.Mutex
	runtime *quickjs.Runtime
	ctx     *quickjs.Context
}

func NewTSTransform(config map[string]any) (*TSTransform, error) {
	code, _ := config["script"].(string)
	if code == "" {
		code, _ = config["code"].(string)
	}
	if code == "" {
		return nil, fmt.Errorf("ts: script or code is required")
	}

	t := &TSTransform{code: code}

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

	// Create a persistent runtime+context reused across all Apply calls.
	// Guard with the existing mutex to serialize access.
	t.runtime = quickjs.NewRuntime()
	t.runtime.SetMemoryLimit(32 * 1024 * 1024) // 32MB cap; prevents malicious scripts from unbounded memory
	t.ctx = t.runtime.NewContext()

	// Eval the script body once so the transform function is defined.
	setupVal := t.ctx.Eval(t.wrappedCode())
	if setupVal.IsException() {
		setupVal.Free()
		t.ctx.Close()
		t.runtime.Close()
		if err := setupVal.Error(); err != nil {
			return nil, fmt.Errorf("ts setup: %w", err)
		}
		return nil, fmt.Errorf("ts setup: unknown exception")
	}
	setupVal.Free()

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
	t.mu.Lock()
	defer t.mu.Unlock()

	// The script body is evaluated once in the constructor; the transform
	// function is already defined on the persistent context.

	recJSON, err := json.Marshal(rec)
	if err != nil {
		return rec, fmt.Errorf("ts marshal record: %w", err)
	}

	callCode := fmt.Sprintf(`%s(JSON.parse(%s))`, t.fnName, quoteJSON(string(recJSON)))
	result := t.ctx.Eval(callCode)
	defer result.Free()
	if result.IsException() {
		if err := result.Error(); err != nil {
			return rec, fmt.Errorf("ts execute: %w", err)
		}
		return rec, fmt.Errorf("ts execute: unknown exception")
	}

	if result.IsNull() || result.IsUndefined() {
		return rec, core.ErrRecordFiltered
	}

	jsonVal := t.ctx.Eval(fmt.Sprintf(`JSON.stringify(%s(JSON.parse(%s)))`, t.fnName, quoteJSON(string(recJSON))))
	defer jsonVal.Free()
	if jsonVal.IsException() {
		if err := jsonVal.Error(); err != nil {
			return rec, fmt.Errorf("ts result marshal: %w", err)
		}
		return rec, fmt.Errorf("ts result marshal: exception")
	}

	var out core.Record
	if err := json.Unmarshal([]byte(jsonVal.String()), &out); err != nil {
		return rec, fmt.Errorf("ts result unmarshal: %w", err)
	}
	return out, nil
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

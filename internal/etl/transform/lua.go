//go:build !nolua

package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("lua", func(config map[string]any) (core.Transform, error) {
		script := ""
		if v, ok := config["script"]; ok {
			if vs, ok := v.(string); ok {
				script = vs
			}
		}
		if v, ok := config["code"]; ok {
			if vs, ok := v.(string); ok {
				script = vs
			}
		}
		// Per-record execution budget (TF-1). A runaway script (while-true) or
		// unbounded allocation loop is aborted when this fires (or the pipeline
		// ctx cancels), converting a process hang/OOM into a per-record error
		// that routes to DLQ. Default 5s.
		timeoutMs := 5000
		if v, ok := config["timeout_ms"]; ok {
			if n, ok := toInt(v); ok && n > 0 {
				timeoutMs = n
			}
		}
		lt, err := NewLuaTransform(script)
		if err != nil {
			return nil, err
		}
		if timeoutMs != 5000 {
			lt.timeout = time.Duration(timeoutMs) * time.Millisecond
		}
		return lt, nil
	})
}

// LuaTransform executes a user-provided Lua script for each record.
//
// Implementation:
//   - The Lua state is created ONCE at construction time and reused for every
//     Apply() call (10-100x faster than recreating per record).
//   - Per-record Apply holds a mutex (gopher-lua is not goroutine-safe).
//   - The state is sandboxed: io, loadfile, dofile, loadlib, package, and
//     require are stripped entirely; os is field-pruned to leave only
//     time/date/clock/difftime for time math (execute/exit/remove/rename/
//     getenv/tmpname/setlocale are nilled) so untrusted scripts can't touch
//     the filesystem or spawn processes.
//   - Each Apply runs under a per-record timeout ctx (gopher-lua SetContext):
//     a runaway script is aborted instead of hanging or OOMing the process.
//     (gopher-lua's SetMx memory cap is deliberately NOT used — it calls
//     os.Exit on overflow, killing the whole server.)
type LuaTransform struct {
	script string

	// mu protects L and fn during concurrent Apply() calls.
	mu sync.Mutex
	L  *lua.LState
	fn *lua.LFunction // compiled script as a callable function

	// timeout is the per-record execution budget. See init() docstring.
	timeout time.Duration
}

func NewLuaTransform(script string) (*LuaTransform, error) {
	if script == "" {
		return nil, fmt.Errorf("lua transform: script (or code) is required")
	}
	t := &LuaTransform{script: script, timeout: 5 * time.Second}

	// Initialize the sandboxed Lua state.
	L := lua.NewState()
	sandbox(L)

	// Compile the script once so we don't re-parse on each Apply.
	fn, err := L.LoadString(script)
	if err != nil {
		L.Close()
		return nil, fmt.Errorf("lua compile: %w", err)
	}
	L.Push(fn)

	t.L = L
	t.fn = fn
	return t, nil
}

// sandbox removes dangerous builtins from the Lua state.
func sandbox(L *lua.LState) {
	// Field-prune os (keep date/time/clock/difftime for time math; nil the
	// dangerous ones). Use comma-ok so a non-table `os` global can't panic
	// (TF-12) — previously a bare .(*lua.LTable) assertion.
	if osTbl, ok := L.GetGlobal("os").(*lua.LTable); ok {
		for _, field := range []string{"execute", "exit", "remove", "rename", "getenv", "tmpname", "setlocale"} {
			osTbl.RawSetString(field, lua.LNil)
		}
	}
	L.SetGlobal("io", lua.LNil)
	L.SetGlobal("loadfile", lua.LNil)
	L.SetGlobal("dofile", lua.LNil)
	L.SetGlobal("loadlib", lua.LNil)
	L.SetGlobal("package", lua.LNil)
	L.SetGlobal("require", lua.LNil)
}

func (t *LuaTransform) Name() string { return "lua" }

func (t *LuaTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Per-record execution budget (TF-1). gopher-lua's SetContext makes the VM
	// check ctx.Done() in its main loop and abort with the ctx error. Derive a
	// child ctx with the per-record timeout so a runaway script (while-true) or
	// unbounded allocation loop is converted into a per-record error → DLQ
	// instead of hanging or OOMing the process. Also propagates pipeline cancel.
	execCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	t.L.SetContext(execCtx)

	// Reset record + metadata globals for this call.
	t.L.SetGlobal("record", mapToLuaTable(t.L, rec.Data))

	metaBytes, err := json.Marshal(rec.Metadata)
	if err != nil {
		return rec, fmt.Errorf("marshal metadata: %w", err)
	}
	meta := map[string]any{}
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		meta = map[string]any{}
	}
	t.L.SetGlobal("metadata", mapToLuaTable(t.L, meta))

	// Re-invoke the already-compiled function.
	t.L.Push(t.fn)
	if err := t.L.PCall(0, 0, nil); err != nil {
		// On script error we return the original record unchanged so the
		// pipeline can route it to DLQ via the normal sink-failure path if
		// desired. We still return the error so callers can react.
		return rec, fmt.Errorf("lua execute: %w", err)
	}

	rec.Data = luaTableToMap(t.L.GetGlobal("record"))

	// Read back metadata table so Lua scripts can modify table name, etc.
	metaResult := luaTableToMap(t.L.GetGlobal("metadata"))
	if metaResult != nil {
		if v, ok := metaResult["table"].(string); ok && v != "" {
			rec.Metadata.Table = v
		}
		if v, ok := metaResult["source"].(string); ok && v != "" {
			rec.Metadata.Source = v
		}
	}

	return rec, nil
}

// Close releases the Lua state. Implement core.Closer if needed.
func (t *LuaTransform) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.L != nil {
		t.L.Close()
		t.L = nil
	}
	return nil
}

func mapToLuaTable(L *lua.LState, m map[string]any) *lua.LTable {
	tbl := L.NewTable()
	for k, v := range m {
		tbl.RawSetString(k, anyToLuaValue(L, v))
	}
	return tbl
}

func anyToLuaValue(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case nil:
		return lua.LNil
	case bool:
		return lua.LBool(x)
	case int:
		return lua.LNumber(x)
	case int32:
		return lua.LNumber(x)
	case int64:
		return lua.LNumber(x)
	case float32:
		return lua.LNumber(x)
	case float64:
		return lua.LNumber(x)
	case string:
		return lua.LString(x)
	case map[string]any:
		return mapToLuaTable(L, x)
	case []any:
		tbl := L.NewTable()
		for i, item := range x {
			tbl.RawSetInt(i+1, anyToLuaValue(L, item))
		}
		return tbl
	default:
		return lua.LString(fmt.Sprintf("%v", x))
	}
}

func luaTableToMap(v lua.LValue) map[string]any {
	tbl, ok := v.(*lua.LTable)
	if !ok {
		return map[string]any{}
	}
	out := map[string]any{}
	tbl.ForEach(func(key, value lua.LValue) {
		out[key.String()] = luaValueToAny(value)
	})
	return out
}

func luaValueToAny(value lua.LValue) any {
	switch value.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		return bool(value.(lua.LBool))
	case lua.LTNumber:
		return float64(value.(lua.LNumber))
	case lua.LTString:
		return value.String()
	case lua.LTTable:
		tbl := value.(*lua.LTable)
		if luaTableIsArray(tbl) {
			out := make([]any, 0, tbl.Len())
			for i := 1; i <= tbl.Len(); i++ {
				out = append(out, luaValueToAny(tbl.RawGetInt(i)))
			}
			return out
		}
		return luaTableToMap(tbl)
	default:
		return value.String()
	}
}

func luaTableIsArray(tbl *lua.LTable) bool {
	count := 0
	maxIndex := 0
	array := true
	tbl.ForEach(func(key, _ lua.LValue) {
		if !array {
			return
		}
		num, ok := key.(lua.LNumber)
		if !ok {
			array = false
			return
		}
		idx := int(num)
		if idx <= 0 || lua.LNumber(idx) != num {
			array = false
			return
		}
		count++
		if idx > maxIndex {
			maxIndex = idx
		}
	})
	return array && count > 0 && count == maxIndex
}

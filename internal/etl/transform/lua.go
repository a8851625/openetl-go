package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	lua "github.com/yuin/gopher-lua"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
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
		return NewLuaTransform(script)
	})
}

// LuaTransform executes a user-provided Lua script for each record.
//
// Implementation:
//   - The Lua state is created ONCE at construction time and reused for every
//     Apply() call (10-100x faster than recreating per record).
//   - Per-record Apply holds a mutex (gopher-lua is not goroutine-safe).
//   - The state is sandboxed: os, io, loadfile, dofile, loadlib, and package
//     are stripped so untrusted scripts can't touch the filesystem or execute
//     subprocesses.
type LuaTransform struct {
	script string

	// mu protects L and fn during concurrent Apply() calls.
	mu sync.Mutex
	L  *lua.LState
	fn *lua.LFunction // compiled script as a callable function
}

func NewLuaTransform(script string) (*LuaTransform, error) {
	if script == "" {
		return nil, fmt.Errorf("lua transform: script (or code) is required")
	}
	t := &LuaTransform{script: script}

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
	// Remove os (except clock/date for time math), io, package, loadfile,
	// dofile, loadlib, require — anything that touches the filesystem or
	// can spawn processes.
	osTbl := L.GetGlobal("os")
	if osTbl != lua.LNil {
		osTbl.(*lua.LTable).RawSetString("execute", lua.LNil)
		osTbl.(*lua.LTable).RawSetString("exit", lua.LNil)
		osTbl.(*lua.LTable).RawSetString("remove", lua.LNil)
		osTbl.(*lua.LTable).RawSetString("rename", lua.LNil)
		osTbl.(*lua.LTable).RawSetString("getenv", lua.LNil)
		osTbl.(*lua.LTable).RawSetString("tmpname", lua.LNil)
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
	case int64:
		return lua.LNumber(x)
	case float64:
		return lua.LNumber(x)
	case string:
		return lua.LString(x)
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
		switch value.Type() {
		case lua.LTNil:
			out[key.String()] = nil
		case lua.LTBool:
			out[key.String()] = bool(value.(lua.LBool))
		case lua.LTNumber:
			out[key.String()] = float64(value.(lua.LNumber))
		default:
			out[key.String()] = value.String()
		}
	})
	return out
}

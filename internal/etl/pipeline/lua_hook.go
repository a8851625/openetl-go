//go:build !nolua

package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// ── Lua Hook ─────────────────────────────────────────────────────────

// LuaHook executes a Lua script at a lifecycle point.
// The script receives globals: config (table), pipeline_name, record_count,
// error_message, timestamp. It can modify the `config` table for state
// persistence across hooks within the same pipeline instance.
type LuaHook struct {
	name   string
	script string
	config map[string]any

	mu sync.Mutex
	L  *lua.LState
	fn *lua.LFunction
}

func NewLuaHook(pipelineName, script string, config map[string]any) (*LuaHook, error) {
	if script == "" {
		return nil, fmt.Errorf("lua hook: code is required")
	}
	h := &LuaHook{
		name:   "lua:" + pipelineName,
		script: script,
		config: config,
	}
	if h.config == nil {
		h.config = map[string]any{}
	}

	L := lua.NewState()
	sandboxLuaHook(L)
	fn, err := L.LoadString(script)
	if err != nil {
		L.Close()
		return nil, fmt.Errorf("lua hook compile: %w", err)
	}
	L.Push(fn)
	h.L = L
	h.fn = fn
	return h, nil
}

func (h *LuaHook) Name() string { return h.name }

// execLua runs the hook script with the given HookContext injected as globals.
func (h *LuaHook) execLua(hctx core.HookContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.L.SetGlobal("pipeline_name", lua.LString(hctx.PipelineName))
	h.L.SetGlobal("config", mapToLuaTable(h.L, h.config))
	h.L.SetGlobal("record_count", lua.LNumber(hctx.RecordCount))
	h.L.SetGlobal("error_message", lua.LString(hctx.ErrorMessage))
	h.L.SetGlobal("timestamp", lua.LString(hctx.Timestamp.Format(time.RFC3339)))

	h.L.Push(h.fn)
	if err := h.L.PCall(0, 0, nil); err != nil {
		return fmt.Errorf("lua hook execute: %w", err)
	}

	// Read back config in case the script modified it (state persistence).
	updatedConfig := luaTableToMap(h.L.GetGlobal("config"))
	if updatedConfig != nil {
		h.config = updatedConfig
	}
	return nil
}

// Implement all optional hook interfaces by delegating to execLua.

func (h *LuaHook) OnInit(ctx context.Context, hctx core.HookContext) error {
	return h.execLua(hctx)
}

func (h *LuaHook) OnPreBatch(ctx context.Context, hctx core.HookContext) error {
	return h.execLua(hctx)
}

func (h *LuaHook) OnPostBatch(ctx context.Context, hctx core.HookContext) error {
	return h.execLua(hctx)
}

func (h *LuaHook) OnError(ctx context.Context, hctx core.HookContext) error {
	return h.execLua(hctx)
}

func (h *LuaHook) OnCheckpoint(ctx context.Context, hctx core.HookContext) error {
	return h.execLua(hctx)
}

func (h *LuaHook) OnShutdown(ctx context.Context, hctx core.HookContext) error {
	return h.execLua(hctx)
}

// Close releases the Lua state.
func (h *LuaHook) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.L != nil {
		h.L.Close()
		h.L = nil
	}
	return nil
}

// ── Lua helpers (local copies to avoid cross-package private deps) ───

func sandboxLuaHook(L *lua.LState) {
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
		return nil
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

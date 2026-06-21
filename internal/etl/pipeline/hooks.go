package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/core"
)

// ── Hook Builder ─────────────────────────────────────────────────────

// BuildHooks constructs all hooks defined in the spec and returns them
// keyed by HookKind. Hooks that fail to build are logged and skipped
// (best-effort: a broken webhook shouldn't prevent the pipeline from starting).
func BuildHooks(pipelineName string, spec *HooksSpec) map[core.HookKind]core.LifecycleHook {
	if spec == nil {
		return nil
	}
	hooks := make(map[core.HookKind]core.LifecycleHook)

	buildOne := func(kind core.HookKind, hs *HookSpec) {
		if hs == nil {
			return
		}
		h, err := buildHook(pipelineName, hs)
		if err != nil {
			g.Log().Warningf(context.Background(), "[hook] skip %s for pipeline %s: %v", kind, pipelineName, err)
			return
		}
		hooks[kind] = h
	}

	buildOne(core.HookOnInit, spec.OnInit)
	buildOne(core.HookOnPreBatch, spec.OnPreBatch)
	buildOne(core.HookOnPostBatch, spec.OnPostBatch)
	buildOne(core.HookOnError, spec.OnError)
	buildOne(core.HookOnCheckpoint, spec.OnCheckpoint)
	buildOne(core.HookOnShutdown, spec.OnShutdown)

	if len(hooks) == 0 {
		return nil
	}
	return hooks
}

func buildHook(pipelineName string, hs *HookSpec) (core.LifecycleHook, error) {
	switch hs.Type {
	case "lua":
		return NewLuaHook(pipelineName, hs.Code, hs.Config)
	case "webhook":
		return NewWebhookHook(pipelineName, hs.Name, hs.Config)
	default:
		return nil, fmt.Errorf("unsupported hook type: %s", hs.Type)
	}
}

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

// ── Webhook Hook ─────────────────────────────────────────────────────

// WebhookHook fires an HTTP request to an external endpoint at a lifecycle point.
type WebhookHook struct {
	name    string
	url     string
	method  string
	headers map[string]string
	timeout time.Duration
}

func NewWebhookHook(pipelineName, hookName string, config map[string]any) (*WebhookHook, error) {
	url, _ := config["url"].(string)
	if url == "" {
		return nil, fmt.Errorf("webhook hook: url is required")
	}
	method, _ := config["method"].(string)
	if method == "" {
		method = "POST"
	}
	headers := make(map[string]string)
	if rawHeaders, ok := config["headers"].(map[string]any); ok {
		for k, v := range rawHeaders {
			headers[k] = fmt.Sprintf("%v", v)
		}
	}
	timeoutSec := 10
	if v, ok := config["timeout_sec"].(int); ok && v > 0 {
		timeoutSec = v
	}
	name := hookName
	if name == "" {
		name = "webhook:" + pipelineName
	}
	return &WebhookHook{
		name:    name,
		url:     url,
		method:  method,
		headers: headers,
		timeout: time.Duration(timeoutSec) * time.Second,
	}, nil
}

func (h *WebhookHook) Name() string { return h.name }

// execWebhook sends the HookContext as JSON to the configured URL.
func (h *WebhookHook) execWebhook(ctx context.Context, hctx core.HookContext) error {
	body, err := json.Marshal(hctx)
	if err != nil {
		return fmt.Errorf("marshal hook context: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, h.method, h.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}

func (h *WebhookHook) OnInit(ctx context.Context, hctx core.HookContext) error {
	return h.execWebhook(ctx, hctx)
}

func (h *WebhookHook) OnPreBatch(ctx context.Context, hctx core.HookContext) error {
	return h.execWebhook(ctx, hctx)
}

func (h *WebhookHook) OnPostBatch(ctx context.Context, hctx core.HookContext) error {
	return h.execWebhook(ctx, hctx)
}

func (h *WebhookHook) OnError(ctx context.Context, hctx core.HookContext) error {
	return h.execWebhook(ctx, hctx)
}

func (h *WebhookHook) OnCheckpoint(ctx context.Context, hctx core.HookContext) error {
	return h.execWebhook(ctx, hctx)
}

func (h *WebhookHook) OnShutdown(ctx context.Context, hctx core.HookContext) error {
	return h.execWebhook(ctx, hctx)
}

// ── Hook Dispatcher ──────────────────────────────────────────────────

// fireHook is a helper that type-asserts and calls the appropriate hook method.
// It logs errors but never returns them (hooks must not break the pipeline).
func fireHook(ctx context.Context, hooks map[core.HookKind]core.LifecycleHook, kind core.HookKind, hctx core.HookContext) {
	if hooks == nil {
		return
	}
	h, ok := hooks[kind]
	if !ok {
		return
	}
	hctx.Timestamp = time.Now()
	var err error
	switch kind {
	case core.HookOnInit:
		if hh, ok := h.(core.InitHook); ok {
			err = hh.OnInit(ctx, hctx)
		}
	case core.HookOnPreBatch:
		if hh, ok := h.(core.PreBatchHook); ok {
			err = hh.OnPreBatch(ctx, hctx)
		}
	case core.HookOnPostBatch:
		if hh, ok := h.(core.PostBatchHook); ok {
			err = hh.OnPostBatch(ctx, hctx)
		}
	case core.HookOnError:
		if hh, ok := h.(core.ErrorHook); ok {
			err = hh.OnError(ctx, hctx)
		}
	case core.HookOnCheckpoint:
		if hh, ok := h.(core.CheckpointHook); ok {
			err = hh.OnCheckpoint(ctx, hctx)
		}
	case core.HookOnShutdown:
		if hh, ok := h.(core.ShutdownHook); ok {
			err = hh.OnShutdown(ctx, hctx)
		}
	}
	if err != nil {
		g.Log().Warningf(ctx, "[hook] %s (%s): %v", kind, h.Name(), err)
	}
}

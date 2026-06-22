//go:build extism

package pluginsystem

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/extism/go-sdk"
	"github.com/gogf/gf/v2/frame/g"
)

// HostFunctionContext holds per-pipeline, per-plugin state accessible to host functions.
type HostFunctionContext struct {
	PipelineName string
	PluginName   string
	Config       map[string]any

	mu    sync.Mutex
	state map[string]string // simple in-memory KV state (also persisted via storage)
}

func NewHostFunctionContext(pipelineName, pluginName string, config map[string]any) *HostFunctionContext {
	return &HostFunctionContext{
		PipelineName: pipelineName,
		PluginName:   pluginName,
		Config:       config,
		state:        make(map[string]string),
	}
}

// KVStateGet retrieves a value from the plugin's in-memory KV store.
func (h *HostFunctionContext) KVStateGet(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state[key]
}

// KVStateSet stores a value in the plugin's in-memory KV store.
func (h *HostFunctionContext) KVStateSet(key, value string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state[key] = value
}

// ConfigGet retrieves a config value by key.
func (h *HostFunctionContext) ConfigGet(key string) string {
	if v, ok := h.Config[key]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// hostFunctionRegistry builds the extism HostFunction slice for a given plugin context.
// These functions are exposed to the WASM plugin under the "extism:host/user" namespace.
func buildHostFunctions(hctx *HostFunctionContext) []extism.HostFunction {
	return []extism.HostFunction{
		// host_log(level_offset, msg_offset) -> i64 (0 = success)
		extism.NewHostFunctionWithStack(
			"host_log",
			func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
				levelOff := stack[0]
				msgOff := stack[1]
				level, _ := p.ReadString(levelOff)
				msg, _ := p.ReadString(msgOff)
				switch level {
				case "error":
					g.Log().Errorf(ctx, "[plugin:%s] %s", hctx.PluginName, msg)
				case "warn":
					g.Log().Warningf(ctx, "[plugin:%s] %s", hctx.PluginName, msg)
				default:
					g.Log().Infof(ctx, "[plugin:%s] %s", hctx.PluginName, msg)
				}
				stack[0] = 0
			},
			[]extism.ValueType{extism.ValueTypeI64, extism.ValueTypeI64},
			[]extism.ValueType{extism.ValueTypeI64},
		),

		// host_config_get(key_offset) -> value_offset (0 = not found)
		extism.NewHostFunctionWithStack(
			"host_config_get",
			func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
				keyOff := stack[0]
				key, _ := p.ReadString(keyOff)
				val := hctx.ConfigGet(key)
				if val == "" {
					stack[0] = 0
					return
				}
				off, err := p.WriteString(val)
				if err != nil {
					stack[0] = 0
					return
				}
				stack[0] = off
			},
			[]extism.ValueType{extism.ValueTypeI64},
			[]extism.ValueType{extism.ValueTypeI64},
		),

		// host_kv_get(key_offset) -> value_offset (0 = not found)
		extism.NewHostFunctionWithStack(
			"host_kv_get",
			func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
				keyOff := stack[0]
				key, _ := p.ReadString(keyOff)
				val := hctx.KVStateGet(key)
				if val == "" {
					stack[0] = 0
					return
				}
				off, err := p.WriteString(val)
				if err != nil {
					stack[0] = 0
					return
				}
				stack[0] = off
			},
			[]extism.ValueType{extism.ValueTypeI64},
			[]extism.ValueType{extism.ValueTypeI64},
		),

		// host_kv_set(key_offset, value_offset) -> i64 (0 = success)
		extism.NewHostFunctionWithStack(
			"host_kv_set",
			func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
				keyOff := stack[0]
				valOff := stack[1]
				key, _ := p.ReadString(keyOff)
				val, _ := p.ReadString(valOff)
				hctx.KVStateSet(key, val)
				stack[0] = 0
			},
			[]extism.ValueType{extism.ValueTypeI64, extism.ValueTypeI64},
			[]extism.ValueType{extism.ValueTypeI64},
		),

		// host_clock_now() -> i64 (unix nanoseconds)
		extism.NewHostFunctionWithStack(
			"host_clock_now",
			func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
				stack[0] = uint64(time.Now().UnixNano())
			},
			[]extism.ValueType{},
			[]extism.ValueType{extism.ValueTypeI64},
		),

		// host_metric_inc(name_offset, value_offset) -> i64 (0 = success)
		extism.NewHostFunctionWithStack(
			"host_metric_inc",
			func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
				nameOff := stack[0]
				valOff := stack[1]
				name, _ := p.ReadString(nameOff)
				_ = valOff // value unused for now; could wire to Prometheus
				g.Log().Debugf(ctx, "[plugin:%s] metric %s incremented", hctx.PluginName, name)
				stack[0] = 0
			},
			[]extism.ValueType{extism.ValueTypeI64, extism.ValueTypeI64},
			[]extism.ValueType{extism.ValueTypeI64},
		),
	}
}

// marshalConfigForPlugin converts the config map to a JSON string for injection.
func marshalConfigForPlugin(config map[string]any) string {
	if config == nil {
		return "{}"
	}
	b, err := json.Marshal(config)
	if err != nil {
		return "{}"
	}
	return string(b)
}

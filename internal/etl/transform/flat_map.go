//go:build !nolua

package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lua "github.com/yuin/gopher-lua"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("flat_map", func(config map[string]any) (core.Transform, error) {
		return NewFlatMapTransform("flat_map", config)
	})
	registry.RegisterTransform("udtf", func(config map[string]any) (core.Transform, error) {
		return NewFlatMapTransform("udtf", config)
	})
}

// FlatMapTransform expands one input record into zero, one, or many output
// records. The first ABI is Lua-backed and intentionally small: scripts return
// nil/false to drop, a data map or full record for one output, or an array of
// data maps/full records for multiple outputs.
type FlatMapTransform struct {
	name    string
	script  string
	onError string
	timeout time.Duration

	mu sync.Mutex
	L  *lua.LState
	fn *lua.LFunction

	inputRecords   int64
	outputRecords  int64
	droppedRecords int64
	parseErrors    int64
}

func NewFlatMapTransform(name string, config map[string]any) (*FlatMapTransform, error) {
	language, _ := config["language"].(string)
	if language == "" {
		language = "lua"
	}
	if strings.ToLower(language) != "lua" {
		return nil, fmt.Errorf("%s: unsupported language %q; only lua is implemented in the core flat_map ABI", name, language)
	}

	script, _ := config["script"].(string)
	if script == "" {
		script, _ = config["code"].(string)
	}
	if script == "" {
		return nil, fmt.Errorf("%s: script or code is required", name)
	}

	onError, _ := config["on_error"].(string)
	if onError == "" {
		onError = "dlq"
	}
	onError = strings.ToLower(onError)
	switch onError {
	case "dlq", "drop", "error":
	default:
		return nil, fmt.Errorf("%s: unsupported on_error %q", name, onError)
	}

	timeoutMs := 5000
	if v, ok := config["timeout_ms"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			timeoutMs = n
		}
	}

	L, fn, err := newFlatMapLuaState(script)
	if err != nil {
		return nil, fmt.Errorf("%s compile: %w", name, err)
	}

	return &FlatMapTransform{
		name:    name,
		script:  script,
		onError: onError,
		timeout: time.Duration(timeoutMs) * time.Millisecond,
		L:       L,
		fn:      fn,
	}, nil
}

func (t *FlatMapTransform) Name() string { return t.name }

func (t *FlatMapTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
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
		return rec, fmt.Errorf("%s produced %d records; use batch execution for multi-output flat_map results", t.name, len(out))
	}
}

func (t *FlatMapTransform) ApplyBatch(ctx context.Context, recs []core.Record) ([]core.Record, error) {
	out := make([]core.Record, 0, len(recs))
	var failures []core.TransformRecordFailure

	for _, rec := range recs {
		atomic.AddInt64(&t.inputRecords, 1)
		produced, err := t.applyOne(ctx, rec)
		if err != nil {
			atomic.AddInt64(&t.parseErrors, 1)
			classified := core.ClassifiedError{Class: core.ErrorClassData, Err: err}
			switch t.onError {
			case "drop":
				atomic.AddInt64(&t.droppedRecords, 1)
				continue
			case "error":
				return out, classified
			default:
				failures = append(failures, core.TransformRecordFailure{Record: rec, Err: classified})
				continue
			}
		}
		if len(produced) == 0 {
			atomic.AddInt64(&t.droppedRecords, 1)
			continue
		}
		atomic.AddInt64(&t.outputRecords, int64(len(produced)))
		out = append(out, produced...)
	}

	if len(failures) > 0 {
		return out, core.NewPartialTransformError(t.name+" partial transform failed", failures)
	}
	return out, nil
}

func (t *FlatMapTransform) TransformMetrics() core.TransformMetrics {
	return core.TransformMetrics{
		Transform: t.name,
		Counters: map[string]int64{
			"input_records":   atomic.LoadInt64(&t.inputRecords),
			"output_records":  atomic.LoadInt64(&t.outputRecords),
			"dropped_records": atomic.LoadInt64(&t.droppedRecords),
			"parse_errors":    atomic.LoadInt64(&t.parseErrors),
		},
	}
}

func (t *FlatMapTransform) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.L != nil {
		t.L.Close()
		t.L = nil
		t.fn = nil
	}
	return nil
}

func (t *FlatMapTransform) applyOne(ctx context.Context, rec core.Record) ([]core.Record, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.L == nil || t.fn == nil {
		L, fn, err := newFlatMapLuaState(t.script)
		if err != nil {
			return nil, fmt.Errorf("%s compile: %w", t.name, err)
		}
		t.L = L
		t.fn = fn
	}

	execCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	t.L.SetContext(execCtx)

	t.L.SetGlobal("record", recordToLuaTable(t.L, rec))
	t.L.SetGlobal("data", mapToLuaTable(t.L, rec.Data))
	t.L.SetGlobal("metadata", mapToLuaTable(t.L, metadataToMap(rec.Metadata)))

	top := t.L.GetTop()
	t.L.Push(t.fn)
	if err := t.L.PCall(0, 1, nil); err != nil {
		if current := t.L.GetTop(); current > top {
			t.L.Pop(current - top)
		}
		return nil, fmt.Errorf("%s execute: %w", t.name, err)
	}
	result := t.L.Get(-1)
	defer t.L.Pop(1)

	return luaFlatMapResultToRecords(result, rec)
}

func newFlatMapLuaState(script string) (*lua.LState, *lua.LFunction, error) {
	L := lua.NewState()
	sandbox(L)
	fn, err := L.LoadString(script)
	if err != nil {
		L.Close()
		return nil, nil, err
	}
	return L, fn, nil
}

func recordToLuaTable(L *lua.LState, rec core.Record) *lua.LTable {
	return mapToLuaTable(L, map[string]any{
		"operation": string(rec.Operation),
		"data":      rec.Data,
		"before":    rec.Before,
		"metadata":  metadataToMap(rec.Metadata),
	})
}

func metadataToMap(meta core.Metadata) map[string]any {
	body, err := json.Marshal(meta)
	if err != nil {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(body, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func luaFlatMapResultToRecords(value lua.LValue, base core.Record) ([]core.Record, error) {
	if value == lua.LNil {
		return nil, nil
	}
	if b, ok := value.(lua.LBool); ok && !bool(b) {
		return nil, nil
	}
	tbl, ok := value.(*lua.LTable)
	if !ok {
		return nil, fmt.Errorf("flat_map result must be nil, a record map, or an array of record maps; got %s", value.Type().String())
	}
	if luaTableEntryCount(tbl) == 0 {
		return nil, nil
	}
	if luaTableIsArray(tbl) {
		out := make([]core.Record, 0, tbl.Len())
		for i := 1; i <= tbl.Len(); i++ {
			rec, err := luaTableToRecord(tbl.RawGetInt(i), base)
			if err != nil {
				return nil, fmt.Errorf("flat_map result[%d]: %w", i, err)
			}
			out = append(out, rec)
		}
		return out, nil
	}
	rec, err := luaTableToRecord(tbl, base)
	if err != nil {
		return nil, err
	}
	return []core.Record{rec}, nil
}

func luaTableToRecord(value lua.LValue, base core.Record) (core.Record, error) {
	tbl, ok := value.(*lua.LTable)
	if !ok {
		return base, fmt.Errorf("record output must be a table; got %s", value.Type().String())
	}
	out := base
	if data := tbl.RawGetString("data"); data != lua.LNil {
		dataTbl, ok := data.(*lua.LTable)
		if !ok {
			return base, fmt.Errorf("record.data must be a table")
		}
		out.Data = luaTableToMap(dataTbl)
	} else {
		out.Data = luaTableToMap(tbl)
	}
	if before := tbl.RawGetString("before"); before != lua.LNil {
		beforeTbl, ok := before.(*lua.LTable)
		if !ok {
			return base, fmt.Errorf("record.before must be a table")
		}
		out.Before = luaTableToMap(beforeTbl)
	}
	if op := tbl.RawGetString("operation"); op != lua.LNil {
		if op.Type() != lua.LTString {
			return base, fmt.Errorf("record.operation must be a string")
		}
		out.Operation = core.OpType(op.String())
	}
	if metadata := tbl.RawGetString("metadata"); metadata != lua.LNil {
		metadataTbl, ok := metadata.(*lua.LTable)
		if !ok {
			return base, fmt.Errorf("record.metadata must be a table")
		}
		if err := applyMetadataMap(&out.Metadata, luaTableToMap(metadataTbl)); err != nil {
			return base, err
		}
	}
	return out, nil
}

func applyMetadataMap(meta *core.Metadata, patch map[string]any) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal record.metadata: %w", err)
	}
	updated := *meta
	if err := json.Unmarshal(body, &updated); err != nil {
		return fmt.Errorf("unmarshal record.metadata: %w", err)
	}
	*meta = updated
	return nil
}

func luaTableEntryCount(tbl *lua.LTable) int {
	count := 0
	tbl.ForEach(func(_, _ lua.LValue) {
		count++
	})
	return count
}

package transform

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("window", func(config map[string]any) (core.Transform, error) {
		return NewWindowTransform(config)
	})
}

// WindowTransform is a BatchTransform that groups records into time windows
// and emits aggregated results when the window closes.
//
// It implements:
//   - core.BatchTransform (ApplyBatch for batch-level processing)
//   - core.Flusher (Flush to drain remaining windows on shutdown)
//
// YAML config:
//
//	transforms:
//	  - type: window
//	    config:
//	      window_type: tumbling      # tumbling | sliding (only tumbling for now)
//	      window_size_seconds: 60
//	      group_by: [product_id, region]
//	      aggregates:
//	        total_sales: { func: sum, field: amount }
//	        order_count: { func: count }
//	        avg_amount: { func: avg, field: amount }
//	        max_amount: { func: max, field: amount }
//	        min_amount: { func: min, field: amount }
type WindowTransform struct {
	windowSize    time.Duration
	groupByFields []string
	aggregators   map[string]*aggDef

	mu      sync.Mutex
	buffer  map[int64]map[string]*aggState // windowStart → groupKey → state
	pending []core.Record                  // records accumulated since last emit
}

type aggDef struct {
	Func  string // count | sum | avg | min | max | first | last
	Field string
}

type aggState struct {
	count   int64
	sum     float64 // used by sum aggregator
	avgSum  float64 // independent accumulator for avg
	avgCnt  int64   // count for avg
	min     float64
	max     float64
	minInit bool
	maxInit bool
	first   any
	last    any
	groupKV map[string]any
}

func NewWindowTransform(config map[string]any) (*WindowTransform, error) {
	wt := &WindowTransform{
		aggregators: make(map[string]*aggDef),
		buffer:      make(map[int64]map[string]*aggState),
	}

	sizeSec, _ := config["window_size_seconds"].(int)
	if sizeSec <= 0 {
		sizeSec = 60
	}
	wt.windowSize = time.Duration(sizeSec) * time.Second

	if rawGB, ok := config["group_by"].([]any); ok {
		for _, g := range rawGB {
			if gs, ok := g.(string); ok {
				wt.groupByFields = append(wt.groupByFields, gs)
			}
		}
	}

	rawAggs, ok := config["aggregates"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("window: aggregates config is required")
	}
	for name, def := range rawAggs {
		aggMap, ok := def.(map[string]any)
		if !ok {
			continue
		}
		ad := &aggDef{}
		ad.Func, _ = aggMap["func"].(string)
		ad.Field, _ = aggMap["field"].(string)
		if ad.Func == "" {
			ad.Func = "count"
		}
		wt.aggregators[name] = ad
	}

	if len(wt.aggregators) == 0 {
		return nil, fmt.Errorf("window: at least one aggregate is required")
	}

	return wt, nil
}

func (w *WindowTransform) Name() string { return "window" }

// Apply is the per-record fallback. For window transforms we accumulate
// and return ErrRecordFiltered (the record is consumed but won't pass
// through individually). The batch path (ApplyBatch) handles emission.
func (w *WindowTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.accumulate(rec)
	return rec, core.ErrRecordFiltered
}

// ApplyBatch processes a batch of records: accumulates them into windows,
// then emits any windows that have closed.
func (w *WindowTransform) ApplyBatch(ctx context.Context, recs []core.Record) ([]core.Record, error) {
	if len(recs) == 0 {
		return nil, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, rec := range recs {
		w.accumulate(rec)
	}

	// Determine the current window boundary.
	now := time.Now()
	currentWindow := now.Truncate(w.windowSize).Unix()

	// Emit all windows that have closed (windowStart < currentWindow).
	var emitted []core.Record
	var closedWindows []int64
	for ws := range w.buffer {
		if ws < currentWindow {
			closedWindows = append(closedWindows, ws)
		}
	}
	sort.Slice(closedWindows, func(i, j int) bool { return closedWindows[i] < closedWindows[j] })

	for _, ws := range closedWindows {
		groups := w.buffer[ws]
		for _, state := range groups {
			emitted = append(emitted, w.buildOutputRecord(ws, state))
		}
		delete(w.buffer, ws)
	}

	return emitted, nil
}

// Flush emits all remaining buffered windows (called on shutdown).
func (w *WindowTransform) Flush(ctx context.Context) ([]core.Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var emitted []core.Record
	for ws, groups := range w.buffer {
		for _, state := range groups {
			emitted = append(emitted, w.buildOutputRecord(ws, state))
		}
	}
	w.buffer = make(map[int64]map[string]*aggState)
	return emitted, nil
}

func (w *WindowTransform) accumulate(rec core.Record) {
	ts := time.Now()
	if !rec.Metadata.Timestamp.IsZero() {
		ts = rec.Metadata.Timestamp
	}
	windowStart := ts.Truncate(w.windowSize).Unix()

	groupKey := w.makeGroupKey(rec)

	windowMap, ok := w.buffer[windowStart]
	if !ok {
		windowMap = make(map[string]*aggState)
		w.buffer[windowStart] = windowMap
	}

	state, ok := windowMap[groupKey]
	if !ok {
		state = &aggState{
			groupKV: w.makeGroupKV(rec),
		}
		windowMap[groupKey] = state
	}

	state.count++
	for name, ad := range w.aggregators {
		_ = name
		switch ad.Func {
		case "count":
			// count is stored in state.count globally
		case "sum":
			if v, ok := toFloatWin(rec.Data[ad.Field]); ok {
				state.sum += v
			}
		case "avg":
			if v, ok := toFloatWin(rec.Data[ad.Field]); ok {
				state.avgSum += v
				state.avgCnt++
			}
		case "min":
			if v, ok := toFloatWin(rec.Data[ad.Field]); ok {
				if !state.minInit {
					state.min = v
					state.minInit = true
				} else if v < state.min {
					state.min = v
				}
			}
		case "max":
			if v, ok := toFloatWin(rec.Data[ad.Field]); ok {
				if !state.maxInit {
					state.max = v
					state.maxInit = true
				} else if v > state.max {
					state.max = v
				}
			}
		case "first":
			if state.first == nil {
				state.first = rec.Data[ad.Field]
			}
		case "last":
			state.last = rec.Data[ad.Field]
		}
	}
}

func (w *WindowTransform) buildOutputRecord(windowStart int64, state *aggState) core.Record {
	data := make(map[string]any)
	data["window_start"] = time.Unix(windowStart, 0).UTC().Format(time.RFC3339)
	data["window_end"] = time.Unix(windowStart+int64(w.windowSize.Seconds()), 0).UTC().Format(time.RFC3339)

	for k, v := range state.groupKV {
		data[k] = v
	}

	for name, ad := range w.aggregators {
		switch ad.Func {
		case "count":
			data[name] = state.count
		case "sum":
			data[name] = state.sum
		case "avg":
			if state.avgCnt > 0 {
				data[name] = state.avgSum / float64(state.avgCnt)
			} else {
				data[name] = 0
			}
		case "min":
			data[name] = state.min
		case "max":
			data[name] = state.max
		case "first":
			data[name] = state.first
		case "last":
			data[name] = state.last
		}
	}

	return core.Record{
		Operation: core.OpInsert,
		Data:      data,
		Metadata: core.Metadata{
			Timestamp: time.Now(),
			Source:    "window",
			Table:     "window_aggregate",
		},
	}
}

func (w *WindowTransform) makeGroupKey(rec core.Record) string {
	if len(w.groupByFields) == 0 {
		return "_all"
	}
	key := ""
	for _, f := range w.groupByFields {
		key += fmt.Sprintf("%v|", rec.Data[f])
	}
	return key
}

func (w *WindowTransform) makeGroupKV(rec core.Record) map[string]any {
	kv := make(map[string]any)
	for _, f := range w.groupByFields {
		kv[f] = rec.Data[f]
	}
	return kv
}

func toFloatWin(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case float32:
		return float64(x), true
	default:
		return 0, false
	}
}

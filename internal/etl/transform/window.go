package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/state"
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
//	      state_backend: sqlite       # optional durable tumbling-window state
type WindowTransform struct {
	windowSize    time.Duration
	groupByFields []string
	aggregators   map[string]*aggDef

	// Event-time watermark support (TF-14). When allowedLateness > 0:
	//   - maxEventTime tracks the newest event timestamp seen;
	//   - records older than maxEventTime-allowedLateness are dropped (too late);
	//   - lastEmittedWindow guards against re-emitting a window that already
	//     closed (a late record can't reopen and double-count an emitted window).
	// When allowedLateness == 0 (default) none of this applies and behavior is
	// exactly the original processing-time tumbling window.
	allowedLateness   time.Duration
	maxEventTime      time.Time
	lastEmittedWindow int64

	mu      sync.Mutex
	buffer  map[int64]map[string]*aggState // windowStart → groupKey → state
	pending []core.Record                  // records accumulated since last emit

	store         state.Store
	stateOwner    bool
	pipeline      string
	node          string
	stateTTL      time.Duration
	stateRestored bool

	accumulated    int64
	lateDropped    int64
	emittedRecords int64
	emittedWindows int64
	flushedRecords int64
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

type persistedWindowState struct {
	MaxEventTime      time.Time                        `json:"max_event_time,omitempty"`
	LastEmittedWindow int64                            `json:"last_emitted_window,omitempty"`
	Buffer            map[string]map[string]persistAgg `json:"buffer,omitempty"`
}

type persistAgg struct {
	Count   int64          `json:"count"`
	Sum     float64        `json:"sum"`
	AvgSum  float64        `json:"avg_sum"`
	AvgCnt  int64          `json:"avg_cnt"`
	Min     float64        `json:"min"`
	Max     float64        `json:"max"`
	MinInit bool           `json:"min_init"`
	MaxInit bool           `json:"max_init"`
	First   any            `json:"first,omitempty"`
	Last    any            `json:"last,omitempty"`
	GroupKV map[string]any `json:"group_kv,omitempty"`
}

const windowStateKey = "__window_state__"

func NewWindowTransform(config map[string]any) (*WindowTransform, error) {
	wt := &WindowTransform{
		aggregators: make(map[string]*aggDef),
		buffer:      make(map[int64]map[string]*aggState),
		pipeline:    "default",
		node:        "window",
	}

	sizeSec, _ := config["window_size_seconds"].(int)
	if sizeSec <= 0 {
		sizeSec = 60
	}
	wt.windowSize = time.Duration(sizeSec) * time.Second

	// Optional event-time lateness budget (TF-14). Default 0 = processing-time
	// tumbling window (original behavior, unchanged). When > 0, late records
	// (older than maxEventSeen - allowedLateness) are dropped and already-
	// emitted windows can't be reopened, preventing duplicate aggregates on
	// out-of-order CDC streams.
	if v, ok := config["allowed_lateness_seconds"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			wt.allowedLateness = time.Duration(n) * time.Second
		}
	}
	_ = config["window_type"] // sliding/session remain unsupported; accepted for forward-compat

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
	if v, ok := config["state_pipeline"].(string); ok && v != "" {
		wt.pipeline = v
	}
	if v, ok := config["state_node"].(string); ok && v != "" {
		wt.node = v
	}
	if v, ok := config["state_ttl_seconds"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			wt.stateTTL = time.Duration(n) * time.Second
		}
	}
	if backend, ok := config["state_backend"].(string); ok && backend != "" {
		switch strings.ToLower(backend) {
		case "sqlite":
			path, _ := config["state_path"].(string)
			if path == "" {
				path = "./data/etl-state.db"
			}
			store, err := state.NewSQLiteStore(path)
			if err != nil {
				return nil, fmt.Errorf("window: open state store: %w", err)
			}
			wt.store = store
			wt.stateOwner = true
		default:
			return nil, fmt.Errorf("window: unsupported state_backend %q", backend)
		}
	}

	return wt, nil
}

func (w *WindowTransform) Name() string { return "window" }

// WithStateStore wires a shared state backend into window. It is primarily used
// by tests today and provides the same future runner-injection seam as other
// stateful transforms.
func (w *WindowTransform) WithStateStore(store state.Store, pipeline, node string, ttl time.Duration) *WindowTransform {
	w.store = store
	w.stateOwner = false
	if pipeline != "" {
		w.pipeline = pipeline
	}
	if node != "" {
		w.node = node
	}
	w.stateTTL = ttl
	w.stateRestored = false
	return w
}

// Apply is the per-record fallback. For window transforms we accumulate
// and return ErrRecordFiltered (the record is consumed but won't pass
// through individually). The batch path (ApplyBatch) handles emission.
func (w *WindowTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.restoreStateLocked(ctx); err != nil {
		return rec, err
	}
	w.accumulate(rec)
	if err := w.persistStateLocked(ctx); err != nil {
		return rec, err
	}
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
	if err := w.restoreStateLocked(ctx); err != nil {
		return nil, err
	}

	for _, rec := range recs {
		w.accumulate(rec)
	}

	// Determine the current window boundary.
	now := time.Now()
	currentWindow := now.Truncate(w.windowSize).Unix()

	// Emit all windows that have closed (windowStart < currentWindow), skipping
	// any window at or before lastEmittedWindow so a late record can't reopen
	// and double-emit an already-closed window (TF-14).
	var emitted []core.Record
	var closedWindows []int64
	for ws := range w.buffer {
		if ws < currentWindow && ws > w.lastEmittedWindow {
			closedWindows = append(closedWindows, ws)
		}
	}
	sort.Slice(closedWindows, func(i, j int) bool { return closedWindows[i] < closedWindows[j] })

	for _, ws := range closedWindows {
		groups := w.buffer[ws]
		for _, state := range groups {
			emitted = append(emitted, w.buildOutputRecord(ws, state))
		}
		if len(groups) > 0 {
			atomic.AddInt64(&w.emittedWindows, 1)
			atomic.AddInt64(&w.emittedRecords, int64(len(groups)))
		}
		delete(w.buffer, ws)
		if ws > w.lastEmittedWindow {
			w.lastEmittedWindow = ws
		}
	}
	if err := w.persistStateLocked(ctx); err != nil {
		return nil, err
	}

	return emitted, nil
}

// Flush emits all remaining buffered windows (called on shutdown).
func (w *WindowTransform) Flush(ctx context.Context) ([]core.Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.restoreStateLocked(ctx); err != nil {
		return nil, err
	}

	var emitted []core.Record
	for ws, groups := range w.buffer {
		for _, state := range groups {
			emitted = append(emitted, w.buildOutputRecord(ws, state))
		}
		if len(groups) > 0 {
			atomic.AddInt64(&w.emittedWindows, 1)
			atomic.AddInt64(&w.emittedRecords, int64(len(groups)))
			atomic.AddInt64(&w.flushedRecords, int64(len(groups)))
		}
		if ws > w.lastEmittedWindow {
			w.lastEmittedWindow = ws
		}
	}
	w.buffer = make(map[int64]map[string]*aggState)
	if err := w.persistStateLocked(ctx); err != nil {
		return nil, err
	}
	return emitted, nil
}

func (w *WindowTransform) accumulate(rec core.Record) {
	ts := time.Now()
	if !rec.Metadata.Timestamp.IsZero() {
		ts = rec.Metadata.Timestamp
	}

	// Event-time lateness check (TF-14): when allowedLateness is configured,
	// drop records that arrive too late (older than the watermark). This
	// prevents stale records from polluting/reopening windows. Skipped when
	// allowedLateness == 0 (default) so behavior is unchanged.
	if w.allowedLateness > 0 && !rec.Metadata.Timestamp.IsZero() {
		if w.maxEventTime.IsZero() || ts.After(w.maxEventTime) {
			w.maxEventTime = ts
		} else if ts.Before(w.maxEventTime.Add(-w.allowedLateness)) {
			// Too late — drop silently (would otherwise double-count or reopen).
			atomic.AddInt64(&w.lateDropped, 1)
			return
		}
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
	atomic.AddInt64(&w.accumulated, 1)
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

func (w *WindowTransform) restoreStateLocked(ctx context.Context) error {
	if w.stateRestored {
		return nil
	}
	w.stateRestored = true
	if w.store == nil {
		return nil
	}
	value, ok, err := w.store.Get(ctx, w.pipeline, w.node, windowStateKey)
	if err != nil {
		return fmt.Errorf("window: get state: %w", err)
	}
	if !ok || len(value) == 0 {
		return nil
	}
	var persisted persistedWindowState
	if err := json.Unmarshal(value, &persisted); err != nil {
		return fmt.Errorf("window: unmarshal state: %w", err)
	}
	w.maxEventTime = persisted.MaxEventTime
	w.lastEmittedWindow = persisted.LastEmittedWindow
	w.buffer = make(map[int64]map[string]*aggState, len(persisted.Buffer))
	for wsText, groups := range persisted.Buffer {
		var ws int64
		if _, err := fmt.Sscanf(wsText, "%d", &ws); err != nil {
			return fmt.Errorf("window: parse state window %q: %w", wsText, err)
		}
		groupMap := make(map[string]*aggState, len(groups))
		for groupKey, st := range groups {
			groupMap[groupKey] = &aggState{
				count:   st.Count,
				sum:     st.Sum,
				avgSum:  st.AvgSum,
				avgCnt:  st.AvgCnt,
				min:     st.Min,
				max:     st.Max,
				minInit: st.MinInit,
				maxInit: st.MaxInit,
				first:   st.First,
				last:    st.Last,
				groupKV: st.GroupKV,
			}
		}
		if len(groupMap) > 0 {
			w.buffer[ws] = groupMap
		}
	}
	return nil
}

func (w *WindowTransform) persistStateLocked(ctx context.Context) error {
	if w.store == nil {
		return nil
	}
	persisted := persistedWindowState{
		MaxEventTime:      w.maxEventTime,
		LastEmittedWindow: w.lastEmittedWindow,
		Buffer:            make(map[string]map[string]persistAgg, len(w.buffer)),
	}
	for ws, groups := range w.buffer {
		groupMap := make(map[string]persistAgg, len(groups))
		for groupKey, st := range groups {
			groupMap[groupKey] = persistAgg{
				Count:   st.count,
				Sum:     st.sum,
				AvgSum:  st.avgSum,
				AvgCnt:  st.avgCnt,
				Min:     st.min,
				Max:     st.max,
				MinInit: st.minInit,
				MaxInit: st.maxInit,
				First:   st.first,
				Last:    st.last,
				GroupKV: st.groupKV,
			}
		}
		if len(groupMap) > 0 {
			persisted.Buffer[fmt.Sprintf("%d", ws)] = groupMap
		}
	}
	value, err := json.Marshal(persisted)
	if err != nil {
		return fmt.Errorf("window: marshal state: %w", err)
	}
	if err := w.store.Set(ctx, w.pipeline, w.node, windowStateKey, value, w.stateTTL); err != nil {
		return fmt.Errorf("window: set state: %w", err)
	}
	return nil
}

func (w *WindowTransform) Close() error {
	if w.stateOwner && w.store != nil {
		return w.store.Close()
	}
	return nil
}

func (w *WindowTransform) SnapshotState(ctx context.Context) (string, string, bool, error) {
	if w.store == nil {
		return "", "", false, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.persistStateLocked(ctx); err != nil {
		return w.node, "", false, err
	}
	snap, err := w.store.Snapshot(ctx, w.pipeline, w.node)
	if err != nil {
		return w.node, "", false, fmt.Errorf("window: snapshot state: %w", err)
	}
	if snap == nil || len(snap.Entries) == 0 {
		return w.node, "", false, nil
	}
	return w.node, snap.Version, true, nil
}

func (w *WindowTransform) StateMetrics(ctx context.Context) (core.StateMetrics, bool, error) {
	return stateMetrics(ctx, w.store, w.pipeline, w.node, "window")
}

func (w *WindowTransform) TransformMetrics() core.TransformMetrics {
	return core.TransformMetrics{
		Node:      w.node,
		Transform: w.Name(),
		Counters: map[string]int64{
			"accumulated":     atomic.LoadInt64(&w.accumulated),
			"late_dropped":    atomic.LoadInt64(&w.lateDropped),
			"emitted_records": atomic.LoadInt64(&w.emittedRecords),
			"emitted_windows": atomic.LoadInt64(&w.emittedWindows),
			"flushed_records": atomic.LoadInt64(&w.flushedRecords),
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

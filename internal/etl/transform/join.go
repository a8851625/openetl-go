package transform

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("join", func(config map[string]any) (core.Transform, error) {
		return NewJoinTransform(config)
	})
}

// JoinTransform implements stream-stream interval join (Flink-style).
// It joins records from the current stream against a windowed state of
// previously seen records on the same join key.
//
// Config:
//
//	join_type:      "inner" (default) | "left" | "right"
//	join_key:       Field name in records to join on
//	join_window_sec:  How long to keep records in the join state (default 60)
//	join_fields:    List of fields from the matched record to copy
//	join_prefix:    Prefix for joined fields (default "joined_")
//	where:          Optional filter expression for the join side
//	on_miss:        Inner-join behavior when a record has no match:
//	                "drop" (default, silent — indistinguishable from a filter)
//	                | "dlq" (route the unmatched record to the DLQ so misses
//	                are visible — useful for catching schema/data drift)
type JoinTransform struct {
	name       string
	joinType   string
	joinKey    string
	windowDur  time.Duration
	joinFields []string
	prefix     string
	whereExpr  string
	onMiss     string

	mu    sync.RWMutex
	state map[string][]joinEntry // key → buffered records
}

type joinEntry struct {
	record    core.Record
	timestamp time.Time
}

func NewJoinTransform(config map[string]any) (*JoinTransform, error) {
	t := &JoinTransform{
		name:      "join",
		joinType:  "inner",
		windowDur: 60 * time.Second,
		prefix:    "joined_",
	}
	if v, ok := config["join_type"]; ok {
		if s, ok := v.(string); ok {
			t.joinType = s
		}
	}
	if v, ok := config["join_key"]; ok {
		if s, ok := v.(string); ok {
			t.joinKey = s
		}
	}
	if v, ok := config["join_window_sec"]; ok {
		switch n := v.(type) {
		case int:
			t.windowDur = time.Duration(n) * time.Second
		case float64:
			t.windowDur = time.Duration(n) * time.Second
		}
	}
	if v, ok := config["join_fields"]; ok {
		if arr, ok := v.([]interface{}); ok {
			for _, e := range arr {
				if s, ok := e.(string); ok {
					t.joinFields = append(t.joinFields, s)
				}
			}
		}
	}
	if v, ok := config["join_prefix"]; ok {
		if s, ok := v.(string); ok && s != "" {
			t.prefix = s
		}
	}
	if v, ok := config["where"]; ok {
		if s, ok := v.(string); ok {
			t.whereExpr = s
		}
	}
	t.onMiss = "drop"
	if v, ok := config["on_miss"]; ok {
		if s, ok := v.(string); ok {
			switch s {
			case "drop", "dlq", "error":
				t.onMiss = s
			default:
				return nil, fmt.Errorf("join: on_miss must be drop|dlq|error, got %q", s)
			}
		}
	}
	if t.joinKey == "" {
		return nil, fmt.Errorf("join transform requires join_key")
	}
	if t.joinType == "right" {
		// A right join in a streaming model would require holding the
		// *right* (incoming) side and probing against the buffered
		// *left* side, which is the opposite of how this transform
		// stores state (it buffers left-side records and probes with
		// incoming records). Supporting it correctly would require a
		// separate state machine, so we reject it explicitly rather
		// than silently producing wrong results.
		return nil, fmt.Errorf("join: right join is not supported in stream model")
	}
	t.state = make(map[string][]joinEntry)
	return t, nil
}

func (t *JoinTransform) Name() string { return "join" }

// handleMiss routes an inner-join miss per on_miss: "drop" (default) returns
// ErrRecordFiltered so the runner drops it silently; "dlq"/"error" return a
// real error so the pipeline routes the record to the DLQ — making misses
// visible instead of indistinguishable from a filter (TF-7).
func (t *JoinTransform) handleMiss(rec core.Record) (core.Record, error) {
	if t.onMiss == "dlq" || t.onMiss == "error" {
		return rec, fmt.Errorf("join: no match for key=%v (on_miss=%s)", rec.Data[t.joinKey], t.onMiss)
	}
	return rec, core.ErrRecordFiltered
}

// Apply processes each record through the stream-stream join.
func (t *JoinTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	keyVal, ok := rec.Data[t.joinKey]
	if !ok {
		// No join key → pass through unchanged for left join, drop/dlq for inner.
		if t.joinType == "inner" {
			return t.handleMiss(rec)
		}
		return rec, nil
	}
	key := fmt.Sprint(keyVal)
	if key == "" {
		if t.joinType == "inner" {
			return t.handleMiss(rec)
		}
		return rec, nil
	}

	now := time.Now()

	t.mu.Lock()
	// Evict expired entries from the window.
	t.evictLocked(now)

	// Find match in state.
	entries := t.state[key]
	var matched *core.Record
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if t.whereExpr != "" {
			if !t.evalWhere(e.record, t.whereExpr) {
				continue
			}
		}
		matched = &e.record
		break
	}

	// Store current record for future joins.
	t.state[key] = append(entries, joinEntry{record: rec, timestamp: now})
	t.mu.Unlock()

	if matched == nil {
		if t.joinType == "inner" {
			return t.handleMiss(rec)
		}
		// left join, no match: explicitly populate the configured join
		// fields with nil so downstream nodes can distinguish "no match"
		// (field present, value nil) from "matched with a NULL value"
		// (field present, value nil would be ambiguous, but at least the
		// keys always exist after this point). Without this, downstream
		// nodes couldn't tell a missed join apart from a record that was
		// never joined.
		if rec.Data == nil {
			rec.Data = make(map[string]any)
		}
		for _, f := range t.joinFields {
			rec.Data[t.prefix+f] = nil
		}
		return rec, nil
	}

	// Copy joined fields with prefix.
	for _, f := range t.joinFields {
		if v, ok := matched.Data[f]; ok {
			rec.Data[t.prefix+f] = v
		}
	}

	return rec, nil
}

// evictLocked removes records older than the join window. Caller must hold t.mu.
func (t *JoinTransform) evictLocked(now time.Time) {
	cutoff := now.Add(-t.windowDur)
	for key, entries := range t.state {
		kept := entries[:0]
		for _, e := range entries {
			if e.timestamp.After(cutoff) {
				kept = append(kept, e)
			}
		}
		if len(kept) == 0 {
			delete(t.state, key)
		} else {
			t.state[key] = kept
		}
	}
}

// evalWhere evaluates a simple filter expression against a record.
// Supports: field==val, field!=val, field>val, field<val, field>=val, field<=val
// Values may be quoted with single or double quotes (e.g. status=="active"),
// in which case the surrounding quotes are stripped before comparison so
// that quoted and unquoted forms compare equal.
// Only used for the join side (matched records), not the incoming stream.
func (t *JoinTransform) evalWhere(rec core.Record, expr string) bool {
	ops := []string{">=", "<=", "!=", "==", ">", "<"}
	for _, op := range ops {
		for i := 0; i < len(expr); i++ {
			if i+len(op) <= len(expr) && expr[i:i+len(op)] == op {
				field := strings.TrimSpace(expr[:i])
				val := strings.TrimSpace(expr[i+len(op):])
				val = stripQuotes(val)
				recVal, ok := rec.Data[field]
				if !ok {
					return false
				}
				return compareVals(fmt.Sprint(recVal), op, val)
			}
		}
	}
	return true
}

// stripQuotes removes a single layer of surrounding single or double
// quotes from val (e.g. "active" → active, 'active' → active). Values
// that are not quoted are returned unchanged. This lets where
// expressions such as status=="active" compare equal to the literal
// value active rather than the string "active" (with quotes).
func stripQuotes(val string) string {
	if len(val) >= 2 {
		first, last := val[0], val[len(val)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			return val[1 : len(val)-1]
		}
	}
	return val
}

func compareVals(a, op, b string) bool {
	// Try numeric comparison first.
	var fa, fb float64
	_, ea := fmt.Sscanf(a, "%f", &fa)
	_, eb := fmt.Sscanf(b, "%f", &fb)
	if ea == nil && eb == nil {
		return compareFloats(fa, op, fb)
	}
	// String comparison.
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	}
	return false
}

func compareFloats(a float64, op string, b float64) bool {
	switch op {
	case "==":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	}
	return false
}

// silence unused imports
var _ = sort.Strings

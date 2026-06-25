package transform

import (
	"context"
	"fmt"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("normalize_envelope", func(config map[string]any) (core.Transform, error) {
		return NewEnvelopeNormalizeTransform(config)
	})
	registry.RegisterTransform("debezium_envelope", func(config map[string]any) (core.Transform, error) {
		return NewEnvelopeNormalizeTransform(config)
	})
}

// EnvelopeNormalizeTransform standardizes plain JSON and Debezium-like Kafka
// records before lookup/window processing. Plain JSON passes through; Debezium
// payloads are flattened to the row image and annotated with operation/source
// metadata.
type EnvelopeNormalizeTransform struct {
	keepMetadata bool
}

func NewEnvelopeNormalizeTransform(config map[string]any) (*EnvelopeNormalizeTransform, error) {
	t := &EnvelopeNormalizeTransform{keepMetadata: true}
	if v, ok := config["keep_metadata"].(bool); ok {
		t.keepMetadata = v
	}
	return t, nil
}

func (t *EnvelopeNormalizeTransform) Name() string { return "normalize_envelope" }

func (t *EnvelopeNormalizeTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if err := ctx.Err(); err != nil {
		return rec, err
	}
	if rec.Data == nil {
		rec.Data = map[string]any{}
		return rec, nil
	}

	payload := rec.Data
	if rawPayload, ok := rec.Data["payload"]; ok {
		if m, ok := asStringAnyMap(rawPayload); ok {
			payload = m
		}
	}

	after, hasAfter := asStringAnyMap(payload["after"])
	before, hasBefore := asStringAnyMap(payload["before"])
	op, _ := payload["op"].(string)
	source, _ := asStringAnyMap(payload["source"])

	if !hasAfter && !hasBefore && op == "" {
		if t.keepMetadata {
			if rec.Operation != "" {
				rec.Data["_op"] = string(rec.Operation)
			}
			if rec.Metadata.Table != "" {
				rec.Data["_source_table"] = rec.Metadata.Table
			}
		}
		return rec, nil
	}

	switch op {
	case "c", "r":
		rec.Operation = core.OpInsert
	case "u":
		rec.Operation = core.OpUpdate
	case "d":
		rec.Operation = core.OpDelete
	default:
		if rec.Operation == "" {
			rec.Operation = core.OpUpdate
		}
	}

	if hasBefore {
		rec.Before = cloneMap(before)
	}
	if rec.Operation == core.OpDelete {
		if hasBefore {
			rec.Data = cloneMap(before)
		} else {
			rec.Data = map[string]any{}
		}
	} else if hasAfter {
		rec.Data = cloneMap(after)
	} else if hasBefore {
		rec.Data = cloneMap(before)
	} else {
		return rec, fmt.Errorf("normalize_envelope: Debezium payload has op=%q but no row image", op)
	}

	if table, ok := source["table"].(string); ok && table != "" {
		rec.Metadata.Table = table
	}
	if ts, ok := eventTime(payload["ts_ms"]); ok {
		rec.Metadata.Timestamp = ts
	}

	if t.keepMetadata {
		rec.Data["_op"] = string(rec.Operation)
		if rec.Metadata.Table != "" {
			rec.Data["_source_table"] = rec.Metadata.Table
		}
		if !rec.Metadata.Timestamp.IsZero() {
			rec.Data["_event_time"] = rec.Metadata.Timestamp.UTC().Format(time.RFC3339Nano)
		}
	}
	return rec, nil
}

func asStringAnyMap(v any) (map[string]any, bool) {
	switch m := v.(type) {
	case map[string]any:
		return m, true
	default:
		return nil, false
	}
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func eventTime(v any) (time.Time, bool) {
	switch n := v.(type) {
	case int64:
		return time.UnixMilli(n).UTC(), true
	case int:
		return time.UnixMilli(int64(n)).UTC(), true
	case float64:
		return time.UnixMilli(int64(n)).UTC(), true
	default:
		return time.Time{}, false
	}
}

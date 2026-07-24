package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("cast", func(config map[string]any) (core.Transform, error) {
		return NewCastTransform(config)
	})
}

// CastTransform converts fields to explicit target types with configurable
// error handling. Target types: to_string, to_int, to_float, to_bool,
// to_timestamp, to_date, to_json, to_array.
//
// YAML:
//
//	transforms:
//	  - type: cast
//	    config:
//	      casts:
//	        price: to_float
//	        active: to_bool
//	        tags: to_array
//	      on_error: null
type CastTransform struct {
	casts   map[string]string
	onError string // null | skip | fail
}

func NewCastTransform(config map[string]any) (*CastTransform, error) {
	casts, err := stringMapConfig(config, "casts")
	if err != nil {
		return nil, fmt.Errorf("cast: %w", err)
	}
	if len(casts) == 0 {
		return nil, fmt.Errorf("cast: casts is required")
	}
	for field, target := range casts {
		if !isSupportedCast(target) {
			return nil, fmt.Errorf("cast: unsupported target type %q for field %q", target, field)
		}
	}
	onError := "null"
	if v, ok := config["on_error"].(string); ok && v != "" {
		onError = strings.ToLower(strings.TrimSpace(v))
	}
	switch onError {
	case "null", "skip", "fail":
	default:
		return nil, fmt.Errorf("cast: on_error must be null, skip, or fail")
	}
	return &CastTransform{casts: casts, onError: onError}, nil
}

func isSupportedCast(target string) bool {
	switch normalizeCastTarget(target) {
	case "to_string", "to_int", "to_float", "to_bool", "to_timestamp", "to_date", "to_json", "to_array":
		return true
	default:
		return false
	}
}

func normalizeCastTarget(target string) string {
	t := strings.ToLower(strings.TrimSpace(target))
	switch t {
	case "string", "str":
		return "to_string"
	case "int", "int64", "integer":
		return "to_int"
	case "float", "float64", "double", "number":
		return "to_float"
	case "bool", "boolean":
		return "to_bool"
	case "timestamp", "datetime":
		return "to_timestamp"
	case "date":
		return "to_date"
	case "json":
		return "to_json"
	case "array", "list":
		return "to_array"
	default:
		return t
	}
}

func (t *CastTransform) Name() string { return "cast" }

func (t *CastTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	_ = ctx
	if rec.Data == nil {
		rec.Data = map[string]any{}
	}
	for field, target := range t.casts {
		v, ok := rec.Data[field]
		if !ok {
			continue
		}
		if v == nil {
			continue
		}
		converted, err := castValue(v, normalizeCastTarget(target))
		if err != nil {
			switch t.onError {
			case "fail":
				return rec, fmt.Errorf("cast: field %q: %w", field, err)
			case "skip":
				return rec, core.ErrRecordFiltered
			default: // null
				rec.Data[field] = nil
			}
			continue
		}
		rec.Data[field] = converted
	}
	return rec, nil
}

func castValue(v any, target string) (any, error) {
	switch target {
	case "to_string":
		switch x := v.(type) {
		case string:
			return x, nil
		case []byte:
			return string(x), nil
		case json.RawMessage:
			return string(x), nil
		default:
			// Prefer JSON for composite types so nested data survives.
			switch v.(type) {
			case map[string]any, []any, map[string]string:
				b, err := json.Marshal(v)
				if err != nil {
					return nil, err
				}
				return string(b), nil
			}
			return fmt.Sprintf("%v", v), nil
		}
	case "to_int":
		return castToInt(v)
	case "to_float":
		return castToFloat(v)
	case "to_bool":
		return castToBool(v)
	case "to_timestamp":
		return castToTimestamp(v)
	case "to_date":
		ts, err := castToTimestamp(v)
		if err != nil {
			return nil, err
		}
		// Normalize to midnight UTC date.
		d := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
		return d, nil
	case "to_json":
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case "to_array":
		return castToArray(v)
	default:
		return nil, fmt.Errorf("unsupported cast target %q", target)
	}
}

func castToInt(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case float64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	case json.Number:
		i, err := x.Int64()
		if err == nil {
			return i, nil
		}
		f, err2 := x.Float64()
		if err2 != nil {
			return 0, err
		}
		return int64(f), nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case string:
		s := strings.TrimSpace(x)
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return i, nil
		}
		f, err2 := strconv.ParseFloat(s, 64)
		if err2 != nil {
			return 0, fmt.Errorf("cannot cast %q to int", x)
		}
		return int64(f), nil
	case []byte:
		return castToInt(string(x))
	default:
		return 0, fmt.Errorf("cannot cast %T to int", v)
	}
}

func castToFloat(v any) (float64, error) {
	if f, ok := toFloat(v); ok {
		return f, nil
	}
	switch x := v.(type) {
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case json.Number:
		return x.Float64()
	case []byte:
		return castToFloat(string(x))
	default:
		return 0, fmt.Errorf("cannot cast %T to float", v)
	}
}

func castToBool(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int:
		return x != 0, nil
	case int32:
		return x != 0, nil
	case int64:
		return x != 0, nil
	case float64:
		return x != 0, nil
	case float32:
		return x != 0, nil
	case string:
		s := strings.TrimSpace(strings.ToLower(x))
		switch s {
		case "1", "t", "true", "yes", "y", "on":
			return true, nil
		case "0", "f", "false", "no", "n", "off", "":
			return false, nil
		default:
			b, err := strconv.ParseBool(s)
			if err != nil {
				return false, fmt.Errorf("cannot cast %q to bool", x)
			}
			return b, nil
		}
	case []byte:
		return castToBool(string(x))
	default:
		return false, fmt.Errorf("cannot cast %T to bool", v)
	}
}

func castToTimestamp(v any) (time.Time, error) {
	switch x := v.(type) {
	case time.Time:
		return x, nil
	case int:
		return unixLikeTime(int64(x)), nil
	case int32:
		return unixLikeTime(int64(x)), nil
	case int64:
		return unixLikeTime(x), nil
	case float64:
		return unixLikeTime(int64(x)), nil
	case float32:
		return unixLikeTime(int64(x)), nil
	case string:
		return parseCastTimeString(x)
	case []byte:
		return parseCastTimeString(string(x))
	default:
		return time.Time{}, fmt.Errorf("cannot cast %T to timestamp", v)
	}
}

func unixLikeTime(v int64) time.Time {
	if v > 1_000_000_000_000 || v < -1_000_000_000_000 {
		return time.UnixMilli(v).UTC()
	}
	return time.Unix(v, 0).UTC()
}

func parseCastTimeString(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty time value")
	}
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return unixLikeTime(i), nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	var lastErr error
	for _, layout := range layouts {
		ts, err := time.Parse(layout, value)
		if err == nil {
			return ts.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as timestamp: %w", value, lastErr)
}

func castToArray(v any) ([]any, error) {
	switch x := v.(type) {
	case []any:
		return x, nil
	case []string:
		out := make([]any, len(x))
		for i, s := range x {
			out[i] = s
		}
		return out, nil
	case []int:
		out := make([]any, len(x))
		for i, n := range x {
			out[i] = n
		}
		return out, nil
	case []int64:
		out := make([]any, len(x))
		for i, n := range x {
			out[i] = n
		}
		return out, nil
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return []any{}, nil
		}
		// Prefer JSON array; fall back to comma-split.
		if strings.HasPrefix(s, "[") {
			var arr []any
			if err := json.Unmarshal([]byte(s), &arr); err != nil {
				return nil, fmt.Errorf("cannot parse JSON array: %w", err)
			}
			return arr, nil
		}
		parts := strings.Split(s, ",")
		out := make([]any, 0, len(parts))
		for _, p := range parts {
			out = append(out, strings.TrimSpace(p))
		}
		return out, nil
	case []byte:
		return castToArray(string(x))
	default:
		// Wrap scalar as single-element array.
		return []any{v}, nil
	}
}

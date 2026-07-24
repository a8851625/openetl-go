package transform

import (
	"context"
	"fmt"
	"reflect"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("coalesce", func(config map[string]any) (core.Transform, error) {
		return NewCoalesceTransform(config)
	})
}

// CoalesceTransform writes the first non-empty field value into target_field.
//
// YAML:
//
//	transforms:
//	  - type: coalesce
//	    config:
//	      fields: ["nickname", "username", "email"]
//	      target_field: display_name
type CoalesceTransform struct {
	fields      []string
	targetField string
}

func NewCoalesceTransform(config map[string]any) (*CoalesceTransform, error) {
	fields, err := stringSliceConfig(config, "fields")
	if err != nil {
		return nil, fmt.Errorf("coalesce: %w", err)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("coalesce: fields is required")
	}
	target, _ := config["target_field"].(string)
	if target == "" {
		return nil, fmt.Errorf("coalesce: target_field is required")
	}
	return &CoalesceTransform{fields: fields, targetField: target}, nil
}

func (t *CoalesceTransform) Name() string { return "coalesce" }

func (t *CoalesceTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	_ = ctx
	if rec.Data == nil {
		rec.Data = map[string]any{}
	}
	for _, field := range t.fields {
		v, ok := rec.Data[field]
		if !ok || isCoalesceEmpty(v) {
			continue
		}
		rec.Data[t.targetField] = v
		return rec, nil
	}
	// No non-empty candidate: leave target unset (or clear if previously set).
	return rec, nil
}

// isCoalesceEmpty reports whether v should be skipped by coalesce.
// Treats nil, empty string, false, numeric zero, and empty slices/maps as empty.
func isCoalesceEmpty(v any) bool {
	if v == nil {
		return true
	}
	switch x := v.(type) {
	case string:
		return x == ""
	case []byte:
		return len(x) == 0
	case bool:
		return !x
	case int:
		return x == 0
	case int32:
		return x == 0
	case int64:
		return x == 0
	case float32:
		return x == 0
	case float64:
		return x == 0
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array:
		return rv.Len() == 0
	case reflect.Ptr, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}

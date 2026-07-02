package transform

import (
	"context"
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("map_fields", func(config map[string]any) (core.Transform, error) {
		return NewMapFieldsTransform(config)
	})
}

// MapFieldsTransform applies static dictionary mapping to declared fields.
// Each configured field is looked up in its `map`; on hit the value is
// replaced with the mapped value. On miss, `default_value` is used when
// configured, otherwise the original value is preserved (or set to null
// when `on_missing: null`).
type MapFieldsTransform struct {
	fields []mapFieldRule
}

type mapFieldRule struct {
	field        string
	dict         map[string]any
	defaultValue any
	hasDefault   bool
	onMissing    string // "keep" (default) | "null"
}

// NewMapFieldsTransform builds a MapFieldsTransform from config.
//
// Config shape:
//
//	fields:
//	  - field: status_code
//	    map: {"1": "ONLINE", "3": "NOT_CHARGING"}
//	    default: UNKNOWN
//	    on_missing: keep
//	  - field: charge_state
//	    map: {"0": "IDLE"}
func NewMapFieldsTransform(config map[string]any) (*MapFieldsTransform, error) {
	t := &MapFieldsTransform{}
	rawFields, ok := config["fields"]
	if !ok || rawFields == nil {
		return nil, fmt.Errorf("map_fields: fields is required")
	}
	arr, ok := rawFields.([]any)
	if !ok {
		return nil, fmt.Errorf("map_fields: fields must be a list, got %T", rawFields)
	}
	if len(arr) == 0 {
		return nil, fmt.Errorf("map_fields: fields must not be empty")
	}
	for i, raw := range arr {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("map_fields: fields[%d] must be a map, got %T", i, raw)
		}
		fieldName, _ := m["field"].(string)
		if fieldName == "" {
			return nil, fmt.Errorf("map_fields: fields[%d].field is required", i)
		}
		rawMap, ok := m["map"]
		if !ok || rawMap == nil {
			return nil, fmt.Errorf("map_fields: fields[%d].map is required and must be non-empty", i)
		}
		dictMap, ok := rawMap.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("map_fields: fields[%d].map must be a map, got %T", i, rawMap)
		}
		if len(dictMap) == 0 {
			return nil, fmt.Errorf("map_fields: fields[%d].map must be non-empty", i)
		}
		rule := mapFieldRule{field: fieldName, dict: dictMap}
		if dv, ok := m["default"]; ok && dv != nil {
			rule.defaultValue = dv
			rule.hasDefault = true
		}
		if om, ok := m["on_missing"]; ok {
			s := fmt.Sprint(om)
			switch s {
			case "keep", "null":
				rule.onMissing = s
			default:
				return nil, fmt.Errorf("map_fields: fields[%d].on_missing %q is not supported (allowed: keep, null)", i, s)
			}
		}
		t.fields = append(t.fields, rule)
	}
	return t, nil
}

func (t *MapFieldsTransform) Name() string { return "map_fields" }

func (t *MapFieldsTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if rec.Data == nil {
		return rec, nil
	}
	for _, rule := range t.fields {
		val, exists := rec.Data[rule.field]
		if !exists {
			continue
		}
		key := fmt.Sprint(val)
		if mapped, ok := rule.dict[key]; ok {
			rec.Data[rule.field] = mapped
			continue
		}
		if rule.hasDefault {
			rec.Data[rule.field] = rule.defaultValue
			continue
		}
		switch rule.onMissing {
		case "null":
			rec.Data[rule.field] = nil
		default:
			// "keep": leave the original value untouched
		}
	}
	return rec, nil
}

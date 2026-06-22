package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("validate", func(config map[string]any) (core.Transform, error) {
		t := &ValidateTransform{onFailure: "error"}

		if v, ok := config["required_fields"]; ok {
			if arr, ok := v.([]interface{}); ok {
				for _, f := range arr {
					if fs, ok := f.(string); ok {
						t.requiredFields = append(t.requiredFields, fs)
					}
				}
			}
		}

		if v, ok := config["rules"]; ok {
			if arr, ok := v.([]interface{}); ok {
				for _, item := range arr {
					ruleMap, ok := item.(map[string]interface{})
					if !ok {
						continue
					}
					rule := ValidationRule{}
					if f, ok := ruleMap["field"]; ok {
						if fs, ok := f.(string); ok {
							rule.Field = fs
						}
					}
					if typ, ok := ruleMap["type"]; ok {
						if ts, ok := typ.(string); ok {
							rule.Type = ts
						}
					}
					rule.Value = ruleMap["value"]
					// Pre-compile regex patterns at construction time so
					// Apply doesn't recompile on every record.
					if rule.Type == "regex" {
						if pattern, ok := rule.Value.(string); ok {
							re, err := regexp.Compile(pattern)
							if err != nil {
								return nil, fmt.Errorf("validate: regex rule for field %q has invalid pattern %q: %w", rule.Field, pattern, err)
							}
							rule.compiledPattern = re
						}
					}
					t.rules = append(t.rules, rule)
				}
			}
		}

		if v, ok := config["on_failure"]; ok {
			if s, ok := v.(string); ok {
				t.onFailure = s
			}
		}

		if v, ok := config["fail_immediate"]; ok {
			if b, ok := v.(bool); ok {
				t.failImmediate = b
			}
		}

		return t, nil
	})
}

type ValidationRule struct {
	Field string
	Type  string
	Value any

	// compiledPattern holds the pre-compiled regex for rules of Type
	// "regex". It is populated in the constructor so Apply does not
	// have to recompile the pattern on every record.
	compiledPattern *regexp.Regexp
}

type ValidateTransform struct {
	requiredFields []string
	rules          []ValidationRule
	onFailure      string
	failImmediate  bool
}

func (t *ValidateTransform) Name() string { return "validate" }

func (t *ValidateTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	var failures []string

	if !t.checkRequiredFields(rec, &failures) && t.failImmediate {
		return t.handleFailure(ctx, rec, failures)
	}

	for _, rule := range t.rules {
		if !t.checkRule(rec, rule, &failures) && t.failImmediate {
			return t.handleFailure(ctx, rec, failures)
		}
	}

	if len(failures) > 0 {
		return t.handleFailure(ctx, rec, failures)
	}

	return rec, nil
}

func (t *ValidateTransform) checkRequiredFields(rec core.Record, failures *[]string) bool {
	passed := true
	for _, field := range t.requiredFields {
		v, ok := rec.Data[field]
		if !ok || v == nil {
			*failures = append(*failures, fmt.Sprintf("required field %q missing or nil", field))
			passed = false
		} else if s, isStr := v.(string); isStr && s == "" {
			*failures = append(*failures, fmt.Sprintf("required field %q is empty string", field))
			passed = false
		} else if arr, isArr := v.([]interface{}); isArr && len(arr) == 0 {
			*failures = append(*failures, fmt.Sprintf("required field %q is empty array", field))
			passed = false
		}
	}
	return passed
}

func (t *ValidateTransform) checkRule(rec core.Record, rule ValidationRule, failures *[]string) bool {
	fieldVal, fieldExists := rec.Data[rule.Field]

	switch rule.Type {
	case "required":
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is required but missing or nil", rule.Field))
			return false
		}
		if s, ok := fieldVal.(string); ok && s == "" {
			*failures = append(*failures, fmt.Sprintf("field %q is required but empty string", rule.Field))
			return false
		}
		return true

	case "not_null":
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is nil", rule.Field))
			return false
		}
		return true

	case "regex":
		if rule.compiledPattern == nil {
			*failures = append(*failures, fmt.Sprintf("field %q regex pattern was not compiled (invalid or missing pattern)", rule.Field))
			return false
		}
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is nil, cannot match regex", rule.Field))
			return false
		}
		strVal := fmt.Sprintf("%v", fieldVal)
		if !rule.compiledPattern.MatchString(strVal) {
			*failures = append(*failures, fmt.Sprintf("field %q value %q does not match pattern %q", rule.Field, strVal, rule.compiledPattern.String()))
			return false
		}
		return true

	case "range":
		rangeVals, ok := rule.Value.([]interface{})
		if !ok || len(rangeVals) != 2 {
			*failures = append(*failures, fmt.Sprintf("field %q range value must be [min, max]", rule.Field))
			return false
		}
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is nil, cannot check range", rule.Field))
			return false
		}
		if !t.checkRange(fieldVal, rule.Field, rangeVals[0], rangeVals[1], failures) {
			return false
		}
		return true

	case "enum":
		enumVals, ok := rule.Value.([]interface{})
		if !ok {
			*failures = append(*failures, fmt.Sprintf("field %q enum value must be a list", rule.Field))
			return false
		}
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is nil, cannot check enum", rule.Field))
			return false
		}
		strVal := fmt.Sprintf("%v", fieldVal)
		found := false
		for _, ev := range enumVals {
			if fmt.Sprintf("%v", ev) == strVal {
				found = true
				break
			}
		}
		if !found {
			*failures = append(*failures, fmt.Sprintf("field %q value %q not in allowed enum values", rule.Field, strVal))
			return false
		}
		return true

	case "length_min":
		minVal, ok := toFloat64(rule.Value)
		if !ok {
			*failures = append(*failures, fmt.Sprintf("field %q length_min value must be a number", rule.Field))
			return false
		}
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is nil, cannot check length_min", rule.Field))
			return false
		}
		length := getLength(fieldVal)
		if float64(length) < minVal {
			*failures = append(*failures, fmt.Sprintf("field %q length %d is less than minimum %v", rule.Field, length, int64(minVal)))
			return false
		}
		return true

	case "length_max":
		maxVal, ok := toFloat64(rule.Value)
		if !ok {
			*failures = append(*failures, fmt.Sprintf("field %q length_max value must be a number", rule.Field))
			return false
		}
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is nil, cannot check length_max", rule.Field))
			return false
		}
		length := getLength(fieldVal)
		if float64(length) > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q length %d exceeds maximum %v", rule.Field, length, int64(maxVal)))
			return false
		}
		return true

	case "type_is":
		typeName, ok := rule.Value.(string)
		if !ok {
			*failures = append(*failures, fmt.Sprintf("field %q type_is value must be a string", rule.Field))
			return false
		}
		if !fieldExists || fieldVal == nil {
			*failures = append(*failures, fmt.Sprintf("field %q is nil, cannot check type_is", rule.Field))
			return false
		}
		if !t.checkType(fieldVal, typeName) {
			actualType := reflect.TypeOf(fieldVal)
			*failures = append(*failures, fmt.Sprintf("field %q type is %v, expected %s", rule.Field, actualType, typeName))
			return false
		}
		return true

	default:
		*failures = append(*failures, fmt.Sprintf("unknown validation rule type: %s", rule.Type))
		return false
	}
}

func (t *ValidateTransform) checkRange(fieldVal any, fieldName string, minRaw, maxRaw any, failures *[]string) bool {
	minVal, ok1 := toFloat64(minRaw)
	maxVal, ok2 := toFloat64(maxRaw)
	if !ok1 || !ok2 {
		*failures = append(*failures, fmt.Sprintf("field %q range bounds must be numbers", fieldName))
		return false
	}

	switch v := fieldVal.(type) {
	case float64:
		if v < minVal || v > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q value %v is outside range [%v, %v]", fieldName, v, minVal, maxVal))
			return false
		}
	case float32:
		if float64(v) < minVal || float64(v) > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q value %v is outside range [%v, %v]", fieldName, v, minVal, maxVal))
			return false
		}
	case int:
		if float64(v) < minVal || float64(v) > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q value %v is outside range [%v, %v]", fieldName, v, minVal, maxVal))
			return false
		}
	case int32:
		if float64(v) < minVal || float64(v) > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q value %v is outside range [%v, %v]", fieldName, v, minVal, maxVal))
			return false
		}
	case int64:
		if float64(v) < minVal || float64(v) > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q value %v is outside range [%v, %v]", fieldName, v, minVal, maxVal))
			return false
		}
	case string:
		strLen := len(v)
		if float64(strLen) < minVal || float64(strLen) > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q string length %d is outside range [%v, %v]", fieldName, strLen, minVal, maxVal))
			return false
		}
	default:
		fv := toFloat64OrZero(fieldVal)
		if fv < minVal || fv > maxVal {
			*failures = append(*failures, fmt.Sprintf("field %q value %v is outside range [%v, %v]", fieldName, fieldVal, minVal, maxVal))
			return false
		}
	}
	return true
}

func (t *ValidateTransform) checkType(fieldVal any, typeName string) bool {
	switch typeName {
	case "string":
		_, ok := fieldVal.(string)
		return ok
	case "int":
		switch fieldVal.(type) {
		case int, int32, int64:
			return true
		case float64:
			f := fieldVal.(float64)
			return f == float64(int64(f))
		default:
			return false
		}
	case "int64":
		switch fieldVal.(type) {
		case int, int32, int64:
			return true
		case float64:
			f := fieldVal.(float64)
			return f == float64(int64(f))
		default:
			return false
		}
	case "float64":
		switch fieldVal.(type) {
		case float64, float32, int, int32, int64:
			return true
		default:
			return false
		}
	case "bool":
		_, ok := fieldVal.(bool)
		return ok
	default:
		return false
	}
}

func (t *ValidateTransform) handleFailure(ctx context.Context, rec core.Record, failures []string) (core.Record, error) {
	for _, msg := range failures {
		g.Log().Warningf(ctx, "[validate] %s: %s (table=%s, key=%s)", rec.Metadata.Table, msg, rec.Metadata.Table, rec.Metadata.Key)
	}
	switch t.onFailure {
	case "drop":
		// Silently drop: ErrRecordFiltered is recognized by the pipeline
		// and the record is skipped without going to DLQ.
		return core.Record{}, core.ErrRecordFiltered
	case "error":
		// Hard pipeline error: propagates upward and fails the batch.
		return core.Record{}, fmt.Errorf("validation failed: %s", strings.Join(failures, "; "))
	case "dlq":
		// Route to the DLQ. Wrapping with ErrRecordFiltered would silently
		// drop the record; instead we return a typed error that the
		// pipeline can detect and send to the DLQ. The [DLQ] marker in
		// the message makes the intent explicit and auditable.
		return core.Record{}, &ValidationFailedError{
			Failures: failures,
			ToDLQ:    true,
		}
	case "":
		// Default when on_failure is unspecified: send to DLQ.
		return core.Record{}, &ValidationFailedError{Failures: failures}
	default:
		// Unknown on_failure value: surface as a hard error so
		// misconfiguration is loud rather than silently misrouted.
		return core.Record{}, fmt.Errorf("validation failed (unknown on_failure %q): %s", t.onFailure, strings.Join(failures, "; "))
	}
}

// ValidationFailedError indicates a record failed validation rules.
// When ToDLQ is true the pipeline should route the offending record to
// the dead-letter queue; otherwise it is treated as a generic failure.
// This error type is intentionally distinct from core.ErrRecordFiltered
// (which causes silent drops) so the pipeline can decide to persist the
// record for later replay.
type ValidationFailedError struct {
	Failures []string
	// ToDLQ marks this error as one the pipeline should route to the DLQ.
	ToDLQ bool
}

func (e *ValidationFailedError) Error() string {
	msg := "validation failed: " + strings.Join(e.Failures, "; ")
	if e.ToDLQ {
		return "[DLQ] " + msg
	}
	return msg
}

func getLength(v any) int {
	switch val := v.(type) {
	case string:
		return len(val)
	case []interface{}:
		return len(val)
	case []string:
		return len(val)
	case []int:
		return len(val)
	case []float64:
		return len(val)
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
			return rv.Len()
		}
		s := fmt.Sprintf("%v", v)
		return len(s)
	}
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		var f float64
		if _, err := fmt.Sscanf(x, "%f", &f); err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func toFloat64OrZero(v any) float64 {
	f, _ := toFloat64(v)
	return f
}

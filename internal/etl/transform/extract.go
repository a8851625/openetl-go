package transform

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("extract", func(config map[string]any) (core.Transform, error) {
		return NewExtractTransform(config)
	})
}

// ExtractTransform provides minimal field extraction/construction:
//   - regex `pattern` + `group` extracts a substring into a target field
//   - `template` concatenates fields via Go text/template (only .Field variables)
//
// It intentionally does NOT implement an expression engine, conditions, or
// branching — those belong to `filter` or Lua/JS.
type ExtractTransform struct {
	rules []extractRule
}

type extractRule struct {
	target   string
	pattern  *regexp.Regexp
	groupIdx int
	tmpl     *template.Template
	tmplSrc  string
}

// NewExtractTransform builds an ExtractTransform from config.
//
// Config shape:
//
//	rules:
//	  - target: vendor
//	    pattern: "^(.+?)-"
//	    group: 1
//	  - target: material_no
//	    template: "{{.material_name}}.{{.mes_optional_parts}}"
func NewExtractTransform(config map[string]any) (*ExtractTransform, error) {
	t := &ExtractTransform{}
	rawRules, ok := config["rules"]
	if !ok || rawRules == nil {
		return nil, fmt.Errorf("extract: rules is required")
	}
	arr, ok := rawRules.([]any)
	if !ok {
		return nil, fmt.Errorf("extract: rules must be a list, got %T", rawRules)
	}
	if len(arr) == 0 {
		return nil, fmt.Errorf("extract: rules must not be empty")
	}
	for i, raw := range arr {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("extract: rules[%d] must be a map, got %T", i, raw)
		}
		target, _ := m["target"].(string)
		if target == "" {
			return nil, fmt.Errorf("extract: rules[%d].target is required", i)
		}
		rule := extractRule{target: target}
		hasPattern := false
		if p, ok := m["pattern"]; ok && p != nil {
			ps := fmt.Sprint(p)
			re, err := regexp.Compile(ps)
			if err != nil {
				return nil, fmt.Errorf("extract: rules[%d].pattern %q compile error: %w", i, ps, err)
			}
			rule.pattern = re
			rule.groupIdx = 0
			if g, ok := m["group"]; ok {
				switch gv := g.(type) {
				case int:
					rule.groupIdx = gv
				case float64:
					rule.groupIdx = int(gv)
				}
			}
			// Validate group index against pattern subgroup count
			if rule.groupIdx < 0 || rule.groupIdx > re.NumSubexp() {
				return nil, fmt.Errorf("extract: rules[%d].group %d is out of range (pattern has %d subgroups)", i, rule.groupIdx, re.NumSubexp())
			}
			hasPattern = true
		}
		if tmpl, ok := m["template"]; ok && tmpl != nil {
			ts := fmt.Sprint(tmpl)
			tpl, err := template.New("extract").Parse(ts)
			if err != nil {
				return nil, fmt.Errorf("extract: rules[%d].template parse error: %w", i, err)
			}
			rule.tmpl = tpl
			rule.tmplSrc = ts
			if hasPattern {
				return nil, fmt.Errorf("extract: rules[%d] has both pattern and template; choose one", i)
			}
		}
		if !hasPattern && rule.tmpl == nil {
			return nil, fmt.Errorf("extract: rules[%d] requires either pattern or template", i)
		}
		// source_field for pattern defaults to target; can be overridden
		if hasPattern {
			if sf, ok := m["source_field"]; ok && sf != nil {
				// store as a custom field on rule via group name trick: we use a
				// closure-compatible approach by stashing source field in tmplSrc
				// (reused as source-field name when pattern != nil).
				rule.tmplSrc = fmt.Sprint(sf)
			} else {
				rule.tmplSrc = target
			}
		}
		t.rules = append(t.rules, rule)
	}
	return t, nil
}

func (t *ExtractTransform) Name() string { return "extract" }

func (t *ExtractTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if rec.Data == nil {
		rec.Data = map[string]any{}
	}
	for _, rule := range t.rules {
		if rule.pattern != nil {
			srcField := rule.tmplSrc
			if srcField == "" {
				srcField = rule.target
			}
			srcVal, _ := rec.Data[srcField].(string)
			if srcVal == "" {
				// leave target untouched if source missing/empty
				continue
			}
			m := rule.pattern.FindStringSubmatch(srcVal)
			if m == nil {
				continue
			}
			rec.Data[rule.target] = m[rule.groupIdx]
			continue
		}
		if rule.tmpl != nil {
			var b strings.Builder
			if err := rule.tmpl.Execute(&b, rec.Data); err != nil {
				// On template error, leave target untouched (do not fail the batch)
				continue
			}
			rec.Data[rule.target] = b.String()
		}
	}
	return rec, nil
}

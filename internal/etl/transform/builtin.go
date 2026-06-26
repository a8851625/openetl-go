package transform

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registerProjectTransform("project")
	registerProjectTransform("select_fields")

	registry.RegisterTransform("rename", func(config map[string]any) (core.Transform, error) {
		mappings := make(map[string]string)
		if v, ok := config["mappings"]; ok {
			if m, ok := v.(map[string]interface{}); ok {
				for k, val := range m {
					mappings[k] = val.(string)
				}
			}
		}
		return &RenameTransform{mappings: mappings}, nil
	})

	registry.RegisterTransform("drop_field", func(config map[string]any) (core.Transform, error) {
		var fields []string
		if v, ok := config["fields"]; ok {
			if arr, ok := v.([]interface{}); ok {
				for _, f := range arr {
					fields = append(fields, f.(string))
				}
			}
		}
		return &DropFieldTransform{fields: fields}, nil
	})

	registry.RegisterTransform("add_field", func(config map[string]any) (core.Transform, error) {
		field := ""
		value := ""
		if v, ok := config["field"]; ok {
			field = v.(string)
		}
		if v, ok := config["value"]; ok {
			value = fmt.Sprintf("%v", v)
		}
		return &AddFieldTransform{field: field, value: value}, nil
	})

	registry.RegisterTransform("type_convert", func(config map[string]any) (core.Transform, error) {
		conversions := make(map[string]string)
		if v, ok := config["conversions"]; ok {
			if m, ok := v.(map[string]interface{}); ok {
				for k, val := range m {
					conversions[k] = val.(string)
				}
			}
		}
		return &TypeConvertTransform{conversions: conversions}, nil
	})

	registry.RegisterTransform("filter", func(config map[string]any) (core.Transform, error) {
		expression := ""
		if v, ok := config["expression"]; ok {
			expression = v.(string)
		}
		// strict_types (TF-14): when true, a numeric comparison against a
		// non-numeric field value returns an error (→ DLQ) instead of silently
		// filtering the record out — surfaces schema drift. Default false.
		strictTypes := false
		if v, ok := config["strict_types"]; ok {
			if b, ok := v.(bool); ok {
				strictTypes = b
			}
		}
		return &FilterTransform{expression: expression, strictTypes: strictTypes}, nil
	})

	registry.RegisterTransform("identity", func(config map[string]any) (core.Transform, error) {
		return &IdentityTransform{}, nil
	})
}

func registerProjectTransform(name string) {
	registry.RegisterTransform(name, func(config map[string]any) (core.Transform, error) {
		fields, err := stringSliceConfig(config, "fields")
		if err != nil {
			return nil, err
		}
		mappings, err := stringMapConfig(config, "mappings")
		if err != nil {
			return nil, err
		}
		constants, err := anyMapConfig(config, "constants")
		if err != nil {
			return nil, err
		}
		timeFormats, err := stringMapConfig(config, "time_formats")
		if err != nil {
			return nil, err
		}
		keepUnmapped, err := boolConfig(config, "keep_unmapped")
		if err != nil {
			return nil, err
		}
		return &ProjectTransform{
			name:         name,
			fields:       fields,
			mappings:     mappings,
			constants:    constants,
			timeFormats:  timeFormats,
			keepUnmapped: keepUnmapped,
		}, nil
	})
}

func stringSliceConfig(config map[string]any, key string) ([]string, error) {
	v, ok := config[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch arr := v.(type) {
	case []string:
		return append([]string(nil), arr...), nil
	case []interface{}:
		out := make([]string, 0, len(arr))
		for i, item := range arr {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s[%d] must be a string", key, i)
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s must be an array of strings", key)
	}
}

func stringMapConfig(config map[string]any, key string) (map[string]string, error) {
	v, ok := config[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch m := v.(type) {
	case map[string]string:
		out := make(map[string]string, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]string, len(m))
		for k, val := range m {
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("%s[%q] must be a string", key, k)
			}
			out[k] = s
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s must be a string map", key)
	}
}

func anyMapConfig(config map[string]any, key string) (map[string]any, error) {
	v, ok := config[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch m := v.(type) {
	case map[string]string:
		out := make(map[string]any, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out, nil
	case map[string]interface{}:
		out := make(map[string]any, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s must be a map", key)
	}
}

func boolConfig(config map[string]any, key string) (bool, error) {
	v, ok := config[key]
	if !ok || v == nil {
		return false, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return b, nil
}

type ProjectTransform struct {
	name         string
	fields       []string
	mappings     map[string]string
	constants    map[string]any
	timeFormats  map[string]string
	keepUnmapped bool
}

func (t *ProjectTransform) Name() string {
	if t.name != "" {
		return t.name
	}
	return "project"
}

func (t *ProjectTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	projected := make(map[string]any)
	if t.keepUnmapped {
		for k, v := range rec.Data {
			projected[k] = v
		}
	}
	for _, field := range t.fields {
		if v, ok := rec.Data[field]; ok {
			projected[field] = v
		}
	}
	for source, target := range t.mappings {
		if v, ok := rec.Data[source]; ok {
			if t.keepUnmapped && source != target {
				delete(projected, source)
			}
			projected[target] = v
		}
	}
	for field, value := range t.constants {
		projected[field] = value
	}
	for field, layout := range t.timeFormats {
		value, ok := projected[field]
		if !ok || value == nil {
			continue
		}
		formatted, err := formatProjectTime(value, layout)
		if err != nil {
			return rec, fmt.Errorf("project: format time field %q: %w", field, err)
		}
		projected[field] = formatted
	}
	rec.Data = projected
	return rec, nil
}

func formatProjectTime(value any, layout string) (any, error) {
	ts, err := projectTimeValue(value)
	if err != nil {
		return value, err
	}
	switch strings.ToLower(layout) {
	case "unix":
		return ts.Unix(), nil
	case "unix_ms":
		return ts.UnixMilli(), nil
	case "rfc3339", "":
		return ts.Format(time.RFC3339), nil
	default:
		return ts.Format(layout), nil
	}
}

func projectTimeValue(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		return parseProjectTimeString(v)
	case []byte:
		return parseProjectTimeString(string(v))
	case int:
		return unixProjectTime(int64(v)), nil
	case int64:
		return unixProjectTime(v), nil
	case int32:
		return unixProjectTime(int64(v)), nil
	case float64:
		return unixProjectTime(int64(v)), nil
	case float32:
		return unixProjectTime(int64(v)), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported time value type %T", value)
	}
}

func parseProjectTimeString(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("empty time value")
	}
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return unixProjectTime(i), nil
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	var lastErr error
	for _, layout := range layouts {
		ts, err := time.Parse(layout, value)
		if err == nil {
			return ts, nil
		}
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as time: %w", value, lastErr)
}

func unixProjectTime(value int64) time.Time {
	if value > 1_000_000_000_000 || value < -1_000_000_000_000 {
		return time.UnixMilli(value)
	}
	return time.Unix(value, 0)
}

type RenameTransform struct {
	mappings map[string]string
}

func (t *RenameTransform) Name() string { return "rename" }

func (t *RenameTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	for oldName, newName := range t.mappings {
		if v, ok := rec.Data[oldName]; ok {
			delete(rec.Data, oldName)
			rec.Data[newName] = v
		}
	}
	return rec, nil
}

type DropFieldTransform struct {
	fields []string
}

func (t *DropFieldTransform) Name() string { return "drop_field" }

func (t *DropFieldTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	for _, f := range t.fields {
		delete(rec.Data, f)
	}
	return rec, nil
}

type AddFieldTransform struct {
	field string
	value string
}

func (t *AddFieldTransform) Name() string { return "add_field" }

func (t *AddFieldTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	val := t.value
	if val == "{{now}}" || val == "{{.Now}}" {
		rec.Data[t.field] = time.Now().Format(time.RFC3339)
	} else if val == "{{ts}}" {
		rec.Data[t.field] = time.Now().Unix()
	} else {
		rec.Data[t.field] = val
	}
	return rec, nil
}

type TypeConvertTransform struct {
	conversions map[string]string
}

func (t *TypeConvertTransform) Name() string { return "type_convert" }

func (t *TypeConvertTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	for field, targetType := range t.conversions {
		if v, ok := rec.Data[field]; ok {
			converted, err := convertType(v, targetType)
			if err == nil {
				rec.Data[field] = converted
			}
		}
	}
	return rec, nil
}

func convertType(v any, targetType string) (any, error) {
	strVal := fmt.Sprintf("%v", v)
	switch strings.ToLower(targetType) {
	case "int", "int64", "integer":
		i, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			f, err2 := strconv.ParseFloat(strVal, 64)
			if err2 != nil {
				return v, err
			}
			return int64(f), nil
		}
		return i, nil
	case "float", "float64", "double", "number":
		return strconv.ParseFloat(strVal, 64)
	case "bool", "boolean":
		return strconv.ParseBool(strVal)
	case "string", "str":
		return strVal, nil
	case "datetime", "timestamp":
		layouts := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, strVal); err == nil {
				return t, nil
			}
		}
		return v, fmt.Errorf("cannot parse datetime: %s", strVal)
	default:
		return v, nil
	}
}

type FilterTransform struct {
	expression  string
	pred        *filterPredicate // compiled predicate for the expression
	strictTypes bool             // TF-14: error on numeric/non-numeric mismatch
}

// filterPredicate is a compiled boolean evaluator over a record map.
type filterPredicate struct {
	// kind classifies the predicate for fast evaluation.
	kind   predicateKind
	field  string
	op     string
	numVal float64
	strVal string
	isNum  bool
	left   *filterPredicate
	right  *filterPredicate
}

type predicateKind int

const (
	predField   predicateKind = iota // field presence
	predNot                          // !child
	predCompare                      // field op value
	predAnd                          // left && right
	predOr                           // left || right
	predTrue                         // always pass
)

func (t *FilterTransform) Name() string { return "filter" }

func (t *FilterTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if t.expression == "" {
		return rec, nil
	}
	if t.pred == nil {
		// Lazy compile on first Apply (cheap; happens once per transform instance).
		p, err := compileFilter(t.expression)
		if err != nil {
			return rec, fmt.Errorf("filter compile: %w", err)
		}
		t.pred = p
	}

	match, err := t.pred.eval(rec.Data, t.strictTypes)
	if err != nil {
		return rec, err // strict_types: type mismatch → DLQ instead of silent drop
	}
	if !match {
		return rec, core.ErrRecordFiltered
	}
	return rec, nil
}

// eval returns true if the record passes the predicate. When strict is true, a
// numeric comparison against a non-numeric value returns an error so the caller
// can route the record to the DLQ (TF-14) instead of silently dropping it.
func (p *filterPredicate) eval(data map[string]any, strict bool) (bool, error) {
	switch p.kind {
	case predTrue:
		return true, nil
	case predField:
		v, ok := data[p.field]
		return ok && v != nil, nil
	case predNot:
		res, err := p.left.eval(data, strict)
		return !res, err
	case predCompare:
		v, ok := data[p.field]
		if !ok || v == nil {
			return false, nil
		}
		return compareValues(v, p.op, p.isNum, p.numVal, p.strVal, strict)
	case predAnd:
		l, err := p.left.eval(data, strict)
		if err != nil {
			return false, err
		}
		if !l {
			return false, nil
		}
		return p.right.eval(data, strict)
	case predOr:
		l, err := p.left.eval(data, strict)
		if err != nil {
			return false, err
		}
		if l {
			return true, nil
		}
		return p.right.eval(data, strict)
	}
	return false, nil
}

// compareValues applies the operator to a record value vs the predicate constant.
func compareValues(actual any, op string, isNum bool, numVal float64, strVal string, strict bool) (bool, error) {
	if isNum {
		f, ok := toFloat(actual)
		if !ok {
			if strict {
				return false, fmt.Errorf("filter: numeric %q comparison against non-numeric value %v (strict_types=true)", op, actual)
			}
			return false, nil
		}
		switch op {
		case "==", "=":
			return f == numVal, nil
		case "!=":
			return f != numVal, nil
		case ">":
			return f > numVal, nil
		case "<":
			return f < numVal, nil
		case ">=":
			return f >= numVal, nil
		case "<=":
			return f <= numVal, nil
		}
		return false, nil
	}
	s := fmt.Sprintf("%v", actual)
	switch op {
	case "==", "=":
		return s == strVal, nil
	case "!=":
		return s != strVal, nil
	case ">":
		return s > strVal, nil
	case "<":
		return s < strVal, nil
	case ">=":
		return s >= strVal, nil
	case "<=":
		return s <= strVal, nil
	}
	return false, nil
}

// toFloat attempts to convert any numeric-like value to float64.
func toFloat(v any) (float64, bool) {
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
	case string:
		var f float64
		_, err := fmt.Sscanf(x, "%f", &f)
		return f, err == nil
	}
	return 0, false
}

// compileFilter parses a boolean expression into a predicate tree.
// Supported grammar (precedence from low to high):
//
//	expr  := orExpr
//	or    := and ('||' and)*
//	and   := not ('&&' not)*
//	not   := '!' not | atom
//	atom  := '(' expr ')' | fieldExists | field op value
//	field := identifier
//	op    := '==' | '!=' | '>=' | '<=' | '>' | '<' | '='
//	value := number | quotedString
func compileFilter(expr string) (*filterPredicate, error) {
	tokens, err := tokenize(expr)
	if err != nil {
		return nil, err
	}
	p := &tokenParser{tokens: tokens, pos: 0}
	pred, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	// Success means we ended at EOF (the trailing sentinel token).
	if p.peek().kind != tokEOF {
		return nil, fmt.Errorf("unexpected token at end: %v", p.peek())
	}
	return pred, nil
}

type token struct {
	kind tokenKind
	text string
}

type tokenKind int

const (
	tokIdent tokenKind = iota
	tokNumber
	tokString
	tokOp
	tokLParen
	tokRParen
	tokAnd
	tokOr
	tokNot
	tokEOF
)

func tokenize(s string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n':
			i++
		case c == '(':
			toks = append(toks, token{tokLParen, "("})
			i++
		case c == ')':
			toks = append(toks, token{tokRParen, ")"})
			i++
		case c == '\'' || c == '"':
			quote := c
			i++
			start := i
			for i < len(s) && s[i] != quote {
				i++
			}
			if i >= len(s) {
				return nil, fmt.Errorf("unterminated string in expression")
			}
			toks = append(toks, token{tokString, s[start:i]})
			i++
		case c >= '0' && c <= '9' || (c == '-' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9'):
			start := i
			if c == '-' {
				i++
			}
			for i < len(s) && (s[i] == '.' || (s[i] >= '0' && s[i] <= '9')) {
				i++
			}
			toks = append(toks, token{tokNumber, s[start:i]})
		case isIdentStart(c):
			start := i
			for i < len(s) && isIdentPart(s[i]) {
				i++
			}
			text := s[start:i]
			switch text {
			case "and", "AND":
				toks = append(toks, token{tokAnd, text})
			case "or", "OR":
				toks = append(toks, token{tokOr, text})
			case "not", "NOT":
				toks = append(toks, token{tokNot, text})
			case "nil", "null", "NIL", "NULL":
				toks = append(toks, token{tokIdent, text})
			default:
				toks = append(toks, token{tokIdent, text})
			}
		case c == '&' && i+1 < len(s) && s[i+1] == '&':
			toks = append(toks, token{tokAnd, "&&"})
			i += 2
		case c == '|' && i+1 < len(s) && s[i+1] == '|':
			toks = append(toks, token{tokOr, "||"})
			i += 2
		case c == '!':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, token{tokOp, "!="})
				i += 2
			} else {
				toks = append(toks, token{tokNot, "!"})
				i++
			}
		case c == '=' && i+1 < len(s) && s[i+1] == '=':
			toks = append(toks, token{tokOp, "=="})
			i += 2
		case c == '=':
			toks = append(toks, token{tokOp, "="})
			i++
		case c == '>' && i+1 < len(s) && s[i+1] == '=':
			toks = append(toks, token{tokOp, ">="})
			i += 2
		case c == '<' && i+1 < len(s) && s[i+1] == '=':
			toks = append(toks, token{tokOp, "<="})
			i += 2
		case c == '>':
			toks = append(toks, token{tokOp, ">"})
			i++
		case c == '<':
			toks = append(toks, token{tokOp, "<"})
			i++
		default:
			return nil, fmt.Errorf("unexpected character %q in expression", c)
		}
	}
	toks = append(toks, token{tokEOF, ""})
	return toks, nil
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}
func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '.'
}

type tokenParser struct {
	tokens []token
	pos    int
}

func (p *tokenParser) peek() token { return p.tokens[p.pos] }
func (p *tokenParser) next() token { t := p.tokens[p.pos]; p.pos++; return t }

func (p *tokenParser) parseOr() (*filterPredicate, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tokOr {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &filterPredicate{kind: predOr, left: left, right: right}
	}
	return left, nil
}

func (p *tokenParser) parseAnd() (*filterPredicate, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tokAnd {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &filterPredicate{kind: predAnd, left: left, right: right}
	}
	return left, nil
}

func (p *tokenParser) parseNot() (*filterPredicate, error) {
	if p.peek().kind == tokNot {
		p.next()
		child, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &filterPredicate{kind: predNot, left: child}, nil
	}
	return p.parseAtom()
}

func (p *tokenParser) parseAtom() (*filterPredicate, error) {
	tok := p.peek()
	if tok.kind == tokLParen {
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tokRParen {
			return nil, fmt.Errorf("expected ')' got %v", p.peek())
		}
		p.next()
		return inner, nil
	}
	if tok.kind != tokIdent {
		return nil, fmt.Errorf("expected identifier got %v", tok)
	}
	p.next()
	fieldName := tok.text

	// "field nil" or "field null" means "field is null" (NOT presence).
	if p.peek().kind == tokIdent {
		next := p.peek().text
		if next == "nil" || next == "null" || next == "NIL" || next == "NULL" {
			p.next()
			// invert presence semantics
			return &filterPredicate{
				kind: predNot,
				left: &filterPredicate{kind: predField, field: fieldName},
			}, nil
		}
	}

	// Field reference without operator = "field exists".
	if p.peek().kind != tokOp {
		return &filterPredicate{kind: predField, field: fieldName}, nil
	}

	opTok := p.next()
	valTok := p.next()
	pred := &filterPredicate{kind: predCompare, field: fieldName, op: opTok.text}
	if valTok.kind == tokNumber {
		var f float64
		if _, err := fmt.Sscanf(valTok.text, "%f", &f); err != nil {
			return nil, fmt.Errorf("invalid number %q", valTok.text)
		}
		pred.isNum = true
		pred.numVal = f
	} else if valTok.kind == tokString {
		pred.strVal = valTok.text
	} else {
		return nil, fmt.Errorf("expected value after operator, got %v", valTok)
	}
	return pred, nil
}

type IdentityTransform struct{}

func (t *IdentityTransform) Name() string { return "identity" }

func (t *IdentityTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}

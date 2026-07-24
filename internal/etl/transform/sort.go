package transform

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("sort", func(config map[string]any) (core.Transform, error) {
		return NewSortTransform(config)
	})
}

const defaultSortMaxBuffer = 100_000

// SortField describes one ordering key.
type SortField struct {
	Name  string
	Order string // asc | desc
}

// SortTransform buffers a batch, sorts it by configured fields, and emits the
// ordered batch. It implements core.BatchTransform. max_buffer caps how many
// records may be sorted in one call (default 100_000).
//
// YAML:
//
//	transforms:
//	  - type: sort
//	    config:
//	      fields:
//	        - name: created_at
//	          order: desc
//	        - name: id
//	          order: asc
//	      max_buffer: 50000
type SortTransform struct {
	fields    []SortField
	maxBuffer int
}

func NewSortTransform(config map[string]any) (*SortTransform, error) {
	t := &SortTransform{
		maxBuffer: defaultSortMaxBuffer,
	}
	fields, err := parseSortFields(config["fields"])
	if err != nil {
		return nil, fmt.Errorf("sort: %w", err)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("sort: fields is required")
	}
	t.fields = fields

	if v, ok := config["max_buffer"]; ok {
		if n, ok := toInt(v); ok {
			if n <= 0 {
				return nil, fmt.Errorf("sort: max_buffer must be > 0")
			}
			t.maxBuffer = n
		} else {
			return nil, fmt.Errorf("sort: max_buffer must be an integer")
		}
	}
	return t, nil
}

func parseSortFields(raw any) ([]SortField, error) {
	if raw == nil {
		return nil, nil
	}
	switch arr := raw.(type) {
	case []SortField:
		out := make([]SortField, len(arr))
		copy(out, arr)
		return out, nil
	case []any:
		out := make([]SortField, 0, len(arr))
		for i, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				// YAML may decode as map[string]interface{} already covered;
				// also accept map[string]string.
				if ms, ok := item.(map[string]string); ok {
					name := ms["name"]
					order := strings.ToLower(strings.TrimSpace(ms["order"]))
					if name == "" {
						return nil, fmt.Errorf("fields[%d].name is required", i)
					}
					if order == "" {
						order = "asc"
					}
					if order != "asc" && order != "desc" {
						return nil, fmt.Errorf("fields[%d].order must be asc or desc", i)
					}
					out = append(out, SortField{Name: name, Order: order})
					continue
				}
				return nil, fmt.Errorf("fields[%d] must be an object", i)
			}
			name, _ := m["name"].(string)
			order, _ := m["order"].(string)
			order = strings.ToLower(strings.TrimSpace(order))
			if name == "" {
				return nil, fmt.Errorf("fields[%d].name is required", i)
			}
			if order == "" {
				order = "asc"
			}
			if order != "asc" && order != "desc" {
				return nil, fmt.Errorf("fields[%d].order must be asc or desc", i)
			}
			out = append(out, SortField{Name: name, Order: order})
		}
		return out, nil
	default:
		return nil, fmt.Errorf("fields must be an array")
	}
}

func (t *SortTransform) Name() string { return "sort" }

// Apply sorts a single-record "batch". Prefer ApplyBatch for multi-record input.
func (t *SortTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	out, err := t.ApplyBatch(ctx, []core.Record{rec})
	if err != nil {
		return rec, err
	}
	if len(out) == 0 {
		return rec, core.ErrRecordFiltered
	}
	return out[0], nil
}

func (t *SortTransform) ApplyBatch(ctx context.Context, recs []core.Record) ([]core.Record, error) {
	_ = ctx
	if len(recs) == 0 {
		return recs, nil
	}
	if len(recs) > t.maxBuffer {
		return nil, fmt.Errorf("sort: batch size %d exceeds max_buffer %d", len(recs), t.maxBuffer)
	}
	out := make([]core.Record, len(recs))
	copy(out, recs)
	sort.SliceStable(out, func(i, j int) bool {
		return t.less(out[i], out[j])
	})
	return out, nil
}

func (t *SortTransform) less(a, b core.Record) bool {
	for _, f := range t.fields {
		var av, bv any
		if a.Data != nil {
			av = a.Data[f.Name]
		}
		if b.Data != nil {
			bv = b.Data[f.Name]
		}
		cmp := compareSortValues(av, bv)
		if cmp == 0 {
			continue
		}
		if f.Order == "desc" {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

// compareSortValues returns -1 if a<b, 0 if equal, 1 if a>b.
// nil sorts before any non-nil value (SQL-like NULLS FIRST for asc).
func compareSortValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	af, aNum := toFloat(a)
	bf, bNum := toFloat(b)
	if aNum && bNum {
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

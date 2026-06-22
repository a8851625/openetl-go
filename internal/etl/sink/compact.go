package sink

import (
	"fmt"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// CompactRecordsByPK compacts a CDC record batch so that multiple operations
// on the same (table, primary-key) collapse to the final operation in source
// order. This prevents sink grouping from reordering events (e.g. a batch
// containing DELETE(pk=1) then INSERT(pk=1) must NOT become INSERT then DELETE).
//
// Records whose table or PK cannot be determined are passed through unchanged
// (in their original position) so they are never silently dropped.
//
// The returned slice preserves the relative source order of the last event per
// (table, pk) key.
func CompactRecordsByPK(records []core.Record, pkColumns func(table string) []string) []core.Record {
	if len(records) <= 1 {
		return records
	}
	type lastIdx struct {
		idx int
		rec core.Record
	}
	// Map (table|pkKey) -> position in the output ordering.
	last := make(map[string]int)
	out := make([]core.Record, 0, len(records))

	for i, rec := range records {
		table := rec.Metadata.Table
		pkCols := pkColumns(table)
		if len(pkCols) == 0 {
			// No PK info: pass through in order.
			out = append(out, rec)
			last["__passthrough__"+fmt.Sprintf("%d", i)] = len(out) - 1
			continue
		}
		src := rec.Data
		if rec.Operation == core.OpDelete && len(rec.Before) > 0 {
			src = rec.Before
		}
		parts := make([]string, 0, len(pkCols))
		ok := true
		for _, c := range pkCols {
			v, exists := src[c]
			if !exists || v == nil {
				ok = false
				break
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		if !ok {
			out = append(out, rec)
			continue
		}
		mapKey := table + "|" + strings.Join(parts, "|")
		if prev, exists := last[mapKey]; exists {
			// Overwrite in place to preserve the position of the first event.
			out[prev] = rec
		} else {
			out = append(out, rec)
			last[mapKey] = len(out) - 1
		}
	}
	return out
}

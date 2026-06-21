package sink

import (
	"fmt"

	"openetl-go/internal/etl/core"
)

// ResolveDeleteKeys extracts primary-key values for a CDC DELETE record.
// It prefers rec.Before (the pre-image carried by MySQL/PostgreSQL logical
// replication deletes), falls back to rec.Data (some sources put the key
// there), and returns an error when the key is missing so the caller can
// route the record to the DLQ instead of silently deleting nothing.
func ResolveDeleteKeys(rec core.Record, pkColumns []string) (map[string]any, error) {
	if len(pkColumns) == 0 {
		return nil, fmt.Errorf("delete record has no pk_columns configured")
	}
	src := rec.Before
	if len(src) == 0 {
		src = rec.Data
	}
	out := make(map[string]any, len(pkColumns))
	for _, col := range pkColumns {
		v, ok := src[col]
		if !ok || v == nil {
			return nil, fmt.Errorf("delete record missing primary-key column %q", col)
		}
		out[col] = v
	}
	return out, nil
}

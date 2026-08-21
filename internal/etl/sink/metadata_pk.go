package sink

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// parseMetadataKeyColumns extracts the column names from a JSON-object
// Metadata.Key. The key may itself be a JSON-encoded string (double
// encoding), or may carry {payload: ...} / {key: ...} wrappers used by
// Debezium-style envelopes. Property names are sorted for deterministic
// ORDER BY / UNIQUE KEY generation. A scalar or empty key returns nil,
// which callers treat as "no key information".
func parseMetadataKeyColumns(raw string) []string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	var decoded any
	for attempts := 0; attempts < 2; attempts++ {
		if err := json.Unmarshal([]byte(value), &decoded); err != nil {
			return nil
		}
		if nested, ok := decoded.(string); ok {
			value = strings.TrimSpace(nested)
			continue
		}
		break
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil
	}
	if payload, ok := obj["payload"].(map[string]any); ok && len(payload) > 0 {
		obj = payload
	}
	if key, ok := obj["key"].(map[string]any); ok && len(obj) == 1 {
		obj = key
	}
	columns := make([]string, 0, len(obj))
	for column := range obj {
		if strings.TrimSpace(column) != "" {
			columns = append(columns, column)
		}
	}
	sort.Strings(columns)
	return columns
}

// sameIdentifierSet reports whether two identifier lists name the same set
// (order-insensitive).
func sameIdentifierSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	bm := make(map[string]int, len(b))
	for _, id := range b {
		bm[id]++
	}
	for _, id := range a {
		if bm[id] == 0 {
			return false
		}
		bm[id]--
	}
	return true
}

// derivePKFromMetadataShared derives the PK column list for a table from the
// first record whose Metadata.Key parses as a JSON object (shared by the
// mysql and postgres sinks' pk_columns_from_metadata support). Returns nil
// when no usable key exists for the table.
func derivePKFromMetadataShared(table string, records []core.Record) []string {
	for _, rec := range records {
		if rec.Metadata.Table != table && rec.Metadata.Table != "" {
			continue
		}
		if rec.Metadata.Key == "" {
			continue
		}
		if pk := parseMetadataKeyColumns(rec.Metadata.Key); len(pk) > 0 {
			return pk
		}
	}
	return nil
}

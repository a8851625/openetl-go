package sink

import (
	"encoding/json"
	"sort"
	"strings"
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

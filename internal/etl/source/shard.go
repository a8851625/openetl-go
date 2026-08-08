package source

import (
	"encoding/json"
	"fmt"
	"strings"
)

// readShardConfig extracts shard_index and shard_total from a source config map.
// Returns (0, 0) if not present (single-shard mode).
// This is the shared utility used by file, http, redis, and other sources
// that need sharding support.
func readShardConfig(config map[string]any) (shardIndex, shardTotal int) {
	if v, ok := config["shard_index"]; ok {
		switch idx := v.(type) {
		case int:
			shardIndex = idx
		case float64:
			shardIndex = int(idx)
		case int64:
			shardIndex = int(idx)
		}
	}
	if v, ok := config["shard_total"]; ok {
		switch t := v.(type) {
		case int:
			shardTotal = t
		case float64:
			shardTotal = int(t)
		case int64:
			shardTotal = int(t)
		}
	}
	return
}

// readInt reads an integer value from a config map with a default fallback.
func readInt(config map[string]any, key string, defaultVal int) int {
	if v, ok := config[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case float64:
			return int(val)
		case int64:
			return int(val)
		}
	}
	return defaultVal
}

// readStringSlice accepts the common decoded shapes produced by YAML, JSON,
// tests, and UI-generated specs.
func readStringSlice(config map[string]any, key string) []string {
	raw, ok := config[key]
	if !ok {
		return nil
	}
	switch values := raw.(type) {
	case []string:
		out := make([]string, 0, len(values))
		for _, value := range values {
			out = append(out, flattenBrokerListText(value)...)
		}
		return out
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if s, ok := value.(string); ok {
				out = append(out, flattenBrokerListText(s)...)
			}
		}
		return out
	case string:
		return flattenBrokerListText(values)
	default:
		if raw != nil {
			return []string{fmt.Sprint(raw)}
		}
	}
	return nil
}

// flattenBrokerListText interprets a config scalar that may be a plain broker
// address ("redpanda:9092", "[::1]:9092"), a JSON array of addresses
// ("[\"redpanda:9092\"]", "[a:9092,b:9092]"), or a JSON string wrapping either
// form. yaml<->json round-trips and UI form fields can produce every one of
// these shapes; failing to flatten them passes the JSON literal verbatim to
// the Kafka client, which then dials `["redpanda:9092"]` as a single address.
func flattenBrokerListText(text string) []string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}
	if strings.HasPrefix(trimmed, "[") {
		var decoded []string
		if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil {
			out := make([]string, 0, len(decoded))
			for _, value := range decoded {
				if value = strings.TrimSpace(value); value != "" {
					out = append(out, value)
				}
			}
			return out
		}
	}
	// A JSON string wrapping a JSON array ("[\"a:9092\"]") is emitted by some
	// yaml<->json converters when the inner text already quotes the array.
	if strings.HasPrefix(trimmed, "\"") {
		var inner string
		if err := json.Unmarshal([]byte(trimmed), &inner); err == nil {
			return flattenBrokerListText(inner)
		}
	}
	// A bracket-prefixed scalar can be a valid IPv6 broker such as
	// `[::1]:9092`; preserve it when it is not valid JSON.
	return []string{trimmed}
}

// readStringMap reads a map[string]string config value from the shapes
// produced by YAML/JSON decoding. YAML maps decode as map[string]any or
// map[interface{}]interface{}; values may be strings or numbers.
func readStringMap(config map[string]any, key string) map[string]string {
	raw, ok := config[key]
	if !ok || raw == nil {
		return nil
	}
	out := map[string]string{}
	switch m := raw.(type) {
	case map[string]string:
		for k, v := range m {
			if v != "" {
				out[k] = v
			}
		}
	case map[string]any:
		for k, v := range m {
			if s := mapStringValue(v); s != "" {
				out[k] = s
			}
		}
	case map[any]any:
		for k, v := range m {
			ks := fmt.Sprint(k)
			if s := mapStringValue(v); s != "" {
				out[ks] = s
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mapStringValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

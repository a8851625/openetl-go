package server

// Field scope labels used by Connection Catalog and pipeline endpoint forms.
const (
	FieldScopeConnection = "connection"
	FieldScopeBehavior   = "behavior"
)

// sinkBehaviorFields are runtime-behavior fields that should live in the
// pipeline endpoint config, not in a saved connection. The connection catalog
// is meant for pure connection-scope fields (host/port/user/password/database/
// brokers/topic-base/endpoint/bucket etc.) so the same connection can be
// reused across multiple pipelines with different write modes.
//
// When a saved connection still contains any of these fields we keep merging
// them for backward compatibility but surface a deprecation warning via the
// returned `behaviorDeprecations` list.
var sinkBehaviorFields = map[string]bool{
	"batch_mode":                 true,
	"pk_columns":                 true,
	"pk_columns_from_metadata":   true,
	"pre_write":                  true,
	"schema_drift":               true,
	"ddl_policy":                 true,
	"retry":                      true,
	"compression":                true,
	"increment_columns":          true,
	"insert_chunk_size":          true,
	"allow_mixed_cdc_non_atomic": true,
	"version_column":             true,
	"optimize_interval_sec":      true,
	"use_final":                  true,
	"write_mode":                 true,
	"format":                     true,
	"columns":                    true,
	"partition":                  true,
	"table":                      true,
	"index":                      true,
	"topic":                      true,
	"auto_create":                true,
	"source_dialect":             true,
	"allow_mixed_cdc":            true,
	"id_column":                  true,
	"chunk_size":                 true,
	"max_retries":                true,
	"retry_base_ms":              true,
	"prefix":                     true,
	"path":                       true,
	"filename":                   true,
	"key_template":               true,
	"content_type":               true,
	"multipart_threshold":        true,
	"part_size":                  true,
}

// sourceBehaviorFields are task-level source options that belong on the
// pipeline endpoint rather than a reusable connection entry.
var sourceBehaviorFields = map[string]bool{
	"table":              true,
	"tables":             true,
	"query":              true,
	"columns":            true,
	"limit":              true,
	"pk_column":          true,
	"cursor_column":      true,
	"topic":              true,
	"topics":             true,
	"group_id":           true,
	"consumer_group":     true,
	"start_from":         true,
	"start_offset":       true,
	"offset":             true,
	"partition":          true,
	"partitions":         true,
	"sheet_range":        true,
	"sheet_id":           true,
	"spreadsheet_token":  true,
	"poll_interval_sec":  true,
	"path":               true,
	"format":             true,
	"delimiter":          true,
	"url":                true,
	"method":             true,
	"headers":            true,
	"body":               true,
	"result_key":         true,
	"pagination":         true,
	"page_size":          true,
	"interval_ms":        true,
	"count":              true,
	"fields":             true,
	"server_id":          true,
	"server_id_base":     true,
	"shard_index":        true,
	"shard_total":        true,
	"slot_name":          true,
	"publication":        true,
	"enable_snapshot":    true,
	"consistent_snapshot_lock": true,
	"key_column":         true,
	"value_column":       true,
	"initial_offset":     true,
	"include_key":        true,
	"auth_type":          true,
	"auth_token":         true,
	"auth_user":          true,
	"auth_pass":          true,
	"page_param":         true,
	"size_param":         true,
	"max_pages":          true,
	"max_retries":        true,
	"retry_base_ms":      true,
	"has_header":         true,
	"pattern":            true,
	"key_field":          true,
	"schema":             true,
	"sample":             true,
	"oauth2_token_url":   true,
	"oauth2_client_id":   true,
	"oauth2_client_secret": true,
	"oauth2_token_field": true,
	"oauth2_header_format": true,
	"oauth2_scopes":      true,
}

// IsBehaviorField reports whether a config field is endpoint/runtime behavior
// rather than pure connection catalog material.
func IsBehaviorField(kind, name string) bool {
	switch kind {
	case "sink":
		return sinkBehaviorFields[name]
	case "source":
		return sourceBehaviorFields[name]
	case "transform":
		return true
	default:
		return false
	}
}

// FieldScopeFor returns the public scope label for a config field.
func FieldScopeFor(kind, name string) string {
	if IsBehaviorField(kind, name) {
		return FieldScopeBehavior
	}
	return FieldScopeConnection
}

// annotateFieldScopes stamps Scope on a copy of the field list for API/UI use.
func annotateFieldScopes(kind string, fields []ConfigField) []ConfigField {
	if len(fields) == 0 {
		return fields
	}
	out := make([]ConfigField, len(fields))
	copy(out, fields)
	for i := range out {
		if out[i].Scope == "" {
			out[i].Scope = FieldScopeFor(kind, out[i].Name)
		}
	}
	return out
}

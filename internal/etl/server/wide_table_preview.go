package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

type wideTablePreviewRequest struct {
	Spec         pipeline.Spec    `json:"spec"`
	Samples      []map[string]any `json:"samples,omitempty"`
	RunPreflight bool             `json:"run_preflight,omitempty"`
}

type wideTablePreviewResponse struct {
	Valid       bool              `json:"valid"`
	Errors      []string          `json:"errors,omitempty"`
	Warnings    []string          `json:"warnings,omitempty"`
	Source      map[string]any    `json:"source"`
	Envelope    map[string]any    `json:"envelope"`
	Lookups     []map[string]any  `json:"lookups"`
	Window      map[string]any    `json:"window,omitempty"`
	Sink        map[string]any    `json:"sink"`
	Sample      []map[string]any  `json:"sample,omitempty"`
	FieldTypes  map[string]string `json:"field_types,omitempty"`
	ProposedDDL string            `json:"proposed_ddl,omitempty"`
	Preflight   *PreflightResult  `json:"preflight,omitempty"`
}

func (s *Server) handleWideTablePreview(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}

	var req wideTablePreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"valid": false, "errors": []string{"invalid body: " + err.Error()}})
		return
	}

	spec := req.Spec
	pipeline.ApplyDefaults(&spec)
	resp := buildWideTablePreview(&spec, req.Samples)
	if err := pipeline.ValidateSpec(&spec); err != nil {
		resp.Valid = false
		resp.Errors = append(resp.Errors, err.Error())
	}
	resp.Warnings = append(resp.Warnings, pipeline.ValidateIdempotency(&spec)...)
	if req.RunPreflight {
		resp.Preflight = s.RunPreflight(r.Context(), &spec)
		if resp.Preflight != nil {
			for _, issue := range resp.Preflight.Issues {
				msg := fmt.Sprintf("[%s] %s - %s", issue.Check, issue.Message, issue.Remediation)
				if issue.Level == "error" {
					resp.Valid = false
					resp.Errors = append(resp.Errors, msg)
				} else {
					resp.Warnings = append(resp.Warnings, msg)
				}
			}
		}
	}

	if !resp.Valid {
		w.WriteHeader(http.StatusBadRequest)
	}
	json.NewEncoder(w).Encode(resp)
}

func buildWideTablePreview(spec *pipeline.Spec, samples []map[string]any) wideTablePreviewResponse {
	resp := wideTablePreviewResponse{
		Valid:      true,
		Source:     previewSource(spec),
		Envelope:   map[string]any{"present": false},
		Lookups:    []map[string]any{},
		Sink:       previewSink(spec),
		FieldTypes: map[string]string{},
	}

	hasNormalize := false
	for _, tr := range spec.Transforms {
		switch tr.Type {
		case "normalize_envelope", "debezium_envelope":
			hasNormalize = true
			resp.Envelope = map[string]any{
				"present":       true,
				"type":          tr.Type,
				"keep_metadata": boolField(tr.Config, "keep_metadata", true),
			}
		case "lookup":
			resp.Lookups = append(resp.Lookups, map[string]any{
				"join_key":             stringField(tr.Config, "join_key", "id"),
				"dim_key":              stringField(tr.Config, "dim_key", "id"),
				"fields":               stringSliceField(tr.Config, "fields"),
				"refresh_interval_sec": intField(tr.Config, "refresh_interval_sec", 300),
				"max_cache_entries":    intField(tr.Config, "max_cache_entries", 0),
				"on_miss":              stringField(tr.Config, "on_miss", "pass"),
				"on_refresh_error":     stringField(tr.Config, "on_refresh_error", "pass"),
				"query":                tr.Config["query"],
			})
		case "window":
			resp.Window = previewWindow(tr.Config)
		case "join":
			resp.Warnings = append(resp.Warnings, "stream-stream join is not production-ready until state checkpoint restore is implemented")
		}
	}

	if spec.Source.Type != "kafka" {
		resp.Warnings = append(resp.Warnings, "wide-table production candidate expects kafka as the fact source")
	}
	if !hasNormalize {
		resp.Warnings = append(resp.Warnings, "add normalize_envelope before lookup/window to standardize Kafka JSON/Debezium metadata")
	}
	if len(resp.Lookups) == 0 {
		resp.Warnings = append(resp.Warnings, "no lookup transform found; dimension join preview is unavailable")
	}
	if spec.Sink.Type != "clickhouse" {
		resp.Warnings = append(resp.Warnings, "first production wide-table sink is ClickHouse; other sinks need separate certification")
	}

	normalized := make([]map[string]any, 0, len(samples))
	for _, sample := range samples {
		normalized = append(normalized, normalizeWideTableSample(sample, hasNormalize))
	}
	resp.Sample = normalized
	resp.FieldTypes = inferPreviewFieldTypes(normalized, resp.Window)
	resp.ProposedDDL = proposeWideTableDDL(spec, resp.FieldTypes, resp.Window)
	return resp
}

func previewSource(spec *pipeline.Spec) map[string]any {
	out := map[string]any{"type": spec.Source.Type}
	if spec.Source.Type == "kafka" {
		out["topic"] = spec.Source.Config["topic"]
		out["group_id"] = spec.Source.Config["group_id"]
		out["initial_offset"] = spec.Source.Config["initial_offset"]
	}
	return out
}

func previewSink(spec *pipeline.Spec) map[string]any {
	out := map[string]any{"type": spec.Sink.Type}
	for _, key := range []string{"database", "table", "pk_columns", "version_column", "schema_drift", "auto_create"} {
		if v, ok := spec.Sink.Config[key]; ok {
			out[key] = v
		}
	}
	if spec.Sink.Type == "clickhouse" {
		out["recommended_engine"] = "ReplacingMergeTree(version_column) ORDER BY pk_columns"
	}
	return out
}

func previewWindow(cfg map[string]any) map[string]any {
	windowType := stringField(cfg, "window_type", "tumbling")
	out := map[string]any{
		"type":                     windowType,
		"window_size_seconds":      intField(cfg, "window_size_seconds", 60),
		"allowed_lateness_seconds": intField(cfg, "allowed_lateness_seconds", 0),
		"group_by":                 stringSliceField(cfg, "group_by"),
		"aggregates":               cfg["aggregates"],
	}
	if windowType != "tumbling" {
		out["warning"] = "only tumbling is implemented in the production path"
	}
	return out
}

func normalizeWideTableSample(sample map[string]any, normalize bool) map[string]any {
	out := map[string]any{
		"operation": "INSERT",
		"data":      cloneAnyMap(sample),
	}
	if !normalize {
		return out
	}

	payload, _ := sample["payload"].(map[string]any)
	if payload == nil {
		return out
	}
	op, _ := payload["op"].(string)
	switch op {
	case "c", "r":
		out["operation"] = "INSERT"
	case "u":
		out["operation"] = "UPDATE"
	case "d":
		out["operation"] = "DELETE"
	}
	if source, ok := payload["source"].(map[string]any); ok {
		if table, ok := source["table"].(string); ok {
			out["table"] = table
		}
	}
	if ts, ok := millisToTime(payload["ts_ms"]); ok {
		out["event_time"] = ts.Format(time.RFC3339Nano)
	}

	var row map[string]any
	if out["operation"] == "DELETE" {
		row, _ = payload["before"].(map[string]any)
	} else {
		row, _ = payload["after"].(map[string]any)
	}
	if row == nil {
		row = map[string]any{}
	}
	data := cloneAnyMap(row)
	data["_op"] = out["operation"]
	if table, ok := out["table"].(string); ok && table != "" {
		data["_source_table"] = table
	}
	if eventTime, ok := out["event_time"].(string); ok && eventTime != "" {
		data["_event_time"] = eventTime
	}
	out["data"] = data
	return out
}

func inferPreviewFieldTypes(samples []map[string]any, window map[string]any) map[string]string {
	types := map[string]string{}
	if window != nil {
		types["window_start"] = "DateTime64(3)"
		types["window_end"] = "DateTime64(3)"
		for _, field := range stringSliceAny(window["group_by"]) {
			types[field] = "String"
		}
		if aggs, ok := window["aggregates"].(map[string]any); ok {
			for name, raw := range aggs {
				fn := ""
				if m, ok := raw.(map[string]any); ok {
					fn, _ = m["func"].(string)
				}
				if fn == "count" {
					types[name] = "UInt64"
				} else {
					types[name] = "Float64"
				}
			}
		}
		return types
	}
	for _, sample := range samples {
		data, _ := sample["data"].(map[string]any)
		for k, v := range data {
			if _, exists := types[k]; !exists {
				types[k] = inferClickHouseType(v)
			}
		}
	}
	return types
}

func proposeWideTableDDL(spec *pipeline.Spec, fieldTypes map[string]string, window map[string]any) string {
	if spec.Sink.Type != "clickhouse" {
		return ""
	}
	database := stringField(spec.Sink.Config, "database", "default")
	table := stringField(spec.Sink.Config, "table", "")
	if table == "" {
		return ""
	}
	pkCols := stringSliceField(spec.Sink.Config, "pk_columns")
	if len(pkCols) == 0 {
		if window != nil {
			pkCols = append([]string{"window_start"}, stringSliceAny(window["group_by"])...)
		} else {
			pkCols = []string{"id"}
		}
	}
	versionCol := stringField(spec.Sink.Config, "version_column", "_version")
	fieldTypes[versionCol] = "UInt64"
	for _, pk := range pkCols {
		if _, ok := fieldTypes[pk]; !ok {
			fieldTypes[pk] = "String"
		}
	}

	names := make([]string, 0, len(fieldTypes))
	for name := range fieldTypes {
		names = append(names, name)
	}
	sort.Strings(names)
	cols := make([]string, 0, len(names))
	for _, name := range names {
		cols = append(cols, fmt.Sprintf("  %s %s", quoteCHIdent(name), fieldTypes[name]))
	}
	orderBy := make([]string, 0, len(pkCols))
	for _, pk := range pkCols {
		orderBy = append(orderBy, quoteCHIdent(pk))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n%s\n) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
		quoteCHIdent(database), quoteCHIdent(table), strings.Join(cols, ",\n"), quoteCHIdent(versionCol), strings.Join(orderBy, ", "))
}

func inferClickHouseType(v any) string {
	switch v.(type) {
	case bool:
		return "UInt8"
	case int, int8, int16, int32, int64:
		return "Int64"
	case uint, uint8, uint16, uint32, uint64:
		return "UInt64"
	case float32, float64:
		return "Float64"
	case string:
		return "String"
	default:
		return "String"
	}
}

func quoteCHIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func cloneAnyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func boolField(cfg map[string]any, key string, def bool) bool {
	if v, ok := cfg[key].(bool); ok {
		return v
	}
	return def
}

func stringSliceAny(v any) []string {
	switch arr := v.(type) {
	case []string:
		return arr
	case []any:
		out := make([]string, 0, len(arr))
		for _, item := range arr {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func millisToTime(v any) (time.Time, bool) {
	switch n := v.(type) {
	case int:
		return time.UnixMilli(int64(n)).UTC(), true
	case int64:
		return time.UnixMilli(n).UTC(), true
	case float64:
		return time.UnixMilli(int64(n)).UTC(), true
	default:
		return time.Time{}, false
	}
}

package transform

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("debezium_cdc", func(config map[string]any) (core.Transform, error) {
		return NewDebeziumCDCTransform(config)
	})
	registry.RegisterTransform("cdc_policy", func(config map[string]any) (core.Transform, error) {
		return NewCDCPolicyTransform("cdc_policy", config)
	})
	registry.RegisterTransform("ddl_guard", func(config map[string]any) (core.Transform, error) {
		return NewCDCPolicyTransform("ddl_guard", config)
	})
}

type debeziumTableMapping struct {
	template string
	rules    map[string]string
}

// DebeziumCDCTransform normalizes Debezium Kafka messages into core.Record
// operation/data/metadata fields and keeps enough source metadata for downstream
// ODS table mapping and CDC policy checks.
type DebeziumCDCTransform struct {
	keepMetadata  bool
	skipTombstone bool
	tableMapping  debeziumTableMapping
}

func NewDebeziumCDCTransform(config map[string]any) (*DebeziumCDCTransform, error) {
	t := &DebeziumCDCTransform{keepMetadata: true, skipTombstone: true}
	if v, ok := config["keep_metadata"].(bool); ok {
		t.keepMetadata = v
	}
	if v, ok := config["skip_tombstone"].(bool); ok {
		t.skipTombstone = v
	}
	mapping, err := parseDebeziumTableMapping(config)
	if err != nil {
		return nil, err
	}
	t.tableMapping = mapping
	return t, nil
}

func (t *DebeziumCDCTransform) Name() string { return "debezium_cdc" }

func (t *DebeziumCDCTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if err := ctx.Err(); err != nil {
		return rec, err
	}
	if rec.Data == nil || len(rec.Data) == 0 {
		if t.skipTombstone {
			return rec, core.ErrRecordFiltered
		}
		rec.Data = map[string]any{"_debezium_tombstone": true}
		return rec, nil
	}

	payload := rec.Data
	if rawPayload, ok := rec.Data["payload"]; ok {
		if rawPayload == nil {
			if t.skipTombstone {
				return rec, core.ErrRecordFiltered
			}
			rec.Data = map[string]any{"_debezium_tombstone": true}
			return rec, nil
		}
		if m, ok := asStringAnyMap(rawPayload); ok {
			payload = m
		}
	}

	source, _ := asStringAnyMap(payload["source"])
	sourceDB := firstString(source["db"], source["database"], payload["databaseName"])
	sourceTable := firstString(source["table"], payload["table"])
	if sourceDB != "" {
		rec.Metadata.Database = sourceDB
	}
	if sourceTable != "" {
		rec.Metadata.Table = sourceTable
	}
	if ts, ok := eventTime(payload["ts_ms"]); ok {
		rec.Metadata.Timestamp = ts
	}

	if ddl := firstString(payload["ddl"], payload["historyRecord"]); ddl != "" {
		rec.Operation = core.OpDDL
		rec.Metadata.DDL = ddl
		rec.Data = cloneMap(payload)
		if t.keepMetadata {
			t.annotate(rec.Data, "ddl", sourceDB, sourceTable, rec.Metadata.Timestamp)
		}
		rec.Metadata.Table = t.mapTable(rec.Metadata.Database, rec.Metadata.Table, rec.Metadata.Timestamp)
		return rec, nil
	}

	after, hasAfter := asStringAnyMap(payload["after"])
	before, hasBefore := asStringAnyMap(payload["before"])
	op := firstString(payload["op"])

	if !hasAfter && !hasBefore && op == "" {
		return rec, nil
	}

	switch op {
	case "c", "r":
		rec.Operation = core.OpInsert
	case "u":
		rec.Operation = core.OpUpdate
	case "d":
		rec.Operation = core.OpDelete
	default:
		if rec.Operation == "" {
			rec.Operation = core.OpUpdate
		}
	}

	if hasBefore {
		rec.Before = cloneMap(before)
	}
	if rec.Operation == core.OpDelete {
		if hasBefore {
			rec.Data = cloneMap(before)
		} else {
			rec.Data = map[string]any{}
		}
	} else if hasAfter {
		rec.Data = cloneMap(after)
	} else if hasBefore {
		rec.Data = cloneMap(before)
	} else {
		return rec, fmt.Errorf("debezium_cdc: Debezium payload has op=%q but no row image", op)
	}

	rec.Metadata.Table = t.mapTable(rec.Metadata.Database, rec.Metadata.Table, rec.Metadata.Timestamp)
	if t.keepMetadata {
		t.annotate(rec.Data, op, sourceDB, sourceTable, rec.Metadata.Timestamp)
		if snapshot := payload["snapshot"]; snapshot != nil {
			rec.Data["_debezium_snapshot"] = snapshot
		} else if snapshot := source["snapshot"]; snapshot != nil {
			rec.Data["_debezium_snapshot"] = snapshot
		}
	}
	return rec, nil
}

func (t *DebeziumCDCTransform) annotate(data map[string]any, op, sourceDB, sourceTable string, ts time.Time) {
	if data == nil {
		return
	}
	data["_debezium_op"] = op
	data["_op"] = string(debeziumOpType(op))
	if sourceDB != "" {
		data["_source_database"] = sourceDB
	}
	if sourceTable != "" {
		data["_source_table"] = sourceTable
	}
	if !ts.IsZero() {
		data["_event_time"] = ts.UTC().Format(time.RFC3339Nano)
	}
}

func (t *DebeziumCDCTransform) mapTable(sourceDB, sourceTable string, ts time.Time) string {
	if sourceTable == "" {
		return sourceTable
	}
	sourceKey := sourceTable
	if sourceDB != "" {
		sourceKey = sourceDB + "." + sourceTable
	}
	if target, ok := t.tableMapping.rules[sourceKey]; ok {
		return renderDebeziumTableTemplate(target, sourceDB, sourceTable, ts)
	}
	if target, ok := t.tableMapping.rules[sourceTable]; ok {
		return renderDebeziumTableTemplate(target, sourceDB, sourceTable, ts)
	}
	if t.tableMapping.template != "" {
		return renderDebeziumTableTemplate(t.tableMapping.template, sourceDB, sourceTable, ts)
	}
	return sourceTable
}

type CDCPolicyTransform struct {
	name          string
	skipDelete    bool
	skipSnapshot  bool
	skipTombstone bool
	dangerousDDL  string
	includeTables []string
	excludeTables []string
	includeDBs    []string
	excludeDBs    []string
	ddlAllowlist  []string
	ddlDenylist   []string

	processed        int64
	skippedFilter    int64
	skippedDelete    int64
	skippedSnapshot  int64
	skippedTombstone int64
	ddlRejected      int64
	ddlDropped       int64
	ddlPassed        int64
}

func NewCDCPolicyTransform(name string, config map[string]any) (*CDCPolicyTransform, error) {
	t := &CDCPolicyTransform{name: name, skipTombstone: true, dangerousDDL: "reject"}
	if v, ok := config["skip_delete"].(bool); ok {
		t.skipDelete = v
	}
	if v, ok := config["skip_snapshot"].(bool); ok {
		t.skipSnapshot = v
	}
	if v, ok := config["skip_tombstone"].(bool); ok {
		t.skipTombstone = v
	}
	if v, ok := config["dangerous_ddl"].(string); ok && v != "" {
		t.dangerousDDL = strings.ToLower(v)
	}
	switch t.dangerousDDL {
	case "reject", "drop", "pass":
	default:
		return nil, fmt.Errorf("%s: dangerous_ddl must be reject, drop, or pass", name)
	}
	var err error
	t.ddlAllowlist, err = stringSliceConfig(config, "ddl_allowlist")
	if err != nil {
		return nil, err
	}
	t.ddlDenylist, err = stringSliceConfig(config, "ddl_denylist")
	if err != nil {
		return nil, err
	}
	t.includeTables, err = stringSliceConfig(config, "include_tables")
	if err != nil {
		return nil, err
	}
	t.excludeTables, err = stringSliceConfig(config, "exclude_tables")
	if err != nil {
		return nil, err
	}
	t.includeDBs, err = stringSliceConfig(config, "include_databases")
	if err != nil {
		return nil, err
	}
	t.excludeDBs, err = stringSliceConfig(config, "exclude_databases")
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *CDCPolicyTransform) Name() string { return t.name }

func (t *CDCPolicyTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if err := ctx.Err(); err != nil {
		return rec, err
	}
	atomic.AddInt64(&t.processed, 1)
	if t.skipTombstone && isDebeziumTombstone(rec) {
		atomic.AddInt64(&t.skippedTombstone, 1)
		return rec, core.ErrRecordFiltered
	}
	if t.filteredBySourceRules(rec) {
		atomic.AddInt64(&t.skippedFilter, 1)
		return rec, core.ErrRecordFiltered
	}
	if t.skipSnapshot && isDebeziumSnapshot(rec) {
		atomic.AddInt64(&t.skippedSnapshot, 1)
		return rec, core.ErrRecordFiltered
	}
	if t.skipDelete && rec.Operation == core.OpDelete {
		atomic.AddInt64(&t.skippedDelete, 1)
		return rec, core.ErrRecordFiltered
	}
	if rec.Operation != core.OpDDL {
		return rec, nil
	}
	ddl := rec.Metadata.DDL
	if ddl == "" {
		ddl = fmt.Sprintf("%v", rec.Data["ddl"])
	}
	if !t.isDangerousDDL(ddl) {
		atomic.AddInt64(&t.ddlPassed, 1)
		return rec, nil
	}
	switch t.dangerousDDL {
	case "pass":
		atomic.AddInt64(&t.ddlPassed, 1)
		return rec, nil
	case "drop":
		atomic.AddInt64(&t.ddlDropped, 1)
		return rec, core.ErrRecordFiltered
	default:
		atomic.AddInt64(&t.ddlRejected, 1)
		return rec, core.ClassifiedError{Class: core.ErrorClassSchema, Err: fmt.Errorf("%s: dangerous DDL rejected: %s", t.name, ddl)}
	}
}

func (t *CDCPolicyTransform) TransformMetrics() core.TransformMetrics {
	return core.TransformMetrics{
		Transform: t.name,
		Counters: map[string]int64{
			"processed":         atomic.LoadInt64(&t.processed),
			"skipped_filter":    atomic.LoadInt64(&t.skippedFilter),
			"skipped_delete":    atomic.LoadInt64(&t.skippedDelete),
			"skipped_snapshot":  atomic.LoadInt64(&t.skippedSnapshot),
			"skipped_tombstone": atomic.LoadInt64(&t.skippedTombstone),
			"ddl_rejected":      atomic.LoadInt64(&t.ddlRejected),
			"ddl_dropped":       atomic.LoadInt64(&t.ddlDropped),
			"ddl_passed":        atomic.LoadInt64(&t.ddlPassed),
		},
	}
}

func (t *CDCPolicyTransform) filteredBySourceRules(rec core.Record) bool {
	sourceDB, sourceTable := sourceDBAndTable(rec)
	if len(t.includeDBs) > 0 && !anySourcePatternMatches(t.includeDBs, sourceDB) {
		return true
	}
	if anySourcePatternMatches(t.excludeDBs, sourceDB) {
		return true
	}
	if len(t.includeTables) > 0 && !anyTablePatternMatches(t.includeTables, sourceDB, sourceTable) {
		return true
	}
	return anyTablePatternMatches(t.excludeTables, sourceDB, sourceTable)
}

func (t *CDCPolicyTransform) isDangerousDDL(ddl string) bool {
	ddl = strings.TrimSpace(ddl)
	if ddl == "" {
		return false
	}
	for _, pattern := range t.ddlDenylist {
		if ddlPatternMatches(pattern, ddl) {
			return true
		}
	}
	if len(t.ddlAllowlist) > 0 {
		for _, pattern := range t.ddlAllowlist {
			if ddlPatternMatches(pattern, ddl) {
				return false
			}
		}
		return true
	}
	lower := strings.ToLower(ddl)
	for _, keyword := range []string{"drop ", "truncate ", "alter ", "rename "} {
		if strings.Contains(lower, keyword) || strings.HasPrefix(lower, strings.TrimSpace(keyword)+" ") {
			return true
		}
	}
	return false
}

func sourceDBAndTable(rec core.Record) (string, string) {
	sourceDB := rec.Metadata.Database
	sourceTable := rec.Metadata.Table
	if rec.Data != nil {
		if v, ok := rec.Data["_source_database"].(string); ok && v != "" {
			sourceDB = v
		}
		if v, ok := rec.Data["_source_table"].(string); ok && v != "" {
			sourceTable = v
		}
	}
	return sourceDB, sourceTable
}

func anySourcePatternMatches(patterns []string, value string) bool {
	for _, pattern := range patterns {
		if sourcePatternMatches(pattern, value) {
			return true
		}
	}
	return false
}

func anyTablePatternMatches(patterns []string, sourceDB, sourceTable string) bool {
	if sourceTable == "" {
		return false
	}
	candidates := []string{sourceTable}
	if sourceDB != "" {
		candidates = append(candidates, sourceDB+"."+sourceTable)
	}
	for _, pattern := range patterns {
		for _, candidate := range candidates {
			if sourcePatternMatches(pattern, candidate) {
				return true
			}
		}
	}
	return false
}

func sourcePatternMatches(pattern, value string) bool {
	pattern = strings.TrimSpace(pattern)
	value = strings.TrimSpace(value)
	if pattern == "" || value == "" {
		return false
	}
	if strings.EqualFold(pattern, value) {
		return true
	}
	if strings.ContainsAny(pattern, "*?[") {
		matched, err := filepath.Match(strings.ToLower(pattern), strings.ToLower(value))
		return err == nil && matched
	}
	return false
}

func parseDebeziumTableMapping(config map[string]any) (debeziumTableMapping, error) {
	mapping := debeziumTableMapping{rules: map[string]string{}}
	if v, ok := config["target_table_template"].(string); ok {
		mapping.template = v
	}
	raw, ok := config["table_mapping"]
	if !ok || raw == nil {
		return mapping, nil
	}
	switch v := raw.(type) {
	case string:
		mapping.template = v
	case map[string]string:
		for k, value := range v {
			mapping.rules[k] = value
		}
	case map[string]any:
		usedStructuredKeys := false
		if template, ok := v["template"].(string); ok {
			mapping.template = template
			usedStructuredKeys = true
		}
		if rules, ok := v["rules"].(map[string]any); ok {
			usedStructuredKeys = true
			for k, value := range rules {
				s, ok := value.(string)
				if !ok {
					return mapping, fmt.Errorf("debezium_cdc: table_mapping.rules[%q] must be a string", k)
				}
				mapping.rules[k] = s
			}
		}
		if rules, ok := v["rules"].(map[string]string); ok {
			usedStructuredKeys = true
			for k, value := range rules {
				mapping.rules[k] = value
			}
		}
		if !usedStructuredKeys {
			for k, value := range v {
				s, ok := value.(string)
				if !ok {
					return mapping, fmt.Errorf("debezium_cdc: table_mapping[%q] must be a string", k)
				}
				mapping.rules[k] = s
			}
		}
	default:
		return mapping, fmt.Errorf("debezium_cdc: table_mapping must be a string or map")
	}
	return mapping, nil
}

func renderDebeziumTableTemplate(template, sourceDB, sourceTable string, ts time.Time) string {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	out := strings.ReplaceAll(template, "{source_db}", sourceDB)
	out = strings.ReplaceAll(out, "{source_table}", sourceTable)
	out = strings.ReplaceAll(out, "{YYYYMMDD}", ts.UTC().Format("20060102"))
	out = strings.ReplaceAll(out, "{YYYY-MM-DD}", ts.UTC().Format("2006-01-02"))
	return out
}

func firstString(values ...any) string {
	for _, value := range values {
		if s, ok := value.(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func debeziumOpType(op string) core.OpType {
	switch op {
	case "c", "r":
		return core.OpInsert
	case "u":
		return core.OpUpdate
	case "d":
		return core.OpDelete
	case "ddl":
		return core.OpDDL
	default:
		return core.OpUpdate
	}
}

func isDebeziumTombstone(rec core.Record) bool {
	if rec.Data == nil || len(rec.Data) == 0 {
		return true
	}
	v, ok := rec.Data["_debezium_tombstone"].(bool)
	return ok && v
}

func isDebeziumSnapshot(rec core.Record) bool {
	if rec.Data == nil {
		return false
	}
	if op, _ := rec.Data["_debezium_op"].(string); op == "r" {
		return true
	}
	switch v := rec.Data["_debezium_snapshot"].(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "last"
	default:
		return false
	}
}

func ddlPatternMatches(pattern, ddl string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	re, err := regexp.Compile("(?i)" + pattern)
	if err == nil && re.MatchString(ddl) {
		return true
	}
	return strings.Contains(strings.ToLower(ddl), strings.ToLower(pattern))
}

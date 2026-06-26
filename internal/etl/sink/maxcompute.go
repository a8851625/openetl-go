package sink

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSink("maxcompute", func(config map[string]any) (core.Sink, error) {
		return NewMaxComputeSink(config)
	})
	registry.RegisterSink("odps", func(config map[string]any) (core.Sink, error) {
		s, err := NewMaxComputeSink(config)
		if err == nil && s.name == "maxcompute" {
			s.name = "odps"
		}
		return s, err
	})
}

type MaxComputeSink struct {
	name            string
	endpoint        string
	project         string
	table           string
	accessKeyID     string
	accessKeySecret string
	columns         map[string]string
	partition       map[string]string
	partitionFields []string
	writeMode       string
	batchSize       int
	maxRetries      int
	retryBaseMs     int
	sinkCounters
}

func NewMaxComputeSink(config map[string]any) (*MaxComputeSink, error) {
	s := &MaxComputeSink{
		name:        "maxcompute",
		columns:     map[string]string{},
		partition:   map[string]string{},
		writeMode:   "append",
		batchSize:   500,
		maxRetries:  3,
		retryBaseMs: 500,
	}
	if v, ok := config["name"].(string); ok && strings.TrimSpace(v) != "" {
		s.name = strings.TrimSpace(v)
	}
	s.endpoint = stringConfig(config, "endpoint")
	s.project = stringConfig(config, "project")
	s.table = stringConfig(config, "table")
	s.accessKeyID = stringConfig(config, "access_key_id")
	s.accessKeySecret = stringConfig(config, "access_key_secret")
	if v := stringConfig(config, "write_mode"); v != "" {
		s.writeMode = v
	}
	s.batchSize = intConfig(config, "batch_size", s.batchSize)
	s.maxRetries = intConfig(config, "max_retries", s.maxRetries)
	s.retryBaseMs = intConfig(config, "retry_base_ms", s.retryBaseMs)
	s.columns = stringMapConfig(config, "columns")
	s.partition = stringMapConfig(config, "partition")
	s.partitionFields = stringSliceConfig(config, "partition_fields")
	if err := s.validateConfig(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *MaxComputeSink) Name() string { return s.name }

func (s *MaxComputeSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *MaxComputeSink) Open(ctx context.Context) error {
	return fmt.Errorf("maxcompute sink is experimental: batch writer is not enabled in this build; configure it only for descriptor, schema, and partition preflight until a MaxCompute SDK client is wired")
}

func (s *MaxComputeSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() {
		if err != nil {
			s.recordError()
		}
	}()
	return fmt.Errorf("maxcompute sink write is not implemented")
}

func (s *MaxComputeSink) Close() error { return nil }

func (s *MaxComputeSink) ValidateSchema(ctx context.Context, schema core.SchemaInfo) error {
	if len(schema.Columns) == 0 {
		return nil
	}
	if err := s.validatePartitionFields(schema); err != nil {
		return err
	}
	if len(s.columns) == 0 {
		return nil
	}
	target, err := maxComputeColumnInfos(s.columns)
	if err != nil {
		return err
	}
	return validateSchemaCompatibility(s.schemaWithoutDynamicPartitions(schema), target, schemaValidationOptions{
		targetName:     fmt.Sprintf("maxcompute %s.%s", s.project, s.table),
		allowMissing:   false,
		missingRemedy:  "add the target column to sink.config.columns or project the field out before the sink",
		allowTypeSync:  false,
		typeSyncRemedy: "add a type_convert/project transform before the MaxCompute sink or adjust sink.config.columns",
	})
}

func (s *MaxComputeSink) validateConfig() error {
	var missing []string
	for _, field := range []struct {
		name  string
		value string
	}{
		{"endpoint", s.endpoint},
		{"project", s.project},
		{"table", s.table},
		{"access_key_id", s.accessKeyID},
		{"access_key_secret", s.accessKeySecret},
	} {
		if strings.TrimSpace(field.value) == "" {
			missing = append(missing, field.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("maxcompute sink missing required config fields: %s", strings.Join(missing, ", "))
	}
	switch s.writeMode {
	case "append", "partition_overwrite":
	default:
		return fmt.Errorf("maxcompute sink write_mode %q is unsupported; use append or partition_overwrite", s.writeMode)
	}
	if s.batchSize <= 0 {
		return fmt.Errorf("maxcompute sink batch_size must be > 0")
	}
	if s.maxRetries < 0 {
		return fmt.Errorf("maxcompute sink max_retries must be >= 0")
	}
	if s.retryBaseMs <= 0 {
		return fmt.Errorf("maxcompute sink retry_base_ms must be > 0")
	}
	if len(s.partition) == 0 && len(s.partitionFields) == 0 {
		return fmt.Errorf("maxcompute sink requires partition or partition_fields for the first Kafka ODS partitioned-table path")
	}
	if err := validateMaxComputeColumns(s.columns); err != nil {
		return err
	}
	partitionKeys := map[string]bool{}
	for key := range s.partition {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return fmt.Errorf("maxcompute sink partition contains an empty key")
		}
		partitionKeys[strings.ToLower(trimmed)] = true
	}
	for _, field := range s.partitionFields {
		trimmed := strings.TrimSpace(field)
		if trimmed == "" {
			return fmt.Errorf("maxcompute sink partition_fields contains an empty field")
		}
		if partitionKeys[strings.ToLower(trimmed)] {
			return fmt.Errorf("maxcompute sink partition field %q is configured as both static partition and dynamic partition field", trimmed)
		}
	}
	return nil
}

func (s *MaxComputeSink) validatePartitionFields(schema core.SchemaInfo) error {
	if len(s.partitionFields) == 0 {
		return nil
	}
	columns := make(map[string]bool, len(schema.Columns))
	for _, col := range schema.Columns {
		columns[strings.ToLower(col.Name)] = true
	}
	var missing []string
	for _, field := range s.partitionFields {
		if !columns[strings.ToLower(field)] {
			missing = append(missing, field)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("schema validation failed for maxcompute %s.%s: partition field(s) [%s] missing from source schema; add project/constants before the sink or configure a static partition", s.project, s.table, strings.Join(missing, ", "))
}

func (s *MaxComputeSink) schemaWithoutDynamicPartitions(schema core.SchemaInfo) core.SchemaInfo {
	if len(s.partitionFields) == 0 {
		return schema
	}
	dynamicPartitions := make(map[string]bool, len(s.partitionFields))
	for _, field := range s.partitionFields {
		dynamicPartitions[strings.ToLower(field)] = true
	}
	filtered := core.SchemaInfo{Columns: make([]core.ColumnInfo, 0, len(schema.Columns))}
	for _, col := range schema.Columns {
		if dynamicPartitions[strings.ToLower(col.Name)] {
			continue
		}
		filtered.Columns = append(filtered.Columns, col)
	}
	return filtered
}

func maxComputeColumnInfos(columns map[string]string) ([]core.ColumnInfo, error) {
	if err := validateMaxComputeColumns(columns); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]core.ColumnInfo, 0, len(names))
	for _, name := range names {
		out = append(out, core.ColumnInfo{Name: name, DataType: columns[name]})
	}
	return out, nil
}

func validateMaxComputeColumns(columns map[string]string) error {
	for name, typ := range columns {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("maxcompute sink columns contains an empty column name")
		}
		if !isSupportedMaxComputeType(typ) {
			return fmt.Errorf("maxcompute sink column %q uses unsupported type %q; supported: STRING, BIGINT, DOUBLE, DECIMAL, BOOLEAN, DATETIME, TIMESTAMP", name, typ)
		}
	}
	return nil
}

func isSupportedMaxComputeType(typ string) bool {
	base := strings.ToUpper(strings.TrimSpace(typ))
	if base == "" {
		return false
	}
	if idx := strings.IndexAny(base, "( "); idx >= 0 {
		base = base[:idx]
	}
	switch base {
	case "STRING", "VARCHAR", "CHAR", "BIGINT", "INT", "INTEGER", "DOUBLE", "FLOAT", "DECIMAL", "BOOLEAN", "BOOL", "DATETIME", "TIMESTAMP":
		return true
	default:
		return false
	}
}

func stringConfig(config map[string]any, key string) string {
	if v, ok := config[key].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func intConfig(config map[string]any, key string, def int) int {
	v, ok := config[key]
	if !ok {
		return def
	}
	switch vv := v.(type) {
	case int:
		return vv
	case int64:
		return int(vv)
	case float64:
		return int(vv)
	default:
		return def
	}
}

func stringMapConfig(config map[string]any, key string) map[string]string {
	out := map[string]string{}
	v, ok := config[key]
	if !ok {
		return out
	}
	switch vv := v.(type) {
	case map[string]string:
		for k, val := range vv {
			out[strings.TrimSpace(k)] = strings.TrimSpace(val)
		}
	case map[string]any:
		for k, val := range vv {
			if s, ok := val.(string); ok {
				out[strings.TrimSpace(k)] = strings.TrimSpace(s)
			}
		}
	case map[any]any:
		for k, val := range vv {
			ks, ok := k.(string)
			if !ok {
				continue
			}
			vs, ok := val.(string)
			if !ok {
				continue
			}
			out[strings.TrimSpace(ks)] = strings.TrimSpace(vs)
		}
	}
	return out
}

func stringSliceConfig(config map[string]any, key string) []string {
	v, ok := config[key]
	if !ok {
		return nil
	}
	var out []string
	switch vv := v.(type) {
	case []string:
		out = append(out, vv...)
	case []any:
		for _, item := range vv {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
	}
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return out
}

var _ core.SchemaValidator = (*MaxComputeSink)(nil)
var _ core.SinkMetricsProvider = (*MaxComputeSink)(nil)

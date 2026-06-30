package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	odpssdk "github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
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
	name                string
	endpoint            string
	tunnelEndpoint      string
	project             string
	table               string
	accessKeyID         string
	accessKeySecret     string
	quotaName           string
	columns             map[string]string
	partition           map[string]string
	partitionFields     []string
	writeMode           string
	batchSize           int
	maxRetries          int
	retryBaseMs         int
	autoCreatePartition bool
	client              maxComputeClient
	clientFactory       func(*MaxComputeSink) (maxComputeClient, error)
	sinkCounters
}

func NewMaxComputeSink(config map[string]any) (*MaxComputeSink, error) {
	s := &MaxComputeSink{
		name:                "maxcompute",
		columns:             map[string]string{},
		partition:           map[string]string{},
		writeMode:           "append",
		batchSize:           500,
		maxRetries:          3,
		retryBaseMs:         500,
		autoCreatePartition: true,
	}
	s.clientFactory = newSDKMaxComputeClient
	if v, ok := config["name"].(string); ok && strings.TrimSpace(v) != "" {
		s.name = strings.TrimSpace(v)
	}
	s.endpoint = stringConfig(config, "endpoint")
	s.tunnelEndpoint = stringConfig(config, "tunnel_endpoint")
	s.project = stringConfig(config, "project")
	s.table = stringConfig(config, "table")
	s.accessKeyID = stringConfig(config, "access_key_id")
	s.accessKeySecret = stringConfig(config, "access_key_secret")
	s.quotaName = stringConfig(config, "quota_name")
	if v := stringConfig(config, "write_mode"); v != "" {
		s.writeMode = v
	}
	s.batchSize = intConfig(config, "batch_size", s.batchSize)
	s.maxRetries = intConfig(config, "max_retries", s.maxRetries)
	s.retryBaseMs = intConfig(config, "retry_base_ms", s.retryBaseMs)
	s.autoCreatePartition = boolConfig(config, "auto_create_partition", s.autoCreatePartition)
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
	if s.client == nil {
		client, err := s.clientFactory(s)
		if err != nil {
			return err
		}
		s.client = client
	}
	return classifyMaxComputeError(s.client.Open(ctx))
}

func (s *MaxComputeSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() {
		if err != nil {
			s.recordError()
		}
	}()
	if len(records) == 0 {
		return nil
	}
	if s.client == nil {
		if err := s.Open(ctx); err != nil {
			return err
		}
	}

	for start := 0; start < len(records); start += s.batchSize {
		end := start + s.batchSize
		if end > len(records) {
			end = len(records)
		}
		if err := s.writeChunk(ctx, records[start:end], start); err != nil {
			return err
		}
	}
	return nil
}

func (s *MaxComputeSink) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

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

func (s *MaxComputeSink) writeChunk(ctx context.Context, records []core.Record, baseIndex int) error {
	type partitionBatch struct {
		partition string
		rows      []map[string]any
		indices   []int
	}
	groups := map[string]*partitionBatch{}
	groupOrder := make([]string, 0)
	for i, rec := range records {
		partition, err := s.partitionSpec(rec.Data)
		if err != nil {
			return err
		}
		key := partition
		if _, ok := groups[key]; !ok {
			groups[key] = &partitionBatch{partition: partition}
			groupOrder = append(groupOrder, key)
		}
		groups[key].rows = append(groups[key].rows, s.rowData(rec.Data))
		groups[key].indices = append(groups[key].indices, baseIndex+i)
	}

	var partial *maxComputePartialBatchError
	successCount := 0
	var lastErr error
	for _, key := range groupOrder {
		group := groups[key]
		started := time.Now()
		err := s.writePartitionWithRetry(ctx, group.partition, group.rows)
		if err == nil {
			successCount += len(group.rows)
			s.recordMetrics(len(group.rows), time.Since(started))
			continue
		}
		err = classifyMaxComputeError(err)
		lastErr = err
		if partial == nil {
			partial = newMaxComputePartialBatchError()
		}
		for _, idx := range group.indices {
			partial.add(idx, err)
		}
	}
	if partial != nil {
		if successCount == 0 {
			return lastErr
		}
		return partial
	}
	return nil
}

func (s *MaxComputeSink) writePartitionWithRetry(ctx context.Context, partition string, rows []map[string]any) error {
	var last error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.client.Write(ctx, partition, rows)
		if err == nil {
			return nil
		}
		last = classifyMaxComputeError(err)
		if !core.IsRetryableError(last) || attempt == s.maxRetries {
			return last
		}
		delay := time.Duration(s.retryBaseMs) * time.Millisecond * time.Duration(1<<attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return last
}

func (s *MaxComputeSink) partitionSpec(row map[string]any) (string, error) {
	parts := make([]string, 0, len(s.partition)+len(s.partitionFields))
	staticKeys := make([]string, 0, len(s.partition))
	for key := range s.partition {
		staticKeys = append(staticKeys, key)
	}
	sort.Strings(staticKeys)
	for _, key := range staticKeys {
		parts = append(parts, fmt.Sprintf("%s='%s'", key, sanitizeMaxComputePartitionValue(s.partition[key])))
	}
	for _, field := range s.partitionFields {
		value, ok := row[field]
		if !ok || value == nil || strings.TrimSpace(fmt.Sprint(value)) == "" {
			return "", core.ClassifiedError{Class: core.ErrorClassData, Err: fmt.Errorf("maxcompute dynamic partition field %q is missing or empty", field)}
		}
		parts = append(parts, fmt.Sprintf("%s='%s'", field, sanitizeMaxComputePartitionValue(fmt.Sprint(value))))
	}
	return strings.Join(parts, ","), nil
}

func (s *MaxComputeSink) rowData(row map[string]any) map[string]any {
	if len(s.partitionFields) == 0 {
		return row
	}
	dynamicPartitions := make(map[string]bool, len(s.partitionFields))
	for _, field := range s.partitionFields {
		dynamicPartitions[strings.ToLower(field)] = true
	}
	out := make(map[string]any, len(row))
	for key, value := range row {
		if dynamicPartitions[strings.ToLower(key)] {
			continue
		}
		out[key] = value
	}
	return out
}

func sanitizeMaxComputePartitionValue(value string) string {
	value = strings.ReplaceAll(value, "'", "")
	value = strings.ReplaceAll(value, "\"", "")
	return strings.TrimSpace(value)
}

type maxComputeClient interface {
	Open(ctx context.Context) error
	Write(ctx context.Context, partition string, rows []map[string]any) error
	Close() error
}

type sdkMaxComputeClient struct {
	sink   *MaxComputeSink
	odps   *odpssdk.Odps
	tunnel *tunnel.Tunnel
	table  *odpssdk.Table
}

func newSDKMaxComputeClient(s *MaxComputeSink) (maxComputeClient, error) {
	odpsAccount := account.NewAliyunAccount(s.accessKeyID, s.accessKeySecret)
	odpsIns := odpssdk.NewOdps(odpsAccount, s.endpoint)
	odpsIns.SetDefaultProjectName(s.project)
	tunnelIns := tunnel.NewTunnel(odpsIns, s.tunnelEndpoint)
	tunnelIns.SetOdpsUserAgent("openetl-go")
	if s.quotaName != "" {
		tunnelIns.SetQuotaName(s.quotaName)
	}
	return &sdkMaxComputeClient{sink: s, odps: odpsIns, tunnel: tunnelIns}, nil
}

func (c *sdkMaxComputeClient) Open(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	table := c.odps.Table(c.sink.table)
	if err := table.Load(); err != nil {
		return fmt.Errorf("maxcompute preflight load table %s.%s: %w", c.sink.project, c.sink.table, err)
	}
	c.table = table
	if err := c.validateRemoteTable(); err != nil {
		return err
	}
	return ctx.Err()
}

func (c *sdkMaxComputeClient) Write(ctx context.Context, partition string, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	opts := []tunnel.Option{tunnel.SessionCfg.WithPartitionKey(partition)}
	if c.sink.autoCreatePartition {
		opts = append(opts, tunnel.SessionCfg.WithCreatePartition())
	}
	if c.sink.writeMode == "partition_overwrite" {
		opts = append(opts, tunnel.SessionCfg.Overwrite())
	}
	session, err := c.tunnel.CreateUploadSession(c.sink.project, c.sink.table, opts...)
	if err != nil {
		return fmt.Errorf("maxcompute create upload session table=%s.%s partition=%s: %w", c.sink.project, c.sink.table, partition, err)
	}
	writer, err := session.OpenRecordWriter(0)
	if err != nil {
		return fmt.Errorf("maxcompute open record writer table=%s.%s partition=%s: %w", c.sink.project, c.sink.table, partition, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
		}
	}()
	columns := session.Schema().Columns
	for i, row := range rows {
		record, err := maxComputeRecordFromRow(columns, row)
		if err != nil {
			return fmt.Errorf("maxcompute encode row %d table=%s.%s partition=%s: %w", i, c.sink.project, c.sink.table, partition, err)
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("maxcompute write row %d table=%s.%s partition=%s: %w", i, c.sink.project, c.sink.table, partition, err)
		}
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("maxcompute close record writer table=%s.%s partition=%s: %w", c.sink.project, c.sink.table, partition, err)
	}
	closed = true
	if err := session.Commit([]int{0}); err != nil {
		return fmt.Errorf("maxcompute commit upload table=%s.%s partition=%s: %w", c.sink.project, c.sink.table, partition, err)
	}
	return ctx.Err()
}

func (c *sdkMaxComputeClient) Close() error { return nil }

func (c *sdkMaxComputeClient) validateRemoteTable() error {
	schema := c.table.Schema()
	partitionColumns := map[string]bool{}
	for _, col := range schema.PartitionColumns {
		partitionColumns[strings.ToLower(col.Name)] = true
	}
	for key := range c.sink.partition {
		if !partitionColumns[strings.ToLower(key)] {
			return core.ClassifiedError{Class: core.ErrorClassSchema, Err: fmt.Errorf("maxcompute table %s.%s does not contain partition column %q", c.sink.project, c.sink.table, key)}
		}
	}
	for _, field := range c.sink.partitionFields {
		if !partitionColumns[strings.ToLower(field)] {
			return core.ClassifiedError{Class: core.ErrorClassSchema, Err: fmt.Errorf("maxcompute table %s.%s does not contain dynamic partition column %q", c.sink.project, c.sink.table, field)}
		}
	}
	if len(c.sink.partition) > 0 && len(c.sink.partitionFields) == 0 && !c.sink.autoCreatePartition {
		partition, err := c.sink.partitionSpec(nil)
		if err != nil {
			return err
		}
		remotePartition, err := c.table.GetPartition(partition)
		if err != nil {
			return fmt.Errorf("maxcompute preflight get partition %s.%s/%s: %w", c.sink.project, c.sink.table, partition, err)
		}
		if err := remotePartition.Load(); err != nil {
			return fmt.Errorf("maxcompute preflight load partition %s.%s/%s: %w", c.sink.project, c.sink.table, partition, err)
		}
	}
	if len(c.sink.columns) == 0 {
		return nil
	}
	remoteColumns := make(map[string]string, len(schema.Columns))
	for _, col := range schema.Columns {
		remoteColumns[strings.ToLower(col.Name)] = strings.ToUpper(col.Type.Name())
	}
	for name, want := range c.sink.columns {
		got, ok := remoteColumns[strings.ToLower(name)]
		if !ok {
			return core.ClassifiedError{Class: core.ErrorClassSchema, Err: fmt.Errorf("maxcompute table %s.%s missing configured column %q", c.sink.project, c.sink.table, name)}
		}
		if !compatibleMaxComputeRemoteType(want, got) {
			return core.ClassifiedError{Class: core.ErrorClassSchema, Err: fmt.Errorf("maxcompute table %s.%s column %q type mismatch: configured=%s remote=%s", c.sink.project, c.sink.table, name, want, got)}
		}
	}
	return nil
}

func compatibleMaxComputeRemoteType(configured, remote string) bool {
	cfgBase := maxComputeTypeBase(configured)
	remoteBase := maxComputeTypeBase(remote)
	switch cfgBase {
	case "BOOL":
		cfgBase = "BOOLEAN"
	case "INTEGER":
		cfgBase = "INT"
	}
	switch remoteBase {
	case "BOOL":
		remoteBase = "BOOLEAN"
	case "INTEGER":
		remoteBase = "INT"
	}
	return cfgBase == remoteBase
}

func maxComputeTypeBase(typ string) string {
	base := strings.ToUpper(strings.TrimSpace(typ))
	if idx := strings.IndexAny(base, "( "); idx >= 0 {
		base = base[:idx]
	}
	return base
}

func maxComputeRecordFromRow(columns []tableschema.Column, row map[string]any) (data.Record, error) {
	record := data.NewRecord(len(columns))
	for i, col := range columns {
		value, err := maxComputeDataValue(col.Type, row[col.Name])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		record[i] = value
	}
	return record, nil
}

func maxComputeDataValue(typ datatype.DataType, value any) (data.Data, error) {
	if value == nil {
		return data.Null, nil
	}
	switch typ.ID() {
	case datatype.STRING, datatype.CHAR, datatype.VARCHAR:
		return data.String(fmt.Sprint(value)), nil
	case datatype.BIGINT:
		v, err := maxComputeInt64(value)
		return data.BigInt(v), err
	case datatype.INT:
		v, err := maxComputeInt64(value)
		if err != nil {
			return nil, err
		}
		if v < math.MinInt32 || v > math.MaxInt32 {
			return nil, fmt.Errorf("value %d out of INT range", v)
		}
		return data.Int(v), nil
	case datatype.SMALLINT:
		v, err := maxComputeInt64(value)
		if err != nil {
			return nil, err
		}
		if v < math.MinInt16 || v > math.MaxInt16 {
			return nil, fmt.Errorf("value %d out of SMALLINT range", v)
		}
		return data.SmallInt(v), nil
	case datatype.TINYINT:
		v, err := maxComputeInt64(value)
		if err != nil {
			return nil, err
		}
		if v < math.MinInt8 || v > math.MaxInt8 {
			return nil, fmt.Errorf("value %d out of TINYINT range", v)
		}
		return data.TinyInt(v), nil
	case datatype.DOUBLE:
		v, err := maxComputeFloat64(value)
		return data.Double(v), err
	case datatype.FLOAT:
		v, err := maxComputeFloat64(value)
		return data.Float(v), err
	case datatype.BOOLEAN:
		v, err := maxComputeBool(value)
		return data.Bool(v), err
	case datatype.DECIMAL:
		decimalValue, err := data.DecimalFromStr(fmt.Sprint(value))
		if err != nil {
			return nil, err
		}
		return decimalValue, nil
	case datatype.DATETIME:
		t, err := maxComputeTime(value)
		return data.DateTime(t), err
	case datatype.TIMESTAMP:
		t, err := maxComputeTime(value)
		return data.Timestamp(t), err
	case datatype.DATE:
		t, err := maxComputeDate(value)
		return data.Date(t), err
	case datatype.JSON:
		return data.NewJson(value)
	case datatype.BINARY:
		switch v := value.(type) {
		case []byte:
			return data.Binary(v), nil
		case string:
			return data.Binary([]byte(v)), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to BINARY", value)
		}
	default:
		return nil, fmt.Errorf("unsupported MaxCompute target type %s", typ.Name())
	}
}

func maxComputeInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return 0, fmt.Errorf("value %d out of BIGINT range", v)
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d out of BIGINT range", v)
		}
		return int64(v), nil
	case float32:
		return maxComputeFloatToInt(float64(v))
	case float64:
		return maxComputeFloatToInt(v)
	case json.Number:
		return v.Int64()
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to integer", value)
	}
}

func maxComputeFloatToInt(v float64) (int64, error) {
	if math.Trunc(v) != v {
		return 0, fmt.Errorf("value %v is not an integer", v)
	}
	if v < math.MinInt64 || v > math.MaxInt64 {
		return 0, fmt.Errorf("value %v out of BIGINT range", v)
	}
	return int64(v), nil
}

func maxComputeFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", value)
	}
}

func maxComputeBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(strings.TrimSpace(v))
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", value)
	}
}

func maxComputeTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.ParseInLocation(layout, strings.TrimSpace(v), time.Local); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse time value %q", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time", value)
	}
}

func maxComputeDate(value any) (time.Time, error) {
	t, err := maxComputeTime(value)
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
}

type maxComputePartialBatchError struct {
	failed map[int]error
}

func newMaxComputePartialBatchError() *maxComputePartialBatchError {
	return &maxComputePartialBatchError{failed: map[int]error{}}
}

func (e *maxComputePartialBatchError) add(index int, err error) {
	e.failed[index] = err
}

func (e *maxComputePartialBatchError) Error() string {
	return fmt.Sprintf("maxcompute partial batch failed for %d record(s)", len(e.failed))
}

func (e *maxComputePartialBatchError) Unwrap() error {
	return core.ClassifiedError{Class: core.ErrorClassData, Err: errors.New("maxcompute partial batch failure after sink-local retries")}
}

func (e *maxComputePartialBatchError) FailedRecordIndices() []int {
	indices := make([]int, 0, len(e.failed))
	for idx := range e.failed {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	return indices
}

func (e *maxComputePartialBatchError) ErrorForRecord(index int) error {
	return e.failed[index]
}

func classifyMaxComputeError(err error) error {
	if err == nil {
		return nil
	}
	var classified core.ClassifiedError
	if errors.As(err, &classified) {
		return err
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return core.ClassifiedError{Class: core.ErrorClassTransient, Err: err}
	}
	var httpErr restclient.HttpError
	if errors.As(err, &httpErr) {
		class := core.ErrorClassUnknown
		switch {
		case httpErr.StatusCode == 401 || httpErr.StatusCode == 403:
			class = core.ErrorClassAuth
		case httpErr.StatusCode == 404:
			class = core.ErrorClassSchema
		case httpErr.StatusCode == 408 || httpErr.StatusCode == 429 || httpErr.StatusCode >= 500:
			class = core.ErrorClassTransient
		case httpErr.StatusCode >= 400:
			class = classifyMaxComputeMessage(err.Error())
			if class == core.ErrorClassUnknown {
				class = core.ErrorClassData
			}
		}
		return core.ClassifiedError{Class: class, Err: err}
	}
	class := classifyMaxComputeMessage(err.Error())
	if class == core.ErrorClassUnknown {
		class = core.ClassifyError(err)
	}
	return core.ClassifiedError{Class: class, Err: err}
}

func classifyMaxComputeMessage(message string) core.ErrorClass {
	msg := strings.ToLower(message)
	switch {
	case strings.Contains(msg, "accessdenied") || strings.Contains(msg, "access denied") || strings.Contains(msg, "forbidden") || strings.Contains(msg, "unauthorized") || strings.Contains(msg, "permission"):
		return core.ErrorClassAuth
	case strings.Contains(msg, "nosuch") || strings.Contains(msg, "not found") || strings.Contains(msg, "partition") || strings.Contains(msg, "schema") || strings.Contains(msg, "column") || strings.Contains(msg, "table"):
		return core.ErrorClassSchema
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "temporar") || strings.Contains(msg, "throttl") || strings.Contains(msg, "too many") || strings.Contains(msg, "service unavailable"):
		return core.ErrorClassTransient
	case strings.Contains(msg, "parse") || strings.Contains(msg, "invalid data") || strings.Contains(msg, "type mismatch") || strings.Contains(msg, "out of range") || strings.Contains(msg, "decimal"):
		return core.ErrorClassData
	default:
		return core.ErrorClassUnknown
	}
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

func boolConfig(config map[string]any, key string, def bool) bool {
	v, ok := config[key]
	if !ok {
		return def
	}
	switch vv := v.(type) {
	case bool:
		return vv
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(vv))
		if err == nil {
			return parsed
		}
	}
	return def
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

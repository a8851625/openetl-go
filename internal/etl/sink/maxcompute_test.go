package sink

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
)

func TestNewMaxComputeSinkValidatesRequiredConfig(t *testing.T) {
	_, err := NewMaxComputeSink(map[string]any{
		"endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
		"project":  "warehouse",
		"table":    "ods_events",
	})
	if err == nil {
		t.Fatal("NewMaxComputeSink() = nil error, want missing credential error")
	}
	if !strings.Contains(err.Error(), "access_key_id") || !strings.Contains(err.Error(), "access_key_secret") {
		t.Fatalf("error = %v, want missing access key fields", err)
	}
}

func TestMaxComputeSinkParsesPartitionAndValidatesSchema(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"columns": map[string]any{
			"id":         "BIGINT",
			"amount":     "DOUBLE",
			"event_time": "TIMESTAMP",
			"payload":    "STRING",
		},
		"partition":        map[string]any{},
		"partition_fields": []any{"dt"},
	}))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	schema := core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "bigint"},
		{Name: "amount", DataType: "decimal(10,2)"},
		{Name: "event_time", DataType: "datetime"},
		{Name: "payload", DataType: "json"},
		{Name: "dt", DataType: "varchar(10)"},
	}}
	if err := s.ValidateSchema(context.Background(), schema); err != nil {
		t.Fatalf("ValidateSchema() = %v", err)
	}
}

func TestMaxComputeSinkRejectsMissingDynamicPartitionField(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"columns":          map[string]string{"id": "BIGINT"},
		"partition":        map[string]any{},
		"partition_fields": []string{"dt"},
	}))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	err = s.ValidateSchema(context.Background(), core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "bigint"},
	}})
	if err == nil {
		t.Fatal("ValidateSchema() = nil, want partition field error")
	}
	if !strings.Contains(err.Error(), "partition field(s) [dt] missing") {
		t.Fatalf("error = %v, want missing dt partition field", err)
	}
}

func TestMaxComputeSinkRejectsUnsupportedColumnType(t *testing.T) {
	_, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"columns": map[string]string{"payload": "ARRAY<STRING>"},
	}))
	if err == nil {
		t.Fatal("NewMaxComputeSink() = nil, want unsupported type error")
	}
	if !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("error = %v, want unsupported type", err)
	}
}

func TestMaxComputeSinkOpenUsesSDKClientFactory(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(nil))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	fake := &fakeMaxComputeClient{}
	s.clientFactory = func(*MaxComputeSink) (maxComputeClient, error) {
		return fake, nil
	}

	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open() = %v", err)
	}
	if !fake.opened {
		t.Fatal("fake client was not opened")
	}
}

func TestMaxComputeSinkWriteGroupsByPartitionAndChunks(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"partition":             map[string]any{},
		"partition_fields":      []string{"dt"},
		"batch_size":            2,
		"max_retries":           0,
		"retry_base_ms":         1,
		"auto_create_partition": false,
	}))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	fake := &fakeMaxComputeClient{}
	s.client = fake

	records := []core.Record{
		{Data: map[string]any{"id": 1, "payload": "a", "dt": "2026-06-26"}},
		{Data: map[string]any{"id": 2, "payload": "b", "dt": "2026-06-27"}},
		{Data: map[string]any{"id": 3, "payload": "c", "dt": "2026-06-26"}},
	}
	if err := s.Write(context.Background(), records); err != nil {
		t.Fatalf("Write() = %v", err)
	}
	if len(fake.writes) != 3 {
		t.Fatalf("writes = %#v, want 3 partition batches", fake.writes)
	}
	if fake.writes[0].partition != "dt='2026-06-26'" || fake.writes[1].partition != "dt='2026-06-27'" || fake.writes[2].partition != "dt='2026-06-26'" {
		t.Fatalf("write partitions = %#v, want chunk-local partition ordering", fake.writes)
	}
	for _, call := range fake.writes {
		for _, row := range call.rows {
			if _, ok := row["dt"]; ok {
				t.Fatalf("row = %#v, dynamic partition field should be removed", row)
			}
		}
	}
	metrics := s.SinkMetrics()
	if metrics.RowsWritten != 3 || metrics.BatchesSent != 3 {
		t.Fatalf("metrics = %#v, want rows=3 batches=3", metrics)
	}
}

func TestMaxComputeSinkPartialBatchErrorKeepsSuccessfulPartitionsAccepted(t *testing.T) {
	s, err := NewMaxComputeSink(validMaxComputeConfig(map[string]any{
		"partition":        map[string]any{},
		"partition_fields": []string{"dt"},
		"max_retries":      0,
		"retry_base_ms":    1,
	}))
	if err != nil {
		t.Fatalf("NewMaxComputeSink() = %v", err)
	}
	fake := &fakeMaxComputeClient{failPartition: "dt='2026-06-27'", failErr: errors.New("i/o timeout")}
	s.client = fake

	err = s.Write(context.Background(), []core.Record{
		{Data: map[string]any{"id": 1, "dt": "2026-06-26"}},
		{Data: map[string]any{"id": 2, "dt": "2026-06-27"}},
	})
	if err == nil {
		t.Fatal("Write() = nil, want partial batch error")
	}
	var partial core.PartialBatchError
	if !errors.As(err, &partial) {
		t.Fatalf("Write() error = %T %v, want PartialBatchError", err, err)
	}
	indices := partial.FailedRecordIndices()
	if len(indices) != 1 || indices[0] != 1 {
		t.Fatalf("FailedRecordIndices() = %#v, want [1]", indices)
	}
	if got := core.ClassifyError(err); got != core.ErrorClassData {
		t.Fatalf("ClassifyError(partial) = %s, want non-retryable data wrapper", got)
	}
	if got := core.ClassifyError(partial.ErrorForRecord(1)); got != core.ErrorClassTransient {
		t.Fatalf("ClassifyError(record) = %s, want transient record error", got)
	}
}

func TestMaxComputeRecordFromRowConvertsPrimitiveTypes(t *testing.T) {
	record, err := maxComputeRecordFromRow([]tableschema.Column{
		{Name: "id", Type: datatype.BigIntType},
		{Name: "amount", Type: datatype.DoubleType},
		{Name: "ok", Type: datatype.BooleanType},
		{Name: "payload", Type: datatype.StringType},
		{Name: "event_time", Type: datatype.TimestampType},
	}, map[string]any{
		"id":         json.Number("42"),
		"amount":     "12.5",
		"ok":         "true",
		"payload":    "hello",
		"event_time": "2026-06-26T10:11:12Z",
	})
	if err != nil {
		t.Fatalf("maxComputeRecordFromRow() = %v", err)
	}
	if record.Len() != 5 {
		t.Fatalf("record.Len() = %d, want 5", record.Len())
	}
}

func TestODPSSinkAliasIsRegistered(t *testing.T) {
	built, err := registry.BuildSink("odps", validMaxComputeConfig(nil))
	if err != nil {
		t.Fatalf("BuildSink(odps) = %v", err)
	}
	if got := built.Name(); got != "odps" {
		t.Fatalf("built.Name() = %q, want odps", got)
	}
}

func validMaxComputeConfig(extra map[string]any) map[string]any {
	cfg := map[string]any{
		"endpoint":          "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
		"project":           "warehouse",
		"table":             "ods_events",
		"access_key_id":     "ak",
		"access_key_secret": "secret",
		"partition":         map[string]any{"dt": "2026-06-26"},
	}
	for k, v := range extra {
		cfg[k] = v
	}
	return cfg
}

type fakeMaxComputeClient struct {
	opened        bool
	openErr       error
	failPartition string
	failErr       error
	writes        []fakeMaxComputeWrite
}

type fakeMaxComputeWrite struct {
	partition string
	rows      []map[string]any
}

func (f *fakeMaxComputeClient) Open(context.Context) error {
	f.opened = true
	return f.openErr
}

func (f *fakeMaxComputeClient) Write(_ context.Context, partition string, rows []map[string]any) error {
	if partition == f.failPartition {
		return f.failErr
	}
	copied := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := make(map[string]any, len(row))
		for k, v := range row {
			item[k] = v
		}
		copied = append(copied, item)
	}
	f.writes = append(f.writes, fakeMaxComputeWrite{partition: partition, rows: copied})
	return nil
}

func (f *fakeMaxComputeClient) Close() error { return nil }

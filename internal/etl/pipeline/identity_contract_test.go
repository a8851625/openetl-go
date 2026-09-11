package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

type identityCaptureSink struct {
	mu      sync.Mutex
	records []core.Record
}

func (s *identityCaptureSink) Name() string               { return "identity-capture" }
func (s *identityCaptureSink) Open(context.Context) error { return nil }
func (s *identityCaptureSink) Close() error               { return nil }
func (s *identityCaptureSink) Write(_ context.Context, records []core.Record) error {
	s.mu.Lock()
	s.records = append(s.records, records...)
	s.mu.Unlock()
	return nil
}

func metadataIdentityRecord(offset int64, key string, data map[string]any) core.Record {
	return core.Record{
		Operation: core.OpInsert,
		Data:      data,
		Metadata: core.Metadata{
			SourceType: core.SourceTypeKafka, Offset: offset,
			Key: key, PrimaryKeyColumns: []string{"tenant_id", "id"},
			FormatContractID: core.FormatContractCanalJSONV1,
		},
	}
}

func newMetadataIdentityRunner(t *testing.T, store core.CheckpointStore, dlq DLQWriter) (*Runner, *identityCaptureSink) {
	t.Helper()
	r := newCheckpointWriteBatchRunner(t, nil, store, dlq)
	r.spec.Source = SourceSpec{Type: "kafka", Config: map[string]any{"format": "canal_json"}}
	r.spec.Sink = SinkSpec{Type: "clickhouse", Config: map[string]any{"pk_columns_from_metadata": true}}
	r.retryConfig.MaxAttempts = 1
	sink := &identityCaptureSink{}
	r.sink = sink
	return r, sink
}

func TestMetadataIdentityGateRoutesIncompleteRecordToDLQBeforeSinkAndCheckpoints(t *testing.T) {
	store := newMemoryCPStore()
	dlq := &captureDLQ{}
	r, sink := newMetadataIdentityRunner(t, store, dlq)
	record := metadataIdentityRecord(7, `{"tenant_id":"t1"}`, map[string]any{"tenant_id": "t1"})

	r.writeBatch(context.Background(), []core.Record{record})

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.records) != 0 {
		t.Fatalf("sink received %d identity-invalid records", len(sink.records))
	}
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.entries) != 1 || dlq.entries[0].ErrorClass != string(core.ErrorClassData) || !strings.Contains(dlq.entries[0].Error, string(core.RecordIdentityReasonKeyColumnMissing)) {
		t.Fatalf("DLQ entries = %#v, want one classified identity failure", dlq.entries)
	}
	if !checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint did not advance after identity failure was durably stored in DLQ")
	}
}

func TestMetadataIdentityGateDLQFailureBlocksCheckpoint(t *testing.T) {
	store := newMemoryCPStore()
	r, sink := newMetadataIdentityRunner(t, store, failingDLQ{err: errors.New("identity DLQ unavailable")})
	record := metadataIdentityRecord(8, `{"tenant_id":"t1"}`, map[string]any{"tenant_id": "t1"})

	r.writeBatch(context.Background(), []core.Record{record})

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.records) != 0 {
		t.Fatalf("sink received %d identity-invalid records", len(sink.records))
	}
	if checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint advanced after identity DLQ persistence failed")
	}
	if !r.checkpointBlocked {
		t.Fatal("checkpoint boundary was not blocked after identity DLQ failure")
	}
}

func TestSourceRecordRejectionUsesSameDLQCheckpointBoundary(t *testing.T) {
	store := newMemoryCPStore()
	dlq := &captureDLQ{}
	r, sink := newMetadataIdentityRunner(t, store, dlq)
	record := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"value": "not-json"},
		Metadata:  core.Metadata{SourceType: core.SourceTypeKafka, Partition: 0, Offset: 11},
		Rejection: &core.RecordRejection{Code: "kafka_message_parse_failed", Message: "invalid canal payload", Class: core.ErrorClassData},
	}

	r.writeBatch(context.Background(), []core.Record{record})

	sink.mu.Lock()
	if len(sink.records) != 0 {
		t.Fatalf("sink received source-rejected record: %#v", sink.records)
	}
	sink.mu.Unlock()
	dlq.mu.Lock()
	if len(dlq.entries) != 1 || !strings.Contains(dlq.entries[0].Error, "kafka_message_parse_failed") {
		t.Fatalf("DLQ entries = %#v", dlq.entries)
	}
	dlq.mu.Unlock()
	if !checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint did not advance after source rejection was durable in DLQ")
	}
}

func TestMetadataIdentityGateWritesOnlyCompleteSurvivors(t *testing.T) {
	store := newMemoryCPStore()
	dlq := &captureDLQ{}
	r, sink := newMetadataIdentityRunner(t, store, dlq)
	valid := metadataIdentityRecord(9, `{"id":"a","tenant_id":"t1"}`, map[string]any{"tenant_id": "t1", "id": "a"})
	invalid := metadataIdentityRecord(10, `{"tenant_id":"t1"}`, map[string]any{"tenant_id": "t1"})

	r.writeBatch(context.Background(), []core.Record{valid, invalid})

	sink.mu.Lock()
	if len(sink.records) != 1 || sink.records[0].Metadata.Offset != 9 {
		t.Fatalf("sink records = %#v, want only complete record", sink.records)
	}
	sink.mu.Unlock()
	if !checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint did not cover sink acknowledgement plus durable identity DLQ")
	}
}

func TestMetadataIdentityCompatibility(t *testing.T) {
	base := func(sourceType string, sourceConfig map[string]any) *Spec {
		return &Spec{
			Source: SourceSpec{Type: sourceType, Config: sourceConfig},
			Sink:   SinkSpec{Type: "postgres", Config: map[string]any{"pk_columns_from_metadata": true}},
		}
	}
	for _, spec := range []*Spec{
		base("mysql_cdc", nil),
		base("mysql_snapshot_cdc", nil),
		base("postgres_cdc", nil),
		base("mysql_batch", nil),
		base("kafka", map[string]any{"format": "canal_json"}),
		base("kafka", map[string]any{"format": "envelope"}),
	} {
		if issue := CheckMetadataIdentityCompatibility(spec); issue != nil {
			t.Fatalf("compatible spec %s/%v rejected: %+v", spec.Source.Type, spec.Source.Config, issue)
		}
	}
	if issue := CheckMetadataIdentityCompatibility(base("file", nil)); issue == nil || issue.Field != "source.type" {
		t.Fatalf("file issue = %+v, want source.type", issue)
	}
	if issue := CheckMetadataIdentityCompatibility(base("kafka", map[string]any{"format": "json"})); issue == nil || issue.Field != "source.config.format" {
		t.Fatalf("Kafka JSON issue = %+v, want source.config.format", issue)
	}
}

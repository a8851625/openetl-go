package sink

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestMySQLMetadataPKKeyChangeDeletesOldKeyInSameTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	s := &MySQLSink{
		name: "mysql", database: "target", db: db, batchMode: "upsert", insertChunkSize: 500,
		pkColumnsFromMetadata: true, schemaCache: core.NewSchemaCache(), tableMetrics: newTableMetricsSet(),
	}
	s.schemaCache.SetGeneratedColumns("target.orders", map[string]bool{})
	record := core.Record{
		Operation: core.OpUpdate,
		Before:    map[string]any{"tenant_id": "acme", "id": int64(1), "value": "old"},
		Data:      map[string]any{"tenant_id": "acme", "id": int64(2), "value": "new"},
		Metadata: core.Metadata{
			Table: "orders", Key: `{"tenant_id":"acme","id":1}`,
			PrimaryKeyColumns: []string{"tenant_id", "id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1,
		},
	}

	mock.ExpectBegin()
	insert := s.buildBatchInsertStatement("orders", []string{"id", "tenant_id", "value"}, 1, "upsert")
	mock.ExpectExec(regexp.QuoteMeta(insert)).
		WithArgs(int64(2), "acme", "new").
		WillReturnResult(sqlmock.NewResult(1, 1))
	deleteSQL := s.buildBatchDeleteStatement("orders", []string{"id", "tenant_id"}, 1)
	mock.ExpectExec(regexp.QuoteMeta(deleteSQL)).
		WithArgs(int64(1), "acme").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := s.Write(context.Background(), []core.Record{record}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL expectations: %v", err)
	}
	metrics := s.SinkMetrics()
	if metrics.RowsWritten != 2 || metrics.BatchesSent == 0 || metrics.Errors != 0 {
		t.Fatalf("metrics=%+v, want live write plus old-key delete acknowledged", metrics)
	}
}

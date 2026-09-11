package sink

import (
	"context"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/core/contracttest"
)

type metadataPKConformanceSink struct {
	name    string
	columns func([]core.Record) (map[string][]string, error)
}

func metadataPKConformanceSinks(staticPK []string) []metadataPKConformanceSink {
	clickhouse := &ClickHouseSink{
		name: "clickhouse", database: "target", pkColumnsFromMetadata: true,
		pkColumns: append([]string(nil), staticPK...), tableMetricsImpl: newTableMetricsSet(),
	}
	mysql := &MySQLSink{
		name: "mysql", database: "target", pkColumnsFromMetadata: true,
		pkColumns: append([]string(nil), staticPK...), tableMetrics: newTableMetricsSet(),
	}
	postgres := &PostgresSink{
		name: "postgres", database: "target", pkColumnsFromMetadata: true,
		pkColumns: append([]string(nil), staticPK...), tableMetrics: newTableMetricsSet(),
	}
	doris := &DorisSink{
		name: "doris", database: "target", pkColumnsFromMetadata: true,
		pkColumns: append([]string(nil), staticPK...), tableMetrics: newTableMetricsSet(),
	}
	return []metadataPKConformanceSink{
		{name: "clickhouse", columns: clickhouse.pkColumnsByTable},
		{name: "mysql", columns: mysql.pkColumnsByTable},
		{name: "postgres", columns: postgres.pkColumnsByTable},
		{name: "doris", columns: doris.pkColumnsByTable},
	}
}

func TestMetadataPKDeclaredSinksShareFailClosedConformance(t *testing.T) {
	for _, fixture := range contracttest.MetadataPKSinkFixtures() {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			for _, candidate := range metadataPKConformanceSinks(fixture.LegacySafetyColumns) {
				candidate := candidate
				t.Run(candidate.name, func(t *testing.T) {
					columns, err := candidate.columns(fixture.Records)
					if !fixture.Accepted {
						if err == nil {
							t.Fatalf("accepted invalid fixture; columns=%v", columns)
						}
						if class := core.ClassifyError(err); class != core.ErrorClassData {
							t.Fatalf("error class=%s, want data: %v", class, err)
						}
						if !strings.Contains(err.Error(), fixture.Reason) {
							t.Fatalf("error=%q, want reason %q", err, fixture.Reason)
						}
						return
					}
					if err != nil {
						t.Fatalf("rejected valid fixture: %v", err)
					}
					for table, expected := range fixture.ExpectedColumns {
						if got := columns[table]; !sameIdentifierSet(got, expected) {
							t.Fatalf("table %s columns=%v, want %v", table, got, expected)
						}
					}
				})
			}
		})
	}
}

func TestMetadataPKKeyChangingUpdateExpansionUsesValidatedOldKey(t *testing.T) {
	fixture := contracttest.MetadataPKSinkFixtures()[2]
	validation, err := validateMetadataPKBatch("test", fixture.Records, fixture.LegacySafetyColumns, func(record core.Record) (metadataPKTarget, error) {
		return metadataPKTarget{Database: "target", Table: record.Metadata.Table}, nil
	})
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	expanded, generated := expandMetadataPKKeyChanges(fixture.Records, validation)
	if generated != 1 || len(expanded) != 2 {
		t.Fatalf("generated=%d len=%d, want one delete + live update", generated, len(expanded))
	}
	old, live := expanded[0], expanded[1]
	if old.Operation != core.OpDelete || old.Data["tenant_id"] != "acme" || old.Data["id"] != int64(10) {
		t.Fatalf("old-key tombstone=%#v", old)
	}
	if live.Operation != core.OpUpdate || live.Data["id"] != int64(11) {
		t.Fatalf("live update=%#v", live)
	}
}

func TestClickHouseMetadataPKKeyChangeProducesSameVersionTombstone(t *testing.T) {
	record := contracttest.MetadataPKSinkFixtures()[2].Records[0]
	record.Metadata.SourceType = core.SourceTypeKafka
	record.Metadata.Partition = 0
	record.Metadata.Offset = 41
	sink := &ClickHouseSink{
		name: "clickhouse", database: "target", pkColumnsFromMetadata: true,
		tableMetricsImpl: newTableMetricsSet(),
	}
	prepared, err := sink.prepareDataRecords([]core.Record{record})
	if err != nil {
		t.Fatalf("prepareDataRecords: %v", err)
	}
	if len(prepared) != 2 || prepared[0].Operation != core.OpUpdate || prepared[1].Operation != core.OpDelete {
		t.Fatalf("prepared=%#v, want live update + old-key tombstone", prepared)
	}
	if prepared[0].Data["_version"] != prepared[1].Data["_version"] || prepared[1].Data["id"] != int64(10) {
		t.Fatalf("version/old key mismatch: live=%#v tombstone=%#v", prepared[0].Data, prepared[1].Data)
	}
}

type metadataPKObservableSink struct {
	name       string
	write      func(context.Context, []core.Record) error
	metrics    func() core.SinkMetrics
	tableStats func() []TableWriteStats
}

func metadataPKObservableSinks() []metadataPKObservableSink {
	clickhouse := &ClickHouseSink{name: "clickhouse", database: "target", pkColumnsFromMetadata: true, tableMetricsImpl: newTableMetricsSet()}
	mysql := &MySQLSink{name: "mysql", database: "target", pkColumnsFromMetadata: true, tableMetrics: newTableMetricsSet()}
	postgres := &PostgresSink{name: "postgres", database: "target", pkColumnsFromMetadata: true, tableMetrics: newTableMetricsSet()}
	doris := &DorisSink{name: "doris", database: "target", pkColumnsFromMetadata: true, tableMetrics: newTableMetricsSet()}
	return []metadataPKObservableSink{
		{name: "clickhouse", write: clickhouse.Write, metrics: clickhouse.SinkMetrics, tableStats: clickhouse.TableWriteStats},
		{name: "mysql", write: mysql.Write, metrics: mysql.SinkMetrics, tableStats: mysql.TableWriteStats},
		{name: "postgres", write: postgres.Write, metrics: postgres.SinkMetrics, tableStats: postgres.TableWriteStats},
		{name: "doris", write: doris.Write, metrics: doris.SinkMetrics, tableStats: doris.TableWriteStats},
	}
}

func TestMetadataPKIdentityRejectionIsDataErrorAndNotSinkAck(t *testing.T) {
	record := contracttest.MetadataPKSinkFixtures()[6].Records[0]
	for _, candidate := range metadataPKObservableSinks() {
		t.Run(candidate.name, func(t *testing.T) {
			err := candidate.write(context.Background(), []core.Record{record})
			if err == nil || core.ClassifyError(err) != core.ErrorClassData {
				t.Fatalf("Write error=%v class=%s, want classified data rejection", err, core.ClassifyError(err))
			}
			metrics := candidate.metrics()
			if metrics.RowsWritten != 0 || metrics.BatchesSent != 0 || metrics.Errors != 1 {
				t.Fatalf("sink metrics=%+v, rejection must not count an acknowledged row/batch", metrics)
			}
			stats := candidate.tableStats()
			if len(stats) != 1 || stats[0].Table != "orders" || stats[0].RowsWritten != 0 || stats[0].Errors != 1 {
				t.Fatalf("per-target stats=%+v, want one zero-row identity error", stats)
			}
		})
	}
}

func TestMetadataPKAutoCreatePrefersDeclaredColumnTypes(t *testing.T) {
	columns := []string{"id", "payload"}
	values := map[string]any{
		"id":               "not-a-number",
		"payload":          "sample",
		"__column_types__": map[string]string{"id": "bigint", "payload": "varchar(64)"},
	}

	mysql := &MySQLSink{}
	mysqlDDL, err := mysql.buildCreateTableDDLWithPK("orders", append([]string(nil), columns...), values, []string{"id"})
	if err != nil || !strings.Contains(mysqlDDL, "`id` BIGINT NOT NULL") || !strings.Contains(mysqlDDL, "PRIMARY KEY (`id`)") {
		t.Fatalf("mysql DDL did not prefer declared type/key: err=%v ddl=%s", err, mysqlDDL)
	}

	postgres := &PostgresSink{schema: "public"}
	postgresDDL := buildPgCreateTableDDL("public", "orders", columns, values, []string{"id"}, postgres.resolveColumnDDL)
	if !strings.Contains(postgresDDL, `"id" BIGINT`) || !strings.Contains(postgresDDL, `PRIMARY KEY ("id")`) {
		t.Fatalf("postgres DDL did not prefer declared type/key: %s", postgresDDL)
	}

	doris := &DorisSink{}
	dorisDDL, err := doris.buildCreateTableDDLWithPK("orders", append([]string(nil), columns...), values, []string{"id"})
	if err != nil || !strings.Contains(dorisDDL, "`id` BIGINT NOT NULL") || !strings.Contains(dorisDDL, "UNIQUE KEY(`id`)") {
		t.Fatalf("doris DDL did not prefer declared type/key: err=%v ddl=%s", err, dorisDDL)
	}

	if got := inferClickHouseType("id", "not-a-number", "bigint"); got != "Int64" {
		t.Fatalf("clickhouse declared type=%q, want Int64", got)
	}
}

func TestDorisSchemaInputsCarryMetadataColumnTypes(t *testing.T) {
	doris := &DorisSink{table: "orders"}
	_, values, err := doris.collectSchemaInputs([]core.Record{{
		Data:     map[string]any{"id": "not-a-number"},
		Metadata: core.Metadata{ColumnTypes: map[string]string{"id": "bigint"}},
	}})
	if err != nil {
		t.Fatalf("collectSchemaInputs: %v", err)
	}
	declared, ok := values["orders"]["__column_types__"].(map[string]string)
	if !ok || declared["id"] != "bigint" {
		t.Fatalf("declared types=%#v, want id=bigint", values["orders"]["__column_types__"])
	}
}

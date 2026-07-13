package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestDebeziumCDCParsesSnapshotAndAppliesTableTemplate(t *testing.T) {
	tr, err := NewDebeziumCDCTransform(map[string]any{
		"table_mapping": map[string]any{
			"template": "ods_{source_db}__{source_table}_{YYYYMMDD}",
		},
	})
	if err != nil {
		t.Fatalf("NewDebeziumCDCTransform: %v", err)
	}

	rec, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{
			"payload": map[string]any{
				"op":       "r",
				"ts_ms":    float64(1710000000123),
				"snapshot": "true",
				"source": map[string]any{
					"db":    "dl_vls_dev",
					"table": "vehicle_charge",
				},
				"after": map[string]any{"id": float64(1), "soc": float64(88)},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Operation != core.OpInsert {
		t.Fatalf("Operation = %s, want INSERT", rec.Operation)
	}
	if rec.Metadata.Database != "dl_vls_dev" {
		t.Fatalf("Metadata.Database = %q, want dl_vls_dev", rec.Metadata.Database)
	}
	if rec.Metadata.Table != "ods_dl_vls_dev__vehicle_charge_20240309" {
		t.Fatalf("Metadata.Table = %q", rec.Metadata.Table)
	}
	if rec.Data["soc"] != float64(88) ||
		rec.Data["_source_database"] != "dl_vls_dev" ||
		rec.Data["_source_table"] != "vehicle_charge" ||
		rec.Data["_debezium_op"] != "r" ||
		rec.Data["_debezium_snapshot"] != "true" {
		t.Fatalf("unexpected data = %#v", rec.Data)
	}
}

func TestDebeziumCDCDeletePreservesKafkaKeyMetadata(t *testing.T) {
	tr, err := NewDebeziumCDCTransform(map[string]any{
		"keep_metadata": true,
		"table_mapping": map[string]any{
			"template": "ods_pk_{source_table}",
		},
	})
	if err != nil {
		t.Fatalf("NewDebeziumCDCTransform: %v", err)
	}

	rec, err := tr.Apply(context.Background(), core.Record{
		Metadata: core.Metadata{Key: `{"tenant_id":"fleet-a","seq":7}`},
		Data: map[string]any{
			"payload": map[string]any{
				"op":    "d",
				"ts_ms": float64(1710000012123),
				"source": map[string]any{
					"db":    "dl_vls_dev",
					"table": "vehicle_trip",
				},
				"before": map[string]any{"tenant_id": "fleet-a", "seq": float64(7), "vin": "VIN-PK-TRIP"},
				"after":  nil,
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Operation != core.OpDelete {
		t.Fatalf("Operation = %s, want DELETE", rec.Operation)
	}
	if rec.Metadata.Key != `{"tenant_id":"fleet-a","seq":7}` {
		t.Fatalf("Metadata.Key = %q, want Debezium Kafka key JSON", rec.Metadata.Key)
	}
	if rec.Metadata.Table != "ods_pk_vehicle_trip" {
		t.Fatalf("Metadata.Table = %q, want mapped target table", rec.Metadata.Table)
	}
	if rec.Data["_source_table"] != "vehicle_trip" || rec.Data["_source_database"] != "dl_vls_dev" {
		t.Fatalf("source metadata = %#v, want original source table/database", rec.Data)
	}
	if rec.Data["tenant_id"] != "fleet-a" || rec.Data["seq"] != float64(7) {
		t.Fatalf("delete row image = %#v, want before image", rec.Data)
	}
}

func TestCDCPolicySkipsSnapshotAndDelete(t *testing.T) {
	tr, err := NewCDCPolicyTransform("cdc_policy", map[string]any{
		"skip_snapshot": true,
		"skip_delete":   true,
	})
	if err != nil {
		t.Fatalf("NewCDCPolicyTransform: %v", err)
	}

	_, err = tr.Apply(context.Background(), core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"_debezium_op": "r"},
	})
	if !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("snapshot Apply error = %v, want ErrRecordFiltered", err)
	}
	_, err = tr.Apply(context.Background(), core.Record{
		Operation: core.OpDelete,
		Data:      map[string]any{"id": 1},
	})
	if !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("delete Apply error = %v, want ErrRecordFiltered", err)
	}
	metrics := tr.TransformMetrics().Counters
	if metrics["processed"] != 2 || metrics["skipped_snapshot"] != 1 || metrics["skipped_delete"] != 1 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestCDCPolicyFiltersSourceDatabaseAndTable(t *testing.T) {
	tr, err := NewCDCPolicyTransform("cdc_policy", map[string]any{
		"include_databases": []any{"dl_vls_dev"},
		"include_tables":    []any{"dl_vls_dev.vehicle_*"},
		"exclude_tables":    []any{"*.vehicle_debug"},
	})
	if err != nil {
		t.Fatalf("NewCDCPolicyTransform: %v", err)
	}

	_, err = tr.Apply(context.Background(), core.Record{
		Data:     map[string]any{"_source_database": "dl_vls_dev", "_source_table": "vehicle_charge"},
		Metadata: core.Metadata{Database: "dl_vls_dev", Table: "ods_dl_vls_dev__vehicle_charge"},
	})
	if err != nil {
		t.Fatalf("Apply included record: %v", err)
	}

	for name, rec := range map[string]core.Record{
		"database": {
			Data: map[string]any{"_source_database": "other_db", "_source_table": "vehicle_charge"},
		},
		"table": {
			Data: map[string]any{"_source_database": "dl_vls_dev", "_source_table": "orders"},
		},
		"excluded": {
			Data: map[string]any{"_source_database": "dl_vls_dev", "_source_table": "vehicle_debug"},
		},
	} {
		_, err = tr.Apply(context.Background(), rec)
		if !errors.Is(err, core.ErrRecordFiltered) {
			t.Fatalf("%s Apply error = %v, want ErrRecordFiltered", name, err)
		}
	}

	metrics := tr.TransformMetrics().Counters
	if metrics["processed"] != 4 || metrics["skipped_filter"] != 3 {
		t.Fatalf("metrics = %#v", metrics)
	}
}

func TestDDLGuardRejectsDangerousDDL(t *testing.T) {
	tr, err := NewCDCPolicyTransform("ddl_guard", map[string]any{
		"dangerous_ddl": "reject",
	})
	if err != nil {
		t.Fatalf("NewCDCPolicyTransform: %v", err)
	}

	_, err = tr.Apply(context.Background(), core.Record{
		Operation: core.OpDDL,
		Data:      map[string]any{"ddl": "ALTER TABLE orders DROP COLUMN amount"},
		Metadata:  core.Metadata{DDL: "ALTER TABLE orders DROP COLUMN amount"},
	})
	var classified core.ClassifiedError
	if !errors.As(err, &classified) || classified.Class != core.ErrorClassSchema {
		t.Fatalf("Apply error = %T %v, want schema-classified error", err, err)
	}
	metrics := tr.TransformMetrics().Counters
	if metrics["ddl_rejected"] != 1 {
		t.Fatalf("metrics = %#v, want ddl_rejected=1", metrics)
	}
}

func TestDebeziumCDCTombstoneIsFilteredByDefault(t *testing.T) {
	tr, err := NewDebeziumCDCTransform(nil)
	if err != nil {
		t.Fatalf("NewDebeziumCDCTransform: %v", err)
	}

	_, err = tr.Apply(context.Background(), core.Record{Data: map[string]any{"payload": nil}})
	if !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("Apply error = %v, want ErrRecordFiltered", err)
	}
}

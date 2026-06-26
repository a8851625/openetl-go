package pipeline

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestDebeziumKafkaToMySQLFixtureCoversODSPolicy(t *testing.T) {
	spec, err := LoadSpec(filepath.Join("..", "..", "..", "testdata", "pipes-debezium-mysql", "debezium-kafka-to-mysql.yaml"))
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.Source.Type != "kafka" || spec.Sink.Type != "mysql" {
		t.Fatalf("unexpected endpoints: %s -> %s", spec.Source.Type, spec.Sink.Type)
	}
	if got := spec.Sink.Config["batch_mode"]; got != "upsert" {
		t.Fatalf("sink batch_mode = %#v, want upsert", got)
	}
	if got := spec.Sink.Config["auto_create"]; got != true {
		t.Fatalf("sink auto_create = %#v, want true", got)
	}

	chain := buildFixtureTransformChain(t, spec.Transforms)
	ctx := context.Background()

	out, err := chain.Apply(ctx, debeziumFixtureRecord("c", "dl_vls_dev", "vehicle_charge", map[string]any{
		"id": float64(9001), "vin": "VIN-9001", "soc": float64(68),
	}, nil))
	if err != nil {
		t.Fatalf("Apply create event: %v", err)
	}
	if out.Operation != core.OpInsert {
		t.Fatalf("operation = %s, want INSERT", out.Operation)
	}
	if out.Metadata.Database != "dl_vls_dev" || out.Metadata.Table != "ods_dl_vls_dev__vehicle_charge" {
		t.Fatalf("metadata = %#v", out.Metadata)
	}
	if out.Data["vin"] != "VIN-9001" || out.Data["_source_table"] != "vehicle_charge" {
		t.Fatalf("data = %#v", out.Data)
	}

	filteredCases := map[string]core.Record{
		"snapshot":       debeziumFixtureRecord("r", "dl_vls_dev", "vehicle_charge", map[string]any{"id": float64(9002)}, nil),
		"excluded_table": debeziumFixtureRecord("c", "dl_vls_dev", "vehicle_charge_debug", map[string]any{"id": float64(9003)}, nil),
		"delete":         debeziumFixtureRecord("d", "dl_vls_dev", "vehicle_charge", nil, map[string]any{"id": float64(9001)}),
		"dangerous_ddl": {
			Data: map[string]any{
				"payload": map[string]any{
					"source": map[string]any{"db": "dl_vls_dev", "table": "vehicle_charge"},
					"ddl":    "ALTER TABLE vehicle_charge DROP COLUMN soc",
					"ts_ms":  float64(1710000000123),
				},
			},
		},
	}
	for name, rec := range filteredCases {
		_, err := chain.Apply(ctx, rec)
		if !errors.Is(err, core.ErrRecordFiltered) {
			t.Fatalf("%s Apply error = %v, want ErrRecordFiltered", name, err)
		}
	}

	metrics := findTransformMetrics(t, chain.TransformMetrics(), "cdc_policy")
	if metrics["processed"] != 5 ||
		metrics["skipped_snapshot"] != 1 ||
		metrics["skipped_filter"] != 1 ||
		metrics["skipped_delete"] != 1 ||
		metrics["ddl_dropped"] != 1 {
		t.Fatalf("cdc_policy metrics = %#v", metrics)
	}
}

func buildFixtureTransformChain(t *testing.T, specs []TransformSpec) core.TransformChain {
	t.Helper()
	chain := make(core.TransformChain, 0, len(specs))
	for _, spec := range specs {
		tr, err := registry.BuildTransform(spec.Type, spec.Config)
		if err != nil {
			t.Fatalf("BuildTransform(%s): %v", spec.Type, err)
		}
		chain = append(chain, tr)
	}
	return chain
}

func debeziumFixtureRecord(op, db, table string, after, before map[string]any) core.Record {
	payload := map[string]any{
		"op":    op,
		"ts_ms": float64(1710000000123),
		"source": map[string]any{
			"db":    db,
			"table": table,
		},
	}
	if after != nil {
		payload["after"] = after
	}
	if before != nil {
		payload["before"] = before
	}
	if op == "r" {
		payload["snapshot"] = "true"
	}
	return core.Record{Data: map[string]any{"payload": payload}}
}

func findTransformMetrics(t *testing.T, metrics []core.TransformMetrics, name string) map[string]int64 {
	t.Helper()
	for _, metric := range metrics {
		if metric.Transform == name {
			return metric.Counters
		}
	}
	t.Fatalf("missing transform metrics for %s: %#v", name, metrics)
	return nil
}

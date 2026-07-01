package sink

import "testing"

func TestSinkConfigStringSlices(t *testing.T) {
	kafka, err := NewKafkaSink(map[string]any{
		"brokers": []string{"b1:9092", "b2:9092"},
		"topic":   "events",
	})
	if err != nil {
		t.Fatalf("NewKafkaSink: %v", err)
	}
	if len(kafka.brokers) != 2 || kafka.brokers[0] != "b1:9092" || kafka.brokers[1] != "b2:9092" {
		t.Fatalf("kafka brokers = %#v, want []string brokers", kafka.brokers)
	}

	mysql, err := NewMySQLSink(map[string]any{"pk_columns": []string{"id", "tenant_id"}})
	if err != nil {
		t.Fatalf("NewMySQLSink: %v", err)
	}
	if len(mysql.pkColumns) != 2 || mysql.pkColumns[0] != "id" || mysql.pkColumns[1] != "tenant_id" {
		t.Fatalf("mysql pk_columns = %#v, want []string pk_columns", mysql.pkColumns)
	}

	postgres, err := NewPostgresSink(map[string]any{"pk_columns": []string{"id", "tenant_id"}})
	if err != nil {
		t.Fatalf("NewPostgresSink: %v", err)
	}
	if len(postgres.pkColumns) != 2 || postgres.pkColumns[0] != "id" || postgres.pkColumns[1] != "tenant_id" {
		t.Fatalf("postgres pk_columns = %#v, want []string pk_columns", postgres.pkColumns)
	}

	clickhouse, err := NewClickHouseSink(map[string]any{"pk_columns": []string{"id", "tenant_id"}})
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}
	if len(clickhouse.pkColumns) != 2 || clickhouse.pkColumns[0] != "id" || clickhouse.pkColumns[1] != "tenant_id" {
		t.Fatalf("clickhouse pk_columns = %#v, want []string pk_columns", clickhouse.pkColumns)
	}

	doris, err := NewDorisSink(map[string]any{"pk_columns": []string{"id", "tenant_id"}})
	if err != nil {
		t.Fatalf("NewDorisSink: %v", err)
	}
	if len(doris.pkColumns) != 2 || doris.pkColumns[0] != "id" || doris.pkColumns[1] != "tenant_id" {
		t.Fatalf("doris pk_columns = %#v, want []string pk_columns", doris.pkColumns)
	}

	jdbc, err := NewJDBCSink(map[string]any{"pk_columns": []string{"id", "tenant_id"}})
	if err != nil {
		t.Fatalf("NewJDBCSink: %v", err)
	}
	if len(jdbc.pkColumns) != 2 || jdbc.pkColumns[0] != "id" || jdbc.pkColumns[1] != "tenant_id" {
		t.Fatalf("jdbc pk_columns = %#v, want []string pk_columns", jdbc.pkColumns)
	}

	es, err := NewElasticsearchSink(map[string]any{"hosts": []string{"http://es-1:9200/", "http://es-2:9200/"}, "index": "orders"})
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}
	if len(es.hosts) != 2 || es.hosts[0] != "http://es-1:9200" || es.hosts[1] != "http://es-2:9200" {
		t.Fatalf("elasticsearch hosts = %#v, want trimmed []string hosts", es.hosts)
	}
}

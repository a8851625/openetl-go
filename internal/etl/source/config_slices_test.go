package source

import "testing"

func TestSourceConfigStringSlices(t *testing.T) {
	kafka, err := NewKafkaSource(map[string]any{
		"brokers": []string{"b1:9092", "b2:9092"},
		"topic":   "events",
	})
	if err != nil {
		t.Fatalf("NewKafkaSource: %v", err)
	}
	if len(kafka.brokers) != 2 || kafka.brokers[0] != "b1:9092" || kafka.brokers[1] != "b2:9092" {
		t.Fatalf("kafka brokers = %#v, want []string brokers", kafka.brokers)
	}

	mysqlCDC, err := NewMySQLCDCSource(map[string]any{
		"host":     "mysql",
		"user":     "sync",
		"database": "app",
		"tables":   []string{"orders", "items"},
	})
	if err != nil {
		t.Fatalf("NewMySQLCDCSource: %v", err)
	}
	if len(mysqlCDC.tables) != 2 || mysqlCDC.tables[0] != "orders" || mysqlCDC.tables[1] != "items" {
		t.Fatalf("mysql_cdc tables = %#v, want []string tables", mysqlCDC.tables)
	}

	snapshotCDC, err := NewMySQLSnapshotCDCSource(map[string]any{
		"host":     "mysql",
		"user":     "sync",
		"database": "app",
		"tables":   []string{"orders", "items"},
	})
	if err != nil {
		t.Fatalf("NewMySQLSnapshotCDCSource: %v", err)
	}
	if len(snapshotCDC.tables) != 2 || snapshotCDC.table != "orders" {
		t.Fatalf("mysql_snapshot_cdc tables = %#v table=%q, want []string tables with first table default", snapshotCDC.tables, snapshotCDC.table)
	}

	postgresCDC, err := NewPostgresCDCSource(map[string]any{
		"host":     "postgres",
		"user":     "sync",
		"database": "app",
		"tables":   []string{"orders", "items"},
	})
	if err != nil {
		t.Fatalf("NewPostgresCDCSource: %v", err)
	}
	if len(postgresCDC.tables) != 2 || postgresCDC.tables[0] != "orders" || postgresCDC.tables[1] != "items" {
		t.Fatalf("postgres_cdc tables = %#v, want []string tables", postgresCDC.tables)
	}

	mysqlBatch, err := NewMySQLBatchSource(map[string]any{
		"host":     "mysql",
		"user":     "sync",
		"database": "app",
		"table":    "orders",
		"columns":  []string{"id", "amount"},
	})
	if err != nil {
		t.Fatalf("NewMySQLBatchSource: %v", err)
	}
	if len(mysqlBatch.columns) != 2 || mysqlBatch.columns[0] != "id" || mysqlBatch.columns[1] != "amount" {
		t.Fatalf("mysql_batch columns = %#v, want []string columns", mysqlBatch.columns)
	}
}

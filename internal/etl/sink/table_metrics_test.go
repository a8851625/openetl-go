package sink

import (
	"testing"
	"time"
)

func TestTableMetricsSetRecordAndSnapshot(t *testing.T) {
	tm := newTableMetricsSet()
	tm.record("ods_a", 10, 5*time.Millisecond, false)
	tm.record("ods_a", 2, 3*time.Millisecond, true)
	tm.record("ods_b", 1, 1*time.Millisecond, false)
	tm.record("", 99, time.Hour, false) // empty table ignored

	stats := tm.snapshot()
	if len(stats) != 2 || stats[0].Table != "ods_a" || stats[1].Table != "ods_b" {
		t.Fatalf("sorted snapshot = %+v", stats)
	}
	a := stats[0]
	if a.RowsWritten != 12 || a.BatchesSent != 2 || a.Errors != 1 {
		t.Fatalf("a = %+v", a)
	}
	if a.WriteLatencyMs < 3.9 || a.WriteLatencyMs > 4.1 {
		t.Fatalf("a latency = %v", a.WriteLatencyMs)
	}
}

func TestMySQLPostgresTableMetricsWired(t *testing.T) {
	ms, err := NewMySQLSink(map[string]any{"host": "m", "database": "d"})
	if err != nil {
		t.Fatal(err)
	}
	if ms.tableMetrics == nil {
		t.Fatal("mysql tableMetrics not initialized")
	}
	ms.tableMetrics.record("t1", 5, time.Millisecond, false)
	if len(ms.TableWriteStats()) != 1 {
		t.Fatal("mysql TableWriteStats empty")
	}
	ps, err := NewPostgresSink(map[string]any{"host": "p", "database": "d"})
	if err != nil {
		t.Fatal(err)
	}
	if ps.tableMetrics == nil {
		t.Fatal("postgres tableMetrics not initialized")
	}
	ps.tableMetrics.record("t2", 5, time.Millisecond, false)
	if len(ps.TableWriteStats()) != 1 {
		t.Fatal("postgres TableWriteStats empty")
	}
}

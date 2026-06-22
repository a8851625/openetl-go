package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// makeRecords builds N synthetic INSERT records with identical column shape.
func makeRecords(n int) []core.Record {
	recs := make([]core.Record, n)
	for i := 0; i < n; i++ {
		recs[i] = core.Record{
			Operation: core.OpInsert,
			Data: map[string]any{
				"id":    i + 1,
				"name":  fmt.Sprintf("user-%d", i),
				"email": fmt.Sprintf("u%d@example.com", i),
			},
		}
	}
	return recs
}

func TestMySQLBuildBatchInsertSQL(t *testing.T) {
	cases := []struct {
		name        string
		mode        string
		rows        int
		cols        []string
		wantRows    int
		wantKeyword string
	}{
		{"insert_2", "insert", 2, []string{"id", "name"}, 2, "INSERT"},
		{"insert_ignore_2", "insert", 2, []string{"id", "name"}, 2, "INSERT IGNORE"},
		{"upsert_3", "upsert", 3, []string{"id", "name"}, 3, "ON DUPLICATE KEY UPDATE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &MySQLSink{name: "mysql", table: "users", batchMode: tc.mode, insertChunkSize: 500}
			stmt := s.buildBatchInsertStatement("users", tc.cols, tc.rows, tc.mode)
			if !contains(stmt, tc.wantKeyword) {
				t.Errorf("missing keyword %q in: %s", tc.wantKeyword, stmt)
			}
			valuesCount := strings.Count(stmt, "),(") + 1
			if valuesCount != tc.wantRows {
				t.Errorf("VALUES rows = %d, want %d (stmt=%s)", valuesCount, tc.wantRows, stmt)
			}
			// Arg count should be rows*cols.
			questionCount := strings.Count(stmt, "?")
			if want := tc.rows * len(tc.cols); questionCount != want {
				t.Errorf("placeholder count = %d, want %d", questionCount, want)
			}
		})
	}
}

func TestMySQLBuildBatchDeleteSQL(t *testing.T) {
	s := &MySQLSink{name: "mysql", table: "users", pkColumns: []string{"id"}, insertChunkSize: 500}
	stmt := s.buildBatchDeleteStatement("users", 5)
	if !contains(stmt, "DELETE FROM") {
		t.Errorf("not a DELETE: %s", stmt)
	}
	// 5 conditions joined by OR, each with 1 placeholder.
	if got := strings.Count(stmt, "?"); got != 5 {
		t.Errorf("placeholder count = %d, want 5", got)
	}
}

func TestMySQLBatchChunking(t *testing.T) {
	s := &MySQLSink{
		name:            "mysql",
		table:           "users",
		insertChunkSize: 50,
	}
	recs := makeRecords(150)
	// Group records like Write() does, count resulting statements.
	_ = recs
	stmts := 0
	for offset := 0; offset < len(recs); offset += s.insertChunkSize {
		end := offset + s.insertChunkSize
		if end > len(recs) {
			end = len(recs)
		}
		if end > offset {
			stmts++
		}
	}
	if stmts != 3 {
		t.Errorf("stmt count = %d, want 3", stmts)
	}
}

// TestMySQLBenchmarkRealDB is an integration test gated by MYSQL_HOST.
func TestMySQLBenchmarkRealDB(t *testing.T) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		t.Skip("MYSQL_HOST not set; skipping integration benchmark")
	}
	s, err := NewMySQLSink(map[string]any{
		"host":     host,
		"port":     atoiOr(os.Getenv("MYSQL_PORT"), 3306),
		"user":     getenvOr("MYSQL_USER", "root"),
		"password": os.Getenv("MYSQL_PASSWORD"),
		"database": getenvOr("MYSQL_DATABASE", "test"),
		"table":    "perf_test",
	})
	if err != nil {
		t.Fatalf("NewMySQLSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if _, err := s.db.Exec("DROP TABLE IF EXISTS perf_test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TABLE perf_test (id INT PRIMARY KEY, name VARCHAR(64), email VARCHAR(128))"); err != nil {
		t.Fatal(err)
	}
	recs := makeRecords(1000)
	start := time.Now()
	if err := s.Write(context.Background(), recs); err != nil {
		t.Fatalf("Write: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("1000-row write took %v, want <=100ms", elapsed)
	}
	t.Logf("1000 rows written in %v", elapsed)
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

func atoiOr(s string, def int) int {
	if s == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return def
	}
	return n
}

func getenvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

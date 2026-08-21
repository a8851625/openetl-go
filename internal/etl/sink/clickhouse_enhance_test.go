package sink

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// TestClickHouseHostsConfigParsing verifies the http failover host list is
// parsed from both list and comma-separated string forms.
func TestClickHouseHostsConfigParsing(t *testing.T) {
	s, err := NewClickHouseSink(map[string]any{
		"host": "ch1", "port": 8123, "database": "d", "protocol": "http",
		"hosts": []any{"ch2:8123", "ch3:8124"},
	})
	if err != nil {
		t.Fatalf("list form: %v", err)
	}
	if len(s.httpHosts) != 2 || s.httpHosts[0] != "ch2:8123" || s.httpHosts[1] != "ch3:8124" {
		t.Fatalf("httpHosts = %v", s.httpHosts)
	}

	s2, err := NewClickHouseSink(map[string]any{
		"host": "ch1", "database": "d",
		"hosts": " ch2:8123 , ch3 ",
	})
	if err != nil {
		t.Fatalf("string form: %v", err)
	}
	if len(s2.httpHosts) != 2 || s2.httpHosts[0] != "ch2:8123" || s2.httpHosts[1] != "ch3" {
		t.Fatalf("string httpHosts = %v", s2.httpHosts)
	}

	s3, _ := NewClickHouseSink(map[string]any{"host": "ch1", "database": "d"})
	if len(s3.httpHosts) != 0 {
		t.Fatalf("no hosts config must keep empty list, got %v", s3.httpHosts)
	}
}

// TestClickHouseTableWriteStats verifies per-table metrics accumulate across
// multiple recordTableMetrics calls and snapshot correctly.
func TestClickHouseTableWriteStats(t *testing.T) {
	s, err := NewClickHouseSink(map[string]any{"host": "ch", "database": "d"})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	s.tableMetricsImpl.record("ods_orders", 10, 5_000_000, false)
	s.tableMetricsImpl.record("ods_orders", 5, 3_000_000, true)
	s.tableMetricsImpl.record("ods_users", 2, 1_000_000, false)

	stats := s.TableWriteStats()
	byTable := map[string]TableWriteStats{}
	for _, st := range stats {
		byTable[st.Table] = st
	}
	if len(stats) != 2 {
		t.Fatalf("tables = %d, want 2", len(stats))
	}
	o := byTable["ods_orders"]
	if o.RowsWritten != 15 || o.BatchesSent != 2 || o.Errors != 1 {
		t.Fatalf("orders stats = %+v", o)
	}
	if o.WriteLatencyMs < 3.9 || o.WriteLatencyMs > 4.1 { // (5ms+3ms)/2
		t.Fatalf("orders latency = %v, want ~4ms", o.WriteLatencyMs)
	}
	if byTable["ods_users"].RowsWritten != 2 {
		t.Fatalf("users stats = %+v", byTable["ods_users"])
	}
}

var _ = core.OpInsert // keep import if helpers change

// TestClickHouseHTTPInsertFailover verifies the HTTP write path retries the
// next configured host after a connection-level failure (dead first host)
// and succeeds via the second, using real httptest servers.
func TestClickHouseHTTPInsertFailover(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "INSERT") {
			io.WriteString(w, "")
			return
		}
		w.WriteHeader(400)
	}))
	defer good.Close()
	goodAddr := strings.TrimPrefix(good.URL, "http://")

	s, err := NewClickHouseSink(map[string]any{
		"host": "127.0.0.1", "port": 1, "database": "d", "protocol": "http",
		// First host: unroutable port 1 -> connection error -> failover.
		"hosts": []any{goodAddr},
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	recs := []core.Record{{Operation: core.OpInsert, Data: map[string]any{"id": int64(1)}, Metadata: core.Metadata{Table: "t"}}}
	err = s.writeInsertHTTP(t.Context(), "t", []clickhouseColumn{{Name: "id", Type: "Int64"}}, recs)
	if err != nil {
		t.Fatalf("writeInsertHTTP with failover: %v", err)
	}
}

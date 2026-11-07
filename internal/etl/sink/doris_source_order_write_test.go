package sink

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

// binlogRecord builds a MySQL-CDC record whose metadata carries a durable
// numeric source position, which is what source_order derives the sequence
// value from.
func binlogRecord(op core.OpType, table string, pos uint32, data map[string]any) core.Record {
	return core.Record{
		Operation: op,
		Data:      data,
		Metadata: core.Metadata{
			SourceType: core.SourceTypeMySQLCDC,
			Source:     "mysql_cdc",
			Database:   "app",
			Table:      table,
			BinlogFile: "mysql-bin.000001",
			BinlogPos:  pos,
		},
	}
}

func TestDorisPrepareSourceOrderRecords(t *testing.T) {
	t.Run("off mode leaves records untouched", func(t *testing.T) {
		s := sourceOrderSink()
		s.versionMode = dorisVersionModeOff
		rec := binlogRecord(core.OpInsert, "orders", 100, map[string]any{"order_id": 1, "amount": 5})
		out, err := s.prepareSourceOrderRecords([]core.Record{rec}, nil)
		if err != nil {
			t.Fatalf("prepareSourceOrderRecords: %v", err)
		}
		if len(out) != 1 {
			t.Fatalf("records = %d, want 1", len(out))
		}
		if _, ok := out[0].Data["_version"]; ok {
			t.Fatal("off mode must not inject a sequence column")
		}
		if len(rec.Data) != 2 {
			t.Fatalf("caller record was mutated: %v", rec.Data)
		}
	})

	t.Run("injects the source position", func(t *testing.T) {
		s := sourceOrderSink()
		rec := binlogRecord(core.OpInsert, "orders", 4242, map[string]any{"order_id": 1, "amount": 5})
		out, err := s.prepareSourceOrderRecords([]core.Record{rec}, nil)
		if err != nil {
			t.Fatalf("prepareSourceOrderRecords: %v", err)
		}
		want := core.SourceOrder(rec).Version
		if want == 0 {
			t.Fatal("test precondition: the connector must expose a numeric version")
		}
		if got := out[0].Data["_version"]; got != want {
			t.Fatalf("_version = %v (%T), want %d (core.SourceOrder version)", got, got, want)
		}
		if _, ok := rec.Data["_version"]; ok {
			t.Fatal("the caller's record must not be mutated")
		}
	})

	t.Run("deletes keep the native delete path", func(t *testing.T) {
		s := sourceOrderSink()
		rec := binlogRecord(core.OpDelete, "orders", 5000, map[string]any{"order_id": 1})
		out, err := s.prepareSourceOrderRecords([]core.Record{rec}, nil)
		if err != nil {
			t.Fatalf("prepareSourceOrderRecords: %v", err)
		}
		if _, ok := out[0].Data["_version"]; ok {
			t.Fatal("delete records must not carry a sequence value")
		}
		if out[0].Operation != core.OpDelete {
			t.Fatalf("operation = %v, want delete", out[0].Operation)
		}
	})

	t.Run("key change expands to a stale-key delete", func(t *testing.T) {
		s := sourceOrderSink()
		rec := binlogRecord(core.OpUpdate, "orders", 6000, map[string]any{"order_id": 2, "amount": 7})
		rec.Before = map[string]any{"order_id": 1}
		out, err := s.prepareSourceOrderRecords([]core.Record{rec}, map[string][]string{"orders": {"order_id"}})
		if err != nil {
			t.Fatalf("prepareSourceOrderRecords: %v", err)
		}
		if len(out) != 2 {
			t.Fatalf("records = %d, want 2 (live row + stale-key delete)", len(out))
		}
		if out[0].Operation != core.OpUpdate || out[0].Data["order_id"] != 2 {
			t.Fatalf("first record = %v %v, want the live new-key row", out[0].Operation, out[0].Data)
		}
		if out[1].Operation != core.OpDelete {
			t.Fatalf("second record operation = %v, want delete", out[1].Operation)
		}
		if got := out[1].Data["order_id"]; got != 1 {
			t.Fatalf("stale-key delete order_id = %v, want 1 (old key)", got)
		}
	})

	t.Run("unchanged key does not expand", func(t *testing.T) {
		s := sourceOrderSink()
		rec := binlogRecord(core.OpUpdate, "orders", 6001, map[string]any{"order_id": 1, "amount": 9})
		rec.Before = map[string]any{"order_id": 1, "amount": 7}
		out, err := s.prepareSourceOrderRecords([]core.Record{rec}, map[string][]string{"orders": {"order_id"}})
		if err != nil {
			t.Fatalf("prepareSourceOrderRecords: %v", err)
		}
		if len(out) != 1 {
			t.Fatalf("records = %d, want 1", len(out))
		}
	})

	t.Run("bigint rejects positions above MaxInt64", func(t *testing.T) {
		s := sourceOrderSink()
		s.sequenceType = dorisSequenceTypeBigInt
		// A binlog-derived version is above MaxInt64 by construction (file
		// ordinal << 32 | pos), which is exactly why LARGEINT is the default.
		rec := binlogRecord(core.OpInsert, "orders", 1, map[string]any{"order_id": 1})
		if v := core.SourceOrder(rec).Version; v <= uint64(math.MaxInt64) {
			t.Skipf("binlog version %d fits in bigint; nothing to reject", v)
		}
		_, err := s.prepareSourceOrderRecords([]core.Record{rec}, nil)
		if err == nil || !strings.Contains(err.Error(), "largeint") {
			t.Fatalf("err = %v, want a largeint recommendation", err)
		}
	})

	t.Run("missing source position fails closed", func(t *testing.T) {
		s := sourceOrderSink()
		rec := core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"order_id": 1},
			Metadata:  core.Metadata{SourceType: core.SourceTypeMySQLCDC, Table: "orders"},
		}
		_, err := s.prepareSourceOrderRecords([]core.Record{rec}, nil)
		if err == nil || !strings.Contains(err.Error(), "binlog_file") {
			t.Fatalf("err = %v, want the binlog_file remediation", err)
		}
	})
}

// TestDorisWriteInjectsSequenceColumn proves the injection reaches the Stream
// Load payload end to end (contract validation -> payload -> write).
func TestDorisWriteInjectsSequenceColumn(t *testing.T) {
	var bodies []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","StatusCode":200,"NumberTotalRows":1,"NumberLoadedRows":1}`))
	}))
	defer ts.Close()
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
	port, _ := strconv.Atoi(portText)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := sourceOrderSink()
	s.host = host
	s.httpPort = port
	s.streamLoadScheme = "http"
	s.streamLoadFormat = "json"
	s.httpClient = ts.Client()
	s.insertChunkSize = 500
	s.db = db
	s.schemaCache = core.NewSchemaCache()
	s.tableMetrics = newTableMetricsSet()

	// The write path validates the table contract once per routed table.
	mock.ExpectQuery("SHOW CREATE TABLE").WillReturnRows(
		sqlmock.NewRows([]string{"Create Table"}).AddRow(dorisSourceOrderTableStmt))

	rec := binlogRecord(core.OpInsert, "orders", 777, map[string]any{"order_id": 1, "amount": 12})
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(bodies) != 1 {
		t.Fatalf("stream load calls = %d, want 1", len(bodies))
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(bodies[0])), &payload); err != nil {
		t.Fatalf("decode payload %q: %v", bodies[0], err)
	}
	wantVersion := float64(core.SourceOrder(rec).Version)
	if got, ok := payload["_version"]; !ok {
		t.Fatalf("payload has no _version: %v", payload)
	} else if got.(float64) != wantVersion {
		t.Fatalf("_version = %v, want %v", got, wantVersion)
	}
}

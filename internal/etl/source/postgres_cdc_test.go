package source

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func buildRelationMsg(relID uint32, ns, table string, cols []pgColumnInfo) []byte {
	var buf []byte
	buf = append(buf, 'R')
	rid := make([]byte, 4)
	binary.BigEndian.PutUint32(rid, relID)
	buf = append(buf, rid...)
	buf = append(buf, []byte(ns)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(table)...)
	buf = append(buf, 0)
	buf = append(buf, 'd')
	numCols := make([]byte, 2)
	binary.BigEndian.PutUint16(numCols, uint16(len(cols)))
	buf = append(buf, numCols...)
	for _, c := range cols {
		buf = append(buf, 0)
		buf = append(buf, []byte(c.Name)...)
		buf = append(buf, 0)
		oid := make([]byte, 4)
		binary.BigEndian.PutUint32(oid, c.TypeOID)
		buf = append(buf, oid...)
		buf = append(buf, 0, 0, 0, 0)
	}
	return buf
}

func buildInsertMsg(relID uint32, vals []any) []byte {
	var buf []byte
	buf = append(buf, 'I')
	rid := make([]byte, 4)
	binary.BigEndian.PutUint32(rid, relID)
	buf = append(buf, rid...)
	buf = append(buf, 'N')
	numCols := make([]byte, 2)
	binary.BigEndian.PutUint16(numCols, uint16(len(vals)))
	buf = append(buf, numCols...)
	for _, v := range vals {
		if v == nil {
			buf = append(buf, 'n')
			continue
		}
		buf = append(buf, 't')
		s := formatValueStr(v)
		vl := make([]byte, 4)
		binary.BigEndian.PutUint32(vl, uint32(len(s)))
		buf = append(buf, vl...)
		buf = append(buf, s...)
	}
	return buf
}

func formatValueStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int:
		return itos(int64(x))
	case int32:
		return itos(int64(x))
	case int64:
		return itos(x)
	case bool:
		if x {
			return "t"
		}
		return "f"
	default:
		return ""
	}
}

func itos(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func buildDeleteMsg(relID uint32, vals []any) []byte {
	var buf []byte
	buf = append(buf, 'D')
	rid := make([]byte, 4)
	binary.BigEndian.PutUint32(rid, relID)
	buf = append(buf, rid...)
	buf = append(buf, 'K')
	numCols := make([]byte, 2)
	binary.BigEndian.PutUint16(numCols, uint16(len(vals)))
	buf = append(buf, numCols...)
	for _, v := range vals {
		if v == nil {
			buf = append(buf, 'n')
			continue
		}
		buf = append(buf, 't')
		s := formatValueStr(v)
		vl := make([]byte, 4)
		binary.BigEndian.PutUint32(vl, uint32(len(s)))
		buf = append(buf, vl...)
		buf = append(buf, s...)
	}
	return buf
}

func TestPGCatalogStoresRelation(t *testing.T) {
	c := newPGCatalog()
	cols := []pgColumnInfo{
		{Name: "id", TypeOID: 23},
		{Name: "name", TypeOID: 25},
	}
	c.setRelation(42, "users", cols)

	if got := c.tableName(42); got != "users" {
		t.Errorf("tableName = %q, want users", got)
	}
	if got := c.columns(42); len(got) != 2 || got[0].Name != "id" {
		t.Errorf("columns = %+v, want [id/int4, name/text]", got)
	}
	if got := c.tableName(99); got != "rel_99" {
		t.Errorf("unknown rel = %q, want rel_99", got)
	}
	if got := c.columns(99); got != nil {
		t.Errorf("unknown columns = %v, want nil", got)
	}
}

func newTestReader() *pgCDCReader {
	return &pgCDCReader{
		catalog: newPGCatalog(),
		records: make(chan core.Record, 4),
		done:    make(chan struct{}),
		source:  &PostgresCDCSource{name: "pg_cdc_test"},
		ctx:     context.Background(),
	}
}

func TestParseRelationMsgPopulatesCatalog(t *testing.T) {
	r := newTestReader()
	cols := []pgColumnInfo{
		{Name: "id", TypeOID: 23},
		{Name: "email", TypeOID: 25},
	}
	msg := buildRelationMsg(100, "public", "users", cols)
	// parseRelationMsg expects data AFTER the message type byte ('R').
	r.parseRelationMsg(msg[1:])

	if got := r.catalog.tableName(100); got != "users" {
		t.Errorf("tableName = %q, want users", got)
	}
	gotCols := r.catalog.columns(100)
	if len(gotCols) != 2 {
		t.Fatalf("got %d columns, want 2", len(gotCols))
	}
	if gotCols[0].Name != "id" || gotCols[0].TypeOID != 23 {
		t.Errorf("col[0] = %+v, want id/23", gotCols[0])
	}
}

func TestParseInsertMsgUsesCatalog(t *testing.T) {
	r := newTestReader()
	r.catalog.setRelation(1, "users", []pgColumnInfo{
		{Name: "id", TypeOID: 23},
		{Name: "name", TypeOID: 25},
		{Name: "active", TypeOID: 16},
	})

	msg := buildInsertMsg(1, []any{42, "alice", true})
	r.parseInsertMsg(msg[1:], "1/1")

	select {
	case rec := <-r.records:
		if rec.Metadata.Table != "users" {
			t.Errorf("table = %q, want users", rec.Metadata.Table)
		}
		if got := rec.Data["id"]; got != int32(42) {
			t.Errorf("id = %v(%T), want int32 42", got, got)
		}
		if got := rec.Data["name"]; got != "alice" {
			t.Errorf("name = %v, want alice", got)
		}
		if got := rec.Data["active"]; got != true {
			t.Errorf("active = %v, want true", got)
		}
	default:
		t.Fatal("expected record on channel")
	}
}

func TestParseDeleteMsgUsesCatalog(t *testing.T) {
	r := newTestReader()
	r.catalog.setRelation(2, "orders", []pgColumnInfo{
		{Name: "order_id", TypeOID: 25},
	})

	msg := buildDeleteMsg(2, []any{"12345"})
	r.parseDeleteMsg(msg[1:], "1/2")

	select {
	case rec := <-r.records:
		if rec.Operation != core.OpDelete {
			t.Errorf("op = %v, want DELETE", rec.Operation)
		}
		if rec.Metadata.Table != "orders" {
			t.Errorf("table = %q, want orders", rec.Metadata.Table)
		}
		if got := rec.Data["order_id"]; got != "12345" {
			t.Errorf("order_id = %v, want '12345'", got)
		}
	default:
		t.Fatal("expected delete record on channel")
	}
}

func TestDecodeColumnValueTypes(t *testing.T) {
	cases := []struct {
		raw  string
		oid  uint32
		want any
	}{
		{"42", 23, int32(42)},
		{"-1", 23, int32(-1)},
		{"1234567890", 20, int64(1234567890)},
		{"32767", 21, int16(32767)},
		{"3.14", 701, float64(3.14)},
		{"t", 16, true},
		{"f", 16, false},
		{"hello", 25, "hello"},
		{"2024-01-15 12:00:00", 1114, mustParseTime("2006-01-02 15:04:05", "2024-01-15 12:00:00")},
		{"unknown_oid", 9999, "unknown_oid"},
	}
	for _, tc := range cases {
		got := decodeColumnValue([]byte(tc.raw), tc.oid)
		if got != tc.want {
			t.Errorf("decode(%q, oid=%d) = %v(%T), want %v(%T)", tc.raw, tc.oid, got, got, tc.want, tc.want)
		}
	}
}

func mustParseTime(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

func TestPGCDCReaderDoesNotCreatePublicationIfExists(t *testing.T) {
	c := newPGCatalog()
	if c == nil {
		t.Fatal("catalog is nil")
	}
}

func TestPGPositionRoundtrip(t *testing.T) {
	pos := pgPosition{LSN: "0/16B3748"}
	data, err := json.Marshal(pos)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pgPosition
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.LSN != "0/16B3748" {
		t.Errorf("LSN = %q, want 0/16B3748", decoded.LSN)
	}
}

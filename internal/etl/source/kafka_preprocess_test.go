package source

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestTryCanalJSONInsert(t *testing.T) {
	rec := core.Record{Operation: core.OpInsert, Metadata: core.Metadata{}}
	data := map[string]any{}
	raw := []byte(`{"type":"INSERT","database":"shop","table":"orders","es":1690000000000,"ts":1690000001000,"sqlType":{"id":200009},"mysqlType":{"id":"bigint","name":"varchar(32)"},"pkNames":["id"],"data":[{"id":42,"name":"x"}]}`)
	if !tryCanalJSON(raw, &rec, data) {
		t.Fatal("canal INSERT message not parsed")
	}
	if rec.Operation != core.OpInsert {
		t.Errorf("op = %v, want insert", rec.Operation)
	}
	if rec.Metadata.Table != "orders" || rec.Metadata.Database != "shop" {
		t.Errorf("table/db = %s/%s", rec.Metadata.Table, rec.Metadata.Database)
	}
	if data["id"] != float64(42) {
		t.Errorf("data id = %#v", data["id"])
	}
	if rec.Metadata.ColumnTypes["name"] != "varchar(32)" {
		t.Errorf("column types missing mysqlType: %#v", rec.Metadata.ColumnTypes)
	}
	if rec.Metadata.Key == "" {
		t.Error("PK JSON key not derived from pkNames")
	}
}

func TestTryCanalJSONUpdateBefore(t *testing.T) {
	rec := core.Record{Metadata: core.Metadata{}}
	data := map[string]any{}
	raw := []byte(`{"type":"UPDATE","database":"shop","table":"orders","pkNames":["id"],"data":[{"id":42,"name":"new"}],"old":[{"name":"old"}]}`)
	if !tryCanalJSON(raw, &rec, data) {
		t.Fatal("canal UPDATE not parsed")
	}
	if rec.Operation != core.OpUpdate {
		t.Errorf("op = %v, want update", rec.Operation)
	}
	if rec.Before["name"] != "old" {
		t.Errorf("before image = %#v, want old name", rec.Before)
	}
}

func TestTryCanalJSONRejectsDDLAndMultiRow(t *testing.T) {
	rec := core.Record{Metadata: core.Metadata{}}
	data := map[string]any{}
	if tryCanalJSON([]byte(`{"type":"QUERY","isDdl":true,"sql":"ALTER TABLE t ADD c INT"}`), &rec, data) {
		t.Error("DDL message must not parse as DML")
	}
	if tryCanalJSON([]byte(`{"type":"INSERT","data":[{"id":1},{"id":2}]}`), &rec, data) {
		t.Error("multi-row message must not parse (unsupported batch shape)")
	}
	if tryCanalJSON([]byte(`not json`), &rec, data) {
		t.Error("invalid JSON must fail")
	}
	if tryCanalJSON([]byte(`{"type":"OTHER","data":[{"id":1}]}`), &rec, data) {
		t.Error("unknown type must fail")
	}
}

func TestKafkaSourceParseErrorPolicyConfig(t *testing.T) {
	if _, err := NewKafkaSource(map[string]any{
		"brokers": []string{"b:9092"}, "topic": "t",
		"on_parse_error": "bogus",
	}); err == nil {
		t.Fatal("invalid on_parse_error accepted")
	}
	s, err := NewKafkaSource(map[string]any{
		"brokers": []string{"b:9092"}, "topic": "t",
		"on_parse_error": "dlq", "tombstone_policy": "skip", "expand_key_json": true,
	})
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if s.onParseError != "dlq" || s.tombstonePolicy != "skip" || !s.expandKeyJSON {
		t.Fatalf("fields = %v/%v/%v", s.onParseError, s.tombstonePolicy, s.expandKeyJSON)
	}
	// defaults
	s2, _ := NewKafkaSource(map[string]any{"brokers": []string{"b:9092"}, "topic": "t"})
	if s2.onParseError != "raw" || s2.tombstonePolicy != "delete" || s2.expandKeyJSON {
		t.Fatalf("defaults = %v/%v/%v", s2.onParseError, s2.tombstonePolicy, s2.expandKeyJSON)
	}
	if _, err := NewKafkaSource(map[string]any{"brokers": []string{"b:9092"}, "topic": "t", "tombstone_policy": "bogus"}); err == nil {
		t.Fatal("invalid tombstone_policy accepted")
	}
}

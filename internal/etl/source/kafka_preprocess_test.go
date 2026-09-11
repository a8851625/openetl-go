package source

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
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
	if len(rec.Metadata.PrimaryKeyColumns) != 1 || rec.Metadata.PrimaryKeyColumns[0] != "id" {
		t.Errorf("pkNames contract = %v, want [id]", rec.Metadata.PrimaryKeyColumns)
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
	if rec.Metadata.FormatContractID != core.FormatContractCanalJSONV1 || rec.Metadata.BeforeImageState != core.BeforeImageStatePartial {
		t.Fatalf("format contract/state = %q/%q", rec.Metadata.FormatContractID, rec.Metadata.BeforeImageState)
	}
	if identity := core.RecordIdentity(core.Record{Operation: rec.Operation, Data: data, Before: rec.Before, Metadata: rec.Metadata}); !identity.Complete {
		t.Fatalf("canal unchanged PK identity = %+v, want complete", identity)
	}
}

func TestTryCanalJSONCompleteCompositeKeyMatrix(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantKey    string
		complete   bool
		wantReason core.RecordIdentityReason
	}{
		{
			name: "insert", raw: `{"type":"INSERT","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t1","id":"n1"}]}`,
			wantKey: `{"id":"n1","tenant_id":"t1"}`, complete: true,
		},
		{
			name: "delete", raw: `{"type":"DELETE","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t1","id":"n1"}]}`,
			wantKey: `{"id":"n1","tenant_id":"t1"}`, complete: true,
		},
		{
			name: "key changing update", raw: `{"type":"UPDATE","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t1","id":"new"}],"old":[{"id":"old"}]}`,
			wantKey: `{"id":"old","tenant_id":"t1"}`, complete: true,
		},
		{
			name: "partial insert", raw: `{"type":"INSERT","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t1"}]}`,
			wantKey: `{"tenant_id":"t1"}`, wantReason: core.RecordIdentityReasonKeyColumnMissing,
		},
		{
			name: "update missing old", raw: `{"type":"UPDATE","pkNames":["tenant_id","id"],"data":[{"tenant_id":"t1","id":"n1"}]}`,
			wantKey: `{"id":"n1","tenant_id":"t1"}`, wantReason: core.RecordIdentityReasonBeforeStateInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := core.Record{}
			data := map[string]any{}
			if !tryCanalJSON([]byte(test.raw), &rec, data) {
				t.Fatal("valid Canal DML was treated as a syntax parse failure")
			}
			rec.Data = data
			if rec.Metadata.Key != test.wantKey {
				t.Fatalf("key = %s, want %s", rec.Metadata.Key, test.wantKey)
			}
			identity := core.RecordIdentity(rec)
			if identity.Complete != test.complete || identity.Reason != test.wantReason {
				t.Fatalf("identity = %+v, want complete=%v reason=%s", identity, test.complete, test.wantReason)
			}
		})
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

func TestKafkaParseErrorDLQPolicyEmitsPositionedRecordRejection(t *testing.T) {
	src := &KafkaSource{name: "kafka", topic: "canal", format: "canal_json", onParseError: "dlq"}
	reader := &kafkaReader{
		source: src, records: make(chan core.Record, 1), errors: make(chan error, 1), done: make(chan struct{}),
		offsets: make(map[int32]int64), committedOffsets: make(map[int32]int64), sessions: make(map[int32]sarama.ConsumerGroupSession),
	}
	session := newFakeSession()
	defer session.cancel()
	claim := &fakeConsumerGroupClaim{ch: make(chan *sarama.ConsumerMessage, 1)}
	claim.ch <- &sarama.ConsumerMessage{Topic: "canal", Partition: 2, Offset: 17, Value: []byte("not-json")}
	close(claim.ch)

	if err := (&kafkaHandler{reader: reader}).ConsumeClaim(session, claim); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}
	select {
	case rec := <-reader.records:
		if rec.Rejection == nil || rec.Rejection.Code != "kafka_message_parse_failed" || rec.Rejection.Class != core.ErrorClassData {
			t.Fatalf("rejection = %#v", rec.Rejection)
		}
		if rec.Metadata.Partition != 2 || rec.Metadata.Offset != 17 || rec.Data["value"] != "not-json" {
			t.Fatalf("positioned rejected record = %#v", rec)
		}
	case <-time.After(time.Second):
		t.Fatal("parse rejection was not emitted to the runner")
	}
	select {
	case err := <-reader.errors:
		t.Fatalf("parse rejection leaked into connection-error channel: %v", err)
	default:
	}
}

func TestPrimaryKeyColumnsFromKafkaKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "schemaless composite", key: `{"tenant_id":"t1","id":7}`, want: "id,tenant_id"},
		{name: "schemaful Debezium", key: `{"schema":{"type":"struct","fields":[{"field":"tenant_id","type":"string"},{"field":"id","type":"int64"}]},"payload":{"tenant_id":"t1","id":7}}`, want: "tenant_id,id"},
		{name: "scalar", key: `7`, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := primaryKeyColumnsFromKafkaKey([]byte(test.key))
			joined := ""
			for i, column := range got {
				if i > 0 {
					joined += ","
				}
				joined += column
			}
			if joined != test.want {
				t.Fatalf("columns = %q, want %q", joined, test.want)
			}
		})
	}
}

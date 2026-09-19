package sink

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// CH-C3 (IT-5/T5.3): produced messages carry the SOURCE event time.
func TestKafkaSinkUsesSourceTimestamp(t *testing.T) {
	producer := &captureKafkaProducer{}
	s := &KafkaSink{topic: "t", producer: producer, useSourceTimestamp: true}
	srcTS := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rec := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"id": 1},
		Metadata:  core.Metadata{Timestamp: srcTS},
	}
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !producer.messages[0].Timestamp.Equal(srcTS) {
		t.Fatalf("message timestamp = %v, want source %v", producer.messages[0].Timestamp, srcTS)
	}

	// Zero-value source timestamp falls back to the write clock.
	producer2 := &captureKafkaProducer{}
	s2 := &KafkaSink{topic: "t", producer: producer2, useSourceTimestamp: true}
	if err := s2.Write(context.Background(), []core.Record{{Operation: core.OpInsert, Data: map[string]any{"id": 1}}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if producer2.messages[0].Timestamp.IsZero() {
		t.Fatal("zero source timestamp must fall back to write clock, not zero")
	}

	// Opt-out keeps the write clock even with a source timestamp present.
	producer3 := &captureKafkaProducer{}
	s3 := &KafkaSink{topic: "t", producer: producer3, useSourceTimestamp: false}
	before := time.Now()
	if err := s3.Write(context.Background(), []core.Record{{Operation: core.OpInsert, Data: map[string]any{"id": 1},
		Metadata: core.Metadata{Timestamp: srcTS}}}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if producer3.messages[0].Timestamp.Equal(srcTS) {
		t.Fatal("use_source_timestamp=false must not use source event time")
	}
	if producer3.messages[0].Timestamp.Before(before.Add(-time.Second)) {
		t.Fatal("write-clock fallback expected")
	}
}

// Headers forward; internal "__" markers filtered; oversized rejected.
func TestKafkaSinkHeaderPassthrough(t *testing.T) {
	producer := &captureKafkaProducer{}
	s := &KafkaSink{topic: "t", producer: producer, passHeaders: true, maxHeaderBytes: 1024}
	rec := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"id": 1},
		Metadata: core.Metadata{
			Headers: map[string][]byte{
				"trace-id": []byte("abc123"),
				"tenant":   []byte("acme"),
				"__dlq":    []byte("internal"),
			},
		},
	}
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	msg := producer.messages[0]
	got := map[string]string{}
	for _, h := range msg.Headers {
		got[string(h.Key)] = string(h.Value)
	}
	if got["trace-id"] != "abc123" || got["tenant"] != "acme" {
		t.Fatalf("headers forwarded = %#v", got)
	}
	if _, internal := got["__dlq"]; internal {
		t.Fatalf("internal __ header must be filtered: %#v", got)
	}
	if len(got) != 2 {
		t.Fatalf("expected exactly 2 forwarded headers, got %#v", got)
	}

	// Oversized headers -> explicit error (record goes to DLQ, not silently
	// bloating the producer).
	over := &KafkaSink{topic: "t", producer: &captureKafkaProducer{}, passHeaders: true, maxHeaderBytes: 8}
	big := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1},
		Metadata: core.Metadata{Headers: map[string][]byte{"big": make([]byte, 64)}}}
	if err := over.Write(context.Background(), []core.Record{big}); err == nil || !strings.Contains(err.Error(), "max_header_bytes") {
		t.Fatalf("oversized headers must error explicitly, got %v", err)
	}

	// pass_headers=false drops everything without error.
	off := &KafkaSink{topic: "t", producer: producer, passHeaders: false}
	producer.messages = nil
	if err := off.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(producer.messages[0].Headers) != 0 {
		t.Fatalf("pass_headers=false must not forward headers")
	}
}

// Metadata.Headers round-trips through JSON (checkpoint/DLQ envelope compat:
// old payloads without the field deserialize with nil Headers).
func TestMetadataHeadersJSONRoundTrip(t *testing.T) {
	m := core.Metadata{
		Source: "kafka-src", Table: "t", Partition: 1, Offset: 9,
		Headers: map[string][]byte{"h1": []byte("v1"), "h2": []byte{0, 1, 2}},
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back core.Metadata
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(back.Headers["h1"]) != "v1" || len(back.Headers["h2"]) != 3 {
		t.Fatalf("headers round-trip = %#v", back.Headers)
	}

	// Legacy payload without headers field.
	var legacy core.Metadata
	if err := json.Unmarshal([]byte(`{"source":"x","table":"t"}`), &legacy); err != nil {
		t.Fatalf("legacy unmarshal: %v", err)
	}
	if legacy.Headers != nil {
		t.Fatalf("legacy payload must leave Headers nil, got %#v", legacy.Headers)
	}
}

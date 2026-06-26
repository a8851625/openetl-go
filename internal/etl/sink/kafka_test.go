package sink

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestKafkaSinkWriteSendsEnvelopeAndRecordsMetrics(t *testing.T) {
	producer := &captureKafkaProducer{}
	s := &KafkaSink{
		name:      "kafka",
		topic:     "raw-device-ods",
		keyColumn: "event_id",
		producer:  producer,
	}
	ts := time.Date(2026, 6, 26, 10, 0, 0, 0, time.UTC)
	rec := core.Record{
		Operation: core.OpInsert,
		Data: map[string]any{
			"event_id":     "dev-1:2026-06-26T10:00:00Z:speed",
			"metric_type":  "speed",
			"metric_value": 12.5,
		},
		Metadata: core.Metadata{
			Table:     "raw_device_ods",
			Key:       "device-meta-key",
			Timestamp: ts,
			Partition: 0,
			Offset:    7,
		},
	}

	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if len(producer.messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(producer.messages))
	}
	msg := producer.messages[0]
	if msg.Topic != "raw-device-ods" {
		t.Fatalf("topic = %q, want raw-device-ods", msg.Topic)
	}
	key, err := msg.Key.Encode()
	if err != nil {
		t.Fatalf("encode key: %v", err)
	}
	if string(key) != "dev-1:2026-06-26T10:00:00Z:speed" {
		t.Fatalf("key = %q", string(key))
	}
	value, err := msg.Value.Encode()
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}
	var env kafkaEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.EventID != deriveKafkaEventID(rec) {
		t.Fatalf("event_id = %q, want %q", env.EventID, deriveKafkaEventID(rec))
	}
	if env.Op != string(core.OpInsert) || env.Table != "raw_device_ods" || env.Key != "device-meta-key" {
		t.Fatalf("unexpected envelope metadata: %#v", env)
	}
	if env.Timestamp != ts.Format(time.RFC3339Nano) {
		t.Fatalf("timestamp = %q, want %q", env.Timestamp, ts.Format(time.RFC3339Nano))
	}
	if env.Data["metric_type"] != "speed" {
		t.Fatalf("data.metric_type = %#v", env.Data["metric_type"])
	}

	metrics := s.SinkMetrics()
	if metrics.RowsWritten != 1 || metrics.BatchesSent != 1 || metrics.Errors != 0 {
		t.Fatalf("metrics rows/batches/errors = %d/%d/%d, want 1/1/0", metrics.RowsWritten, metrics.BatchesSent, metrics.Errors)
	}
}

func TestKafkaSinkWriteReturnsProducerErrorAndRecordsFailureMetric(t *testing.T) {
	writeErr := errors.New("injected kafka write failure")
	producer := &captureKafkaProducer{err: writeErr}
	s := &KafkaSink{
		name:      "kafka",
		topic:     "raw-device-ods",
		keyColumn: "event_id",
		producer:  producer,
	}

	err := s.Write(context.Background(), []core.Record{{
		Operation: core.OpInsert,
		Data: map[string]any{
			"event_id": "dev-1:2026-06-26T10:00:00Z:speed",
		},
		Metadata: core.Metadata{Table: "raw_device_ods", Offset: 7},
	}})

	if !errors.Is(err, writeErr) {
		t.Fatalf("Write error = %v, want %v", err, writeErr)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(producer.messages))
	}
	metrics := s.SinkMetrics()
	if metrics.RowsWritten != 0 || metrics.BatchesSent != 0 || metrics.Errors != 1 {
		t.Fatalf("metrics rows/batches/errors = %d/%d/%d, want 0/0/1", metrics.RowsWritten, metrics.BatchesSent, metrics.Errors)
	}
}

type captureKafkaProducer struct {
	messages []*sarama.ProducerMessage
	err      error
	closed   bool
}

func (p *captureKafkaProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	p.messages = append(p.messages, msgs...)
	return p.err
}

func (p *captureKafkaProducer) Close() error {
	p.closed = true
	return nil
}

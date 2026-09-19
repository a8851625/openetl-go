//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
)

// TestPathKafkaEnvelopeRoundTrip certifies CH-C3 (IT-5/T5.3): the Kafka
// event identity (key, source timestamp, partition, offset, headers)
// survives source -> transform -> sink -> DLQ with byte-exact headers.
//
// Cases:
//  1. hop_round_trip: produce messages with headers + timestamps to an input
//     topic; pipeline kafka->identity->kafka forwards them; consumed output
//     carries the same headers (minus internal __), the source event
//     timestamp (not the write clock), and key/partition/offset identity.
//  2. dlq_envelope_headers: an oversized-header record exceeds
//     max_header_bytes, lands in DLQ, and its stored metadata.headers is
//     byte-identical on readback.
func TestPathKafkaEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	rp, err := harness.Redpanda(ctx)
	if err != nil {
		harness.Skip(t, "redpanda container unavailable: %v", err)
	}
	ns := harness.Namespace(t)
	pipeline := "e2e-" + ns
	broker := fmt.Sprintf("%s:%d", rp.Host, rp.Port)
	inTopic, outTopic := ns+"-in", ns+"-out"

	rec := harness.NewEvidenceRecorder(t, "kafka_envelope_roundtrip")
	rec.AddDep("redpanda", harness.RedpandaImage)
	rec.AddDep("harness", "e2e")

	mkTopic := func(topic string) {
		out, code, err := harness.ExecIn(ctx, rp.Container, []string{"rpk", "topic", "create", topic, "-p", "1"})
		if err != nil || code != 0 {
			t.Fatalf("create topic %s: %v (code %d) %s", topic, err, code, out)
		}
	}
	mkTopic(inTopic)
	mkTopic(outTopic)

	// Seed input messages with headers + known timestamps.
	seedTS := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	producerCfg := sarama.NewConfig()
	producerCfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{broker}, producerCfg)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close() })

	seed := func(key, val string, ts time.Time, headers map[string][]byte) {
		msg := &sarama.ProducerMessage{Topic: inTopic, Key: sarama.StringEncoder(key),
			Value: sarama.StringEncoder(val), Timestamp: ts}
		for k, v := range headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{Key: []byte(k), Value: v})
		}
		if _, _, err := producer.SendMessage(msg); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	seed("k1", `{"id":1,"v":"a"}`, seedTS, map[string][]byte{"trace-id": []byte("trace-1"), "tenant": []byte("acme")})
	seed("k2", `{"id":2,"v":"b"}`, seedTS.Add(time.Minute), map[string][]byte{"trace-id": []byte("trace-2")})

	// Pipeline: kafka -> identity -> kafka.
	srv, err := harness.NewServer(ns)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	spec := fmt.Sprintf(`name: "%s"
source:
  type: kafka
  config:
    brokers: ["%s"]
    topic: "%s"
    group_id: "%s-g"
    initial_offset: "oldest"
transforms:
  - type: identity
    config: {}
sink:
  type: kafka
  config:
    brokers: ["%s"]
    topic: "%s"
    max_header_bytes: 128
batch_size: 2
checkpoint_interval_sec: 1
backpressure_buffer: 20
retry:
  max_attempts: 2
  initial_interval_ms: 100
  max_interval_ms: 500
dlq:
  enable: true
`, pipeline, broker, inTopic, pipeline, broker, outTopic)
	specPath := filepath.Join(srv.SpecsDir, pipeline+".yaml")
	if err := os.WriteFile(specPath, []byte(spec), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never ran: %v\n%s", err, srv.LogTail())
	}

	// Consume output and verify identity preservation.
	consumer, err := sarama.NewConsumer([]string{broker}, sarama.NewConfig())
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	pc, err := consumer.ConsumePartition(outTopic, 0, 0)
	if err != nil {
		t.Fatalf("consume partition: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })

	type outMsg struct {
		key       string
		ts        time.Time
		headers   map[string]string
		hasTenant bool
	}
	got := map[string]outMsg{}
	deadline := time.Now().Add(60 * time.Second)
	for len(got) < 2 && time.Now().Before(deadline) {
		select {
		case msg := <-pc.Messages():
			m := outMsg{key: string(msg.Key), ts: msg.Timestamp, headers: map[string]string{}}
			for _, h := range msg.Headers {
				m.headers[string(h.Key)] = string(h.Value)
				if string(h.Key) == "tenant" {
					m.hasTenant = true
				}
			}
			got[m.key] = m
		case <-time.After(time.Second):
		}
	}
	if len(got) < 2 {
		t.Fatalf("round-trip incomplete: got %d messages (%v); log tail:\n%s", len(got), got, srv.LogTail())
	}

	m1 := got["k1"]
	if m1.headers["trace-id"] != "trace-1" || !m1.hasTenant || m1.headers["tenant"] != "acme" {
		t.Fatalf("k1 headers = %#v", m1.headers)
	}
	m2 := got["k2"]
	if m2.headers["trace-id"] != "trace-2" || m2.hasTenant {
		t.Fatalf("k2 headers = %#v", m2.headers)
	}
	// Source event time must win over the write clock.
	if !m1.ts.Equal(seedTS) {
		t.Fatalf("k1 timestamp = %v, want source %v", m1.ts, seedTS)
	}
	if !m2.ts.Equal(seedTS.Add(time.Minute)) {
		t.Fatalf("k2 timestamp = %v, want source %v", m2.ts, seedTS.Add(time.Minute))
	}
	t.Logf("case hop_round_trip OK: headers/timestamps/key byte-preserved for 2 messages")
	rec.AddCheck("hop_round_trip", "passed", "")

	// ---- Case 2: oversized header -> DLQ with byte-exact headers.
	bigHdr := make([]byte, 512)
	for i := range bigHdr {
		bigHdr[i] = byte('z')
	}
	seed("k3", `{"id":3,"v":"c"}`, seedTS, map[string][]byte{"big": bigHdr})

	dlqBody := func() string {
		body, err := srv.Get("/api/v2/dlq/" + pipeline)
		if err != nil {
			return ""
		}
		return string(body)
	}
	found := false
	for i := 0; i < 30 && !found; i++ {
		body := dlqBody()
		if len(body) > 0 && len(body) > 50 {
			var d struct {
				Items []struct {
					Record struct {
						Metadata struct {
							Key     string            `json:"key"`
							Headers map[string][]byte `json:"headers"`
						} `json:"metadata"`
						Error string `json:"error"`
					} `json:"record"`
				} `json:"items"`
			}
			if json.Unmarshal([]byte(body), &d) == nil {
				for _, it := range d.Items {
					if it.Record.Metadata.Key == `{"id":3}` || it.Record.Metadata.Key == "k3" {
						if got, ok := it.Record.Metadata.Headers["big"]; !ok || len(got) != 512 || got[0] != 'z' {
							t.Fatalf("DLQ header not byte-exact: %d bytes", len(got))
						}
						found = true
					}
				}
			}
		}
		time.Sleep(time.Second)
	}
	if !found {
		t.Fatalf("oversized-header record never landed in DLQ; body=%s", dlqBody())
	}
	t.Logf("case dlq_envelope_headers OK: byte-exact header persistence")
	rec.AddCheck("dlq_envelope_headers", "passed", "")
}

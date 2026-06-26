package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

func TestPipelineCheckpointSetKeepsRawPositionCompatibility(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	body := []byte(`{"position":{"file":"mysql-bin.000001","pos":123}}`)
	resp, err := http.Post(ts.URL+"/api/v2/pipelines/raw-job/checkpoint/set", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST checkpoint set: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	cp, err := s.cpAdapter.Load(resp.Request.Context(), "raw-job")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if cp == nil || string(cp.Position) != `{"file":"mysql-bin.000001","pos":123}` {
		t.Fatalf("checkpoint = %#v", cp)
	}
}

func TestPipelineCheckpointSetKafkaReplayFromOffset(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	body := []byte(`{"source":"kafka","topic":"debezium.orders","partition":0,"offset":42}`)
	resp, err := http.Post(ts.URL+"/api/v2/pipelines/kafka-job/checkpoint/set", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST checkpoint set: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	cp, err := s.cpAdapter.Load(resp.Request.Context(), "kafka-job")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if cp == nil || cp.Source != "kafka" {
		t.Fatalf("checkpoint = %#v", cp)
	}
	var pos kafkaCheckpointPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("unmarshal checkpoint position: %v", err)
	}
	if pos.Topic != "debezium.orders" || pos.Offsets[0] != 41 {
		t.Fatalf("position = %#v, want replay offset 42 stored as committed offset 41", pos)
	}
}

func TestPipelineCheckpointSetKafkaOffsetsInferTopicFromSpec(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	s.mu.Lock()
	s.specs["saved-kafka"] = &pipeline.Spec{
		Name:   "saved-kafka",
		Source: pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"topic": "orders.cdc"}},
	}
	s.mu.Unlock()

	body := []byte(`{"source":"kafka","mode":"last_committed","offsets":{"0":100,"2":205}}`)
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/pipelines/saved-kafka/checkpoint/set", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT checkpoint set: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	cp, err := s.cpAdapter.Load(req.Context(), "saved-kafka")
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	var pos kafkaCheckpointPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("unmarshal checkpoint position: %v", err)
	}
	if pos.Topic != "orders.cdc" || pos.Offsets[0] != 100 || pos.Offsets[2] != 205 {
		t.Fatalf("position = %#v", pos)
	}
}

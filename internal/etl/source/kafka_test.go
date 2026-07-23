package source

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

// fakeConsumerGroupClaim implements sarama.ConsumerGroupClaim for tests.
type fakeConsumerGroupClaim struct {
	ch     chan *sarama.ConsumerMessage
	offset int64
}

func (f *fakeConsumerGroupClaim) Topic() string                            { return "test" }
func (f *fakeConsumerGroupClaim) Partition() int32                         { return 0 }
func (f *fakeConsumerGroupClaim) InitialOffset() int64                     { return f.offset }
func (f *fakeConsumerGroupClaim) HighWaterMarkOffset() int64               { return 1 << 30 }
func (f *fakeConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage { return f.ch }
func (f *fakeConsumerGroupClaim) IsEmpty() bool                            { return false }

// fakeSession implements the methods we use from sarama.ConsumerGroupSession.
type fakeSession struct {
	mu         sync.Mutex
	marked     map[int32]int64
	reset      map[int32]int64
	committed  bool
	ctx        context.Context
	cancelOnce sync.Once
	cancel     context.CancelFunc
	claims     map[string][]int32
}

func newFakeSession() *fakeSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeSession{
		marked: make(map[int32]int64),
		reset:  make(map[int32]int64),
		ctx:    ctx,
		cancel: cancel,
		claims: map[string][]int32{"test": {0}},
	}
}

func (s *fakeSession) Claims() map[string][]int32 { return s.claims }
func (s *fakeSession) MemberID() string           { return "test-member" }
func (s *fakeSession) GenerationID() int32        { return 1 }
func (s *fakeSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked[partition] = offset
}
func (s *fakeSession) Commit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.committed = true
}
func (s *fakeSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reset[partition] = offset
}
func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked[msg.Partition] = msg.Offset + 1
}
func (s *fakeSession) Context() context.Context { return s.ctx }

// TestKafkaHandlerDoesNotMarkOnConsume verifies at-least-once: ConsumeClaim
// must NOT mark the message as consumed. The pipeline only commits after the
// sink persists the batch.
func TestKafkaHandlerDoesNotMarkOnConsume(t *testing.T) {
	src := &KafkaSource{
		name:      "kafka",
		topic:     "test",
		keyColumn: "raw_key",
	}
	reader := &kafkaReader{
		source:           src,
		records:          make(chan core.Record, 8),
		offsets:          make(map[int32]int64),
		committedOffsets: make(map[int32]int64),
		sessions:         make(map[int32]sarama.ConsumerGroupSession),
	}
	sess := newFakeSession()
	defer sess.cancel()

	handler := &kafkaHandler{reader: reader}
	// Register session so CheckpointForRecord can use it.
	reader.mu.Lock()
	reader.sessions[0] = sess
	reader.mu.Unlock()

	claim := &fakeConsumerGroupClaim{ch: make(chan *sarama.ConsumerMessage, 4)}
	claim.ch <- &sarama.ConsumerMessage{
		Topic:     "test",
		Partition: 0,
		Offset:    42,
		Key:       []byte(`{"id":42}`),
		Value:     []byte(`{"k":"v"}`),
	}
	close(claim.ch)

	if err := handler.ConsumeClaim(sess, claim); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}

	// Verify: no offset marked during ConsumeClaim (at-least-once contract).
	sess.mu.Lock()
	if len(sess.marked) != 0 {
		t.Errorf("ConsumeClaim marked offset during consume: %v (want none until commit)", sess.marked)
	}
	sess.mu.Unlock()

	// Verify record was delivered.
	select {
	case rec := <-reader.records:
		if rec.Metadata.Offset != 42 {
			t.Errorf("delivered offset = %d, want 42", rec.Metadata.Offset)
		}
		if rec.Metadata.Key != `{"id":42}` {
			t.Errorf("metadata key = %q, want Debezium key JSON", rec.Metadata.Key)
		}
		if got, _ := rec.Data["raw_key"].(string); got != `{"id":42}` {
			t.Errorf("data raw_key = %q, want key_column copy", got)
		}
	case <-time.After(time.Second):
		t.Fatal("record not delivered")
	}
}

// TestKafkaCheckpointForRecordMarksOffset verifies that after pipeline commit,
// calling CheckpointForRecord marks the NEXT offset so a restart resumes after
// the committed record.
func TestKafkaCheckpointForRecordMarksOffset(t *testing.T) {
	src := &KafkaSource{name: "kafka", topic: "test"}
	reader := &kafkaReader{
		source:           src,
		records:          make(chan core.Record, 4),
		offsets:          make(map[int32]int64),
		committedOffsets: make(map[int32]int64),
		sessions:         make(map[int32]sarama.ConsumerGroupSession),
	}
	sess := newFakeSession()
	defer sess.cancel()
	reader.sessions[0] = sess

	rec := core.Record{
		Metadata: core.Metadata{Partition: 0, Offset: 100},
	}
	cp, err := reader.CheckpointForRecord(context.Background(), rec)
	if err != nil {
		t.Fatalf("CheckpointForRecord: %v", err)
	}
	if len(cp.Position) == 0 {
		t.Fatal("empty checkpoint position")
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.marked[0] != 101 {
		t.Errorf("marked offset = %d, want 101 (offset+1)", sess.marked[0])
	}
	if !sess.committed {
		t.Error("Commit not called after CheckpointForRecord")
	}
}

func TestKafkaCheckpointForRecordPersistsOffsetZero(t *testing.T) {
	src := &KafkaSource{name: "kafka", topic: "test"}
	reader := &kafkaReader{
		source:           src,
		records:          make(chan core.Record, 1),
		offsets:          make(map[int32]int64),
		committedOffsets: make(map[int32]int64),
		sessions:         make(map[int32]sarama.ConsumerGroupSession),
	}

	cp, err := reader.CheckpointForRecord(context.Background(), core.Record{
		Metadata: core.Metadata{Partition: 0, Offset: 0},
	})
	if err != nil {
		t.Fatalf("CheckpointForRecord: %v", err)
	}
	var pos kafkaPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("unmarshal position: %v", err)
	}
	if offset, ok := pos.Offsets[0]; !ok || offset != 0 {
		t.Fatalf("offsets = %#v, want explicit partition 0 offset 0", pos.Offsets)
	}
}

func TestKafkaSetupResetsStartOffsetsFromCheckpoint(t *testing.T) {
	src := &KafkaSource{name: "kafka", topic: "test"}
	reader := &kafkaReader{
		source:       src,
		startOffsets: map[int32]int64{0: 40, 1: 99},
		sessions:     make(map[int32]sarama.ConsumerGroupSession),
	}
	sess := newFakeSession()
	defer sess.cancel()
	sess.claims = map[string][]int32{"test": {0, 1}}

	handler := &kafkaHandler{reader: reader}
	if err := handler.Setup(sess); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.reset[0] != 41 || sess.reset[1] != 100 {
		t.Fatalf("reset offsets = %#v, want partition 0->41 and 1->100", sess.reset)
	}
	if sess.marked[0] != 41 || sess.marked[1] != 100 {
		t.Fatalf("marked offsets = %#v, want partition 0->41 and 1->100", sess.marked)
	}
}

// TestKafkaSnapshotPersistsOffsets verifies Snapshot returns the highest
// delivered offset per partition.
func TestKafkaSnapshotPersistsOffsets(t *testing.T) {
	src := &KafkaSource{name: "kafka", topic: "test"}
	reader := &kafkaReader{
		source:           src,
		offsets:          map[int32]int64{0: 100, 1: 200, 2: 300},
		committedOffsets: map[int32]int64{0: 100, 1: 200, 2: 300},
		sessions:         make(map[int32]sarama.ConsumerGroupSession),
	}
	cp, err := reader.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	var pos kafkaPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("unmarshal position: %v", err)
	}
	if pos.Offsets[0] != 100 || pos.Offsets[1] != 200 || pos.Offsets[2] != 300 {
		t.Errorf("snapshot offsets = %v, want {0:100 1:200 2:300}", pos.Offsets)
	}
}

// TestKafkaConfigParsing verifies config knobs are parsed.
func TestKafkaConfigParsing(t *testing.T) {
	s, err := NewKafkaSource(map[string]any{
		"brokers":        []interface{}{"b1:9092", "b2:9092"},
		"topic":          "events",
		"group_id":       "etl-1",
		"format":         "json",
		"key_column":     "k",
		"value_column":   "payload",
		"initial_offset": "oldest",
	})
	if err != nil {
		t.Fatalf("NewKafkaSource: %v", err)
	}
	if len(s.brokers) != 2 || s.brokers[0] != "b1:9092" {
		t.Errorf("brokers = %v", s.brokers)
	}
	if s.topic != "events" || s.groupID != "etl-1" {
		t.Errorf("topic/group = %q/%q", s.topic, s.groupID)
	}
	if s.initialOffset != "oldest" {
		t.Errorf("initialOffset = %q, want oldest", s.initialOffset)
	}
}

// TestKafkaFetchConfigDefaults verifies omitted fetch knobs keep Sarama-compatible
// defaults so existing pipelines are unchanged.
func TestKafkaFetchConfigDefaults(t *testing.T) {
	s, err := NewKafkaSource(map[string]any{
		"brokers": []any{"localhost:9092"},
		"topic":   "events",
	})
	if err != nil {
		t.Fatalf("NewKafkaSource: %v", err)
	}
	if s.fetchMinBytes != 1 {
		t.Errorf("fetchMinBytes = %d, want 1", s.fetchMinBytes)
	}
	if s.fetchMaxBytes != 1024*1024 {
		t.Errorf("fetchMaxBytes = %d, want 1048576", s.fetchMaxBytes)
	}
	if s.fetchMaxWaitMs != 500 {
		t.Errorf("fetchMaxWaitMs = %d, want 500", s.fetchMaxWaitMs)
	}
	if s.channelBufferSize != 256 {
		t.Errorf("channelBufferSize = %d, want 256", s.channelBufferSize)
	}
	if s.maxProcessingTimeMs != 100 {
		t.Errorf("maxProcessingTimeMs = %d, want 100", s.maxProcessingTimeMs)
	}
	if s.maxOpenRequests != 5 {
		t.Errorf("maxOpenRequests = %d, want 5", s.maxOpenRequests)
	}

	cfg, err := s.buildSaramaConfig()
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}
	if cfg.Consumer.Fetch.Min != 1 {
		t.Errorf("Fetch.Min = %d, want 1", cfg.Consumer.Fetch.Min)
	}
	if cfg.Consumer.Fetch.Default != 1024*1024 {
		t.Errorf("Fetch.Default = %d, want 1048576", cfg.Consumer.Fetch.Default)
	}
	if cfg.Consumer.MaxWaitTime != 500*time.Millisecond {
		t.Errorf("MaxWaitTime = %v, want 500ms", cfg.Consumer.MaxWaitTime)
	}
	if cfg.ChannelBufferSize != 256 {
		t.Errorf("ChannelBufferSize = %d, want 256", cfg.ChannelBufferSize)
	}
	if cfg.Consumer.MaxProcessingTime != 100*time.Millisecond {
		t.Errorf("MaxProcessingTime = %v, want 100ms", cfg.Consumer.MaxProcessingTime)
	}
	if cfg.Net.MaxOpenRequests != 5 {
		t.Errorf("MaxOpenRequests = %d, want 5", cfg.Net.MaxOpenRequests)
	}
}

// TestKafkaFetchConfigAppliedToSarama verifies YAML/config knobs are parsed and
// applied to the underlying sarama.Config before Validate().
func TestKafkaFetchConfigAppliedToSarama(t *testing.T) {
	s, err := NewKafkaSource(map[string]any{
		"brokers":                 []any{"b1:9092"},
		"topic":                   "cdc-events",
		"fetch_min_bytes":         2048,
		"fetch_max_bytes":         4 * 1024 * 1024,
		"fetch_max_wait_ms":        250,
		"channel_buffer_size":     512,
		"max_processing_time_ms":  1000,
		"max_open_requests":       3,
	})
	if err != nil {
		t.Fatalf("NewKafkaSource: %v", err)
	}
	if s.fetchMinBytes != 2048 || s.fetchMaxBytes != 4*1024*1024 {
		t.Fatalf("parsed fetch bytes = %d/%d", s.fetchMinBytes, s.fetchMaxBytes)
	}
	if s.fetchMaxWaitMs != 250 || s.channelBufferSize != 512 {
		t.Fatalf("parsed wait/buffer = %d/%d", s.fetchMaxWaitMs, s.channelBufferSize)
	}
	if s.maxProcessingTimeMs != 1000 || s.maxOpenRequests != 3 {
		t.Fatalf("parsed processing/open = %d/%d", s.maxProcessingTimeMs, s.maxOpenRequests)
	}

	cfg, err := s.buildSaramaConfig()
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}
	if cfg.Consumer.Fetch.Min != 2048 {
		t.Errorf("Fetch.Min = %d, want 2048", cfg.Consumer.Fetch.Min)
	}
	if cfg.Consumer.Fetch.Default != 4*1024*1024 {
		t.Errorf("Fetch.Default = %d, want 4194304", cfg.Consumer.Fetch.Default)
	}
	if cfg.Consumer.MaxWaitTime != 250*time.Millisecond {
		t.Errorf("MaxWaitTime = %v, want 250ms", cfg.Consumer.MaxWaitTime)
	}
	if cfg.ChannelBufferSize != 512 {
		t.Errorf("ChannelBufferSize = %d, want 512", cfg.ChannelBufferSize)
	}
	if cfg.Consumer.MaxProcessingTime != 1000*time.Millisecond {
		t.Errorf("MaxProcessingTime = %v, want 1000ms", cfg.Consumer.MaxProcessingTime)
	}
	if cfg.Net.MaxOpenRequests != 3 {
		t.Errorf("MaxOpenRequests = %d, want 3", cfg.Net.MaxOpenRequests)
	}
}

// TestKafkaFetchConfigYAMLParsing verifies YAML-decoded numeric types (float64)
// are accepted for the fetch knobs, matching real pipeline spec loading.
func TestKafkaFetchConfigYAMLParsing(t *testing.T) {
	// YAML unmarshals integers into map[string]any as float64.
	s, err := NewKafkaSource(map[string]any{
		"brokers":                []any{"b1:9092"},
		"topic":                  "events",
		"fetch_min_bytes":        float64(4096),
		"fetch_max_bytes":        float64(2 * 1024 * 1024),
		"fetch_max_wait_ms":       float64(100),
		"channel_buffer_size":    float64(128),
		"max_processing_time_ms": float64(200),
		"max_open_requests":      float64(10),
	})
	if err != nil {
		t.Fatalf("NewKafkaSource: %v", err)
	}
	if s.fetchMinBytes != 4096 {
		t.Errorf("fetchMinBytes = %d, want 4096", s.fetchMinBytes)
	}
	if s.fetchMaxBytes != 2*1024*1024 {
		t.Errorf("fetchMaxBytes = %d, want 2097152", s.fetchMaxBytes)
	}
	if s.fetchMaxWaitMs != 100 {
		t.Errorf("fetchMaxWaitMs = %d, want 100", s.fetchMaxWaitMs)
	}
	if s.channelBufferSize != 128 {
		t.Errorf("channelBufferSize = %d, want 128", s.channelBufferSize)
	}
	if s.maxProcessingTimeMs != 200 {
		t.Errorf("maxProcessingTimeMs = %d, want 200", s.maxProcessingTimeMs)
	}
	if s.maxOpenRequests != 10 {
		t.Errorf("maxOpenRequests = %d, want 10", s.maxOpenRequests)
	}

	cfg, err := s.buildSaramaConfig()
	if err != nil {
		t.Fatalf("buildSaramaConfig: %v", err)
	}
	if cfg.Consumer.Fetch.Min != 4096 || cfg.Consumer.Fetch.Default != 2*1024*1024 {
		t.Fatalf("sarama fetch = %d/%d", cfg.Consumer.Fetch.Min, cfg.Consumer.Fetch.Default)
	}
	if cfg.Consumer.MaxWaitTime != 100*time.Millisecond {
		t.Fatalf("MaxWaitTime = %v", cfg.Consumer.MaxWaitTime)
	}
	if cfg.ChannelBufferSize != 128 || cfg.Net.MaxOpenRequests != 10 {
		t.Fatalf("buffer/open = %d/%d", cfg.ChannelBufferSize, cfg.Net.MaxOpenRequests)
	}
	if cfg.Consumer.MaxProcessingTime != 200*time.Millisecond {
		t.Fatalf("MaxProcessingTime = %v", cfg.Consumer.MaxProcessingTime)
	}
}

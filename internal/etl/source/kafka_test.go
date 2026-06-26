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
		name:  "kafka",
		topic: "test",
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

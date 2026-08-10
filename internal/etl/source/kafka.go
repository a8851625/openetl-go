package source

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/sink"
)

func init() {
	registry.RegisterSource("kafka", func(config map[string]any) (core.Source, error) {
		return NewKafkaSource(config)
	})
}

type KafkaSource struct {
	name          string
	brokers       []string
	topic         string
	groupID       string
	format        string
	keyColumn     string
	valueColumn   string
	initialOffset string

	// Consumer fetch / throughput knobs (mapped to sarama.Config).
	fetchMinBytes       int // Consumer.Fetch.Min
	fetchMaxBytes       int // Consumer.Fetch.Default
	fetchMaxWaitMs      int // Consumer.MaxWaitTime
	channelBufferSize   int // ChannelBufferSize
	maxProcessingTimeMs int // Consumer.MaxProcessingTime
	maxOpenRequests     int // Net.MaxOpenRequests

	// Security
	saslUser      string
	saslPassword  string
	saslMechanism string
	tls           bool
	tlsSkipVerify bool
}

func NewKafkaSource(config map[string]any) (*KafkaSource, error) {
	s := &KafkaSource{
		name:                "kafka",
		format:              "json",
		groupID:             "etl-consumer",
		initialOffset:       "newest",
		fetchMinBytes:       1,           // sarama default
		fetchMaxBytes:       1024 * 1024, // 1MB, sarama default
		fetchMaxWaitMs:      500,         // sarama default (ms)
		channelBufferSize:   256,         // sarama default
		maxProcessingTimeMs: 100,         // sarama default (ms)
		maxOpenRequests:     5,           // sarama default
	}
	if v, ok := config["name"].(string); ok {
		s.name = v
	}
	s.brokers = append(s.brokers, readStringSlice(config, "brokers")...)
	if v, ok := config["topic"].(string); ok {
		s.topic = v
	}
	if v, ok := config["group_id"].(string); ok {
		s.groupID = v
	}
	if v, ok := config["format"].(string); ok {
		s.format = v
	}
	if v, ok := config["key_column"].(string); ok {
		s.keyColumn = v
	}
	if v, ok := config["value_column"].(string); ok {
		s.valueColumn = v
	}
	if v, ok := config["initial_offset"].(string); ok && (v == "oldest" || v == "newest") {
		s.initialOffset = v
	}
	if v, ok := config["sasl_user"].(string); ok {
		s.saslUser = v
	}
	if v, ok := config["sasl_password"].(string); ok {
		s.saslPassword = v
	}
	if v, ok := config["sasl_mechanism"].(string); ok {
		s.saslMechanism = v
	}
	if v, ok := config["tls"].(bool); ok {
		s.tls = v
	}
	if v, ok := config["tls_skip_verify"].(bool); ok {
		s.tlsSkipVerify = v
	}
	// Consumer fetch / throughput knobs. Zero or negative values keep the
	// struct defaults (matching sarama.NewConfig) so existing pipelines stay
	// unchanged when the fields are omitted from YAML.
	if v := readInt(config, "fetch_min_bytes", 0); v > 0 {
		s.fetchMinBytes = v
	}
	if v := readInt(config, "fetch_max_bytes", 0); v > 0 {
		s.fetchMaxBytes = v
	}
	if v := readInt(config, "fetch_max_wait_ms", 0); v > 0 {
		s.fetchMaxWaitMs = v
	}
	if v := readInt(config, "channel_buffer_size", 0); v > 0 {
		s.channelBufferSize = v
	}
	if v := readInt(config, "max_processing_time_ms", 0); v > 0 {
		s.maxProcessingTimeMs = v
	}
	if v := readInt(config, "max_open_requests", 0); v > 0 {
		s.maxOpenRequests = v
	}
	if len(s.brokers) == 0 {
		s.brokers = []string{"localhost:9092"}
	}
	return s, nil
}

func (s *KafkaSource) Name() string { return s.name }

func (s *KafkaSource) buildSaramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// The runner persists its checkpoint before acknowledging the consumer
	// group. Sarama's periodic auto-commit would advance the broker offset
	// while records are still in flight, so it must remain disabled.
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Version = sarama.V2_1_0_0

	if s.initialOffset == "oldest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Apply consumer fetch / throughput knobs before Validate().
	if s.fetchMinBytes > 0 {
		config.Consumer.Fetch.Min = int32(s.fetchMinBytes)
	}
	if s.fetchMaxBytes > 0 {
		config.Consumer.Fetch.Default = int32(s.fetchMaxBytes)
	}
	if s.fetchMaxWaitMs > 0 {
		config.Consumer.MaxWaitTime = time.Duration(s.fetchMaxWaitMs) * time.Millisecond
	}
	if s.channelBufferSize > 0 {
		config.ChannelBufferSize = s.channelBufferSize
	}
	if s.maxProcessingTimeMs > 0 {
		config.Consumer.MaxProcessingTime = time.Duration(s.maxProcessingTimeMs) * time.Millisecond
	}
	if s.maxOpenRequests > 0 {
		config.Net.MaxOpenRequests = s.maxOpenRequests
	}

	if s.saslUser != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = s.saslUser
		config.Net.SASL.Password = s.saslPassword
		switch s.saslMechanism {
		case "SCRAM-SHA-256", "scram-sha-256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return sink.NewSCRAMClient(sha256.New)
			}
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512", "scram-sha-512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return sink.NewSCRAMClient(sha512.New)
			}
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	if s.tls {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: s.tlsSkipVerify,
		}
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid kafka config: %w", err)
	}
	return config, nil
}

func (s *KafkaSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	if err := s.ValidateCheckpoint(ctx, cp); err != nil {
		return nil, err
	}
	config, err := s.buildSaramaConfig()
	if err != nil {
		return nil, err
	}

	group, err := sarama.NewConsumerGroup(s.brokers, s.groupID, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer group (brokers %v, group %s): %w", s.brokers, s.groupID, err) // P5-15: WHERE context
	}

	reader := &kafkaReader{
		source:           s,
		group:            group,
		saramaConfig:     config,
		records:          make(chan core.Record, 1024),
		errors:           make(chan error, 64),
		done:             make(chan struct{}),
		closeOnce:        sync.Once{},
		offsets:          make(map[int32]int64),
		committedOffsets: make(map[int32]int64),
		sessions:         make(map[int32]sarama.ConsumerGroupSession),
		cpInitial:        cp,
	}

	if cp != nil && len(cp.Position) > 0 && string(cp.Position) != "{}" {
		var pos kafkaPosition
		if err := json.Unmarshal(cp.Position, &pos); err == nil && len(pos.Offsets) > 0 {
			reader.startOffsets = pos.Offsets
		}
	}

	handler := &kafkaHandler{reader: reader}

	go func() {
		defer close(reader.records)
		// Reconnect loop: on transient errors, retry after backoff.
		backoff := time.Second
		const maxBackoff = 30 * time.Second
		for {
			select {
			case <-reader.done:
				return
			default:
			}

			consumeErr := group.Consume(ctx, []string{s.topic}, handler)
			if consumeErr != nil {
				if ctx.Err() != nil || reader.isClosed() {
					return
				}
				select {
				case reader.errors <- fmt.Errorf("kafka consume: %v", consumeErr):
				default:
				}
				select {
				case <-time.After(backoff):
				case <-reader.done:
					return
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			if ctx.Err() != nil || reader.isClosed() {
				return
			}
			backoff = time.Second
		}
	}()

	return reader, nil
}

type kafkaHandler struct {
	reader *kafkaReader
}

func (h *kafkaHandler) Setup(sess sarama.ConsumerGroupSession) error {
	h.reader.mu.Lock()
	defer h.reader.mu.Unlock()
	for _, partitions := range sess.Claims() {
		for _, p := range partitions {
			h.reader.sessions[p] = sess
		}
	}
	if h.reader.startOffsets != nil {
		for partition, off := range h.reader.startOffsets {
			sess.ResetOffset(h.reader.source.topic, partition, off+1, "")
			sess.MarkOffset(h.reader.source.topic, partition, off+1, "")
		}
	}
	return nil
}

func (h *kafkaHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.reader.mu.Lock()
	h.reader.sessions = make(map[int32]sarama.ConsumerGroupSession)
	h.reader.mu.Unlock()
	return nil
}

func (h *kafkaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			rec := core.Record{
				Operation: core.OpInsert,
				Metadata: core.Metadata{
					Source:    h.reader.source.name,
					Table:     h.reader.source.topic,
					Timestamp: msg.Timestamp,
					Partition: msg.Partition,
					Offset:    msg.Offset,
				},
			}

			data := make(map[string]any)
			if h.reader.source.keyColumn != "" && msg.Key != nil {
				data[h.reader.source.keyColumn] = string(msg.Key)
			}
			if msg.Key != nil {
				rec.Metadata.Key = string(msg.Key)
			}

			switch {
			case h.reader.source.format == "envelope":
				// OpenETL kafkaEnvelope (as written by the kafka sink):
				// {event_id, op, table, key, data, timestamp}. Restores the
				// original operation/table/row so downstream sinks (e.g. Doris
				// upsert+delete) behave exactly like a direct CDC consumer.
				var env struct {
					EventID     string            `json:"event_id"`
					Op          string            `json:"op"`
					Table       string            `json:"table"`
					Key         string            `json:"key"`
					Data        map[string]any    `json:"data"`
					Timestamp   string            `json:"timestamp"`
					ColumnTypes map[string]string `json:"column_types"`
				}
				if err := json.Unmarshal(msg.Value, &env); err == nil && env.Data != nil {
					switch env.Op {
					case "UPDATE":
						rec.Operation = core.OpUpdate
					case "DELETE":
						rec.Operation = core.OpDelete
					default:
						rec.Operation = core.OpInsert
					}
					if env.Table != "" {
						rec.Metadata.Table = env.Table
					}
					if env.Key != "" {
						rec.Metadata.Key = env.Key
					}
					if len(env.ColumnTypes) > 0 {
						rec.Metadata.ColumnTypes = env.ColumnTypes
					}
					for k, v := range env.Data {
						data[k] = v
					}
				} else {
					data["value"] = string(msg.Value)
				}
			case h.reader.source.format == "json" && h.reader.source.valueColumn == "":
				var parsed map[string]any
				if err := json.Unmarshal(msg.Value, &parsed); err == nil {
					for k, v := range parsed {
						data[k] = v
					}
				} else {
					data["value"] = string(msg.Value)
				}
			case h.reader.source.valueColumn != "":
				data[h.reader.source.valueColumn] = string(msg.Value)
			default:
				data["value"] = string(msg.Value)
			}

			rec.Data = data

			h.reader.mu.Lock()
			if current, ok := h.reader.offsets[msg.Partition]; !ok || msg.Offset > current {
				h.reader.offsets[msg.Partition] = msg.Offset
			}
			h.reader.mu.Unlock()

			select {
			case h.reader.records <- rec:
			case <-session.Context().Done():
				return nil
			case <-h.reader.done:
				return nil
			}
		case <-session.Context().Done():
			return nil
		case <-h.reader.done:
			return nil
		}
	}
}

type kafkaReader struct {
	source           *KafkaSource
	group            sarama.ConsumerGroup
	saramaConfig     *sarama.Config
	records          chan core.Record
	errors           chan error
	done             chan struct{}
	closed           bool
	closeOnce        sync.Once
	mu               sync.Mutex
	offsets          map[int32]int64
	committedOffsets map[int32]int64
	sessions         map[int32]sarama.ConsumerGroupSession
	startOffsets     map[int32]int64
	cpInitial        *core.Checkpoint
}

func (r *kafkaReader) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *kafkaReader) Read(ctx context.Context) (core.Record, error) {
	groupErrors := r.consumerGroupErrors()
	select {
	case rec, ok := <-r.records:
		if !ok {
			return core.Record{}, fmt.Errorf("kafka stream closed")
		}
		return rec, nil
	case err := <-r.errors:
		return core.Record{}, err
	case err, ok := <-groupErrors:
		if !ok || err == nil {
			return core.Record{}, fmt.Errorf("kafka consumer group closed")
		}
		// Broker disconnects are often reported as io.EOF. Do not wrap that
		// sentinel: pipeline.handleReadError treats io.EOF as a finite source
		// completion, while Kafka must reconnect and continue streaming.
		return core.Record{}, fmt.Errorf("kafka consumer group: %v", err)
	case <-ctx.Done():
		return core.Record{}, ctx.Err()
	}
}

func (r *kafkaReader) consumerGroupErrors() <-chan error {
	if r.group == nil {
		return nil
	}
	return r.group.Errors()
}

func (r *kafkaReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	var batch []core.Record
	timeout := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case rec, ok := <-r.records:
			if !ok {
				return batch, nil
			}
			batch = append(batch, rec)
		case <-timeout:
			return batch, nil
		case <-ctx.Done():
			return batch, ctx.Err()
		}
	}
	return batch, nil
}

func (r *kafkaReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	r.mu.Lock()
	snapshot := make(map[int32]int64, len(r.committedOffsets))
	for k, v := range r.committedOffsets {
		snapshot[k] = v
	}
	r.mu.Unlock()

	pos := kafkaPosition{Topic: r.source.topic, Offsets: snapshot}
	raw, err := json.Marshal(pos)
	if err != nil {
		return core.Checkpoint{}, fmt.Errorf("marshal kafka position: %w", err)
	}
	return core.Checkpoint{
		Source:    r.source.name,
		Position:  raw,
		Timestamp: time.Now(),
	}, nil
}

// CheckpointForRecord merges the record's offset into the durable-checkpoint
// candidate map
// so multi-partition batches don't lose other partitions' progress. Only the
// given record's partition advances; other partitions keep their last
// committed value (NOT the read-ahead offset), preventing checkpoint skips.
// It deliberately has no Sarama side effects. AckCheckpoint performs the
// external consumer-group acknowledgement after the checkpoint store returns
// successfully.
func (r *kafkaReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	partition := rec.Metadata.Partition
	offset := rec.Metadata.Offset

	r.mu.Lock()
	if current, ok := r.committedOffsets[partition]; !ok || offset > current {
		r.committedOffsets[partition] = offset
	}
	snapshot := make(map[int32]int64, len(r.committedOffsets))
	for k, v := range r.committedOffsets {
		snapshot[k] = v
	}
	r.mu.Unlock()

	pos := kafkaPosition{
		Topic:   r.source.topic,
		Offsets: snapshot,
	}
	raw, err := json.Marshal(pos)
	if err != nil {
		return core.Checkpoint{}, fmt.Errorf("marshal kafka checkpoint: %w", err)
	}
	return core.Checkpoint{
		Source:    r.source.name,
		Position:  raw,
		Timestamp: time.Now(),
	}, nil
}

// AckCheckpoint acknowledges the Kafka consumer-group offsets represented by
// a checkpoint. It is called only after the checkpoint store has durably saved
// the same source position. Missing consumer sessions are treated as an error:
// silently skipping the external acknowledgement would make the ordering
// contract unverifiable and could strand the checkpoint at a rebalance.
func (r *kafkaReader) AckCheckpoint(ctx context.Context, cp core.Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var pos kafkaPosition
	if len(cp.Position) == 0 || !json.Valid(cp.Position) {
		return fmt.Errorf("kafka checkpoint position is not valid JSON")
	}
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		return fmt.Errorf("decode kafka checkpoint: %w", err)
	}
	if pos.Topic != "" && pos.Topic != r.source.topic {
		return fmt.Errorf("kafka checkpoint topic %q does not match source topic %q", pos.Topic, r.source.topic)
	}
	if len(pos.Offsets) == 0 {
		return nil
	}

	type pendingMark struct {
		session   sarama.ConsumerGroupSession
		partition int32
		offset    int64
	}
	r.mu.Lock()
	marks := make([]pendingMark, 0, len(pos.Offsets))
	for partition, lastOffset := range pos.Offsets {
		session, ok := r.sessions[partition]
		if !ok || session == nil {
			r.mu.Unlock()
			return fmt.Errorf("kafka consumer session unavailable for partition %d", partition)
		}
		marks = append(marks, pendingMark{
			session:   session,
			partition: partition,
			offset:    lastOffset + 1,
		})
	}
	r.mu.Unlock()

	// A ConsumerGroupSession usually owns all partitions in this reader. Mark
	// every partition first, then commit once per distinct session. Sarama's
	// session values are pointers in production; the small identity helper also
	// keeps tests with value-backed fakes safe.
	committed := make([]sarama.ConsumerGroupSession, 0, len(marks))
	for _, mark := range marks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := mark.session.Context().Err(); err != nil {
			return fmt.Errorf("kafka consumer session ended before checkpoint ack for partition %d: %w", mark.partition, err)
		}
		mark.session.MarkOffset(r.source.topic, mark.partition, mark.offset, "")
		duplicate := false
		for _, existing := range committed {
			if sameConsumerSession(existing, mark.session) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			committed = append(committed, mark.session)
		}
	}
	for _, session := range committed {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := session.Context().Err(); err != nil {
			return fmt.Errorf("kafka consumer session ended before offset commit: %w", err)
		}
		session.Commit()
		select {
		case err, ok := <-r.consumerGroupErrors():
			if ok && err != nil {
				return fmt.Errorf("commit kafka consumer-group offset: %w", err)
			}
		default:
		}
	}
	return nil
}

func sameConsumerSession(a, b sarama.ConsumerGroupSession) bool {
	// Sarama's concrete session is a pointer. Non-pointer implementations are
	// conservatively treated as distinct so this helper never relies on an
	// interface's dynamic value being comparable.
	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)
	if !av.IsValid() || !bv.IsValid() || av.Type() != bv.Type() {
		return false
	}
	if av.Kind() != reflect.Pointer {
		return false
	}
	return av.Pointer() == bv.Pointer()
}

func (r *kafkaReader) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		close(r.done)
		err = r.group.Close()
	})
	return err
}

type kafkaPosition struct {
	Topic   string          `json:"topic"`
	Offsets map[int32]int64 `json:"offsets"`
}

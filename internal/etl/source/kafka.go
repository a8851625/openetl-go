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

	// onParseError controls what happens when a message fails to parse in the
	// configured format: "raw" (default, pre-existing: fall back to storing the
	// raw payload under data["value"]), "skip" (drop the message), or "dlq"
	// (route to the pipeline DLQ via the error channel so it lands in the DLQ
	// store with full context).
	onParseError string
	// expandKeyJSON unfolds a JSON-object message key into virtual __key_<col>
	// columns (e.g. the per-table PK JSON emitted by OpenETL CDC sinks).
	expandKeyJSON bool
	// tombstonePolicy controls records with a nil message value: "delete"
	// (treat as OpDelete; kafka log-compaction semantics) or "skip" (drop).
	tombstonePolicy string

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
	if v, ok := config["on_parse_error"].(string); ok {
		switch v {
		case "raw", "skip", "dlq":
			s.onParseError = v
		default:
			return nil, fmt.Errorf("kafka on_parse_error must be raw, skip or dlq, got %q", v)
		}
	}
	if s.onParseError == "" {
		s.onParseError = "raw"
	}
	if v, ok := config["expand_key_json"].(bool); ok {
		s.expandKeyJSON = v
	}
	if v, ok := config["tombstone_policy"].(string); ok {
		switch v {
		case "delete", "skip":
			s.tombstonePolicy = v
		default:
			return nil, fmt.Errorf("kafka tombstone_policy must be delete or skip, got %q", v)
		}
	}
	if s.tombstonePolicy == "" {
		s.tombstonePolicy = "delete"
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

			if msg.Value == nil {
				// Tombstone (log-compaction delete marker).
				if h.reader.source.tombstonePolicy == "skip" {
					continue
				}
				rec.Operation = core.OpDelete
				rec.Data = map[string]any{"__tombstone": true}
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
				continue
			}

			parseOK := true
			switch {
			case h.reader.source.format == "canal_json":
				parseOK = tryCanalJSON(msg.Value, &rec, data)
			case h.reader.source.format == "envelope":
				// Two envelope shapes are accepted:
				//  1. Debezium-style (emitted by the kafka sink since the schema
				//     propagation change): {payload:{before,after,source,op,ts_ms},
				//     schema:{fields:[{field:after,fields:[{field:col,type:...}]}]}}.
				//     Preferred: restores op/before/after and column types from schema.
				//  2. Legacy OpenETL envelope: {event_id,op,table,key,data,timestamp,
				//     column_types}.
				if tryDebeziumEnvelope(msg.Value, &rec, data) {
					// populated above
				} else {
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
				}
			case h.reader.source.format == "json" && h.reader.source.valueColumn == "":
				var parsed map[string]any
				if err := json.Unmarshal(msg.Value, &parsed); err == nil {
					for k, v := range parsed {
						data[k] = v
					}
				} else {
					parseOK = false
				}
			case h.reader.source.valueColumn != "":
				data[h.reader.source.valueColumn] = string(msg.Value)
			default:
				data["value"] = string(msg.Value)
			}

			if !parseOK {
				switch h.reader.source.onParseError {
				case "skip":
					continue
				case "dlq":
					err := fmt.Errorf("kafka message parse failed (format %s, topic %s partition %d offset %d): rerun with on_parse_error=raw to pass the raw payload through, or fix the producer format", h.reader.source.format, msg.Topic, msg.Partition, msg.Offset)
					select {
					case h.reader.errors <- err:
					case <-session.Context().Done():
					case <-h.reader.done:
					}
					continue
				default: // raw: pre-existing behavior
					data["value"] = string(msg.Value)
				}
			}

			// Optional: expand a JSON-object message key into virtual __key_<col>
			// columns (e.g. the per-table PK JSON emitted by OpenETL CDC sinks).
			if h.reader.source.expandKeyJSON && msg.Key != nil {
				var keyObj map[string]any
				if err := json.Unmarshal(msg.Key, &keyObj); err == nil {
					for k, v := range keyObj {
						data["__key_"+k] = v
					}
				}
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

// asMapKV is a local map[string]any type assertion helper.
func asMapKV(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

// debeziumOp maps a Debezium op code to a core operation.
func debeziumOp(v any) core.OpType {
	s, _ := v.(string)
	switch s {
	case "u":
		return core.OpUpdate
	case "d":
		return core.OpDelete
	default: // c (create), r (read/snapshot), or anything else
		return core.OpInsert
	}
}

// columnTypesFromConnectSchemaKV mirrors the debezium_cdc transform: it walks
// schema.fields[] for the named row field (after/before) and returns
// field-name -> Connect primitive type, which downstream sinks pass to
// MapSourceType to derive target DDL.
func columnTypesFromConnectSchemaKV(schema map[string]any, rowField string) map[string]string {
	fields, ok := schema["fields"].([]any)
	if !ok {
		return nil
	}
	for _, item := range fields {
		fm, ok := asMapKV(item)
		if !ok {
			continue
		}
		name, _ := fm["field"].(string)
		if name != rowField {
			continue
		}
		nested, ok := fm["fields"].([]any)
		if !ok {
			continue
		}
		out := make(map[string]string, len(nested))
		for _, nf := range nested {
			nfm, ok := asMapKV(nf)
			if !ok {
				continue
			}
			fn, _ := nfm["field"].(string)
			if fn == "" {
				continue
			}
			typ, _ := nfm["type"].(string)
			if logical, _ := nfm["name"].(string); logical != "" && logical != fn {
				typ = logical
			}
			if typ != "" {
				out[fn] = typ
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

// tryCanalJSON parses an Alibaba canal JSON flat message
// (https://github.com/alibaba/canal, the canal server's MQ producer mode):
//
//	{"type":"INSERT","database":"db","table":"t","ts":...,"es":...,
//	 "sqlType":{...},"mysqlType":{...},"data":[{...}],"old":[{...}]}
//
// Each data element becomes one record. Canal flat messages are
// single-event, so this parser handles the common per-message single row;
// multi-row array messages are unsupported (canal server emits one message
// per row in flat mode by default). DDL/query messages return false so the
// caller's on_parse_error policy applies (or a downstream DDL-aware
// transform sees the raw payload).
func tryCanalJSON(raw []byte, rec *core.Record, data map[string]any) bool {
	var env struct {
		Type      string            `json:"type"`
		Database  string            `json:"database"`
		Table     string            `json:"table"`
		SQL       string            `json:"sql"`
		IsDDL     bool              `json:"isDdl"`
		PKs       []string          `json:"pkNames"`
		SQLType   map[string]int32  `json:"sqlType"`
		MySQLType map[string]string `json:"mysqlType"`
		Data      []map[string]any  `json:"data"`
		Old       []map[string]any  `json:"old"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return false
	}
	switch env.Type {
	case "INSERT":
		rec.Operation = core.OpInsert
	case "UPDATE":
		rec.Operation = core.OpUpdate
	case "DELETE":
		rec.Operation = core.OpDelete
	case "TRUNCATE", "REPLACE", "ALTER", "CREATE", "ERASE", "QUERY", "DDL":
		// DDL and non-DML messages: let the caller's policy decide.
		return false
	default:
		return false
	}
	if len(env.Data) == 0 {
		return false
	}
	// Single-row messages only: multi-row arrays would need batch emission
	// which ConsumeClaim's single-record shape cannot express here.
	if len(env.Data) > 1 {
		return false
	}
	for k, v := range env.Data[0] {
		data[k] = v
	}
	if env.Database != "" {
		rec.Metadata.Database = env.Database
	}
	if env.Table != "" {
		rec.Metadata.Table = env.Table
	}
	if rec.Operation == core.OpUpdate && len(env.Old) > 0 {
		rec.Before = env.Old[0]
	}
	// mysqlType carries declared column types (e.g. "bigint", "varchar(32)") —
	// the same ColumnTypes contract mysql_batch/canal-driven sources fill, so
	// auto_create sinks resolve real source types instead of name hints.
	if len(env.MySQLType) > 0 {
		rec.Metadata.ColumnTypes = env.MySQLType
	}
	if len(env.PKs) > 0 {
		key := make(map[string]any, len(env.PKs))
		for _, pk := range env.PKs {
			if v, ok := data[pk]; ok {
				key[pk] = v
			}
		}
		if len(key) > 0 {
			if b, err := json.Marshal(key); err == nil {
				rec.Metadata.Key = string(b)
			}
		}
	}
	return true
}

// tryDebeziumEnvelope parses a Debezium-style envelope written by the kafka
// sink. Returns true when it recognized and populated the record; false lets
// the caller fall back to the legacy OpenETL envelope.
func tryDebeziumEnvelope(raw []byte, rec *core.Record, data map[string]any) bool {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return false
	}
	payload, ok := asMapKV(root["payload"])
	if !ok {
		return false
	}
	// Require the Debezium marker: payload must carry op (and usually after/before).
	if _, ok := payload["op"]; !ok {
		return false
	}
	rec.Operation = debeziumOp(payload["op"])
	if after, ok := asMapKV(payload["after"]); ok {
		for k, v := range after {
			data[k] = v
		}
	}
	if before, ok := asMapKV(payload["before"]); len(before) > 0 && ok {
		rec.Before = before
	}
	if src, ok := asMapKV(payload["source"]); ok {
		if db, ok := src["db"].(string); ok && db != "" {
			rec.Metadata.Database = db
		}
		if tbl, ok := src["table"].(string); ok && tbl != "" {
			rec.Metadata.Table = tbl
		}
		// Note: source.event_id is a dedup-only identifier emitted by the kafka
		// sink; it is NOT the per-table primary key. The per-table PK JSON object
		// travels in the Kafka message key (msg.Key), which ConsumeClaim already
		// restored into rec.Metadata.Key at line ~331. Overwriting it here with
		// event_id would break pk_columns_from_metadata downstream (DELETE
		// records would lose their PK and fall back to the static pk_columns,
		// producing "delete record missing primary-key column" errors).
		if file, ok := src["file"].(string); ok && file != "" {
			rec.Metadata.BinlogFile = file
		}
		if pos, ok := src["pos"].(float64); ok {
			rec.Metadata.BinlogPos = uint32(pos)
		}
	}
	if schema, ok := asMapKV(root["schema"]); ok {
		if ct := columnTypesFromConnectSchemaKV(schema, "after"); len(ct) > 0 {
			rec.Metadata.ColumnTypes = ct
		}
	}
	if len(data) == 0 {
		data["value"] = string(raw)
	}
	return true
}

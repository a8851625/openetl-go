package sink

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/gogf/gf/v2/frame/g"
	"golang.org/x/crypto/pbkdf2"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSink("kafka", func(config map[string]any) (core.Sink, error) {
		return NewKafkaSink(config)
	})
}

type KafkaSink struct {
	name            string
	brokers         []string
	topic           string
	keyColumn       string
	compression     string
	saslUser        string
	saslPassword    string
	saslMechanism   string
	tls             bool
	tlsSkipVerify   bool
	autoCreateTopic bool
	retryBackoff    time.Duration
	producer        sarama.SyncProducer
	sinkCounters // P4-20: per-sink write metrics (SK-4)
}

// kafkaEnvelope wraps CDC records with operation metadata so downstream
// consumers can distinguish INSERT/UPDATE/DELETE.
type kafkaEnvelope struct {
	EventID   string         `json:"event_id"`
	Op        string         `json:"op"`
	Table     string         `json:"table,omitempty"`
	Key       string         `json:"key,omitempty"`
	Data      map[string]any `json:"data"`
	Timestamp string         `json:"timestamp"`
}

func NewKafkaSink(config map[string]any) (*KafkaSink, error) {
	s := &KafkaSink{name: "kafka", compression: "none"}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	if v, ok := config["brokers"]; ok {
		if brokers, ok := v.([]interface{}); ok {
			for _, b := range brokers {
				if bs, ok := b.(string); ok {
					s.brokers = append(s.brokers, bs)
				}
			}
		}
	}
	if v, ok := config["topic"]; ok {
		if vs, ok := v.(string); ok {
			s.topic = vs
		}
	}
	if v, ok := config["key_column"]; ok {
		if vs, ok := v.(string); ok {
			s.keyColumn = vs
		}
	}
	if v, ok := config["compression"]; ok {
		if vs, ok := v.(string); ok {
			s.compression = vs
		}
	}
	if v, ok := config["sasl_user"]; ok {
		if vs, ok := v.(string); ok {
			s.saslUser = vs
		}
	}
	if v, ok := config["sasl_password"]; ok {
		if vs, ok := v.(string); ok {
			s.saslPassword = vs
		}
	}
	if v, ok := config["sasl_mechanism"]; ok {
		if vs, ok := v.(string); ok {
			s.saslMechanism = vs
		}
	}
	if v, ok := config["tls"]; ok {
		if b, ok := v.(bool); ok {
			s.tls = b
		}
	}
	if v, ok := config["tls_skip_verify"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsSkipVerify = b
		}
	}
	if v, ok := config["auto_create_topic"]; ok {
		if b, ok := v.(bool); ok {
			s.autoCreateTopic = b
		}
	}
	if v, ok := config["retry_backoff_ms"]; ok {
		switch vv := v.(type) {
		case float64:
			s.retryBackoff = time.Duration(vv) * time.Millisecond
		case int:
			s.retryBackoff = time.Duration(vv) * time.Millisecond
		case int64:
			s.retryBackoff = time.Duration(vv) * time.Millisecond
		case string:
			if n, err := strconv.Atoi(vv); err == nil {
				s.retryBackoff = time.Duration(n) * time.Millisecond
			}
		}
	}
	if len(s.brokers) == 0 {
		s.brokers = []string{"localhost:9092"}
	}
	return s, nil
}

func (s *KafkaSink) Name() string { return s.name }

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *KafkaSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *KafkaSink) Open(ctx context.Context) error {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 3
	if s.retryBackoff > 0 {
		cfg.Producer.Retry.Backoff = s.retryBackoff
	} else {
		cfg.Producer.Retry.Backoff = 200 * time.Millisecond
	}
	cfg.Version = sarama.V2_1_0_0 // minimum for idempotent producer

	// Enable idempotent producer to prevent duplicates on retry.
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1

	// Compression
	switch s.compression {
	case "gzip":
		cfg.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		cfg.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		cfg.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		cfg.Producer.Compression = sarama.CompressionZSTD
	}

	// SASL authentication
	if s.saslUser != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = s.saslUser
		cfg.Net.SASL.Password = s.saslPassword
		switch s.saslMechanism {
		case "SCRAM-SHA-256", "scram-sha-256":
			cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return NewSCRAMClient(sha256.New) }
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512", "scram-sha-512":
			cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return NewSCRAMClient(sha512.New) }
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	// TLS
	if s.tls {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: s.tlsSkipVerify,
		}
	}

	producer, err := sarama.NewSyncProducer(s.brokers, cfg)
	if err != nil {
		// Fall back to non-idempotent if the broker doesn't support idempotent
		// producers (broker < 0.11, or in-flight-requests mismatch). Warn loudly:
		// without idempotency, a partial-network-failure retry can duplicate
		// records (P5-6) — rely on an idempotent downstream or accept dups.
		g.Log().Warningf(ctx, "kafka sink (topic=%s): idempotent producer unavailable (%v); falling back to non-idempotent mode — duplicates are possible on retry", s.topic, err)
		cfg.Producer.Idempotent = false
		cfg.Net.MaxOpenRequests = 5
		producer, err = sarama.NewSyncProducer(s.brokers, cfg)
		if err != nil {
			return fmt.Errorf("create kafka producer: %w", err)
		}
	}
	s.producer = producer

	// Validate / auto-create the target topic using a cluster admin client.
	admin, err := sarama.NewClusterAdmin(s.brokers, cfg)
	if err != nil {
		return fmt.Errorf("create kafka cluster admin: %w", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("list kafka topics: %w", err)
	}
	if _, exists := topics[s.topic]; !exists {
		if !s.autoCreateTopic {
			return fmt.Errorf("kafka topic %q does not exist and auto_create_topic is false", s.topic)
		}
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     -1,
			ReplicationFactor: -1,
		}
		if err := admin.CreateTopic(s.topic, topicDetail, false); err != nil {
			return fmt.Errorf("auto-create kafka topic %q: %w", s.topic, err)
		}
	}
	return nil
}

func (s *KafkaSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() { if err != nil { s.recordError() } }() // P5-12: count write failures
	start := time.Now()
	messages := make([]*sarama.ProducerMessage, 0, len(records))
	for _, rec := range records {
		// Deterministic event id from source metadata so downstream consumers
		// can dedupe on replay. The envelope timestamp carries the source
		// event time (not "now") so replays are byte-identical.
		eventID := deriveKafkaEventID(rec)
		srcTS := rec.Metadata.Timestamp
		if srcTS.IsZero() {
			srcTS = time.Now()
		}
		env := kafkaEnvelope{
			EventID:   eventID,
			Op:        string(rec.Operation),
			Table:     rec.Metadata.Table,
			Key:       rec.Metadata.Key,
			Data:      rec.Data,
			Timestamp: srcTS.Format(time.RFC3339Nano),
		}
		value, err := json.Marshal(env)
		if err != nil {
			return fmt.Errorf("kafka marshal record: %w", err)
		}

		msg := &sarama.ProducerMessage{
			Topic:     s.topic,
			Value:     sarama.ByteEncoder(value),
			Timestamp: time.Now(),
		}

		// Set partition key for ordering
		if s.keyColumn != "" {
			if key, ok := rec.Data[s.keyColumn]; ok {
				msg.Key = sarama.StringEncoder(fmt.Sprintf("%v", key))
			}
		} else if rec.Metadata.Key != "" {
			msg.Key = sarama.StringEncoder(rec.Metadata.Key)
		}

		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := s.producer.SendMessages(messages); err != nil {
		return err
	}
	s.recordMetrics(len(records), time.Since(start))
	return nil
}

func (s *KafkaSink) Close() error {
	if s.producer != nil {
		return s.producer.Close()
	}
	return nil
}

// scramClient implements RFC5802 client-side SCRAM for Kafka SASL.
type scramClient struct {
	hashFn          func() hash.Hash
	user            string
	password        string
	nonce           string
	clientFirstBare string
	serverFirst     string
	serverSignature string
	step            int
	done            bool
}

func NewSCRAMClient(hashFn func() hash.Hash) sarama.SCRAMClient {
	return &scramClient{hashFn: hashFn}
}

func (c *scramClient) Begin(userName, password, authzID string) error {
	c.user = userName
	c.password = password
	c.nonce = randomNonce()
	c.clientFirstBare = "n=" + scramName(userName) + ",r=" + c.nonce
	c.step = 0
	c.done = false
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	if c.step == 0 {
		c.step++
		return "n,," + c.clientFirstBare, nil
	}
	if c.step == 1 {
		c.step++
		c.serverFirst = challenge
		attrs := parseSCRAMAttrs(challenge)
		serverNonce := attrs["r"]
		if serverNonce == "" || !strings.HasPrefix(serverNonce, c.nonce) {
			return "", fmt.Errorf("invalid SCRAM nonce")
		}
		salt, err := base64.StdEncoding.DecodeString(attrs["s"])
		if err != nil {
			return "", fmt.Errorf("decode SCRAM salt: %w", err)
		}
		iterations, err := strconv.Atoi(attrs["i"])
		if err != nil || iterations <= 0 {
			return "", fmt.Errorf("invalid SCRAM iteration count")
		}

		clientFinalWithoutProof := "c=biws,r=" + serverNonce
		authMessage := c.clientFirstBare + "," + c.serverFirst + "," + clientFinalWithoutProof
		saltedPassword := pbkdf2.Key([]byte(c.password), salt, iterations, c.hashFn().Size(), c.hashFn)
		clientKey := hmacHash(c.hashFn, saltedPassword, []byte("Client Key"))
		storedKeyHasher := c.hashFn()
		storedKeyHasher.Write(clientKey)
		storedKey := storedKeyHasher.Sum(nil)
		clientSignature := hmacHash(c.hashFn, storedKey, []byte(authMessage))
		serverKey := hmacHash(c.hashFn, saltedPassword, []byte("Server Key"))
		c.serverSignature = base64.StdEncoding.EncodeToString(hmacHash(c.hashFn, serverKey, []byte(authMessage)))
		proof := make([]byte, len(clientKey))
		for i := range clientKey {
			proof[i] = clientKey[i] ^ clientSignature[i]
		}
		return clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
	}
	attrs := parseSCRAMAttrs(challenge)
	if errMsg := attrs["e"]; errMsg != "" {
		return "", fmt.Errorf("SCRAM server error: %s", errMsg)
	}
	if attrs["v"] != c.serverSignature {
		return "", fmt.Errorf("invalid SCRAM server signature")
	}
	c.done = true
	return "", nil
}

func (c *scramClient) Done() bool { return c.done }

func hmacHash(hashFn func() hash.Hash, key, msg []byte) []byte {
	h := hmac.New(hashFn, key)
	h.Write(msg)
	return h.Sum(nil)
}

func randomNonce() string {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return base64.RawStdEncoding.EncodeToString(b)
}

func scramName(s string) string {
	s = strings.ReplaceAll(s, "=", "=3D")
	return strings.ReplaceAll(s, ",", "=2C")
}

func parseSCRAMAttrs(s string) map[string]string {
	attrs := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		if len(part) < 3 || part[1] != '=' {
			continue
		}
		attrs[part[:1]] = part[2:]
	}
	return attrs
}

// deriveKafkaEventID produces a deterministic event id from source metadata so
// downstream consumers can dedupe on replay.
func deriveKafkaEventID(rec core.Record) string {
	var key string
	if rec.Metadata.Table != "" {
		key = rec.Metadata.Table + "|"
	}
	if rec.Metadata.Offset > 0 {
		key += fmt.Sprintf("off=%d", rec.Metadata.Offset)
	} else if rec.Metadata.Partition != 0 || rec.Metadata.Offset != 0 {
		key += fmt.Sprintf("p=%d:o=%d", rec.Metadata.Partition, rec.Metadata.Offset)
	} else if rec.Metadata.BinlogFile != "" {
		key += fmt.Sprintf("b=%s:%d", rec.Metadata.BinlogFile, rec.Metadata.BinlogPos)
	} else if rec.Metadata.LSN != "" {
		key += "lsn=" + rec.Metadata.LSN
	} else {
		key = fmt.Sprintf("data=%v", rec.Data)
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16])
}

package sink

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSink("redis", func(config map[string]any) (core.Sink, error) {
		return NewRedisSink(config)
	})
}

// RedisSink writes records to Redis as HASH, STRING, or LIST entries.
//
// Config:
//
//	host:           Redis host (default "localhost")
//	port:           Redis port (default 6379)
//	password:       Redis password (optional)
//	db:             Redis database index (default 0)
//	key_field:      Record field to use as the Redis key (default "id")
//	key_prefix:     Prefix for all keys (optional)
//	data_type:      Storage type: "hash" (default), "string", "list"
//	tls_skip_verify:    Skip TLS cert verification (default false)
//	max_retries:        Max command retries (default 3)
//	pipeline_chunk_size: Number of records per pipeline exec batch (default 1000)
type RedisSink struct {
	host                   string
	port                   int
	password               string
	db                     int
	keyField               string
	keyPrefix              string
	dataType               string
	ttl                    time.Duration
	valueField             string
	tls                    bool
	tlsSkipVerify          bool
	maxRetries             int
	pipelineChunkSize      int
	allowNonIdempotentList bool

	skipCounter uint64

	client *redis.Client
	sinkCounters // P4-20: per-sink write metrics (SK-4)
}

func NewRedisSink(config map[string]any) (*RedisSink, error) {
	s := &RedisSink{
		host:      "localhost",
		port:      6379,
		dataType:  "hash",
		keyField:  "id",
		keyPrefix: "",
	}
	if v, ok := config["host"].(string); ok {
		s.host = v
	}
	if v, ok := config["port"]; ok {
		switch p := v.(type) {
		case int:
			s.port = p
		case float64:
			s.port = int(p)
		}
	}
	if v, ok := config["password"].(string); ok {
		s.password = v
	}
	if v, ok := config["db"]; ok {
		switch d := v.(type) {
		case int:
			s.db = d
		case float64:
			s.db = int(d)
		}
	}
	if v, ok := config["key_field"].(string); ok && v != "" {
		s.keyField = v
	}
	if v, ok := config["key_prefix"].(string); ok {
		s.keyPrefix = v
	}
	if v, ok := config["data_type"].(string); ok && v != "" {
		s.dataType = v
	}
	if v, ok := config["allow_non_idempotent_list"]; ok {
		if b, ok := v.(bool); ok {
			s.allowNonIdempotentList = b
		}
	}
	if v, ok := config["ttl_seconds"]; ok {
		switch t := v.(type) {
		case int:
			if t > 0 {
				s.ttl = time.Duration(t) * time.Second
			}
		case float64:
			if t > 0 {
				s.ttl = time.Duration(int(t)) * time.Second
			}
		}
	}
	if v, ok := config["value_field"].(string); ok {
		s.valueField = v
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
	if v, ok := config["max_retries"]; ok {
		switch r := v.(type) {
		case int:
			s.maxRetries = r
		case float64:
			s.maxRetries = int(r)
		}
	}
	s.pipelineChunkSize = 1000
	if v, ok := config["pipeline_chunk_size"]; ok {
		switch c := v.(type) {
		case int:
			if c > 0 {
				s.pipelineChunkSize = c
			}
		case float64:
			if c > 0 {
				s.pipelineChunkSize = int(c)
			}
		}
	}
	if s.dataType == "list" && !s.allowNonIdempotentList {
		return nil, fmt.Errorf("redis sink data_type=list is not replay-idempotent; use hash/string or set allow_non_idempotent_list=true")
	}
	return s, nil
}

func (s *RedisSink) Name() string { return "redis" }

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *RedisSink) SinkMetrics() core.SinkMetrics { return s.metricsFor("redis") }

func (s *RedisSink) Open(ctx context.Context) error {
	opts := &redis.Options{
		Addr:         fmt.Sprintf("%s:%d", s.host, s.port),
		Password:     s.password,
		DB:           s.db,
		PoolSize:     10,
		MinIdleConns: 2,
		MaxRetries:   3,
	}
	if s.maxRetries > 0 {
		opts.MaxRetries = s.maxRetries
	}
	if s.tls {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.tlsSkipVerify,
		}
	}
	s.client = redis.NewClient(opts)
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

func (s *RedisSink) Write(ctx context.Context, records []core.Record) error {
	start := time.Now()
	chunkSize := s.pipelineChunkSize
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	for start := 0; start < len(records); start += chunkSize {
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[start:end]

		pipe := s.client.Pipeline()
		for _, rec := range chunk {
			key := s.makeKey(rec)
			if key == "" {
				count := atomic.AddUint64(&s.skipCounter, 1)
				if count%100 == 0 {
					fmt.Printf("[WARN] redis: skipping record with empty key (total skipped: %d)\n", count)
				}
				continue
			}

			// DELETE: short-circuit before any storage command to avoid
			// appending then deleting (which can destroy concurrent writes
			// on list-type keys).
			if rec.Operation == core.OpDelete {
				pipe.Del(ctx, key)
				continue
			}

			switch s.dataType {
			case "string":
				val := ""
				if s.valueField != "" {
					if v, ok := rec.Data[s.valueField]; ok {
						val = fmt.Sprintf("%v", v)
					}
				} else {
					if b, err := json.Marshal(rec.Data); err == nil {
						val = string(b)
					} else {
						val = fmt.Sprintf("%v", rec.Data)
					}
				}
				pipe.Set(ctx, key, val, s.ttl)

			case "list":
				val := ""
				if s.valueField != "" {
					if v, ok := rec.Data[s.valueField]; ok {
						val = fmt.Sprintf("%v", v)
					}
				}
				pipe.RPush(ctx, key, val)
				if s.ttl > 0 {
					pipe.Expire(ctx, key, s.ttl)
				}

			default: // hash
				fields := make(map[string]any)
				for k, v := range rec.Data {
					fields[k] = fmt.Sprintf("%v", v)
				}
				pipe.HSet(ctx, key, fields)
				if s.ttl > 0 {
					pipe.Expire(ctx, key, s.ttl)
				}
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	s.recordMetrics(len(records), time.Since(start))
	return nil
}

func (s *RedisSink) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func (s *RedisSink) makeKey(rec core.Record) string {
	var raw string
	if v, ok := rec.Data[s.keyField]; ok {
		raw = fmt.Sprintf("%v", v)
	} else {
		return ""
	}
	if _, err := strconv.Atoi(raw); err == nil && raw != "" {
		// It's a numeric ID, use as-is.
	}
	return s.keyPrefix + raw
}

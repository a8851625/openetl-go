package source

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

var errScanDone = errors.New("redis scan done")

func init() {
	registry.RegisterSource("redis", func(config map[string]any) (core.Source, error) {
		return NewRedisSource(config)
	})
}

// RedisSource reads from Redis keys matching a pattern, returning each key's
// value as a record. Uses SCAN (non-blocking) to stream keys incrementally
// without blocking the Redis server.
//
// Supports HASH (fields become record data), STRING (value stored under "value"
// key), LIST ("values"), SET ("members"), and other types ("type" field).
//
// Checkpoint: tracks the number of keys committed. On resume, re-scans and
// skips the first N keys (at-least-once semantics — key ordering may differ
// across restarts).
//
// Config:
//
//	host:       Redis host (default "localhost")
//	port:       Redis port (default 6379)
//	password:   Redis password (optional)
//	db:         Redis database index (default 0)
//	pattern:    Key scan pattern (default "*")
//	key_field:  Field name to store the Redis key in the output record (default "_key")
//	count:      SCAN count hint per page (default 100)
type RedisSource struct {
	host     string
	port     int
	password string
	db       int
	pattern  string
	keyField string
	count    int64

	client *redis.Client
}

func NewRedisSource(config map[string]any) (*RedisSource, error) {
	s := &RedisSource{
		host:     "localhost",
		port:     6379,
		pattern:  "*",
		keyField: "_key",
		count:    100,
	}
	if v, ok := config["host"].(string); ok {
		s.host = v
	}
	if v, ok := config["port"]; ok {
		switch iv := v.(type) {
		case int:
			s.port = iv
		case float64:
			s.port = int(iv)
		}
	}
	if v, ok := config["password"].(string); ok {
		s.password = v
	}
	if v, ok := config["db"]; ok {
		switch iv := v.(type) {
		case int:
			s.db = iv
		case float64:
			s.db = int(iv)
		}
	}
	if v, ok := config["pattern"].(string); ok && v != "" {
		s.pattern = v
	}
	if v, ok := config["key_field"].(string); ok && v != "" {
		s.keyField = v
	}
	if v, ok := config["count"].(int64); ok && v > 0 {
		s.count = v
	}
	if v, ok := config["count"].(int); ok && v > 0 {
		s.count = int64(v)
	}
	return s, nil
}

func (s *RedisSource) Name() string { return "redis" }

type redisReader struct {
	source  *RedisSource
	cursor  uint64   // current SCAN cursor (0 = start or done)
	buffer  []string // buffered keys from the most recent SCAN page
	bufIdx  int      // position within buffer
	// processedCount tracks total keys emitted. Used for checkpoint offset.
	processedCount int
	// committedCount is the last checkpointed offset.
	committedCount int
	// scanDone is true when SCAN cursor returns to 0 and buffer is exhausted.
	scanDone bool
	// skipRemaining is the number of keys to skip on resume (from checkpoint).
	skipRemaining int
}

func (s *RedisSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	s.client = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", s.host, s.port),
		Password: s.password,
		DB:       s.db,
	})
	if err := s.client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping (host %s:%d, db %d): %w", s.host, s.port, s.db, err) // P5-15: WHERE context
	}

	rd := &redisReader{
		source: s,
	}

	// Resume from checkpoint: record the offset of keys to skip.
	// Key ordering from SCAN is not guaranteed stable across restarts;
	// this provides at-least-once semantics.
	if cp != nil && len(cp.Position) > 0 {
		if v, err := strconv.Atoi(string(cp.Position)); err == nil && v > 0 {
			rd.skipRemaining = v
			rd.processedCount = v
			rd.committedCount = v
		}
	}

	// Fetch the first page of keys.
	if err := rd.fetchNextPage(ctx); err != nil {
		return nil, fmt.Errorf("redis initial scan: %w", err)
	}

	return rd, nil
}

// fetchNextPage runs one SCAN iteration and fills the buffer with sorted keys.
func (r *redisReader) fetchNextPage(ctx context.Context) error {
	keys, cursor, err := r.source.client.Scan(ctx, r.cursor, r.source.pattern, r.source.count).Result()
	if err != nil {
		return err
	}
	r.cursor = cursor
	sort.Strings(keys)

	// Apply skip offset on the first page (checkpoint resume).
	if r.skipRemaining > 0 && len(keys) > 0 {
		if r.skipRemaining >= len(keys) {
			r.skipRemaining -= len(keys)
			r.buffer = nil
		} else {
			r.buffer = keys[r.skipRemaining:]
			r.skipRemaining = 0
		}
	} else {
		r.buffer = keys
	}
	r.bufIdx = 0

	if r.cursor == 0 && len(r.buffer) == 0 {
		r.scanDone = true
	}
	return nil
}

func (r *redisReader) Read(ctx context.Context) (core.Record, error) {
	// Refill buffer from SCAN when exhausted.
	for r.bufIdx >= len(r.buffer) && !r.scanDone {
		if err := r.fetchNextPage(ctx); err != nil {
			return core.Record{}, fmt.Errorf("redis scan: %w", err)
		}
	}
	if r.scanDone && r.bufIdx >= len(r.buffer) {
		return core.Record{}, errScanDone
	}

	key := r.buffer[r.bufIdx]
	r.bufIdx++
	r.processedCount++

	rec, err := r.readKey(ctx, key)
	if err != nil {
		return core.Record{}, err
	}
	rec.Metadata.Offset = int64(r.processedCount)
	return rec, nil
}

func (r *redisReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	recs := make([]core.Record, 0, n)
	for len(recs) < n {
		rec, err := r.Read(ctx)
		if err != nil {
			if errors.Is(err, errScanDone) {
				break
			}
			return recs, err
		}
		recs = append(recs, rec)
	}
	return recs, nil
}

func (r *redisReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{
		ID:        fmt.Sprintf("redis-%d", time.Now().UnixNano()),
		Source:    "redis",
		Position:  []byte(strconv.Itoa(r.committedCount)),
		Timestamp: time.Now(),
	}, nil
}

func (r *redisReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	if rec.Metadata.Offset > 0 {
		r.committedCount = int(rec.Metadata.Offset)
	}
	return r.Snapshot(ctx)
}

func (r *redisReader) Close() error {
	if r.source != nil && r.source.client != nil {
		_ = r.source.client.Close()
	}
	return nil
}

func (r *redisReader) readKey(ctx context.Context, key string) (core.Record, error) {
	t, err := r.source.client.Type(ctx, key).Result()
	if err != nil {
		return core.Record{}, err
	}

	rec := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{},
		Metadata: core.Metadata{
			Source:    "redis",
			Table:     t,
			Timestamp: time.Now(),
		},
	}
	rec.Data[r.source.keyField] = key

	switch t {
	case "hash":
		fields, err := r.source.client.HGetAll(ctx, key).Result()
		if err != nil {
			return rec, err
		}
		for k, v := range fields {
			rec.Data[k] = v
		}
	case "string":
		val, err := r.source.client.Get(ctx, key).Result()
		if err != nil {
			return rec, err
		}
		rec.Data["value"] = val
	case "list":
		vals, err := r.source.client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return rec, err
		}
		rec.Data["values"] = vals
	case "set":
		vals, err := r.source.client.SMembers(ctx, key).Result()
		if err != nil {
			return rec, err
		}
		rec.Data["members"] = vals
	default:
		rec.Data["type"] = t
	}

	return rec, nil
}

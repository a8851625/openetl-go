package state

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/redis/go-redis/v9"
)

const defaultRedisKeyPrefix = "openetl:state"

type RedisConfig struct {
	Addr      string
	Password  string
	DB        int
	KeyPrefix string
}

// RedisConfigured reports whether the runtime has a Redis state/cache backend
// configured. SQL metadata storage is intentionally ignored here.
func RedisConfigured(ctx context.Context) bool {
	cfg := RedisConfigFromMap(ctx, nil)
	return strings.TrimSpace(cfg.Addr) != ""
}

// RedisConfigFromMap resolves transform-local overrides first, then deployment
// config/env. This keeps pipeline specs portable while allowing tests or
// advanced specs to point a stateful transform at a specific Redis instance.
func RedisConfigFromMap(ctx context.Context, config map[string]any) RedisConfig {
	cfg := RedisConfig{
		Addr:      stringFromConfig(config, "state_redis_addr", ""),
		Password:  stringFromConfig(config, "state_redis_password", ""),
		DB:        intFromConfig(config, "state_redis_db", 0),
		KeyPrefix: stringFromConfig(config, "state_redis_key_prefix", ""),
	}
	if cfg.Addr == "" {
		cfg.Addr = stringFromConfig(config, "redis_addr", "")
	}
	if cfg.Password == "" {
		cfg.Password = stringFromConfig(config, "redis_password", "")
	}
	if cfg.DB == 0 {
		cfg.DB = intFromConfig(config, "redis_db", 0)
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = stringFromConfig(config, "redis_key_prefix", "")
	}

	if cfg.Addr == "" {
		cfg.Addr = strings.TrimSpace(os.Getenv("ETL_STATE_REDIS_ADDR"))
	}
	if cfg.Password == "" {
		cfg.Password = os.Getenv("ETL_STATE_REDIS_PASSWORD")
	}
	if cfg.DB == 0 {
		if raw := strings.TrimSpace(os.Getenv("ETL_STATE_REDIS_DB")); raw != "" {
			if n, err := strconv.Atoi(raw); err == nil {
				cfg.DB = n
			}
		}
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = strings.TrimSpace(os.Getenv("ETL_STATE_REDIS_KEY_PREFIX"))
	}

	if cfg.Addr == "" {
		cfg.Addr = g.Cfg().MustGet(ctx, "etl.state.redis.addr", "").String()
	}
	if cfg.Password == "" {
		cfg.Password = g.Cfg().MustGet(ctx, "etl.state.redis.password", "").String()
	}
	if cfg.DB == 0 {
		cfg.DB = g.Cfg().MustGet(ctx, "etl.state.redis.db", 0).Int()
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = g.Cfg().MustGet(ctx, "etl.state.redis.keyPrefix", defaultRedisKeyPrefix).String()
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = defaultRedisKeyPrefix
	}
	return cfg
}

type RedisStore struct {
	client *redis.Client
	prefix string
	owns   bool
}

func NewRedisStore(ctx context.Context, cfg RedisConfig) (*RedisStore, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, fmt.Errorf("redis state backend requires etl.state.redis.addr or ETL_STATE_REDIS_ADDR")
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = defaultRedisKeyPrefix
	}
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis state backend ping %s db %d: %w", cfg.Addr, cfg.DB, err)
	}
	return &RedisStore{client: client, prefix: cfg.KeyPrefix, owns: true}, nil
}

func NewRedisStoreFromConfig(ctx context.Context, config map[string]any) (*RedisStore, error) {
	return NewRedisStore(ctx, RedisConfigFromMap(ctx, config))
}

func (s *RedisStore) Get(ctx context.Context, pipeline, node, key string) ([]byte, bool, error) {
	value, err := s.client.Get(ctx, s.entryKey(pipeline, node, key)).Bytes()
	if err == redis.Nil {
		_ = s.client.SRem(ctx, s.indexKey(pipeline, node), key).Err()
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get redis state: %w", err)
	}
	return append([]byte(nil), value...), true, nil
}

func (s *RedisStore) Set(ctx context.Context, pipeline, node, key string, value []byte, ttl time.Duration) error {
	if err := s.client.Set(ctx, s.entryKey(pipeline, node, key), append([]byte(nil), value...), ttl).Err(); err != nil {
		return fmt.Errorf("set redis state: %w", err)
	}
	if err := s.client.SAdd(ctx, s.indexKey(pipeline, node), key).Err(); err != nil {
		return fmt.Errorf("index redis state key: %w", err)
	}
	return nil
}

func (s *RedisStore) Delete(ctx context.Context, pipeline, node, key string) error {
	if err := s.client.Del(ctx, s.entryKey(pipeline, node, key)).Err(); err != nil {
		return fmt.Errorf("delete redis state: %w", err)
	}
	if err := s.client.SRem(ctx, s.indexKey(pipeline, node), key).Err(); err != nil {
		return fmt.Errorf("unindex redis state key: %w", err)
	}
	return nil
}

func (s *RedisStore) Snapshot(ctx context.Context, pipeline, node string) (*Snapshot, error) {
	keys, err := s.client.SMembers(ctx, s.indexKey(pipeline, node)).Result()
	if err != nil {
		return nil, fmt.Errorf("list redis state keys: %w", err)
	}
	sort.Strings(keys)
	now := time.Now()
	entries := make([]Entry, 0, len(keys))
	for _, key := range keys {
		value, ok, err := s.Get(ctx, pipeline, node, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		entries = append(entries, Entry{
			Pipeline:  pipeline,
			Node:      node,
			Key:       key,
			Value:     value,
			UpdatedAt: now,
		})
	}
	return &Snapshot{
		Pipeline:  pipeline,
		Node:      node,
		Version:   fmt.Sprintf("%d", now.UnixNano()),
		Entries:   entries,
		CreatedAt: now,
	}, nil
}

func (s *RedisStore) Restore(ctx context.Context, snap *Snapshot) error {
	if snap == nil {
		return nil
	}
	keys, err := s.client.SMembers(ctx, s.indexKey(snap.Pipeline, snap.Node)).Result()
	if err != nil {
		return fmt.Errorf("list redis state before restore: %w", err)
	}
	for _, key := range keys {
		if err := s.client.Del(ctx, s.entryKey(snap.Pipeline, snap.Node, key)).Err(); err != nil {
			return fmt.Errorf("clear redis state key %q: %w", key, err)
		}
	}
	if err := s.client.Del(ctx, s.indexKey(snap.Pipeline, snap.Node)).Err(); err != nil {
		return fmt.Errorf("clear redis state index: %w", err)
	}
	for _, entry := range snap.Entries {
		ttl := time.Duration(0)
		if !entry.ExpiresAt.IsZero() {
			ttl = time.Until(entry.ExpiresAt)
			if ttl <= 0 {
				continue
			}
		}
		key := entry.Key
		if key == "" {
			continue
		}
		value := entry.Value
		if err := s.Set(ctx, snap.Pipeline, snap.Node, key, value, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (s *RedisStore) Stats(ctx context.Context, pipeline, node string) (Stats, error) {
	keys, err := s.client.SMembers(ctx, s.indexKey(pipeline, node)).Result()
	if err != nil {
		return Stats{}, fmt.Errorf("list redis state keys: %w", err)
	}
	var stats Stats
	for _, key := range keys {
		value, ok, err := s.Get(ctx, pipeline, node, key)
		if err != nil {
			return Stats{}, err
		}
		if !ok {
			continue
		}
		stats.Keys++
		stats.Bytes += int64(len(value))
	}
	if stats.Keys > 0 {
		stats.UpdatedAt = time.Now()
	}
	return stats, nil
}

func (s *RedisStore) Close() error {
	if !s.owns || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *RedisStore) entryKey(pipeline, node, key string) string {
	return strings.Join([]string{s.prefix, "entry", encodeKeyPart(pipeline), encodeKeyPart(node), encodeKeyPart(key)}, ":")
}

func (s *RedisStore) indexKey(pipeline, node string) string {
	return strings.Join([]string{s.prefix, "index", encodeKeyPart(pipeline), encodeKeyPart(node)}, ":")
}

func encodeKeyPart(v string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(v))
}

func stringFromConfig(config map[string]any, key, def string) string {
	if config == nil {
		return def
	}
	if v, ok := config[key].(string); ok {
		return strings.TrimSpace(v)
	}
	return def
}

func intFromConfig(config map[string]any, key string, def int) int {
	if config == nil {
		return def
	}
	switch v := config[key].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	case float32:
		return int(v)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil {
			return n
		}
	}
	return def
}

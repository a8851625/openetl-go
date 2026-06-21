package source

import (
	"testing"
)

func TestNewRedisSourceDefaults(t *testing.T) {
	s, err := NewRedisSource(map[string]any{})
	if err != nil {
		t.Fatalf("NewRedisSource: %v", err)
	}
	if s.host != "localhost" {
		t.Errorf("host = %q, want localhost", s.host)
	}
	if s.port != 6379 {
		t.Errorf("port = %d, want 6379", s.port)
	}
	if s.pattern != "*" {
		t.Errorf("pattern = %q, want *", s.pattern)
	}
	if s.keyField != "_key" {
		t.Errorf("keyField = %q, want _key", s.keyField)
	}
	if s.count != 100 {
		t.Errorf("count = %d, want 100", s.count)
	}
}

func TestNewRedisSourceCustomConfig(t *testing.T) {
	s, err := NewRedisSource(map[string]any{
		"host":      "redis.example.com",
		"port":      6380,
		"password":  "secret",
		"db":        2,
		"pattern":   "user:*",
		"key_field": "redis_key",
		"count":     int64(200),
	})
	if err != nil {
		t.Fatalf("NewRedisSource: %v", err)
	}
	if s.host != "redis.example.com" {
		t.Errorf("host = %q", s.host)
	}
	if s.port != 6380 {
		t.Errorf("port = %d", s.port)
	}
	if s.password != "secret" {
		t.Errorf("password = %q", s.password)
	}
	if s.db != 2 {
		t.Errorf("db = %d", s.db)
	}
	if s.pattern != "user:*" {
		t.Errorf("pattern = %q", s.pattern)
	}
	if s.keyField != "redis_key" {
		t.Errorf("keyField = %q", s.keyField)
	}
	if s.count != 200 {
		t.Errorf("count = %d", s.count)
	}
}

func TestNewRedisSourceFloatPort(t *testing.T) {
	s, err := NewRedisSource(map[string]any{
		"port": float64(7000),
		"db":   float64(5),
	})
	if err != nil {
		t.Fatalf("NewRedisSource: %v", err)
	}
	if s.port != 7000 {
		t.Errorf("port = %d", s.port)
	}
	if s.db != 5 {
		t.Errorf("db = %d", s.db)
	}
}

func TestRedisSourceName(t *testing.T) {
	s := &RedisSource{}
	if s.Name() != "redis" {
		t.Errorf("Name() = %q, want redis", s.Name())
	}
}

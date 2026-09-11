// redis-backup-drill seeds/verifies the documented offline SQL + Redis RDB
// procedure. It exercises the real state adapter; it never runs a pipeline.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/state"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 4 || (os.Args[1] != "seed" && os.Args[1] != "verify") {
		return fmt.Errorf("usage: redis-backup-drill seed|verify REDIS_ADDR WORK_DIR")
	}
	mode, addr, dir := os.Args[1], os.Args[2], os.Args[3]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const pipeline = "drill-pipeline"
	const prefix = "openetl:drill"
	cache, err := state.NewRedisStore(ctx, state.RedisConfig{Addr: addr, KeyPrefix: prefix})
	if err != nil {
		return err
	}
	defer cache.Close()
	sql, err := sqlite.New(filepath.Join(dir, mode+".db"))
	if err != nil {
		return err
	}
	defer sql.Close()
	values := map[string]string{
		"lookup":      `{"customer":"C-42","region":"east"}`,
		"deduplicate": `{"id":"order-42","source_version":12}`,
		"window":      `{"window_start":1788680000,"sum":123.45,"count":7}`,
	}
	const key = "state-key"
	backupFile := filepath.Join(dir, "control-plane.json")
	if mode == "seed" {
		if err := sql.SavePipeline(ctx, &storage.PipelineRow{ID: pipeline, Name: pipeline, Status: "stopped", Generation: 7}); err != nil {
			return err
		}
		for node, value := range values {
			if err := cache.Set(ctx, pipeline, node, key, []byte(value), time.Hour); err != nil {
				return err
			}
		}
		if err := sql.SaveCheckpoint(ctx, &storage.CheckpointRecord{JobName: pipeline, Source: "kafka", Generation: 7, Position: json.RawMessage(`{"topic":"orders","offset":42}`), Timestamp: time.Now().UTC()}); err != nil {
			return err
		}
		snap, err := backup.Export(ctx, sql, backup.Options{Backend: "sqlite"})
		if err != nil {
			return err
		}
		if err := backup.WriteFile(backupFile, snap); err != nil {
			return err
		}
		fmt.Println("QUIESCED_SQL_AND_REDIS_SEEDED generation=7 offset=42 state_entries=3")
		return nil
	}
	snap, err := backup.ReadFile(backupFile)
	if err != nil {
		return err
	}
	if err := backup.Restore(ctx, sql, snap, backup.Options{ClearBeforeRestore: true}); err != nil {
		return err
	}
	cp, err := sql.LoadCheckpoint(ctx, pipeline)
	if err != nil || cp == nil || cp.Generation != 7 {
		return fmt.Errorf("checkpoint mismatch after SQL restore: %+v %v", cp, err)
	}
	var position struct {
		Topic  string `json:"topic"`
		Offset int64  `json:"offset"`
	}
	if err := json.Unmarshal(cp.Position, &position); err != nil || position.Topic != "orders" || position.Offset != 42 {
		return fmt.Errorf("source position mismatch after SQL restore: %+v %v", position, err)
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	encode := base64.RawURLEncoding.EncodeToString
	for node, expected := range values {
		value, ok, err := cache.Get(ctx, pipeline, node, key)
		if err != nil || !ok || string(value) != expected {
			return fmt.Errorf("state mismatch after RDB restore for %s: found=%t err=%v", node, ok, err)
		}
		stats, err := cache.Stats(ctx, pipeline, node)
		if err != nil || stats.Keys != 1 {
			return fmt.Errorf("state index mismatch for %s: %+v %v", node, stats, err)
		}
		redisKey := prefix + ":entry:" + encode([]byte(pipeline)) + ":" + encode([]byte(node)) + ":" + encode([]byte(key))
		ttl, err := client.PTTL(ctx, redisKey).Result()
		if err != nil || ttl <= 0 || ttl > time.Hour {
			return fmt.Errorf("RDB lost expiry for %s: ttl=%v err=%v", node, ttl, err)
		}
	}
	fmt.Println("SQL_AND_REDIS_RDB_RESTORE_PASS generation=7 offset=42 bytes_and_indexes_and_ttl=3")
	return nil
}

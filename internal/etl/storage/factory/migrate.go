package factory

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// migrateFileData reads any existing file-based checkpoint and DLQ files
// and imports them into the SQL store (one-time migration).
func migrateFileData(ctx context.Context, store storage.Storage, checkpointDir, dlqDir string) {
	migratedCheckpoints := migrateCheckpoints(ctx, store, checkpointDir)
	migratedDLQ := migrateDeadLetters(ctx, store, dlqDir)

	total := migratedCheckpoints + migratedDLQ
	if total > 0 {
		g.Log().Infof(ctx, "Migrated %d checkpoints and %d dead-letters from files to storage",
			migratedCheckpoints, migratedDLQ)
	}
}

func migrateCheckpoints(ctx context.Context, store storage.Storage, dir string) int {
	if dir == "" {
		return 0
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		jobName := strings.TrimSuffix(entry.Name(), ".json")
		existing, _ := store.LoadCheckpoint(ctx, jobName)
		if existing != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		var raw struct {
			JobName   string          `json:"job_name"`
			Source    string          `json:"source"`
			Position  json.RawMessage `json:"position"`
			Timestamp time.Time       `json:"timestamp"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			continue
		}
		if raw.JobName == "" {
			raw.JobName = jobName
		}
		rec := &storage.CheckpointRecord{
			JobName:   raw.JobName,
			Source:    raw.Source,
			Position:  raw.Position,
			Timestamp: raw.Timestamp,
		}
		if err := store.SaveCheckpoint(ctx, rec); err == nil {
			count++
		}
	}
	return count
}

func migrateDeadLetters(ctx context.Context, store storage.Storage, dir string) int {
	if dir == "" {
		return 0
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		jobName := strings.TrimSuffix(entry.Name(), ".jsonl")

		f, err := os.Open(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 10<<20)
		for scanner.Scan() {
			var raw struct {
				JobName    string    `json:"job_name"`
				Record     any       `json:"record"`
				Error      string    `json:"error"`
				ErrorClass string    `json:"error_class"`
				Timestamp  time.Time `json:"timestamp"`
				Attempt    int       `json:"attempt"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil {
				continue
			}
			if raw.JobName == "" {
				raw.JobName = jobName
			}
			recJSON, _ := json.Marshal(raw.Record)
			dlqRec := &storage.DLQRecord{
				JobName:    raw.JobName,
				Error:      raw.Error,
				ErrorClass: raw.ErrorClass,
				Attempt:    raw.Attempt,
				CreatedAt:  raw.Timestamp,
			}
			_ = json.Unmarshal(recJSON, &dlqRec.Record)
			if err := store.WriteDeadLetter(ctx, dlqRec); err == nil {
				count++
			}
		}
		f.Close()
	}
	return count
}

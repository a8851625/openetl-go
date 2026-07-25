package worker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func workerSpecKey(fill byte) string {
	return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{fill}, 32))
}

func TestExecuteShardReadsEncryptedPipelineSpec(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "input.jsonl")
	if err := os.WriteFile(sourcePath, []byte("{\"id\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cipher, err := storage.NewSpecCipher("worker-key", workerSpecKey(1), "")
	if err != nil {
		t.Fatal(err)
	}
	specStore := storage.NewPipelineSpecStore(store, cipher)
	spec := &pipeline.Spec{
		Name:   "worker-encrypted",
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": sourcePath, "format": "json", "api_token": "worker-secret-value"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": filepath.Join(dir, "out"), "format": "jsonl"}},
	}
	pipeline.ApplyDefaults(spec)
	specYAML, err := pipeline.MarshalSpecYAML(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := specStore.SaveWithID(context.Background(), "worker-pipeline-id", spec.Name, string(specYAML), "created"); err != nil {
		t.Fatal(err)
	}
	raw, err := store.GetPipeline(context.Background(), "worker-pipeline-id")
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil || !strings.HasPrefix(raw.SpecYAML, "enc:v1:worker-key:") || strings.Contains(raw.SpecYAML, "worker-secret-value") {
		t.Fatalf("stored worker spec is not safely encrypted: %#v", raw)
	}

	task := &storage.TaskAssignment{TaskID: "worker-task", Pipeline: "worker-pipeline-id", ShardIndex: 0, ShardTotal: 1}
	taskJSON, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(taskJSON), "worker-secret-value") || strings.Contains(string(taskJSON), "enc:v1:") {
		t.Fatalf("task row leaked spec material: %s", taskJSON)
	}
	am := alert.NewManager()
	if err := ExecuteShard(context.Background(), ExecutorDeps{
		Store:     store,
		SpecStore: specStore,
		CPAdapter: storage.NewCheckpointStoreAdapter(store),
		DLQWriter: storage.NewDLQCompatWriter(store),
		AlertMgr:  am,
	}, task); err != nil {
		t.Fatalf("ExecuteShard: %v", err)
	}
}

func TestExecuteShardFailsWhenEncryptedSpecKeyIsMissing(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	encryptingCipher, err := storage.NewSpecCipher("worker-key", workerSpecKey(2), "")
	if err != nil {
		t.Fatal(err)
	}
	encryptingStore := storage.NewPipelineSpecStore(store, encryptingCipher)
	if err := encryptingStore.SaveWithID(context.Background(), "worker-pipeline-id", "worker-encrypted", "name: worker-encrypted", "created"); err != nil {
		t.Fatal(err)
	}
	plaintextCipher, err := storage.NewSpecCipher("default", "", "")
	if err != nil {
		t.Fatal(err)
	}
	err = ExecuteShard(context.Background(), ExecutorDeps{
		Store:     store,
		SpecStore: storage.NewPipelineSpecStore(store, plaintextCipher),
	}, &storage.TaskAssignment{TaskID: "worker-task", Pipeline: "worker-pipeline-id", ShardIndex: 0, ShardTotal: 1})
	if !errors.Is(err, storage.ErrSpecEncryptionKeyUnavailable) {
		t.Fatalf("ExecuteShard error = %v, want missing-key error", err)
	}
	if strings.Contains(err.Error(), "enc:v1:") {
		t.Fatalf("ExecuteShard error leaked ciphertext: %v", err)
	}
}

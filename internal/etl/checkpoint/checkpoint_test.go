package checkpoint

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestFileStoreSaveLoadListDelete(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	pos, _ := json.Marshal(map[string]any{"file": "mysql-bin.000001", "pos": 123})
	cp := core.Checkpoint{JobName: "job-a", Source: "mysql_cdc", Position: pos}
	if err := store.Save(context.Background(), cp); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	loaded, err := store.Load(context.Background(), "job-a")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded == nil || loaded.JobName != "job-a" || loaded.Source != "mysql_cdc" {
		t.Fatalf("Load() = %#v", loaded)
	}

	items, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("List() len = %d, want 1", len(items))
	}

	if err := store.Delete(context.Background(), "job-a"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	loaded, err = store.Load(context.Background(), "job-a")
	if err != nil {
		t.Fatalf("Load() after Delete error = %v", err)
	}
	if loaded != nil {
		t.Fatalf("Load() after Delete = %#v, want nil", loaded)
	}
}

func TestFileStoreSaveUsesAtomicFile(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	cp := core.Checkpoint{JobName: "atomic-job", Source: "test", Position: []byte(`{"offset":42}`)}
	if err := store.Save(context.Background(), cp); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	loaded, err := store.Load(context.Background(), "atomic-job")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded == nil || string(loaded.Position) != `{"offset":42}` {
		t.Fatalf("loaded checkpoint = %#v", loaded)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("unexpected temp checkpoint file left behind: %s", entry.Name())
		}
	}
}

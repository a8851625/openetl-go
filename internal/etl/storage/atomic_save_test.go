package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestPipelineSpecStoreAtomicSaveRollsBackCurrentOnVersionFailure(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.SetFailureInjector(func(operation string) error {
		if operation == "pipeline.version" {
			return errors.New("injected version write failure")
		}
		return nil
	})
	specStore := storage.NewPipelineSpecStore(store)
	err = specStore.SaveWithID(context.Background(), "atomic-id", "atomic", "name: atomic\n", "created")
	if err == nil {
		t.Fatal("SaveWithID succeeded despite injected version failure")
	}
	if row, getErr := store.GetPipeline(context.Background(), "atomic-id"); getErr != nil {
		t.Fatal(getErr)
	} else if row != nil {
		t.Fatalf("current row survived rolled-back save: %#v", row)
	}
	versions, err := store.ListPipelineVersions(context.Background(), "atomic-id")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 0 {
		t.Fatalf("versions survived rolled-back save: %#v", versions)
	}
}

func TestPipelineSpecStoreAtomicSaveCommitsCurrentAndVersionTogether(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	specStore := storage.NewPipelineSpecStore(store)
	if err := specStore.SaveWithID(context.Background(), "atomic-id", "atomic", "name: atomic\n", "created"); err != nil {
		t.Fatal(err)
	}
	row, err := store.GetPipeline(context.Background(), "atomic-id")
	if err != nil || row == nil {
		t.Fatalf("current row missing after commit: row=%#v err=%v", row, err)
	}
	versions, err := store.ListPipelineVersions(context.Background(), "atomic-id")
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions after commit = %#v err=%v, want one", versions, err)
	}
	if versions[0].SpecYAML != row.SpecYAML {
		t.Fatalf("current/version payload mismatch: current=%q version=%q", row.SpecYAML, versions[0].SpecYAML)
	}
}

func TestPipelineSpecStoreAtomicCheckpointResetRollsBackSpecOnFailure(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	specStore := storage.NewPipelineSpecStore(store)
	if err := specStore.SaveWithID(ctx, "atomic-id", "atomic", "name: old\n", "created"); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCheckpoint(ctx, &storage.CheckpointRecord{JobName: "atomic-id", Source: "file", Position: json.RawMessage(`{"offset":1}`)}); err != nil {
		t.Fatal(err)
	}
	store.SetFailureInjector(func(operation string) error {
		if operation == "checkpoint.delete" {
			return errors.New("injected checkpoint delete failure")
		}
		return nil
	})
	err = specStore.SaveWithIDAndCheckpointReset(ctx, "atomic-id", "atomic", "name: new\n", "updated", true)
	if err == nil {
		t.Fatal("checkpoint reset update succeeded despite injected failure")
	}
	row, err := store.GetPipeline(ctx, "atomic-id")
	if err != nil || row == nil || row.SpecYAML != "name: old\n" {
		t.Fatalf("current row after rollback = %#v err=%v", row, err)
	}
	versions, err := store.ListPipelineVersions(ctx, "atomic-id")
	if err != nil || len(versions) != 1 || versions[0].SpecYAML != "name: old\n" {
		t.Fatalf("versions after rollback = %#v err=%v", versions, err)
	}
	cp, err := store.LoadCheckpoint(ctx, "atomic-id")
	if err != nil || cp == nil {
		t.Fatalf("checkpoint was lost after rolled-back reset: cp=%#v err=%v", cp, err)
	}
}

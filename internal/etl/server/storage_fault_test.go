package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

type serverFaultFixture struct {
	store      *sqlite.Store
	server     *Server
	sourcePath string
}

func newServerFaultFixture(t *testing.T) *serverFaultFixture {
	t.Helper()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "input.jsonl")
	if err := os.WriteFile(sourcePath, []byte("{\"id\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")
	store, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s, err := NewServer(store, filepath.Join(dir, "pipes"))
	if err != nil {
		t.Fatal(err)
	}
	return &serverFaultFixture{store: store, server: s, sourcePath: sourcePath}
}

func createServerFaultPipeline(t *testing.T, f *serverFaultFixture, dag bool, name, revision string) string {
	t.Helper()
	spec := recoverySpec(dag, name, f.sourcePath, filepath.Join(filepath.Dir(f.sourcePath), name+"-out-"+revision), name+"-secret", revision)
	response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines", map[string]any{"spec": spec})
	if response.Code != http.StatusOK {
		t.Fatalf("create %s status=%d body=%s", name, response.Code, response.Body.String())
	}
	id, _ := decodeSpecCryptoResponse(t, response)["id"].(string)
	if id == "" {
		t.Fatal("create response missing id")
	}
	return id
}

func TestPipelineCreateStorageFailureLeavesNoRuntimeOrRows(t *testing.T) {
	for _, dag := range []bool{false, true} {
		name := "linear"
		if dag {
			name = "dag"
		}
		t.Run(name, func(t *testing.T) {
			f := newServerFaultFixture(t)
			f.store.SetFailureInjector(func(operation string) error {
				if operation == "pipeline.version" {
					return errors.New("injected version failure")
				}
				return nil
			})
			spec := recoverySpec(dag, "create-failure-"+name, f.sourcePath, filepath.Join(filepath.Dir(f.sourcePath), "out"), "secret", "revision-one")
			response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines", map[string]any{"spec": spec})
			if response.Code < 400 {
				t.Fatalf("create status=%d body=%s, want non-2xx", response.Code, response.Body.String())
			}
			f.server.mu.RLock()
			runtimeCount := len(f.server.pipelines)
			f.server.mu.RUnlock()
			if runtimeCount != 0 {
				t.Fatalf("runtime pipelines = %d, want 0", runtimeCount)
			}
			rows, err := f.store.ListPipelines(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 0 {
				t.Fatalf("persisted rows after failed create: %#v", rows)
			}
		})
	}
}

func TestPipelineUpdateStorageFailureKeepsLastSuccessfulRuntimeAndDB(t *testing.T) {
	for _, dag := range []bool{false, true} {
		name := "linear"
		if dag {
			name = "dag"
		}
		t.Run(name, func(t *testing.T) {
			f := newServerFaultFixture(t)
			pipelineName := "update-failure-" + name
			id := createServerFaultPipeline(t, f, dag, pipelineName, "revision-one")
			f.server.mu.RLock()
			oldRunner := f.server.pipelines[id]
			f.server.mu.RUnlock()
			f.store.SetFailureInjector(func(operation string) error {
				if operation == "pipeline.version" {
					return errors.New("injected version failure")
				}
				return nil
			})
			updatedSpec := recoverySpec(dag, pipelineName, f.sourcePath, filepath.Join(filepath.Dir(f.sourcePath), "out-two"), pipelineName+"-secret", "revision-two")
			response := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines", map[string]any{"id": id, "spec": updatedSpec})
			if response.Code < 400 {
				t.Fatalf("update status=%d body=%s, want non-2xx", response.Code, response.Body.String())
			}
			assertRecoveredRevision(t, f.server, id, dag, "revision-one")
			f.server.mu.RLock()
			if f.server.pipelines[id] != oldRunner {
				f.server.mu.RUnlock()
				t.Fatal("failed update replaced the in-memory runner")
			}
			f.server.mu.RUnlock()
			row, err := f.store.GetPipeline(context.Background(), id)
			if err != nil || row == nil || !strings.Contains(row.SpecYAML, "revision-one") || strings.Contains(row.SpecYAML, "revision-two") {
				t.Fatalf("current row after failed update = %#v err=%v", row, err)
			}
			versions, err := f.store.ListPipelineVersions(context.Background(), id)
			if err != nil || len(versions) != 1 {
				t.Fatalf("versions after failed update = %#v err=%v", versions, err)
			}
		})
	}
}

func TestPipelineUpdateCheckpointFailureRollsBackSpecAndCheckpoint(t *testing.T) {
	f := newServerFaultFixture(t)
	id := createServerFaultPipeline(t, f, false, "checkpoint-update-failure", "revision-one")
	ctx := context.Background()
	if err := f.store.SaveCheckpoint(ctx, &storage.CheckpointRecord{JobName: id, Source: "file", Position: json.RawMessage(`{"offset":1}`)}); err != nil {
		t.Fatal(err)
	}
	f.server.mu.RLock()
	oldRunner := f.server.pipelines[id]
	f.server.mu.RUnlock()
	f.store.SetFailureInjector(func(operation string) error {
		if operation == "checkpoint.delete" {
			return errors.New("injected checkpoint failure")
		}
		return nil
	})
	updatedSpec := recoverySpec(false, "checkpoint-update-failure", f.sourcePath, filepath.Join(filepath.Dir(f.sourcePath), "out-two"), "secret", "revision-two")
	response := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines", map[string]any{"id": id, "spec": updatedSpec, "reset_checkpoint": true})
	if response.Code < 400 {
		t.Fatalf("update status=%d body=%s, want non-2xx", response.Code, response.Body.String())
	}
	assertRecoveredRevision(t, f.server, id, false, "revision-one")
	f.server.mu.RLock()
	if f.server.pipelines[id] != oldRunner {
		f.server.mu.RUnlock()
		t.Fatal("checkpoint failure replaced the in-memory runner")
	}
	f.server.mu.RUnlock()
	row, err := f.store.GetPipeline(ctx, id)
	if err != nil || row == nil || !strings.Contains(row.SpecYAML, "revision-one") {
		t.Fatalf("current row after checkpoint failure = %#v err=%v", row, err)
	}
	versions, err := f.store.ListPipelineVersions(ctx, id)
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions after checkpoint failure = %#v err=%v", versions, err)
	}
	cp, err := f.store.LoadCheckpoint(ctx, id)
	if err != nil || cp == nil {
		t.Fatalf("checkpoint lost after rolled-back update: cp=%#v err=%v", cp, err)
	}
}

func TestPipelineRollbackStorageFailureKeepsCurrentVersion(t *testing.T) {
	for _, dag := range []bool{false, true} {
		name := "linear"
		if dag {
			name = "dag"
		}
		t.Run(name, func(t *testing.T) {
			f := newServerFaultFixture(t)
			pipelineName := "rollback-failure-" + name
			id := createServerFaultPipeline(t, f, dag, pipelineName, "revision-one")
			updatedSpec := recoverySpec(dag, pipelineName, f.sourcePath, filepath.Join(filepath.Dir(f.sourcePath), "out-two"), pipelineName+"-secret", "revision-two")
			updated := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines", map[string]any{"id": id, "spec": updatedSpec})
			if updated.Code != http.StatusOK {
				t.Fatalf("update status=%d body=%s", updated.Code, updated.Body.String())
			}
			f.server.mu.RLock()
			oldRunner := f.server.pipelines[id]
			f.server.mu.RUnlock()
			f.store.SetFailureInjector(func(operation string) error {
				if operation == "pipeline.version" {
					return errors.New("injected rollback version failure")
				}
				return nil
			})
			response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines/"+id+"/versions/1/rollback", nil)
			if response.Code < 400 {
				t.Fatalf("rollback status=%d body=%s, want non-2xx", response.Code, response.Body.String())
			}
			assertRecoveredRevision(t, f.server, id, dag, "revision-two")
			f.server.mu.RLock()
			if f.server.pipelines[id] != oldRunner {
				f.server.mu.RUnlock()
				t.Fatal("failed rollback replaced the in-memory runner")
			}
			f.server.mu.RUnlock()
			row, err := f.store.GetPipeline(context.Background(), id)
			if err != nil || row == nil || !strings.Contains(row.SpecYAML, "revision-two") {
				t.Fatalf("current row after failed rollback = %#v err=%v", row, err)
			}
			versions, err := f.store.ListPipelineVersions(context.Background(), id)
			if err != nil || len(versions) != 2 {
				t.Fatalf("versions after failed rollback = %#v err=%v", versions, err)
			}
		})
	}
}

func TestPipelineDeleteStorageFailureKeepsRuntimeRowsVersionsAndCheckpoint(t *testing.T) {
	f := newServerFaultFixture(t)
	id := createServerFaultPipeline(t, f, false, "delete-failure", "revision-one")
	ctx := context.Background()
	if err := f.store.SaveCheckpoint(ctx, &storage.CheckpointRecord{JobName: id, Source: "file", Position: json.RawMessage(`{"offset":1}`)}); err != nil {
		t.Fatal(err)
	}
	f.server.mu.RLock()
	oldRunner := f.server.pipelines[id]
	f.server.mu.RUnlock()
	f.store.SetFailureInjector(func(operation string) error {
		if operation == "checkpoint.delete" {
			return errors.New("injected delete checkpoint failure")
		}
		return nil
	})
	response := specCryptoRequest(t, f.server, http.MethodDelete, "/api/v2/pipelines/"+id, nil)
	if response.Code < 400 {
		t.Fatalf("delete status=%d body=%s, want non-2xx", response.Code, response.Body.String())
	}
	f.server.mu.RLock()
	if f.server.pipelines[id] != oldRunner {
		f.server.mu.RUnlock()
		t.Fatal("failed delete removed or replaced the in-memory runner")
	}
	f.server.mu.RUnlock()
	row, err := f.store.GetPipeline(ctx, id)
	if err != nil || row == nil {
		t.Fatalf("pipeline row lost after failed delete: row=%#v err=%v", row, err)
	}
	versions, err := f.store.ListPipelineVersions(ctx, id)
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions after failed delete = %#v err=%v", versions, err)
	}
	cp, err := f.store.LoadCheckpoint(ctx, id)
	if err != nil || cp == nil {
		t.Fatalf("checkpoint lost after failed delete: cp=%#v err=%v", cp, err)
	}
}

func TestCheckpointResetFailureReturnsNon2xxAndKeepsCheckpoint(t *testing.T) {
	f := newServerFaultFixture(t)
	id := createServerFaultPipeline(t, f, false, "checkpoint-reset-failure", "revision-one")
	ctx := context.Background()
	if err := f.store.SaveCheckpoint(ctx, &storage.CheckpointRecord{JobName: id, Source: "file", Position: json.RawMessage(`{"offset":1}`)}); err != nil {
		t.Fatal(err)
	}
	f.store.SetFailureInjector(func(operation string) error {
		if operation == "checkpoint.delete" {
			return errors.New("injected checkpoint reset failure")
		}
		return nil
	})
	response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines/"+id+"/checkpoint/reset", nil)
	if response.Code < 400 {
		t.Fatalf("checkpoint reset status=%d body=%s, want non-2xx", response.Code, response.Body.String())
	}
	cp, err := f.store.LoadCheckpoint(ctx, id)
	if err != nil || cp == nil {
		t.Fatalf("checkpoint lost after failed reset: cp=%#v err=%v", cp, err)
	}
}

func TestSpecImportStorageFailureKeepsExistingRuntimeAndDB(t *testing.T) {
	for _, dag := range []bool{false, true} {
		name := "linear"
		if dag {
			name = "dag"
		}
		t.Run(name, func(t *testing.T) {
			f := newServerFaultFixture(t)
			pipelineName := "import-failure-" + name
			id := createServerFaultPipeline(t, f, dag, pipelineName, "revision-one")
			f.server.mu.RLock()
			oldRunner := f.server.pipelines[id]
			f.server.mu.RUnlock()
			f.store.SetFailureInjector(func(operation string) error {
				if operation == "pipeline.version" {
					return errors.New("injected import version failure")
				}
				return nil
			})
			updatedSpec := recoverySpec(dag, pipelineName, f.sourcePath, filepath.Join(filepath.Dir(f.sourcePath), "out-two"), pipelineName+"-secret", "revision-two")
			body, err := json.Marshal(updatedSpec)
			if err != nil {
				t.Fatal(err)
			}
			response := specCryptoRawRequest(t, f.server, http.MethodPost, "/api/v2/specs/import", "application/x-yaml", body)
			if response.Code < 400 {
				t.Fatalf("import status=%d body=%s, want non-2xx", response.Code, response.Body.String())
			}
			assertRecoveredRevision(t, f.server, id, dag, "revision-one")
			f.server.mu.RLock()
			if f.server.pipelines[id] != oldRunner {
				f.server.mu.RUnlock()
				t.Fatal("failed import replaced the in-memory runner")
			}
			f.server.mu.RUnlock()
			row, err := f.store.GetPipeline(context.Background(), id)
			if err != nil || row == nil || !strings.Contains(row.SpecYAML, "revision-one") || strings.Contains(row.SpecYAML, "revision-two") {
				t.Fatalf("current row after failed import = %#v err=%v", row, err)
			}
		})
	}
}

func TestScheduleStorageFailureKeepsInMemoryScheduleUnchanged(t *testing.T) {
	f := newServerFaultFixture(t)
	id := createServerFaultPipeline(t, f, false, "schedule-failure", "revision-one")
	f.server.mu.RLock()
	beforeSchedule := f.server.specs[id].Schedule
	if beforeSchedule != nil {
		copySchedule := *beforeSchedule
		beforeSchedule = &copySchedule
	}
	f.server.mu.RUnlock()
	beforeRow, err := f.store.GetPipeline(context.Background(), id)
	if err != nil || beforeRow == nil {
		t.Fatalf("load pipeline before schedule update: row=%#v err=%v", beforeRow, err)
	}
	f.store.SetFailureInjector(func(operation string) error {
		if operation == "pipeline.version" {
			return errors.New("injected schedule version failure")
		}
		return nil
	})
	response := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines/"+id+"/schedule", map[string]any{
		"type": "periodic", "interval_sec": 60,
	})
	if response.Code < 400 {
		t.Fatalf("schedule update status=%d body=%s, want non-2xx", response.Code, response.Body.String())
	}
	f.server.mu.RLock()
	spec := f.server.specs[id]
	f.server.mu.RUnlock()
	if spec == nil || !reflect.DeepEqual(spec.Schedule, beforeSchedule) {
		t.Fatalf("in-memory schedule after failed update = %#v, want %#v", spec.Schedule, beforeSchedule)
	}
	row, err := f.store.GetPipeline(context.Background(), id)
	if err != nil || row == nil || row.SpecYAML != beforeRow.SpecYAML {
		t.Fatalf("persisted schedule changed after failed update: row=%#v err=%v", row, err)
	}
}

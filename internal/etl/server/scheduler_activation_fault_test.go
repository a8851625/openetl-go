package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

const schedulerFaultDependency = "scheduler-fault-upstream"

func enableRuntimeSchedulerForFaultTest(t *testing.T, f *serverFaultFixture) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.server.ctx = ctx
	f.server.scheduler.SetContext(ctx)
	t.Cleanup(func() {
		f.server.scheduler.SetRegisterFailureInjector(nil)
		f.server.scheduler.StopAll()
		cancel()
	})
}

func schedulerFaultSchedule(kind string) map[string]any {
	switch kind {
	case "cron":
		return map[string]any{"type": "cron", "cron": "0 0 * * *"}
	case "periodic":
		return map[string]any{"type": "periodic", "interval_sec": 60}
	case "dependency":
		return map[string]any{"type": "dependency", "depends_on": []string{schedulerFaultDependency}}
	default:
		panic("unsupported schedule kind: " + kind)
	}
}

func scheduledRecoverySpec(f *serverFaultFixture, dag bool, name, revision, scheduleKind string) map[string]any {
	spec := recoverySpec(
		dag,
		name,
		f.sourcePath,
		filepath.Join(filepath.Dir(f.sourcePath), name+"-out-"+revision),
		name+"-secret",
		revision,
	)
	spec["schedule"] = schedulerFaultSchedule(scheduleKind)
	return spec
}

func createScheduledFaultPipeline(t *testing.T, f *serverFaultFixture, dag bool, name, revision, scheduleKind string) string {
	t.Helper()
	response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines", map[string]any{
		"spec": scheduledRecoverySpec(f, dag, name, revision, scheduleKind),
	})
	if response.Code != http.StatusOK {
		t.Fatalf("create scheduled pipeline status=%d body=%s", response.Code, response.Body.String())
	}
	id, _ := decodeSpecCryptoResponse(t, response)["id"].(string)
	if id == "" {
		t.Fatal("create scheduled pipeline response missing id")
	}
	return id
}

func installObservableDependencyRunner(t *testing.T, f *serverFaultFixture, id string) pipeline.RunnerInterface {
	t.Helper()
	f.server.mu.RLock()
	oldRunner := f.server.pipelines[id]
	f.server.mu.RUnlock()
	observer := newTestScheduledRunner()
	observer.mu.Lock()
	observer.status = pipeline.StatusStopped
	observer.mu.Unlock()
	replacement, err := f.server.scheduler.PrepareExecutor(id, observer, &orchestrator.ScheduleConfig{
		Type:      orchestrator.ScheduleDependency,
		DependsOn: []string{schedulerFaultDependency},
	})
	if err != nil {
		t.Fatalf("prepare observable dependency runner: %v", err)
	}
	replacement.Commit()
	f.server.mu.Lock()
	f.server.pipelines[id] = observer
	f.server.mu.Unlock()
	if oldRunner != nil {
		_ = oldRunner.Stop()
	}
	return observer
}

func installObservableMemoryRunner(t *testing.T, f *serverFaultFixture, id string) pipeline.RunnerInterface {
	t.Helper()
	observer := newTestScheduledRunner()
	observer.mu.Lock()
	observer.status = pipeline.StatusStopped
	observer.mu.Unlock()
	f.server.mu.Lock()
	oldRunner := f.server.pipelines[id]
	f.server.pipelines[id] = observer
	f.server.mu.Unlock()
	if oldRunner != nil {
		_ = oldRunner.Stop()
	}
	return observer
}

func assertScheduledMutationPreservedLastSuccess(
	t *testing.T,
	f *serverFaultFixture,
	id string,
	dag bool,
	wantRevision string,
	wantVersions int,
	oldRunner pipeline.RunnerInterface,
) {
	t.Helper()
	assertRecoveredRevision(t, f.server, id, dag, wantRevision)
	f.server.mu.RLock()
	gotRunner := f.server.pipelines[id]
	f.server.mu.RUnlock()
	if gotRunner != oldRunner {
		t.Fatalf("failed mutation replaced in-memory runner: got=%p want=%p", gotRunner, oldRunner)
	}
	row, err := f.store.GetPipeline(context.Background(), id)
	if err != nil || row == nil || !strings.Contains(row.SpecYAML, wantRevision) {
		t.Fatalf("current row after failed mutation = %#v err=%v, want revision %q", row, err, wantRevision)
	}
	versions, err := f.store.ListPipelineVersions(context.Background(), id)
	if err != nil || len(versions) != wantVersions {
		t.Fatalf("versions after failed mutation = %#v err=%v, want %d", versions, err, wantVersions)
	}

	// The old dependency registration is the observable scheduler boundary. If
	// staging removed it without compensation, this notification does nothing.
	if oldRunner.Status() != pipeline.StatusStopped {
		t.Fatalf("old runner status before dependency notification = %q, want stopped", oldRunner.Status())
	}
	f.server.scheduler.NotifyDependents(schedulerFaultDependency)
	deadline := time.Now().Add(2 * time.Second)
	for oldRunner.Status() == pipeline.StatusStopped && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if oldRunner.Status() == pipeline.StatusStopped {
		t.Fatal("old dependency scheduler registration was not restored")
	}
	select {
	case <-oldRunner.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("restored dependency runner did not finish")
	}
	// Scheduler run-history/status writes happen immediately after Done closes.
	// Let that goroutine finish before fixture cleanup closes the SQLite store.
	time.Sleep(20 * time.Millisecond)
}

func TestPipelineCreateSchedulerFailureLeavesNoRuntimeOrRows(t *testing.T) {
	for _, dag := range []bool{false, true} {
		format := "linear"
		if dag {
			format = "dag"
		}
		for _, scheduleKind := range []string{"cron", "periodic", "dependency"} {
			t.Run(format+"/"+scheduleKind, func(t *testing.T) {
				f := newServerFaultFixture(t)
				enableRuntimeSchedulerForFaultTest(t, f)
				f.server.scheduler.SetRegisterFailureInjector(func(_ string, cfg *orchestrator.ScheduleConfig) error {
					if cfg != nil && string(cfg.Type) == scheduleKind {
						return errors.New("injected scheduler registration failure")
					}
					return nil
				})

				name := "create-scheduler-failure-" + format + "-" + scheduleKind
				response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines", map[string]any{
					"spec": scheduledRecoverySpec(f, dag, name, "revision-one", scheduleKind),
				})
				if response.Code < 400 {
					t.Fatalf("create status=%d body=%s, want non-2xx", response.Code, response.Body.String())
				}
				f.server.mu.RLock()
				runtimeCount := len(f.server.pipelines)
				f.server.mu.RUnlock()
				if runtimeCount != 0 {
					t.Fatalf("runtime pipelines after scheduler failure = %d, want 0", runtimeCount)
				}
				rows, err := f.store.ListPipelines(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(rows) != 0 {
					t.Fatalf("persisted rows after scheduler failure: %#v", rows)
				}
			})
		}
	}
}

func TestPipelineUpdateSchedulerFailureKeepsLastSuccessfulBoundary(t *testing.T) {
	for _, dag := range []bool{false, true} {
		format := "linear"
		if dag {
			format = "dag"
		}
		t.Run(format, func(t *testing.T) {
			f := newServerFaultFixture(t)
			enableRuntimeSchedulerForFaultTest(t, f)
			name := "update-scheduler-failure-" + format
			id := createScheduledFaultPipeline(t, f, dag, name, "revision-one", "dependency")
			oldRunner := installObservableDependencyRunner(t, f, id)

			f.server.scheduler.SetRegisterFailureInjector(func(string, *orchestrator.ScheduleConfig) error {
				return errors.New("injected scheduler update failure")
			})
			response := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines", map[string]any{
				"id": id, "spec": scheduledRecoverySpec(f, dag, name, "revision-two", "periodic"),
			})
			if response.Code < 400 {
				t.Fatalf("update status=%d body=%s, want non-2xx", response.Code, response.Body.String())
			}
			assertScheduledMutationPreservedLastSuccess(t, f, id, dag, "revision-one", 1, oldRunner)
		})
	}
}

func TestSpecImportSchedulerFailureKeepsLastSuccessfulBoundary(t *testing.T) {
	for _, dag := range []bool{false, true} {
		format := "linear"
		if dag {
			format = "dag"
		}
		t.Run(format, func(t *testing.T) {
			f := newServerFaultFixture(t)
			enableRuntimeSchedulerForFaultTest(t, f)
			name := "import-scheduler-failure-" + format
			id := createScheduledFaultPipeline(t, f, dag, name, "revision-one", "dependency")
			oldRunner := installObservableDependencyRunner(t, f, id)

			f.server.scheduler.SetRegisterFailureInjector(func(string, *orchestrator.ScheduleConfig) error {
				return errors.New("injected scheduler import failure")
			})
			body, err := json.Marshal(scheduledRecoverySpec(f, dag, name, "revision-two", "cron"))
			if err != nil {
				t.Fatal(err)
			}
			response := specCryptoRawRequest(t, f.server, http.MethodPost, "/api/v2/specs/import", "application/x-yaml", body)
			if response.Code < 400 {
				t.Fatalf("import status=%d body=%s, want non-2xx", response.Code, response.Body.String())
			}
			assertScheduledMutationPreservedLastSuccess(t, f, id, dag, "revision-one", 1, oldRunner)
		})
	}
}

func TestPipelineRollbackSchedulerFailureKeepsLastSuccessfulBoundary(t *testing.T) {
	for _, dag := range []bool{false, true} {
		format := "linear"
		if dag {
			format = "dag"
		}
		t.Run(format, func(t *testing.T) {
			f := newServerFaultFixture(t)
			enableRuntimeSchedulerForFaultTest(t, f)
			name := "rollback-scheduler-failure-" + format
			id := createScheduledFaultPipeline(t, f, dag, name, "revision-one", "periodic")
			updated := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines", map[string]any{
				"id": id, "spec": scheduledRecoverySpec(f, dag, name, "revision-two", "dependency"),
			})
			if updated.Code != http.StatusOK {
				t.Fatalf("update status=%d body=%s", updated.Code, updated.Body.String())
			}
			oldRunner := installObservableDependencyRunner(t, f, id)

			f.server.scheduler.SetRegisterFailureInjector(func(string, *orchestrator.ScheduleConfig) error {
				return errors.New("injected scheduler rollback failure")
			})
			response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines/"+id+"/versions/1/rollback", nil)
			if response.Code < 400 {
				t.Fatalf("rollback status=%d body=%s, want non-2xx", response.Code, response.Body.String())
			}
			assertScheduledMutationPreservedLastSuccess(t, f, id, dag, "revision-two", 2, oldRunner)
		})
	}
}

func TestStorageFailureAfterSchedulerStagingRestoresOldRegistration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, f *serverFaultFixture, id, pipelineName string) *httptest.ResponseRecorder
	}{
		{
			name: "update",
			mutate: func(t *testing.T, f *serverFaultFixture, id, pipelineName string) *httptest.ResponseRecorder {
				return specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines", map[string]any{
					"id": id, "spec": scheduledRecoverySpec(f, false, pipelineName, "revision-two", "periodic"),
				})
			},
		},
		{
			name: "import",
			mutate: func(t *testing.T, f *serverFaultFixture, _ string, pipelineName string) *httptest.ResponseRecorder {
				body, err := json.Marshal(scheduledRecoverySpec(f, false, pipelineName, "revision-two", "periodic"))
				if err != nil {
					t.Fatal(err)
				}
				return specCryptoRawRequest(t, f.server, http.MethodPost, "/api/v2/specs/import", "application/x-yaml", body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newServerFaultFixture(t)
			enableRuntimeSchedulerForFaultTest(t, f)
			name := "storage-after-stage-" + tt.name
			id := createScheduledFaultPipeline(t, f, false, name, "revision-one", "dependency")
			oldRunner := installObservableDependencyRunner(t, f, id)
			f.store.SetFailureInjector(func(operation string) error {
				if operation == "pipeline.version" {
					return errors.New("injected version failure after scheduler staging")
				}
				return nil
			})

			response := tt.mutate(t, f, id, name)
			if response.Code < 400 {
				t.Fatalf("%s status=%d body=%s, want non-2xx", tt.name, response.Code, response.Body.String())
			}
			assertScheduledMutationPreservedLastSuccess(t, f, id, false, "revision-one", 1, oldRunner)
		})
	}

	t.Run("rollback", func(t *testing.T) {
		f := newServerFaultFixture(t)
		enableRuntimeSchedulerForFaultTest(t, f)
		name := "storage-after-stage-rollback"
		id := createScheduledFaultPipeline(t, f, false, name, "revision-one", "periodic")
		updated := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines", map[string]any{
			"id": id, "spec": scheduledRecoverySpec(f, false, name, "revision-two", "dependency"),
		})
		if updated.Code != http.StatusOK {
			t.Fatalf("update status=%d body=%s", updated.Code, updated.Body.String())
		}
		oldRunner := installObservableDependencyRunner(t, f, id)
		f.store.SetFailureInjector(func(operation string) error {
			if operation == "pipeline.version" {
				return errors.New("injected rollback version failure after scheduler staging")
			}
			return nil
		})

		response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines/"+id+"/versions/1/rollback", nil)
		if response.Code < 400 {
			t.Fatalf("rollback status=%d body=%s, want non-2xx", response.Code, response.Body.String())
		}
		assertScheduledMutationPreservedLastSuccess(t, f, id, false, "revision-two", 2, oldRunner)
	})
}

func TestScheduleEnableStorageFailureRemovesStagedRegistration(t *testing.T) {
	f := newServerFaultFixture(t)
	enableRuntimeSchedulerForFaultTest(t, f)
	id := createServerFaultPipeline(t, f, false, "schedule-enable-storage-failure", "revision-one")
	f.server.mu.RLock()
	var beforeSchedule *pipeline.ScheduleConfig
	if f.server.specs[id].Schedule != nil {
		copySchedule := *f.server.specs[id].Schedule
		beforeSchedule = &copySchedule
	}
	f.server.mu.RUnlock()
	oldRunner := installObservableMemoryRunner(t, f, id)
	f.store.SetFailureInjector(func(operation string) error {
		if operation == "pipeline.version" {
			return errors.New("injected schedule enable version failure")
		}
		return nil
	})

	response := specCryptoRequest(t, f.server, http.MethodPut, "/api/v2/pipelines/"+id+"/schedule", schedulerFaultSchedule("dependency"))
	if response.Code < 400 {
		t.Fatalf("schedule enable status=%d body=%s, want non-2xx", response.Code, response.Body.String())
	}
	f.server.mu.RLock()
	gotSchedule := f.server.specs[id].Schedule
	f.server.mu.RUnlock()
	if !reflect.DeepEqual(gotSchedule, beforeSchedule) {
		t.Fatalf("failed schedule enable changed in-memory schedule: got=%#v want=%#v", gotSchedule, beforeSchedule)
	}
	if oldRunner.Status() != pipeline.StatusStopped {
		t.Fatalf("old runner status before dependency notification = %q, want stopped", oldRunner.Status())
	}
	f.server.scheduler.NotifyDependents(schedulerFaultDependency)
	if oldRunner.Status() != pipeline.StatusStopped {
		t.Fatalf("failed schedule enable left staged dependency registration active; status=%q", oldRunner.Status())
	}
}

func TestScheduleDisableStorageFailureRestoresOldRegistration(t *testing.T) {
	f := newServerFaultFixture(t)
	enableRuntimeSchedulerForFaultTest(t, f)
	name := "schedule-disable-storage-failure"
	id := createScheduledFaultPipeline(t, f, false, name, "revision-one", "dependency")
	oldRunner := installObservableDependencyRunner(t, f, id)
	f.store.SetFailureInjector(func(operation string) error {
		if operation == "pipeline.version" {
			return errors.New("injected schedule disable version failure")
		}
		return nil
	})

	response := specCryptoRequest(t, f.server, http.MethodDelete, "/api/v2/pipelines/"+id+"/schedule", nil)
	if response.Code < 400 {
		t.Fatalf("schedule disable status=%d body=%s, want non-2xx", response.Code, response.Body.String())
	}
	assertScheduledMutationPreservedLastSuccess(t, f, id, false, "revision-one", 1, oldRunner)
}

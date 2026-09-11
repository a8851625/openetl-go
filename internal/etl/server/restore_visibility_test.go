package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

type restoreFixture struct {
	id    string
	name  string
	stage string
	spec  string
}

func restoreFailureFixtures(dir string) []restoreFixture {
	input := filepath.Join(dir, "input.jsonl")
	output := filepath.Join(dir, "output")
	return []restoreFixture{
		{
			id:    "restore-dag-yaml",
			name:  "restore-dag-yaml",
			stage: restoreStageDAGYAMLParse,
			spec:  "name: restore-dag-yaml\ndag: invalid\n",
		},
		{
			id:    "restore-linear-yaml",
			name:  "restore-linear-yaml",
			stage: restoreStageLinearYAMLParse,
			spec:  "name: restore-linear-yaml\nsource: [\n",
		},
		{
			id:    "restore-dag-connection",
			name:  "restore-dag-connection",
			stage: restoreStageDAGConnectionResolve,
			spec: `name: restore-dag-connection
dag:
  nodes:
    - id: source1
      kind: source
      plugin: file
      connection: missing-dag-source
    - id: sink1
      kind: sink
      plugin: file_sink
      config:
        output_dir: ` + output + `
  edges:
    - from: source1
      to: sink1
`,
		},
		{
			id:    "restore-linear-connection",
			name:  "restore-linear-connection",
			stage: restoreStageLinearConnectionResolve,
			spec: `name: restore-linear-connection
source:
  type: file
  connection: missing-linear-source
sink:
  type: file_sink
  config:
    output_dir: ` + output + `
`,
		},
		{
			id:    "restore-spec-validate",
			name:  "restore-spec-validate",
			stage: restoreStageSpecValidate,
			spec: `name: restore-spec-validate
source:
  type: restore_unknown_source
sink:
  type: file_sink
  config:
    output_dir: ` + output + `
`,
		},
		{
			id:    "restore-runner-build",
			name:  "restore-runner-build",
			stage: restoreStageRunnerBuild,
			spec: `name: restore-runner-build
source:
  type: file
  config:
    path: ` + input + `
    format: json
sink:
  type: file_sink
  config:
    output_dir: ` + output + `
    format: xml
`,
		},
	}
}

func configureRestoreTestProfile(t *testing.T, profile, strict string) {
	t.Helper()
	t.Setenv("ETL_PROFILE", profile)
	t.Setenv("ETL_INSECURE_DEV", "true")
	t.Setenv("ETL_RESTORE_STRICT", strict)
	t.Setenv("ETL_API_TOKEN", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")
	t.Setenv("ETL_TLS_CERT", "")
	t.Setenv("ETL_TLS_KEY", "")
}

func newRestoreTestServer(t *testing.T, store storage.Storage, dir string) *Server {
	t.Helper()
	s, err := NewServer(store, dir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		if s.alertManager != nil {
			s.alertManager.Close()
		}
	})
	return s
}

func restoreRequest(t *testing.T, s *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder
}

func TestRestoreFailuresPersistAndRemainVisible(t *testing.T) {
	configureRestoreTestProfile(t, RuntimeProfileDevelopment, "false")
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fixtures := restoreFailureFixtures(dir)
	for _, fixture := range fixtures {
		if err := store.SavePipeline(context.Background(), &storage.PipelineRow{
			ID:       fixture.id,
			Name:     fixture.name,
			SpecYAML: fixture.spec,
			Status:   "paused",
		}); err != nil {
			t.Fatalf("save %s: %v", fixture.name, err)
		}
	}

	s := newRestoreTestServer(t, store, dir)
	if err := s.RestoreFromDB(context.Background()); err != nil {
		t.Fatalf("non-strict RestoreFromDB: %v", err)
	}
	if len(s.restoreFailures) != len(fixtures) {
		t.Fatalf("restore failures=%d want=%d: %#v", len(s.restoreFailures), len(fixtures), s.restoreFailures)
	}

	for _, fixture := range fixtures {
		row, err := store.GetPipeline(context.Background(), fixture.id)
		if err != nil || row == nil {
			t.Fatalf("get %s: row=%+v err=%v", fixture.name, row, err)
		}
		if row.Status != pipelineStatusRestoreFailed || row.RestoreError == "" {
			t.Fatalf("persisted %s state=%+v", fixture.name, row)
		}
		var failure RestoreFailure
		if err := json.Unmarshal([]byte(row.RestoreError), &failure); err != nil {
			t.Fatalf("decode %s restore_error: %v", fixture.name, err)
		}
		if failure.Stage != fixture.stage || failure.Code == "" || failure.Message == "" || failure.Remediation == "" {
			t.Fatalf("%s failure=%+v want stage=%s", fixture.name, failure, fixture.stage)
		}
		if failure.PreviousStatus != "paused" || failure.FailedAt.IsZero() {
			t.Fatalf("%s previous/timestamp not preserved: %+v", fixture.name, failure)
		}
	}

	listResponse := restoreRequest(t, s, http.MethodGet, "/api/v2/pipelines")
	if listResponse.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	var list struct {
		Pipelines []struct {
			ID           string         `json:"id"`
			Status       string         `json:"status"`
			RestoreError RestoreFailure `json:"restore_error"`
		} `json:"pipelines"`
	}
	if err := json.Unmarshal(listResponse.Body.Bytes(), &list); err != nil {
		t.Fatalf("decode pipeline list: %v", err)
	}
	if len(list.Pipelines) != len(fixtures) {
		t.Fatalf("list pipelines=%d want=%d body=%s", len(list.Pipelines), len(fixtures), listResponse.Body.String())
	}
	listed := make(map[string]RestoreFailure, len(list.Pipelines))
	for _, item := range list.Pipelines {
		if item.Status != pipelineStatusRestoreFailed {
			t.Errorf("list %s status=%q", item.ID, item.Status)
		}
		listed[item.ID] = item.RestoreError
	}
	for _, fixture := range fixtures {
		if listed[fixture.id].Stage != fixture.stage {
			t.Errorf("list %s stage=%q want=%q", fixture.id, listed[fixture.id].Stage, fixture.stage)
		}
		single := restoreRequest(t, s, http.MethodGet, "/api/v2/pipelines/"+fixture.id)
		if single.Code != http.StatusOK || !strings.Contains(single.Body.String(), `"status":"restore_failed"`) || !strings.Contains(single.Body.String(), fixture.stage) {
			t.Errorf("single %s status=%d body=%s", fixture.id, single.Code, single.Body.String())
		}
	}
	start := restoreRequest(t, s, http.MethodPost, "/api/v2/pipelines/"+fixtures[0].id+"/start")
	if start.Code != http.StatusConflict || !strings.Contains(start.Body.String(), fixtures[0].stage) {
		t.Fatalf("restore-failed start status=%d body=%s", start.Code, start.Body.String())
	}

	health := s.getHealthStatus()
	if health["status"] != "degraded" || health["restore_failed_count"] != fmt.Sprint(len(fixtures)) {
		t.Fatalf("health summary=%#v", health)
	}
	var issues map[string]map[string]string
	if err := json.Unmarshal([]byte(health["pipeline_issues"]), &issues); err != nil {
		t.Fatalf("decode health issues: %v (%q)", err, health["pipeline_issues"])
	}
	for _, fixture := range fixtures {
		if health["pipeline_"+fixture.name] != pipelineStatusRestoreFailed {
			t.Errorf("health %s=%q", fixture.name, health["pipeline_"+fixture.name])
		}
		if issues[fixture.name]["stage"] != fixture.stage || issues[fixture.name]["remediation"] == "" {
			t.Errorf("health issue %s=%#v", fixture.name, issues[fixture.name])
		}
	}

	// Production defaults to strict restore. It still inspects and persists the
	// complete set before returning the startup-blocking error.
	configureRestoreTestProfile(t, RuntimeProfileProduction, "")
	strictServer := newRestoreTestServer(t, store, dir)
	if !strictServer.runtimeProfile.RestoreStrict {
		t.Fatal("production profile did not default restore strict mode to true")
	}
	err = strictServer.RestoreFromDB(context.Background())
	var strictErr *RestoreFailuresError
	if !errors.As(err, &strictErr) {
		t.Fatalf("strict restore error=%T %v, want *RestoreFailuresError", err, err)
	}
	if len(strictErr.Failures) != len(fixtures) {
		t.Fatalf("strict failures=%d want=%d", len(strictErr.Failures), len(fixtures))
	}
	for _, fixture := range fixtures {
		if !strings.Contains(err.Error(), fixture.name) || !strings.Contains(err.Error(), "stage="+fixture.stage) {
			t.Errorf("strict error missing %s/%s: %v", fixture.name, fixture.stage, err)
		}
	}
}

func TestRestoreFailureRepairPreservesCheckpointAndEncryptedSpec(t *testing.T) {
	configureRestoreTestProfile(t, RuntimeProfileDevelopment, "false")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", specCryptoTestKey(81))
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "restore-primary")
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	const id = "restore-repair-id"
	const name = "restore-repair"
	const connectionName = "restore-repair-source"
	spec := `name: ` + name + `
source:
  connection: ` + connectionName + `
sink:
  type: file_sink
  config:
    output_dir: ` + filepath.Join(dir, "output") + `
    format: jsonl
`

	first := newRestoreTestServer(t, store, dir)
	if err := first.specStore.SaveWithID(ctx, id, name, spec, "paused"); err != nil {
		t.Fatalf("save encrypted spec: %v", err)
	}
	rawBefore, err := store.GetPipeline(ctx, id)
	if err != nil || rawBefore == nil || !strings.HasPrefix(rawBefore.SpecYAML, "enc:v1:") {
		t.Fatalf("raw encrypted row=%+v err=%v", rawBefore, err)
	}
	checkpointTime := time.Now().UTC().Truncate(time.Second)
	wantPosition := json.RawMessage(`{"offset":41,"byte_offset":1024}`)
	if err := store.SaveCheckpoint(ctx, &storage.CheckpointRecord{
		JobName:   id,
		Source:    "file",
		Position:  wantPosition,
		Timestamp: checkpointTime,
	}); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	if err := first.RestoreFromDB(ctx); err != nil {
		t.Fatalf("first non-strict restore: %v", err)
	}
	failedRow, err := store.GetPipeline(ctx, id)
	if err != nil || failedRow == nil {
		t.Fatalf("get failed row: %+v %v", failedRow, err)
	}
	if failedRow.Status != pipelineStatusRestoreFailed || failedRow.SpecYAML != rawBefore.SpecYAML {
		t.Fatalf("failure state rewrote encrypted spec: before=%q after=%+v", rawBefore.SpecYAML, failedRow)
	}
	var failure RestoreFailure
	if err := json.Unmarshal([]byte(failedRow.RestoreError), &failure); err != nil {
		t.Fatalf("decode restore error: %v", err)
	}
	if failure.PreviousStatus != "paused" || failure.Stage != restoreStageLinearConnectionResolve {
		t.Fatalf("failure=%+v", failure)
	}

	if err := store.SaveConnection(ctx, &storage.ConnectionEntry{
		Name: connectionName,
		Kind: "source",
		Type: "file",
		Config: map[string]any{
			"path":   filepath.Join(dir, "input.jsonl"),
			"format": "json",
		},
	}); err != nil {
		t.Fatalf("repair connection: %v", err)
	}
	second := newRestoreTestServer(t, store, dir)
	if err := second.RestoreFromDB(ctx); err != nil {
		t.Fatalf("restore after repair: %v", err)
	}
	if second.pipelines[id] == nil {
		t.Fatal("repaired pipeline runner was not restored")
	}
	if _, failed := second.restoreFailures[id]; failed {
		t.Fatal("repaired pipeline retained in-memory restore failure")
	}

	repairedRow, err := store.GetPipeline(ctx, id)
	if err != nil || repairedRow == nil {
		t.Fatalf("get repaired row: %+v %v", repairedRow, err)
	}
	if repairedRow.Status != "paused" || repairedRow.RestoreError != "" {
		t.Fatalf("repaired durable state=%+v", repairedRow)
	}
	if repairedRow.SpecYAML != rawBefore.SpecYAML {
		t.Fatal("repair restore changed encrypted spec bytes")
	}
	cp, err := store.LoadCheckpoint(ctx, id)
	if err != nil || cp == nil {
		t.Fatalf("load checkpoint after repair: cp=%+v err=%v", cp, err)
	}
	if cp.Source != "file" || string(cp.Position) != string(wantPosition) || cp.Timestamp.Unix() != checkpointTime.Unix() {
		t.Fatalf("checkpoint changed across restore failure/repair: %+v", cp)
	}
	versions, err := store.ListPipelineVersions(ctx, id)
	if err != nil || len(versions) != 1 || versions[0].SpecYAML != rawBefore.SpecYAML {
		t.Fatalf("spec versions changed across restore state updates: versions=%+v err=%v", versions, err)
	}
}

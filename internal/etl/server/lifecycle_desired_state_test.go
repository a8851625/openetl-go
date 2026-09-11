package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

const (
	lifecycleProbeSourceType = "test_lifecycle_probe_source"
	lifecycleProbeSinkType   = "test_lifecycle_probe_sink"
)

type lifecycleIOProbe struct {
	sourceOpen atomic.Int64
	sourceRead atomic.Int64
	sinkOpen   atomic.Int64
	sinkWrite  atomic.Int64
}

var lifecycleIOProbes sync.Map

func init() {
	registry.RegisterSource(lifecycleProbeSourceType, func(config map[string]any) (core.Source, error) {
		return lifecycleProbeSource{probe: lifecycleProbeFromConfig(config)}, nil
	})
	registry.RegisterSink(lifecycleProbeSinkType, func(config map[string]any) (core.Sink, error) {
		return lifecycleProbeSink{probe: lifecycleProbeFromConfig(config)}, nil
	})
}

func lifecycleProbeFromConfig(config map[string]any) *lifecycleIOProbe {
	id, _ := config["probe"].(string)
	value, _ := lifecycleIOProbes.LoadOrStore(id, &lifecycleIOProbe{})
	return value.(*lifecycleIOProbe)
}

type lifecycleProbeSource struct{ probe *lifecycleIOProbe }

func (s lifecycleProbeSource) Name() string { return lifecycleProbeSourceType }
func (s lifecycleProbeSource) Open(context.Context, *core.Checkpoint) (core.RecordReader, error) {
	s.probe.sourceOpen.Add(1)
	return lifecycleProbeReader{probe: s.probe}, nil
}

type lifecycleProbeReader struct{ probe *lifecycleIOProbe }

func (r lifecycleProbeReader) Read(context.Context) (core.Record, error) {
	r.probe.sourceRead.Add(1)
	return core.Record{}, io.EOF
}
func (r lifecycleProbeReader) ReadBatch(context.Context, int) ([]core.Record, error) {
	r.probe.sourceRead.Add(1)
	return nil, io.EOF
}
func (r lifecycleProbeReader) Snapshot(context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{Source: lifecycleProbeSourceType, Position: json.RawMessage(`{"offset":0}`)}, nil
}
func (r lifecycleProbeReader) Close() error { return nil }

type lifecycleProbeSink struct{ probe *lifecycleIOProbe }

func (s lifecycleProbeSink) Name() string { return lifecycleProbeSinkType }
func (s lifecycleProbeSink) Open(context.Context) error {
	s.probe.sinkOpen.Add(1)
	return nil
}
func (s lifecycleProbeSink) Write(context.Context, []core.Record) error {
	s.probe.sinkWrite.Add(1)
	return nil
}
func (s lifecycleProbeSink) Close() error { return nil }

func TestDesiredStoppedAndPausedSurviveRestoreWithoutIO(t *testing.T) {
	for _, desired := range []string{storage.PipelineDesiredStopped, storage.PipelineDesiredPaused} {
		t.Run(desired, func(t *testing.T) {
			t.Setenv("ETL_PROFILE", "development")
			t.Setenv("ETL_RESTORE_STRICT", "false")
			t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")

			dir := t.TempDir()
			dbPath := filepath.Join(dir, "etl.db")
			id := "restore-" + desired
			probeID := t.Name()
			probe := &lifecycleIOProbe{}
			lifecycleIOProbes.Store(probeID, probe)
			t.Cleanup(func() { lifecycleIOProbes.Delete(probeID) })

			spec := &pipeline.Spec{
				Name: id,
				Source: pipeline.SourceSpec{Type: lifecycleProbeSourceType, Config: map[string]any{
					"probe": probeID,
				}},
				Sink: pipeline.SinkSpec{Type: lifecycleProbeSinkType, Config: map[string]any{
					"probe": probeID,
				}},
				BatchSize: 1,
			}
			specYAML, err := pipeline.MarshalSpecYAML(spec)
			if err != nil {
				t.Fatalf("MarshalSpecYAML: %v", err)
			}
			seed, err := sqlite.New(dbPath)
			if err != nil {
				t.Fatalf("sqlite.New seed: %v", err)
			}
			if err := seed.SavePipeline(context.Background(), &storage.PipelineRow{
				ID:            id,
				Name:          id,
				SpecYAML:      string(specYAML),
				Status:        desired,
				DesiredState:  desired,
				ObservedState: desired,
				Generation:    7,
			}); err != nil {
				_ = seed.Close()
				t.Fatalf("seed pipeline: %v", err)
			}
			if err := seed.Close(); err != nil {
				t.Fatalf("close seed store: %v", err)
			}

			store, err := sqlite.New(dbPath)
			if err != nil {
				t.Fatalf("sqlite.New restored: %v", err)
			}
			s, err := NewServer(store, filepath.Join(dir, "pipes"))
			if err != nil {
				_ = store.Close()
				t.Fatalf("NewServer: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				s.StopAll()
				_ = store.Close()
			})
			if err := s.RestoreFromDB(ctx); err != nil {
				t.Fatalf("RestoreFromDB: %v", err)
			}
			if err := s.StartAll(ctx); err != nil {
				t.Fatalf("StartAll: %v", err)
			}
			time.Sleep(75 * time.Millisecond)

			if got := probe.sourceOpen.Load(); got != 0 {
				t.Fatalf("source Open calls=%d, want 0", got)
			}
			if got := probe.sourceRead.Load(); got != 0 {
				t.Fatalf("source Read calls=%d, want 0", got)
			}
			if got := probe.sinkOpen.Load(); got != 0 {
				t.Fatalf("sink Open calls=%d, want 0", got)
			}
			if got := probe.sinkWrite.Load(); got != 0 {
				t.Fatalf("sink Write calls=%d, want 0", got)
			}
			row, err := store.GetPipeline(ctx, id)
			if err != nil {
				t.Fatalf("GetPipeline: %v", err)
			}
			if row == nil || row.DesiredState != desired || row.ObservedState != desired || row.Generation != 7 {
				t.Fatalf("lifecycle row=%#v, want desired/observed=%s generation=7", row, desired)
			}
		})
	}
}

func TestDesiredStatePersistenceFailureDoesNotChangeRuntime(t *testing.T) {
	for _, tc := range []struct {
		name          string
		action        string
		initialStatus pipeline.Status
		desired       string
		wantStatus    int
	}{
		{name: "start", action: "start", initialStatus: pipeline.StatusStopped, desired: storage.PipelineDesiredStopped, wantStatus: http.StatusUnprocessableEntity},
		{name: "stop", action: "stop", initialStatus: pipeline.StatusRunning, desired: storage.PipelineDesiredRunning, wantStatus: http.StatusInternalServerError},
		{name: "pause", action: "pause", initialStatus: pipeline.StatusRunning, desired: storage.PipelineDesiredRunning, wantStatus: http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newServerFaultFixture(t)
			id := "desired-failure-" + tc.name
			runner := newTestScheduledRunner()
			runner.mu.Lock()
			runner.status = tc.initialStatus
			runner.mu.Unlock()
			f.server.mu.Lock()
			f.server.registerPipelineLocked(id, id, runner, nil, nil)
			f.server.mu.Unlock()
			if err := f.store.SavePipeline(context.Background(), &storage.PipelineRow{
				ID: id, Name: id, SpecYAML: "name: " + id, Status: string(tc.initialStatus),
				DesiredState: tc.desired, ObservedState: string(tc.initialStatus),
			}); err != nil {
				t.Fatalf("SavePipeline: %v", err)
			}
			f.store.SetFailureInjector(func(operation string) error {
				if operation == "pipeline.desired_state" {
					return io.ErrClosedPipe
				}
				return nil
			})

			response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines/"+id+"/"+tc.action, nil)
			if response.Code != tc.wantStatus {
				t.Fatalf("%s status=%d body=%s, want %d", tc.action, response.Code, response.Body.String(), tc.wantStatus)
			}
			if got := runner.Status(); got != tc.initialStatus {
				t.Fatalf("runtime status=%q, want unchanged %q", got, tc.initialStatus)
			}
			if tc.action == "start" && runner.starts.Load() != 0 {
				t.Fatalf("runner starts=%d, want 0", runner.starts.Load())
			}
			row, err := f.store.GetPipeline(context.Background(), id)
			if err != nil {
				t.Fatalf("GetPipeline: %v", err)
			}
			if row == nil || row.DesiredState != tc.desired {
				t.Fatalf("persisted desired state=%#v, want %q", row, tc.desired)
			}
		})
	}
}

func TestRunningPipelineRejectsCheckpointResetAndSet(t *testing.T) {
	f := newServerFaultFixture(t)
	id := "running-checkpoint-admin"
	runner := newTestScheduledRunner()
	runner.mu.Lock()
	runner.status = pipeline.StatusRunning
	runner.mu.Unlock()
	spec := &pipeline.Spec{Name: id, Source: pipeline.SourceSpec{Type: "kafka"}}
	f.server.mu.Lock()
	f.server.registerPipelineLocked(id, id, runner, spec, nil)
	f.server.mu.Unlock()
	if err := f.store.SavePipeline(context.Background(), &storage.PipelineRow{
		ID: id, Name: id, SpecYAML: "name: " + id, Status: "running",
		DesiredState: "running", ObservedState: "running", Generation: 4,
	}); err != nil {
		t.Fatalf("SavePipeline: %v", err)
	}
	wantPosition := json.RawMessage(`{"topic":"orders","offsets":{"0":19}}`)
	if err := f.store.SaveCheckpoint(context.Background(), &storage.CheckpointRecord{
		JobName: id, Source: "kafka", Position: wantPosition, Generation: 4, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	requests := []struct {
		name string
		path string
		body any
	}{
		{name: "reset", path: "/checkpoint/reset"},
		{name: "set", path: "/checkpoint/set", body: map[string]any{
			"source": "kafka", "topic": "orders", "partition": 0, "offset": 10,
		}},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			response := specCryptoRequest(t, f.server, http.MethodPost, "/api/v2/pipelines/"+id+request.path, request.body)
			if response.Code != http.StatusConflict {
				t.Fatalf("status=%d body=%s, want 409", response.Code, response.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode conflict: %v", err)
			}
			if body["code"] != "pipeline_not_quiescent" || body["remediation"] == "" {
				t.Fatalf("conflict response=%#v", body)
			}
			row, err := f.store.GetPipeline(context.Background(), id)
			if err != nil || row == nil || row.Generation != 4 {
				t.Fatalf("pipeline generation after rejected %s = %#v err=%v", request.name, row, err)
			}
			cp, err := f.store.LoadCheckpoint(context.Background(), id)
			if err != nil || cp == nil || string(cp.Position) != string(wantPosition) || cp.Generation != 4 {
				t.Fatalf("checkpoint after rejected %s = %#v err=%v", request.name, cp, err)
			}
		})
	}
}

func TestCheckpointResetResponseDocumentsSourceBoundary(t *testing.T) {
	tests := []struct {
		source string
		want   string
	}{
		{source: "kafka", want: "consumer-group offsets"},
		{source: "mysql_cdc", want: "current MySQL master position"},
		{source: "mysql_snapshot_cdc", want: "new snapshot phase"},
		{source: "postgres_cdc", want: "replication slot"},
	}
	for _, tc := range tests {
		t.Run(tc.source, func(t *testing.T) {
			s, ts := newTestHTTPServer(t)
			defer ts.Close()
			id := "reset-" + tc.source
			runner := newTestScheduledRunner()
			runner.mu.Lock()
			runner.status = pipeline.StatusStopped
			runner.mu.Unlock()
			s.mu.Lock()
			s.registerPipelineLocked(id, id, runner, &pipeline.Spec{
				Name: id, Source: pipeline.SourceSpec{Type: tc.source},
			}, nil)
			s.mu.Unlock()
			saveLifecycleTestPipeline(t, s, id, id, "stopped")

			response := specCryptoRequest(t, s, http.MethodPost, "/api/v2/pipelines/"+id+"/checkpoint/reset", nil)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var body struct {
				Generation     int64                   `json:"generation"`
				ResetSemantics checkpointResetContract `json:"reset_semantics"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if body.Generation != 1 || body.ResetSemantics.Source != tc.source {
				t.Fatalf("reset response=%#v", body)
			}
			combined := body.ResetSemantics.Effect + " " + body.ResetSemantics.ExternalBoundary + " " + body.ResetSemantics.OperatorAction
			if !strings.Contains(combined, tc.want) {
				t.Fatalf("reset semantics=%q, want %q", combined, tc.want)
			}
		})
	}
}

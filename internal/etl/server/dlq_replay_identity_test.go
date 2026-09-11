package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

const testDLQReplayProbeSink = "test_dlq_replay_identity_sink"

var dlqReplayProbes sync.Map

type dlqReplayProbe struct {
	mu      sync.Mutex
	records []core.Record
}

func (p *dlqReplayProbe) Name() string               { return testDLQReplayProbeSink }
func (p *dlqReplayProbe) Open(context.Context) error { return nil }
func (p *dlqReplayProbe) Close() error               { return nil }
func (p *dlqReplayProbe) Write(_ context.Context, records []core.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, records...)
	return nil
}

func (p *dlqReplayProbe) snapshot() []core.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]core.Record(nil), p.records...)
}

func init() {
	registry.RegisterSink(testDLQReplayProbeSink, func(config map[string]any) (core.Sink, error) {
		id, _ := config["probe_id"].(string)
		value, ok := dlqReplayProbes.Load(id)
		if !ok {
			return nil, fmt.Errorf("dlq replay probe %q not found", id)
		}
		return value.(*dlqReplayProbe), nil
	})
}

func newDLQReplayProbe(t *testing.T) (string, *dlqReplayProbe) {
	t.Helper()
	id := strings.ReplaceAll(t.Name(), "/", "-")
	probe := &dlqReplayProbe{}
	dlqReplayProbes.Store(id, probe)
	t.Cleanup(func() { dlqReplayProbes.Delete(id) })
	return id, probe
}

func installMetadataReplaySpec(s *Server, name, probeID string, pk []string, transforms ...pipeline.TransformSpec) *pipeline.Spec {
	staticPK := make([]any, len(pk))
	for i := range pk {
		staticPK[i] = pk[i]
	}
	spec := &pipeline.Spec{
		Name:       name,
		Source:     pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"format": "canal_json"}},
		Transforms: transforms,
		Sink: pipeline.SinkSpec{Type: testDLQReplayProbeSink, Config: map[string]any{
			"probe_id":                 probeID,
			"pk_columns_from_metadata": true,
			"pk_columns":               staticPK,
		}},
	}
	s.mu.Lock()
	s.specs[name] = spec
	s.mu.Unlock()
	return spec
}

func writeDLQReplayItem(t *testing.T, s *Server, name string, record core.Record, identity core.DLQIdentityContext) storage.DeadLetter {
	t.Helper()
	if err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName: name, Record: record, Error: "identity failure", ErrorClass: string(core.ErrorClassData), IdentityContext: identity,
	}); err != nil {
		t.Fatalf("write DLQ item: %v", err)
	}
	items, err := s.store.ListDeadLetters(context.Background(), storage.DLQFilter{JobName: name, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("read DLQ item: len=%d err=%v", len(items), err)
	}
	return storage.DeadLetter{
		ID: items[0].ID, JobName: items[0].JobName, Record: items[0].Record,
		Error: items[0].Error, ErrorClass: items[0].ErrorClass, IdentityContext: items[0].IdentityContext,
		Timestamp: items[0].CreatedAt, Attempt: items[0].Attempt, RecordHash: items[0].RecordHash,
		PipelineVersion: items[0].PipelineVersion, DAGNode: items[0].DAGNode,
	}
}

func TestDLQReplayReconstructsFrozenCompositeIdentityAndPreservesAPIContext(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	probeID, probe := newDLQReplayProbe(t)
	name := "dlq-reconstruct-composite"
	installMetadataReplaySpec(s, name, probeID, []string{"tenant_id", "id"})

	record := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"tenant_id": "acme", "id": 42, "value": "kept"},
		Metadata: core.Metadata{
			SourceType: "kafka", Database: "src", Table: "orders",
			PrimaryKeyColumns: []string{"tenant_id", "id"},
			FormatContractID:  core.FormatContractCanalJSONV1,
			RawPayload:        []byte(`{"type":"INSERT","pkNames":["tenant_id","id"]}`),
		},
	}
	identity := core.NewDLQIdentityContext(record, "key_missing", "analytics", "ods_orders")
	item := writeDLQReplayItem(t, s, name, record, identity)

	resp, err := http.Get(ts.URL + "/api/v2/dlq/" + name)
	if err != nil {
		t.Fatalf("GET DLQ: %v", err)
	}
	defer resp.Body.Close()
	var listed struct {
		Items []storage.DeadLetter `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listed); err != nil || len(listed.Items) != 1 {
		t.Fatalf("decode DLQ list: len=%d err=%v", len(listed.Items), err)
	}
	gotContext := listed.Items[0].IdentityContext
	if gotContext.RawPayload != identity.RawPayload || gotContext.FormatContractID != core.FormatContractCanalJSONV1 ||
		gotContext.TargetDatabase != "analytics" || gotContext.TargetTable != "ods_orders" {
		t.Fatalf("identity context lost through API: %+v", gotContext)
	}

	resp, err = http.Post(fmt.Sprintf("%s/api/v2/dlq/%s/%d/replay", ts.URL, name, item.ID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200", resp.StatusCode)
	}
	writes := probe.snapshot()
	if len(writes) != 1 {
		t.Fatalf("sink writes = %d, want 1", len(writes))
	}
	identityResult := core.RecordIdentity(writes[0])
	if !identityResult.Complete || writes[0].Metadata.ReplayProvenance != core.DLQReplayProvenanceReconstructed {
		t.Fatalf("replayed identity = %+v record=%+v", identityResult, writes[0])
	}
	if remaining, err := s.store.GetDeadLetterByID(context.Background(), name, item.ID); err != nil || remaining != nil {
		t.Fatalf("replayed DLQ row remains: row=%+v err=%v", remaining, err)
	}
}

func TestDLQReplayUnknownContractIsQuarantinedWithStructuredConflict(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	probeID, probe := newDLQReplayProbe(t)
	name := "dlq-unknown-contract"
	installMetadataReplaySpec(s, name, probeID, []string{"id"})
	record := core.Record{
		Operation: core.OpInsert, Data: map[string]any{"id": 7},
		Metadata: core.Metadata{PrimaryKeyColumns: []string{"id"}, FormatContractID: "custom.unknown/v9"},
	}
	item := writeDLQReplayItem(t, s, name, record, core.NewDLQIdentityContext(record, "key_missing", "", "orders"))

	resp, err := http.Post(fmt.Sprintf("%s/api/v2/dlq/%s/%d/replay", ts.URL, name, item.ID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["replay_state"] != string(core.DLQReplayStateQuarantined) || !strings.Contains(fmt.Sprint(body["reason"]), "format_contract_unknown") {
		t.Fatalf("structured gate response = %#v", body)
	}
	if len(probe.snapshot()) != 0 {
		t.Fatal("quarantined record reached sink")
	}
	persisted, err := s.store.GetDeadLetterByID(context.Background(), name, item.ID)
	if err != nil || persisted == nil || persisted.IdentityContext.ReplayState != core.DLQReplayStateQuarantined {
		t.Fatalf("quarantine not persisted: row=%+v err=%v", persisted, err)
	}
}

func TestDLQLegacyIdentityRepairAPIRequiresExactFrozenDeclaration(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	probeID, probe := newDLQReplayProbe(t)
	name := "dlq-legacy-repair"
	installMetadataReplaySpec(s, name, probeID, []string{"tenant_id", "id"})
	record := core.Record{
		Operation: core.OpInsert, Data: map[string]any{"tenant_id": "acme", "id": 9},
		Metadata: core.Metadata{PrimaryKeyColumns: []string{"tenant_id", "id"}},
	}
	item := writeDLQReplayItem(t, s, name, record, core.DLQIdentityContext{
		ReplayProvenance: core.DLQReplayProvenanceLegacyUnknown,
		ReplayState:      core.DLQReplayStateRepairRequired,
	})

	body := bytes.NewBufferString(`{"primary_key_columns":["tenant_id","id"]}`)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/api/v2/dlq/%s/%d/identity", ts.URL, name, item.ID), body)
	if err != nil {
		t.Fatalf("new repair request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT repair: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repair status = %d, want 200", resp.StatusCode)
	}
	persisted, err := s.store.GetDeadLetterByID(context.Background(), name, item.ID)
	if err != nil || persisted == nil {
		t.Fatalf("read repaired row: row=%+v err=%v", persisted, err)
	}
	if persisted.IdentityContext.ReplayProvenance != core.DLQReplayProvenanceLegacyVerified || !core.RecordIdentity(persisted.Record).Complete {
		t.Fatalf("legacy repair not persisted: %+v", persisted)
	}

	resp, err = http.Post(fmt.Sprintf("%s/api/v2/dlq/%s/%d/replay", ts.URL, name, item.ID), "application/json", nil)
	if err != nil {
		t.Fatalf("POST repaired replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(probe.snapshot()) != 1 {
		t.Fatalf("repaired replay status=%d writes=%d", resp.StatusCode, len(probe.snapshot()))
	}
}

func TestDLQLegacyIdentityRepairRejectsMismatchPartialAndKeyChange(t *testing.T) {
	tests := []struct {
		name       string
		staticPK   []string
		confirmed  []string
		record     core.Record
		wantState  core.DLQReplayState
		wantReason string
	}{
		{
			name: "confirmed declaration mismatch", staticPK: []string{"tenant_id", "id"}, confirmed: []string{"id"},
			record:     core.Record{Operation: core.OpInsert, Data: map[string]any{"tenant_id": "acme", "id": 1}, Metadata: core.Metadata{PrimaryKeyColumns: []string{"tenant_id", "id"}}},
			wantState:  core.DLQReplayStateQuarantined,
			wantReason: "confirmed primary_key_columns",
		},
		{
			name: "static target mismatch", staticPK: []string{"other_id"}, confirmed: []string{"id"},
			record:     core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{PrimaryKeyColumns: []string{"id"}}},
			wantState:  core.DLQReplayStateQuarantined,
			wantReason: "sink.config.pk_columns",
		},
		{
			name: "partial composite key", staticPK: []string{"tenant_id", "id"}, confirmed: []string{"tenant_id", "id"},
			record:     core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{PrimaryKeyColumns: []string{"tenant_id", "id"}}},
			wantState:  core.DLQReplayStateQuarantined,
			wantReason: "data_key_component_missing",
		},
		{
			name: "legacy primary key change", staticPK: []string{"id"}, confirmed: []string{"id"},
			record:     core.Record{Operation: core.OpUpdate, Before: map[string]any{"id": 1}, Data: map[string]any{"id": 2}, Metadata: core.Metadata{PrimaryKeyColumns: []string{"id"}, BeforeImageState: core.BeforeImageStateFull}},
			wantState:  core.DLQReplayStateQuarantined,
			wantReason: "legacy_key_change_unsupported",
		},
		{
			name:       "missing original declaration",
			staticPK:   []string{"id"},
			confirmed:  []string{"id"},
			record:     core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}},
			wantState:  core.DLQReplayStateRepairRequired,
			wantReason: "no original primary_key_columns",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, ts := newTestHTTPServer(t)
			defer ts.Close()
			probeID, probe := newDLQReplayProbe(t)
			name := "legacy-reject-" + strings.ReplaceAll(tt.name, " ", "-")
			installMetadataReplaySpec(s, name, probeID, tt.staticPK)
			item := writeDLQReplayItem(t, s, name, tt.record, core.DLQIdentityContext{
				ReplayProvenance: core.DLQReplayProvenanceLegacyUnknown,
				ReplayState:      core.DLQReplayStateRepairRequired,
			})
			_, err := s.repairLegacyDLQIdentity(context.Background(), name, item.ID, dlqIdentityRepairRequest{PrimaryKeyColumns: tt.confirmed})
			var gateErr *dlqReplayGateError
			if !errors.As(err, &gateErr) || gateErr.State != tt.wantState || !strings.Contains(gateErr.Reason, tt.wantReason) {
				t.Fatalf("repair error = %#v, want state %s reason containing %q", err, tt.wantState, tt.wantReason)
			}
			if len(probe.snapshot()) != 0 {
				t.Fatal("rejected legacy row reached sink")
			}
			persisted, readErr := s.store.GetDeadLetterByID(context.Background(), name, item.ID)
			if readErr != nil || persisted == nil || persisted.IdentityContext.ReplayState != tt.wantState {
				t.Fatalf("rejection not durable: row=%+v err=%v", persisted, readErr)
			}
		})
	}
}

func TestDLQReplayRechecksStaticSafetySetAfterLegacyRepair(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	probeID, probe := newDLQReplayProbe(t)
	name := "legacy-static-config-drift"
	spec := installMetadataReplaySpec(s, name, probeID, []string{"id"})
	record := core.Record{
		Operation: core.OpInsert, Data: map[string]any{"id": 1},
		Metadata: core.Metadata{PrimaryKeyColumns: []string{"id"}},
	}
	item := writeDLQReplayItem(t, s, name, record, core.DLQIdentityContext{
		ReplayProvenance: core.DLQReplayProvenanceLegacyUnknown,
		ReplayState:      core.DLQReplayStateRepairRequired,
	})
	if _, err := s.repairLegacyDLQIdentity(context.Background(), name, item.ID, dlqIdentityRepairRequest{PrimaryKeyColumns: []string{"id"}}); err != nil {
		t.Fatalf("repair legacy identity: %v", err)
	}
	spec.Sink.Config["pk_columns"] = []any{"other_id"}
	_, err := s.replayDLQByID(context.Background(), name, item.ID)
	var gateErr *dlqReplayGateError
	if !errors.As(err, &gateErr) || gateErr.State != core.DLQReplayStateQuarantined || !strings.Contains(gateErr.Reason, "exactly match") {
		t.Fatalf("config-drift gate error = %#v", err)
	}
	if len(probe.snapshot()) != 0 {
		t.Fatal("legacy record reached sink after static safety set changed")
	}
}

func TestDLQReplayRevalidatesIdentityAfterTransforms(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	probeID, probe := newDLQReplayProbe(t)
	name := "dlq-transform-identity"
	installMetadataReplaySpec(s, name, probeID, []string{"id"}, pipeline.TransformSpec{
		Type: "drop_field", Config: map[string]any{"fields": []any{"id"}},
	})
	record := core.Record{
		Operation: core.OpInsert, Data: map[string]any{"id": 1, "value": "x"},
		Metadata: core.Metadata{PrimaryKeyColumns: []string{"id"}, FormatContractID: core.FormatContractCanalJSONV1},
	}
	item := writeDLQReplayItem(t, s, name, record, core.NewDLQIdentityContext(record, "key_missing", "", "orders"))
	_, err := s.replayDLQByID(context.Background(), name, item.ID)
	var gateErr *dlqReplayGateError
	if !errors.As(err, &gateErr) || gateErr.State != core.DLQReplayStateQuarantined || !strings.Contains(gateErr.Reason, "transform output") {
		t.Fatalf("transform gate error = %#v", err)
	}
	if len(probe.snapshot()) != 0 {
		t.Fatal("transform-invalid record reached sink")
	}
}

func TestDLQReplayRestartAfterDeleteFailureDoesNotWriteSinkTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete-failure.db")
	raw, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	s, err := NewServer(raw, t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	probeID, probe := newDLQReplayProbe(t)
	name := "dlq-delete-crash"
	installMetadataReplaySpec(s, name, probeID, []string{"id"})
	record := completeReplayRecord(1)
	item := writeDLQReplayItem(t, s, name, record, core.NewDLQIdentityContext(record, "sink failure", "", "orders"))
	raw.SetFailureInjector(func(operation string) error {
		if operation == "dlq.delete" {
			return errors.New("injected delete failure")
		}
		return nil
	})
	if count, err := s.replayDLQByID(context.Background(), name, item.ID); err == nil || count != 0 {
		t.Fatalf("first replay count=%d err=%v, want delete failure", count, err)
	}
	if len(probe.snapshot()) != 1 {
		t.Fatalf("first replay writes=%d, want 1", len(probe.snapshot()))
	}
	persisted, err := raw.GetDeadLetterByID(context.Background(), name, item.ID)
	if err != nil || persisted == nil || persisted.IdentityContext.ReplayState != core.DLQReplayStateSinkAcked {
		t.Fatalf("sink acknowledgement checkpoint = %+v err=%v", persisted, err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	raw2, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("reopen sqlite: %v", err)
	}
	defer raw2.Close()
	s2, err := NewServer(raw2, t.TempDir())
	if err != nil {
		t.Fatalf("new restarted server: %v", err)
	}
	installMetadataReplaySpec(s2, name, probeID, []string{"id"})
	if count, err := s2.replayDLQByID(context.Background(), name, item.ID); err != nil || count != 1 {
		t.Fatalf("cleanup replay count=%d err=%v", count, err)
	}
	if len(probe.snapshot()) != 1 {
		t.Fatalf("restart rewrote acknowledged item: writes=%d", len(probe.snapshot()))
	}
	if remaining, err := raw2.GetDeadLetterByID(context.Background(), name, item.ID); err != nil || remaining != nil {
		t.Fatalf("cleanup did not delete row: row=%+v err=%v", remaining, err)
	}
}

func TestDLQReplayCheckpointFailureRetainsRowAndAllowsAtLeastOnceRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint-failure.db")
	raw, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	s, err := NewServer(raw, t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	probeID, probe := newDLQReplayProbe(t)
	name := "dlq-checkpoint-crash"
	installMetadataReplaySpec(s, name, probeID, []string{"id"})
	record := completeReplayRecord(2)
	item := writeDLQReplayItem(t, s, name, record, core.NewDLQIdentityContext(record, "sink failure", "", "orders"))
	failOnce := true
	raw.SetFailureInjector(func(operation string) error {
		if operation == "dlq.update" && failOnce {
			failOnce = false
			return errors.New("injected replay checkpoint failure")
		}
		return nil
	})
	if count, err := s.replayDLQByID(context.Background(), name, item.ID); err == nil || count != 0 {
		t.Fatalf("first replay count=%d err=%v, want checkpoint failure", count, err)
	}
	if len(probe.snapshot()) != 1 {
		t.Fatalf("first replay writes=%d, want 1", len(probe.snapshot()))
	}
	persisted, err := raw.GetDeadLetterByID(context.Background(), name, item.ID)
	if err != nil || persisted == nil || persisted.IdentityContext.ReplayState != core.DLQReplayStatePending {
		t.Fatalf("failed checkpoint must retain pending row: row=%+v err=%v", persisted, err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	raw2, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("reopen sqlite: %v", err)
	}
	defer raw2.Close()
	s2, err := NewServer(raw2, t.TempDir())
	if err != nil {
		t.Fatalf("new restarted server: %v", err)
	}
	installMetadataReplaySpec(s2, name, probeID, []string{"id"})
	if count, err := s2.replayDLQByID(context.Background(), name, item.ID); err != nil || count != 1 {
		t.Fatalf("retry count=%d err=%v", count, err)
	}
	if len(probe.snapshot()) != 2 {
		t.Fatalf("at-least-once retry writes=%d, want 2", len(probe.snapshot()))
	}
	if remaining, err := raw2.GetDeadLetterByID(context.Background(), name, item.ID); err != nil || remaining != nil {
		t.Fatalf("successful retry left row: row=%+v err=%v", remaining, err)
	}
}

func completeReplayRecord(id int) core.Record {
	return core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"id": id},
		Metadata: core.Metadata{
			Key:               `{"id":` + fmt.Sprint(id) + `}`,
			PrimaryKeyColumns: []string{"id"},
			FormatContractID:  core.FormatContractCanalJSONV1,
		},
	}
}

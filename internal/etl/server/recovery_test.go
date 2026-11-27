package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/google/uuid"
)

func captureRecoveryRow(t *testing.T, s *Server, job string, id int, node, connection string) storage.DeadLetter {
	t.Helper()
	record := completeReplayRecord(id)
	err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName: job, Record: record, Error: "fixture sink outage", ErrorClass: "transient", DAGNode: node,
		RecoveryContext: core.RecoveryCaptureContext{Endpoint: "sink", NodeID: node, ConnectionName: connection, ConnectionKnown: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	hash, err := storage.RecordHash(record)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.dlqWriter.ReadFiltered(context.Background(), storage.DLQFilter{JobName: job, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.RecordHash == hash {
			return row
		}
	}
	t.Fatal("captured row missing")
	return storage.DeadLetter{}
}

func recoveryRequest(t *testing.T, base, method, path string, wantStatus int, result any) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != wantStatus {
		t.Fatalf("%s %s status=%d, want %d", method, path, res.StatusCode, wantStatus)
	}
	if result != nil {
		if err := json.NewDecoder(res.Body).Decode(result); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoveryAPIAndScopedDLQDeletion(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	ctx := context.Background()
	job := uuid.NewString()
	if err := s.store.SavePipeline(ctx, &storage.PipelineRow{ID: job, Name: "recovery API", Status: "stopped", SpecYAML: "name: recovery API"}); err != nil {
		t.Fatal(err)
	}
	first := captureRecoveryRow(t, s, job, 1, "write-a", "warehouse")
	second := captureRecoveryRow(t, s, job, 2, "write-b", "other")
	if first.RecoveryEventID == "" || first.RecoveryEventID == second.RecoveryEventID {
		t.Fatal("explicit endpoint scopes lost")
	}
	for _, query := range []string{
		"?id=", "?id=-1", "?id=1&id=2", "?recovery_event_id=", "?recovery_event_id=bad",
		"?recovery_event_id=" + first.RecoveryEventID + "&recovery_event_id=" + second.RecoveryEventID,
		"?node_id=", "?node_id=a&node_id=b", "?node_id=bad%2Fnode", "?node_id=%00",
		"?connection_name=", "?connection_name=a&connection_name=b", "?connection_name=bad%2Fname",
	} {
		for _, method := range []string{http.MethodGet, http.MethodDelete, http.MethodPost} {
			path := "/api/v2/dlq/" + job
			if method == http.MethodPost {
				path += "/replay"
			}
			recoveryRequest(t, ts.URL, method, path+query, 400, nil)
		}
	}
	for _, path := range []string{
		"/api/v2/recovery-events?pipeline_id=", "/api/v2/recovery-events?state=guessed",
		"/api/v2/recovery-events?state=open&state=resolved", "/api/v2/recovery-events?limit=0",
		"/api/v2/recovery-events/bad",
	} {
		recoveryRequest(t, ts.URL, http.MethodGet, path, 400, nil)
	}
	recoveryRequest(t, ts.URL, http.MethodGet, "/api/v2/recovery-events/"+uuid.NewString(), 404, nil)
	var detail struct{ Event storage.RecoveryEvent }
	recoveryRequest(t, ts.URL, http.MethodGet, "/api/v2/recovery-events/"+first.RecoveryEventID, 200, &detail)
	if detail.Event.PipelineID != job || detail.Event.NodeID != "write-a" || detail.Event.ConnectionName != "warehouse" {
		t.Fatalf("stored association=%+v", detail.Event)
	}
	var deleted struct{ Deleted int }
	recoveryRequest(t, ts.URL, http.MethodDelete, "/api/v2/dlq/"+job+"?recovery_event_id="+first.RecoveryEventID+"&node_id=write-b", 200, &deleted)
	if deleted.Deleted != 0 {
		t.Fatal("mismatched scope deleted another node")
	}
	recoveryRequest(t, ts.URL, http.MethodDelete, "/api/v2/dlq/"+job+"?recovery_event_id="+first.RecoveryEventID+"&node_id=write-a&connection_name=warehouse", 200, &deleted)
	if deleted.Deleted != 1 {
		t.Fatalf("scoped deletion=%d", deleted.Deleted)
	}
	recoveryRequest(t, ts.URL, http.MethodGet, "/api/v2/recovery-events/"+first.RecoveryEventID, 200, &detail)
	if detail.Event.State != "abandoned" || detail.Event.AbandonedRows != 1 {
		t.Fatalf("retained deletion outcome=%+v", detail.Event)
	}
	rows, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: job})
	if err != nil || len(rows) != 1 || rows[0].ID != second.ID {
		t.Fatalf("unrelated row lost: %+v, %v", rows, err)
	}
}

func TestRecoveryReplayResultFailureRestartPreservesSinkAck(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		failAck    bool
		sqlTrigger bool
	}{
		{name: "all_results"},
		{name: "ack_and_results", failAck: true},
		{name: "completed_result_only", sqlTrigger: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "recovery.db")
			raw, err := sqlite.New(path)
			if err != nil {
				t.Fatal(err)
			}
			s, err := NewServer(raw, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			job := uuid.NewString()
			if err := raw.SavePipeline(ctx, &storage.PipelineRow{ID: job, Name: "recovery checkpoint", Status: "stopped", SpecYAML: "name: recovery checkpoint"}); err != nil {
				t.Fatal(err)
			}
			probeID, probe := newDLQReplayProbe(t)
			installMetadataReplaySpec(s, job, probeID, []string{"id"})
			item := captureRecoveryRow(t, s, job, 4, "", "")
			if scenario.sqlTrigger {
				if _, err := raw.DB().Exec("CREATE TRIGGER result_fault BEFORE UPDATE ON recovery_operations WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'fixture result outage'); END"); err != nil {
					t.Fatal(err)
				}
			}
			raw.SetFailureInjector(func(point string) error {
				if (point == "recovery.result" && !scenario.sqlTrigger) || (scenario.failAck && point == "recovery.ack") {
					return errors.New("fixture result store outage")
				}
				return nil
			})
			if count, err := s.replayDLQByID(ctx, job, item.ID); count != 0 || !errors.Is(err, errRecoveryPersistence) {
				t.Fatalf("result failure count=%d err=%v", count, err)
			}
			persisted, err := raw.GetDeadLetterByID(ctx, job, item.ID)
			if err != nil || persisted == nil || persisted.IdentityContext.ReplayState != core.DLQReplayStateSinkAcked || persisted.IdentityContext.SinkAcknowledgedAt == nil {
				t.Fatalf("bookkeeping rolled back sink ACK: %+v err=%v", persisted, err)
			}
			if len(probe.snapshot()) != 1 {
				t.Fatal("expected one sink write")
			}
			if scenario.sqlTrigger {
				if _, err := raw.DB().Exec("DROP TRIGGER result_fault"); err != nil {
					t.Fatal(err)
				}
			}
			if err := raw.Close(); err != nil {
				t.Fatal(err)
			}
			raw2, err := sqlite.New(path)
			if err != nil {
				t.Fatal(err)
			}
			defer raw2.Close()
			s2, err := NewServer(raw2, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			installMetadataReplaySpec(s2, job, probeID, []string{"id"})
			if count, err := s2.replayDLQByID(ctx, job, item.ID); err != nil || count != 1 {
				t.Fatalf("restart finalize count=%d err=%v", count, err)
			}
			if len(probe.snapshot()) != 1 {
				t.Fatal("restart rewrote acknowledged data")
			}
			event, err := raw2.GetRecoveryEvent(ctx, item.RecoveryEventID)
			if err != nil || event.State != "resolved" || event.ReplayedRows != 1 || event.DLQOpenRows != 0 {
				t.Fatalf("final result=%+v err=%v", event, err)
			}
			ops, err := raw2.ListRecoveryOperations(ctx, item.RecoveryEventID)
			if err != nil || len(ops) == 0 || ops[0].State != "completed" || ops[0].AcknowledgedAt == nil {
				t.Fatalf("durable outcome=%+v err=%v", ops, err)
			}
		})
	}
}

func TestRecoveryAPIRowIdentityIsExact(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	job := uuid.NewString()
	ctx := context.Background()
	if err := s.store.SavePipeline(ctx, &storage.PipelineRow{ID: job, Name: "large DLQ ID", Status: "stopped", SpecYAML: "name: large DLQ ID"}); err != nil {
		t.Fatal(err)
	}
	item := captureRecoveryRow(t, s, job, 7, "", "")
	const exactID = int64(9007199254740995)
	db := storage.UnwrapStorage(s.store).(*sqlite.Store).DB()
	if _, err := db.Exec("UPDATE dead_letters SET id=? WHERE id=?", exactID, item.ID); err != nil {
		t.Fatal(err)
	}
	var listed struct{ Items []storage.DeadLetter }
	recoveryRequest(t, ts.URL, http.MethodGet, "/api/v2/dlq/"+job+"?id="+strconv.FormatInt(exactID, 10), 200, &listed)
	if len(listed.Items) != 1 || listed.Items[0].RowID != "9007199254740995" {
		t.Fatalf("exact row string=%+v", listed.Items)
	}
	r, _ := s.recoveryStore()
	op, err := r.BeginRecoveryOperation(ctx, job, exactID, "dry_run")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.FinishRecoveryOperation(ctx, op, "preview_passed", ""); err != nil {
		t.Fatal(err)
	}
	var detail struct{ Operations []storage.RecoveryOperation }
	recoveryRequest(t, ts.URL, http.MethodGet, "/api/v2/recovery-events/"+item.RecoveryEventID, 200, &detail)
	if len(detail.Operations) != 1 || detail.Operations[0].DLQRowKey != "9007199254740995" {
		t.Fatalf("operation row string=%+v", detail.Operations)
	}
}

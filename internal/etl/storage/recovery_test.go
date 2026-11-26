package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func testRecovery(t *testing.T, factory func(*testing.T) (storage.Storage, func())) {
	s, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	const pipelineID = "3aedc812-3f46-4740-9946-035cc2dac1d9"
	if err := s.SaveConnection(ctx, &storage.ConnectionEntry{Name: "recovery-target", Kind: "sink", Type: "mysql"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SavePipeline(ctx, &storage.PipelineRow{ID: pipelineID, Name: "recovery display", Status: "stopped", SpecYAML: "sink: {type: mysql, connection: recovery-target}"}); err != nil {
		t.Fatal(err)
	}
	r := s.(storage.RecoveryStore)
	faults := s.(interface{ SetFailureInjector(func(string) error) })
	capture := func(id int) *storage.DLQRecord {
		t.Helper()
		rec := &storage.DLQRecord{JobName: pipelineID, Record: core.Record{Data: map[string]any{"id": id, "value": "fixture"}, Metadata: core.Metadata{Table: "orders"}}, Error: "fixture sink outage", ErrorClass: "transient", RecoveryContext: core.RecoveryCaptureContext{Endpoint: "sink", ConnectionName: "recovery-target", ConnectionKnown: true}}
		if err := s.WriteDeadLetter(ctx, rec); err != nil {
			t.Fatal(err)
		}
		rows, err := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: pipelineID, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.RecordHash == rec.RecordHash {
				return row
			}
		}
		t.Fatal("captured row missing")
		return nil
	}
	first := capture(1)
	if first.RecoveryEventID == "" || first.RecoveryTrackingError != "" {
		t.Fatalf("missing durable association: %+v", first)
	}
	duplicate := capture(1)
	if duplicate.ID != first.ID || duplicate.RecoveryEventID != first.RecoveryEventID {
		t.Fatal("repeated capture changed stable identities")
	}
	second := capture(2)
	if second.RecoveryEventID != first.RecoveryEventID {
		t.Fatal("same explicit scope did not share an open recovery record")
	}
	event, err := r.GetRecoveryEvent(ctx, first.RecoveryEventID)
	if err != nil || event.RecordCount != 2 || event.AttemptCount != 3 || event.DLQOpenRows != 2 || event.ConnectionName != "recovery-target" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	preview, err := r.BeginRecoveryOperation(ctx, pipelineID, first.ID, "dry_run")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.FinishRecoveryOperation(ctx, preview, "preview_passed", ""); err != nil {
		t.Fatal(err)
	}
	event, _ = r.GetRecoveryEvent(ctx, event.ID)
	if event.State != "open" || event.ReplayedRows != 0 || event.DLQOpenRows != 2 {
		t.Fatal("preview mutated delivery/recovery state")
	}
	failed, err := r.BeginRecoveryOperation(ctx, pipelineID, first.ID, "replay")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.FinishRecoveryOperation(ctx, failed, "failed", "fixture retry outage"); err != nil {
		t.Fatal(err)
	}
	event, _ = r.GetRecoveryEvent(ctx, event.ID)
	if event.State != "open" {
		t.Fatal("failed request did not remain open")
	}
	op, err := r.BeginRecoveryOperation(ctx, pipelineID, first.ID, "replay")
	if err != nil {
		t.Fatal(err)
	}
	first, err = s.GetDeadLetterByID(ctx, pipelineID, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first.IdentityContext.ReplayState = core.DLQReplayStateSinkAcked
	first.IdentityContext.SinkAcknowledgedAt = &now
	first.RecoveryOperationID = op
	if err := s.UpdateDeadLetter(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := r.AcknowledgeRecoveryOperation(ctx, op, now); err != nil {
		t.Fatal(err)
	}
	faults.SetFailureInjector(func(point string) error {
		if point == "recovery.result" {
			return errors.New("fixture metadata outage")
		}
		return nil
	})
	if err := r.DeleteDeadLetterAfterReplay(ctx, pipelineID, first.ID, op); err == nil {
		t.Fatal("result persistence failure was hidden")
	}
	retained, err := s.GetDeadLetterByID(ctx, pipelineID, first.ID)
	if err != nil || retained == nil || retained.IdentityContext.ReplayState != core.DLQReplayStateSinkAcked {
		t.Fatal("result failure removed durable replay checkpoint")
	}
	faults.SetFailureInjector(nil)
	if err := r.DeleteDeadLetterAfterReplay(ctx, pipelineID, first.ID, op); err != nil {
		t.Fatal(err)
	}
	event, _ = r.GetRecoveryEvent(ctx, event.ID)
	if event.State != "open" || event.ReplayedRows != 1 || event.DLQOpenRows != 1 {
		t.Fatalf("partial recovery=%+v", event)
	}
	if err := s.DeleteDeadLetterByID(ctx, pipelineID, second.ID); err != nil {
		t.Fatal(err)
	}
	event, _ = r.GetRecoveryEvent(ctx, event.ID)
	if event.State != "abandoned" || event.AbandonedRows != 1 || event.ReplayedRows != 1 || event.DLQOpenRows != 0 || event.ClosedAt == nil {
		t.Fatalf("mixed discard falsely resolved: %+v", event)
	}
	ops, err := r.ListRecoveryOperations(ctx, event.ID)
	if err != nil || len(ops) != 4 {
		t.Fatalf("retained outcomes=%+v err=%v", ops, err)
	}
	third := capture(3)
	if third.RecoveryEventID == event.ID {
		t.Fatal("new failure reused a closed recovery identity")
	}
	if _, err := s.(storage.DLQOldestPurger).PurgeOldestDeadLetters(ctx, 1); err != nil {
		t.Fatal(err)
	}
	evicted, _ := r.GetRecoveryEvent(ctx, third.RecoveryEventID)
	if evicted.State != "abandoned" || evicted.AbandonedRows != 1 {
		t.Fatal("eviction did not retain abandonment")
	}
	faults.SetFailureInjector(func(point string) error {
		if point == "recovery.capture" {
			return errors.New("fixture capture metadata failure")
		}
		return nil
	})
	unlinked := capture(4)
	faults.SetFailureInjector(nil)
	if unlinked.RecoveryEventID != "" || unlinked.RecoveryTrackingError == "" {
		t.Fatal("capture fallback guessed an association or hid the failure")
	}
	expiring := capture(5)
	n, err := s.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{JobName: pipelineID, Until: time.Now().Add(time.Minute), DeletionReason: "retention"})
	if err != nil || n != 2 {
		t.Fatalf("retention count=%d err=%v", n, err)
	}
	expired, err := r.GetRecoveryEvent(ctx, expiring.RecoveryEventID)
	if err != nil || expired.State != "abandoned" || expired.AbandonedRows != 1 {
		t.Fatalf("TTL outcome=%+v err=%v", expired, err)
	}
	unassociated, err := r.ListRecoveryOperations(ctx, "")
	if err != nil || len(unassociated) != 1 || unassociated[0].State != "abandoned" || unassociated[0].DLQRowID != unlinked.ID {
		t.Fatalf("fallback row deletion facts lost: %+v err=%v", unassociated, err)
	}
	last := capture(6)
	faults.SetFailureInjector(func(point string) error {
		if point == "recovery.result" {
			return errors.New("fixture deletion bookkeeping failure")
		}
		return nil
	})
	if n, err := s.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{JobName: pipelineID}); err == nil || n != 0 {
		t.Fatalf("failed deletion claimed success: count=%d err=%v", n, err)
	}
	if row, err := s.GetDeadLetterByID(ctx, pipelineID, last.ID); err != nil || row == nil {
		t.Fatal("failed deletion lost DLQ")
	}
	faults.SetFailureInjector(nil)
	if err := s.(interface {
		DeletePipelineWithCheckpoint(context.Context, string) error
	}).DeletePipelineWithCheckpoint(ctx, pipelineID); err != nil {
		t.Fatal(err)
	}
	retainedEvent, err := r.GetRecoveryEvent(ctx, event.ID)
	if err != nil || retainedEvent == nil || retainedEvent.PipelineID != pipelineID {
		t.Fatal("pipeline deletion lost retained recovery identity")
	}
	lastEvent, err := r.GetRecoveryEvent(ctx, last.RecoveryEventID)
	if err != nil || lastEvent.State != "abandoned" || lastEvent.AbandonedRows != 1 {
		t.Fatalf("pipeline deletion outcome=%+v err=%v", lastEvent, err)
	}
}

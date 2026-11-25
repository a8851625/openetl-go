package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/google/uuid"
)

const recoveryEventColumns = `id,scope_key,pipeline_id,pipeline_name,endpoint,node_id,connection_name,connection_known,spec_version,error_class,first_error,state,opened_at,closed_at,attempt_count,record_count,replayed_rows,abandoned_rows,tracking_incomplete`
const recoveryOperationColumns = `id,event_id,pipeline_id,dlq_row_id,record_hash,kind,state,error,started_at,finished_at,acknowledged_at`

type rowScanner interface{ Scan(...any) error }

func scanRecoveryEvent(row rowScanner) (*storage.RecoveryEvent, error) {
	e := new(storage.RecoveryEvent)
	var closed sql.NullTime
	err := row.Scan(&e.ID, &e.ScopeKey, &e.PipelineID, &e.PipelineName, &e.Endpoint,
		&e.NodeID, &e.ConnectionName, &e.ConnectionKnown, &e.SpecVersion, &e.ErrorClass,
		&e.FirstError, &e.State, &e.OpenedAt, &closed, &e.AttemptCount, &e.RecordCount,
		&e.ReplayedRows, &e.AbandonedRows, &e.TrackingIncomplete)
	if closed.Valid {
		e.ClosedAt = &closed.Time
	}
	return e, err
}

func scanRecoveryOperation(row rowScanner) (*storage.RecoveryOperation, error) {
	o := new(storage.RecoveryOperation)
	var finished, acked sql.NullTime
	err := row.Scan(&o.ID, &o.EventID, &o.PipelineID, &o.DLQRowID, &o.RecordHash,
		&o.Kind, &o.State, &o.Error, &o.StartedAt, &finished, &acked)
	if finished.Valid {
		o.FinishedAt = &finished.Time
	}
	if acked.Valid {
		o.AcknowledgedAt = &acked.Time
	}
	return o, err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func recoveryScope(job string, c core.RecoveryCaptureContext) string {
	data, _ := json.Marshal([]any{job, c.Endpoint, c.NodeID, c.ConnectionName, c.ConnectionKnown})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func (s *Store) EnsureRecoveryIndexes(ctx context.Context) error {
	for _, query := range []string{
		"CREATE INDEX idx_recovery_scope ON recovery_events(scope_key,state)",
		"CREATE INDEX idx_recovery_operations_event ON recovery_operations(event_id,started_at)",
		"CREATE INDEX idx_dlq_recovery_event ON dead_letters(recovery_event_id)",
	} {
		if _, err := s.exec(ctx, query); err != nil {
			message := strings.ToLower(err.Error())
			if !strings.Contains(message, "already exists") && !strings.Contains(message, "duplicate key name") {
				return err
			}
		}
	}
	return nil
}

// writeTrackedDeadLetter is failure atomic. A metadata-only failure rolls back
// this transaction, then the caller persists the DLQ without relying on it.
func (s *Store) writeTrackedDeadLetter(ctx context.Context, rec *storage.DLQRecord) error {
	return s.withReferenceGuard(ctx, func(txCtx context.Context) error {
		if err := s.injectFailure("recovery.capture"); err != nil {
			return err
		}
		var pipelineName string
		if err := s.queryRow(txCtx, "SELECT name FROM pipelines WHERE id=?"+s.currentReadSuffix(), rec.JobName).Scan(&pipelineName); err != nil {
			return fmt.Errorf("pipeline identity unavailable for recovery capture")
		}
		var existingID int64
		var existingEvent, trackingError string
		err := s.queryRow(txCtx, `SELECT id,COALESCE(recovery_event_id,''),COALESCE(recovery_tracking_error,'') FROM dead_letters WHERE job_name=? AND record_hash=?`+s.currentReadSuffix(), rec.JobName, rec.RecordHash).Scan(&existingID, &existingEvent, &trackingError)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		key := recoveryScope(rec.JobName, rec.RecoveryContext)
		var event *storage.RecoveryEvent
		if existingEvent != "" {
			event, err = s.getRecoveryEventRow(txCtx, existingEvent)
			if err != nil {
				return err
			}
			if event == nil || event.ScopeKey != key || event.State == "resolved" || event.State == "abandoned" {
				return fmt.Errorf("recovery endpoint association conflicts with the frozen capture")
			}
		} else {
			rows, err := s.query(txCtx, "SELECT "+recoveryEventColumns+" FROM recovery_events WHERE scope_key=? AND state IN ('open','replaying','unknown') LIMIT 2", key)
			if err != nil {
				return err
			}
			for rows.Next() {
				if event != nil {
					rows.Close()
					return fmt.Errorf("recovery scope has multiple active identities")
				}
				event, err = scanRecoveryEvent(rows)
				if err != nil {
					rows.Close()
					return err
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
		if event == nil {
			event = &storage.RecoveryEvent{ID: uuid.NewString(), ScopeKey: key,
				PipelineID: rec.JobName, PipelineName: pipelineName,
				Endpoint: rec.RecoveryContext.Endpoint, NodeID: rec.RecoveryContext.NodeID,
				ConnectionName: rec.RecoveryContext.ConnectionName, ConnectionKnown: rec.RecoveryContext.ConnectionKnown,
				SpecVersion: rec.RecoveryContext.SpecVersion, ErrorClass: rec.ErrorClass,
				FirstError: rec.Error, State: "open", OpenedAt: time.Now().UTC()}
			if err := s.WriteRecoveryEventFidelity(txCtx, event); err != nil {
				return err
			}
		}
		newRow := existingID == 0 || existingEvent == ""
		if trackingError != "" {
			event.TrackingIncomplete = true
		}
		if _, err := s.exec(txCtx, `UPDATE recovery_events SET attempt_count=attempt_count+1,record_count=record_count+?,tracking_incomplete=?,state='open' WHERE id=?`, boolInt(newRow), boolInt(event.TrackingIncomplete), event.ID); err != nil {
			return err
		}
		rec.RecoveryEventID = event.ID
		rec.RecoveryTrackingError = ""
		return s.writeDeadLetterRow(txCtx, rec)
	})
}

func (s *Store) getRecoveryEventRow(ctx context.Context, id string) (*storage.RecoveryEvent, error) {
	e, err := scanRecoveryEvent(s.queryRow(ctx, "SELECT "+recoveryEventColumns+" FROM recovery_events WHERE id=?", id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return e, err
}

func (s *Store) GetRecoveryEvent(ctx context.Context, id string) (*storage.RecoveryEvent, error) {
	e, err := s.getRecoveryEventRow(ctx, id)
	if err != nil || e == nil {
		return e, err
	}
	var incomplete int64
	err = s.queryRow(ctx, "SELECT COUNT(*),COALESCE(SUM(CASE WHEN COALESCE(recovery_tracking_error,'')<>'' THEN 1 ELSE 0 END),0) FROM dead_letters WHERE recovery_event_id=?", id).Scan(&e.DLQOpenRows, &incomplete)
	e.TrackingIncomplete = e.TrackingIncomplete || incomplete > 0
	return e, err
}

func (s *Store) ListRecoveryEvents(ctx context.Context, f storage.RecoveryFilter) ([]*storage.RecoveryEvent, error) {
	query := "SELECT id FROM recovery_events WHERE 1=1"
	var args []any
	if f.PipelineID != "" {
		query += " AND pipeline_id=?"
		args = append(args, f.PipelineID)
	}
	if f.State != "" {
		query += " AND state=?"
		args = append(args, f.State)
	}
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	query += " ORDER BY opened_at DESC,id LIMIT ? OFFSET ?"
	args = append(args, f.Limit, f.Offset)
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]*storage.RecoveryEvent, 0, len(ids))
	for _, id := range ids {
		e, err := s.GetRecoveryEvent(ctx, id)
		if err != nil {
			return nil, err
		}
		if e != nil {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *Store) ListRecoveryOperations(ctx context.Context, eventID string) ([]*storage.RecoveryOperation, error) {
	return s.listRecoveryOperations(ctx, eventID, "")
}

func (s *Store) ListUnassociatedRecoveryOperations(ctx context.Context, pipelineID string) ([]*storage.RecoveryOperation, error) {
	return s.listRecoveryOperations(ctx, "", pipelineID)
}

func (s *Store) listRecoveryOperations(ctx context.Context, eventID, pipelineID string) ([]*storage.RecoveryOperation, error) {
	query := "SELECT " + recoveryOperationColumns + " FROM recovery_operations WHERE event_id=?"
	args := []any{eventID}
	if pipelineID != "" {
		query += " AND pipeline_id=?"
		args = append(args, pipelineID)
	}
	rows, err := s.query(ctx, query+" ORDER BY started_at DESC,id LIMIT 500", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*storage.RecoveryOperation{}
	for rows.Next() {
		o, err := scanRecoveryOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
		o.DLQRowKey = strconv.FormatInt(o.DLQRowID, 10)
	}
	return out, rows.Err()
}

func (s *Store) BeginRecoveryOperation(ctx context.Context, job string, rowID int64, kind string) (string, error) {
	if kind != "replay" && kind != "dry_run" {
		return "", fmt.Errorf("invalid recovery operation")
	}
	var id string
	err := s.withReferenceGuard(ctx, func(txCtx context.Context) error {
		if err := s.injectFailure("recovery.operation"); err != nil {
			return err
		}
		rec, err := s.GetDeadLetterByID(txCtx, job, rowID)
		if err != nil {
			return err
		}
		if rec == nil {
			return fmt.Errorf("recovery DLQ row not found")
		}
		if kind == "replay" && rec.IdentityContext.ReplayState == core.DLQReplayStateSinkAcked && rec.RecoveryOperationID != "" {
			var state string
			err := s.queryRow(txCtx, "SELECT state FROM recovery_operations WHERE id=?", rec.RecoveryOperationID).Scan(&state)
			if err == nil && (state == "started" || state == "acked") {
				if rec.IdentityContext.SinkAcknowledgedAt != nil {
					if _, err := s.exec(txCtx, "UPDATE recovery_operations SET state='acked',acknowledged_at=? WHERE id=?", rec.IdentityContext.SinkAcknowledgedAt, rec.RecoveryOperationID); err != nil {
						return err
					}
				}
				id = rec.RecoveryOperationID
				return nil
			}
			if err != nil && err != sql.ErrNoRows {
				return err
			}
		}
		id = uuid.NewString()
		o := &storage.RecoveryOperation{ID: id, EventID: rec.RecoveryEventID, PipelineID: job, DLQRowID: rowID, RecordHash: rec.RecordHash, Kind: kind, State: "started", StartedAt: time.Now().UTC()}
		if kind == "replay" && rec.IdentityContext.ReplayState == core.DLQReplayStateSinkAcked {
			o.Kind = "finalize"
		}
		if err := s.WriteRecoveryOperationFidelity(txCtx, o); err != nil {
			return err
		}
		if kind == "replay" {
			if _, err := s.exec(txCtx, "UPDATE dead_letters SET recovery_operation_id=? WHERE job_name=? AND id=?", id, job, rowID); err != nil {
				return err
			}
			if rec.RecoveryEventID != "" {
				_, err = s.exec(txCtx, "UPDATE recovery_events SET state='replaying' WHERE id=? AND state IN ('open','unknown')", rec.RecoveryEventID)
				return err
			}
		}
		return nil
	})
	return id, err
}

func (s *Store) FinishRecoveryOperation(ctx context.Context, id, state, message string) error {
	if id == "" {
		return nil
	}
	switch state {
	case "failed", "not_attempted", "preview_passed", "preview_blocked":
	default:
		return fmt.Errorf("invalid recovery outcome")
	}
	return s.withReferenceGuard(ctx, func(txCtx context.Context) error {
		var eventID, previousState string
		if err := s.queryRow(txCtx, "SELECT event_id,state FROM recovery_operations WHERE id=?", id).Scan(&eventID, &previousState); err != nil {
			return err
		}
		if previousState != "started" && previousState != "acked" {
			return nil
		}
		if state == "not_attempted" && previousState == "acked" {
			return nil
		}
		if err := s.injectFailure("recovery.result"); err != nil {
			return err
		}
		result, err := s.exec(txCtx, "UPDATE recovery_operations SET state=?,error=?,finished_at=? WHERE id=? AND state IN ('started','acked')", state, message, time.Now().UTC(), id)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n > 0 && eventID != "" && (state == "failed" || state == "not_attempted") {
			_, err = s.exec(txCtx, "UPDATE recovery_events SET state='open' WHERE id=? AND state='replaying'", eventID)
		}
		return err
	})
}

func (s *Store) AcknowledgeRecoveryOperation(ctx context.Context, id string, at time.Time) error {
	if id == "" {
		return nil
	}
	if err := s.injectFailure("recovery.ack"); err != nil {
		return err
	}
	_, err := s.exec(ctx, "UPDATE recovery_operations SET state='acked',acknowledged_at=? WHERE id=? AND state='started'", at, id)
	return err
}

func (s *Store) closeRecoveryIfDrained(ctx context.Context, eventID string) error {
	if eventID == "" {
		return nil
	}
	e, err := s.GetRecoveryEvent(ctx, eventID)
	if err != nil {
		return err
	}
	if e == nil {
		return fmt.Errorf("recovery identity is missing")
	}
	if e.DLQOpenRows > 0 {
		var unfinished int
		if err := s.queryRow(ctx, "SELECT COUNT(*) FROM recovery_operations WHERE event_id=? AND state IN ('started','acked')", eventID).Scan(&unfinished); err != nil {
			return err
		}
		if unfinished == 0 {
			_, err = s.exec(ctx, "UPDATE recovery_events SET state='open' WHERE id=? AND state='replaying'", eventID)
		}
		return err
	}
	state := "resolved"
	if e.AbandonedRows > 0 {
		state = "abandoned"
	}
	var closed any = time.Now().UTC()
	if e.TrackingIncomplete || e.RecordCount != e.ReplayedRows+e.AbandonedRows {
		state = "unknown"
		closed = nil
	}
	_, err = s.exec(ctx, "UPDATE recovery_events SET state=?,closed_at=? WHERE id=?", state, closed, eventID)
	return err
}

func (s *Store) DeleteDeadLetterAfterReplay(ctx context.Context, job string, rowID int64, operationID string) error {
	return s.withReferenceGuard(ctx, func(txCtx context.Context) error {
		if err := s.injectFailure("dlq.delete"); err != nil {
			return err
		}
		rec, err := s.GetDeadLetterByID(txCtx, job, rowID)
		if err != nil {
			return err
		}
		if rec == nil {
			return nil
		}
		if rec.RecoveryEventID == "" && operationID == "" {
			_, err = s.exec(txCtx, "DELETE FROM dead_letters WHERE job_name=? AND id=?", job, rowID)
			return err
		}
		if rec.IdentityContext.ReplayState != core.DLQReplayStateSinkAcked || rec.IdentityContext.SinkAcknowledgedAt == nil {
			return fmt.Errorf("replay result requires a durable sink acknowledgement")
		}
		if err := s.injectFailure("recovery.result"); err != nil {
			return err
		}
		if operationID == "" {
			operationID = uuid.NewString()
			o := &storage.RecoveryOperation{ID: operationID, EventID: rec.RecoveryEventID, PipelineID: job, DLQRowID: rowID, RecordHash: rec.RecordHash, Kind: "finalize", State: "acked", StartedAt: time.Now().UTC(), AcknowledgedAt: rec.IdentityContext.SinkAcknowledgedAt}
			if err := s.WriteRecoveryOperationFidelity(txCtx, o); err != nil {
				return err
			}
		}
		result, err := s.exec(txCtx, "UPDATE recovery_operations SET state='completed',finished_at=?,acknowledged_at=? WHERE id=? AND event_id=? AND pipeline_id=? AND dlq_row_id=?", time.Now().UTC(), rec.IdentityContext.SinkAcknowledgedAt, operationID, rec.RecoveryEventID, job, rowID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("replay operation identity mismatch")
		}
		if rec.RecoveryEventID != "" {
			trackingError := rec.RecoveryTrackingError
			if trackingError == "replay_result_unavailable" {
				trackingError = ""
			}
			_, err = s.exec(txCtx, "UPDATE recovery_events SET replayed_rows=replayed_rows+1,tracking_incomplete=CASE WHEN ?<>'' THEN 1 ELSE tracking_incomplete END WHERE id=?", trackingError, rec.RecoveryEventID)
			if err != nil {
				return err
			}
		}
		if _, err = s.exec(txCtx, "DELETE FROM dead_letters WHERE job_name=? AND id=?", job, rowID); err != nil {
			return err
		}
		return s.closeRecoveryIfDrained(txCtx, rec.RecoveryEventID)
	})
}

// abandonRecoveryRows reads only retained identity, never raw record payload.
// The selected IDs, outcome rows, event counters and deletion share one tx.
func (s *Store) abandonRecoveryRows(ctx context.Context, selection string, args []any, kind string) (int64, error) {
	var deleted int64
	err := s.withReferenceGuard(ctx, func(txCtx context.Context) error {
		rows, err := s.query(txCtx, selection, args...)
		if err != nil {
			return err
		}
		type selected struct {
			id                         int64
			job, hash, event, tracking string
		}
		var items []selected
		for rows.Next() {
			var item selected
			if err := rows.Scan(&item.id, &item.job, &item.hash, &item.event, &item.tracking); err != nil {
				rows.Close()
				return err
			}
			items = append(items, item)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, item := range items {
			if item.event != "" || item.tracking != "" {
				if err := s.injectFailure("recovery.result"); err != nil {
					return err
				}
				now := time.Now().UTC()
				o := &storage.RecoveryOperation{ID: uuid.NewString(), EventID: item.event, PipelineID: item.job, DLQRowID: item.id, RecordHash: item.hash, Kind: kind, State: "abandoned", StartedAt: now, FinishedAt: &now}
				if err := s.WriteRecoveryOperationFidelity(txCtx, o); err != nil {
					return err
				}
				if _, err := s.exec(txCtx, "UPDATE recovery_events SET abandoned_rows=abandoned_rows+1,tracking_incomplete=CASE WHEN ?<>'' THEN 1 ELSE tracking_incomplete END WHERE id=?", item.tracking, item.event); err != nil {
					return err
				}
			}
			result, err := s.exec(txCtx, "DELETE FROM dead_letters WHERE job_name=? AND id=?", item.job, item.id)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return err
			}
			deleted += n
			if err := s.closeRecoveryIfDrained(txCtx, item.event); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

const recoveryDeletionColumns = `id,job_name,COALESCE(record_hash,''),COALESCE(recovery_event_id,''),COALESCE(recovery_tracking_error,'')`

func (s *Store) WriteRecoveryEventFidelity(ctx context.Context, e *storage.RecoveryEvent) error {
	return s.insertBackupRow(ctx, "recovery_events", recoveryEventColumns, e.ID, e.ScopeKey, e.PipelineID, e.PipelineName, e.Endpoint, e.NodeID, e.ConnectionName, boolInt(e.ConnectionKnown), e.SpecVersion, e.ErrorClass, e.FirstError, e.State, e.OpenedAt, e.ClosedAt, e.AttemptCount, e.RecordCount, e.ReplayedRows, e.AbandonedRows, boolInt(e.TrackingIncomplete))
}

func (s *Store) WriteRecoveryOperationFidelity(ctx context.Context, o *storage.RecoveryOperation) error {
	return s.insertBackupRow(ctx, "recovery_operations", recoveryOperationColumns, o.ID, o.EventID, o.PipelineID, o.DLQRowID, o.RecordHash, o.Kind, o.State, o.Error, o.StartedAt, o.FinishedAt, o.AcknowledgedAt)
}

func recoveryNullable(text string) any {
	if text == "" {
		return nil
	}
	return text
}

func (s *Store) reportCaptureTrackingFailure(ctx context.Context, rec *storage.DLQRecord, err error) {
	log.Printf("recovery capture metadata unavailable; durable DLQ fallback pipeline=%s: %v", rec.JobName, err)
	if rec.RecoveryEventID != "" {
		_, updateErr := s.exec(ctx, "UPDATE recovery_events SET tracking_incomplete=1 WHERE id=?", rec.RecoveryEventID)
		if updateErr != nil {
			log.Printf("recovery incomplete marker unavailable pipeline=%s: %v", rec.JobName, updateErr)
		}
	}
}

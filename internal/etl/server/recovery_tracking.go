package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

var errRecoveryPersistence = errors.New("recovery metadata persistence failed")

func (s *Server) finalizeRecoveryRow(ctx context.Context, item storage.DeadLetter) error {
	if err := s.dlqWriter.DeleteAfterReplay(ctx, item.JobName, item.ID, item.RecoveryOperationID); err != nil {
		if item.RecoveryEventID != "" || item.RecoveryOperationID != "" || item.RecoveryTrackingError != "" {
			return fmt.Errorf("%w: atomic result/deletion: %v", errRecoveryPersistence, err)
		}
		return err
	}
	return nil
}

func (s *Server) recoveryStore() (storage.RecoveryStore, bool) {
	recovery, ok := storage.UnwrapStorage(s.store).(storage.RecoveryStore)
	return recovery, ok
}

func (s *Server) beginRecoveryOperations(ctx context.Context, name string, items []storage.DeadLetter, kind string) ([]storage.DeadLetter, error) {
	out := append([]storage.DeadLetter(nil), items...)
	recovery, ok := s.recoveryStore()
	for i := range out {
		if out[i].RecoveryEventID == "" && out[i].RecoveryContext.Endpoint == "" && out[i].RecoveryTrackingError == "" {
			continue // old unassociated rows retain their existing semantics.
		}
		if !ok {
			return nil, fmt.Errorf("%w: backend capability unavailable", errRecoveryPersistence)
		}
		id, err := recovery.BeginRecoveryOperation(ctx, name, out[i].ID, kind)
		if err != nil {
			return out, fmt.Errorf("%w: begin %s: %v", errRecoveryPersistence, kind, err)
		}
		out[i].RecoveryOperationID = id
	}
	return out, nil
}

func (s *Server) finishRowRecovery(ctx context.Context, item storage.DeadLetter, state, message string) error {
	if item.RecoveryOperationID == "" {
		return nil
	}
	recovery, ok := s.recoveryStore()
	if !ok {
		return fmt.Errorf("%w: backend capability unavailable", errRecoveryPersistence)
	}
	if err := recovery.FinishRecoveryOperation(ctx, item.RecoveryOperationID, state, message); err != nil {
		// The caller's selection may predate a durable sink acknowledgement.
		// Reload before marking missing facts so bookkeeping never rolls that
		// replay checkpoint back to pending.
		current, readErr := s.dlqWriter.ReadByID(ctx, item.JobName, item.ID)
		if readErr != nil {
			return fmt.Errorf("%w: %v; tracking marker read: %v", errRecoveryPersistence, err, readErr)
		}
		if current != nil && current.RecoveryTrackingError == "" {
			current.RecoveryTrackingError = "replay_result_unavailable"
			if updateErr := s.dlqWriter.Update(ctx, *current); updateErr != nil {
				return fmt.Errorf("%w: %v; tracking marker: %v", errRecoveryPersistence, err, updateErr)
			}
		}
		return fmt.Errorf("%w: %v", errRecoveryPersistence, err)
	}
	return nil
}

func (s *Server) trackedReplayItems(ctx context.Context, name string, items []storage.DeadLetter) (int, error) {
	prepared, err := s.beginRecoveryOperations(ctx, name, items, "replay")
	if err != nil {
		return 0, err
	}
	count, replayErr := s.replayDLQItems(ctx, name, prepared)
	message := "selection ended without a recorded row acknowledgement"
	if replayErr != nil {
		message = replayErr.Error()
	}
	for _, item := range prepared {
		if err := s.finishRowRecovery(ctx, item, "not_attempted", message); err != nil {
			replayErr = errors.Join(replayErr, err)
		}
	}
	return count, replayErr
}

func (s *Server) acknowledgeRowRecovery(ctx context.Context, item storage.DeadLetter, at time.Time) error {
	if item.RecoveryOperationID == "" {
		return nil
	}
	recovery, ok := s.recoveryStore()
	if !ok {
		return fmt.Errorf("%w: backend capability unavailable", errRecoveryPersistence)
	}
	if err := recovery.AcknowledgeRecoveryOperation(ctx, item.RecoveryOperationID, at); err != nil {
		return fmt.Errorf("%w: acknowledgement: %v", errRecoveryPersistence, err)
	}
	return nil
}

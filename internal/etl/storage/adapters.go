package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// ── CheckpointStore adapter ──────────────────────────────────────────

// CheckpointStoreAdapter bridges storage.Storage to core.CheckpointStore.
type CheckpointStoreAdapter struct {
	store Storage
}

func NewCheckpointStoreAdapter(s Storage) *CheckpointStoreAdapter {
	return &CheckpointStoreAdapter{store: s}
}

func (a *CheckpointStoreAdapter) Save(ctx context.Context, cp core.Checkpoint) error {
	rec := &CheckpointRecord{
		JobName:   cp.JobName,
		Source:    cp.Source,
		Position:  cp.Position,
		Timestamp: time.Now(),
	}
	return a.store.SaveCheckpoint(ctx, rec)
}

func (a *CheckpointStoreAdapter) Load(ctx context.Context, jobName string) (*core.Checkpoint, error) {
	rec, err := a.store.LoadCheckpoint(ctx, jobName)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return &core.Checkpoint{
		ID:        jobName,
		JobName:   rec.JobName,
		Source:    rec.Source,
		Position:  rec.Position,
		Timestamp: rec.Timestamp,
	}, nil
}

func (a *CheckpointStoreAdapter) Delete(ctx context.Context, jobName string) error {
	return a.store.DeleteCheckpoint(ctx, jobName)
}

func (a *CheckpointStoreAdapter) List(ctx context.Context) ([]core.Checkpoint, error) {
	recs, err := a.store.ListCheckpoints(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]core.Checkpoint, len(recs))
	for i, rec := range recs {
		result[i] = core.Checkpoint{
			ID:        rec.JobName,
			JobName:   rec.JobName,
			Source:    rec.Source,
			Position:  rec.Position,
			Timestamp: rec.Timestamp,
		}
	}
	return result, nil
}

// ── DLQWriter adapter ────────────────────────────────────────────────

// DLQWriterAdapter bridges storage.Storage to dlq.Writer-like interface.
// It also provides the filter/list/delete capabilities that the SQL-backed
// store enables natively.
type DLQWriterAdapter struct {
	store Storage
}

func NewDLQWriterAdapter(s Storage) *DLQWriterAdapter {
	return &DLQWriterAdapter{store: s}
}

// Write persists a dead-letter record into the database.
func (a *DLQWriterAdapter) Write(ctx context.Context, jobName string, record core.Record, errMsg, errClass string, attempt int) error {
	rec := &DLQRecord{
		JobName:    jobName,
		Record:     record,
		Error:      errMsg,
		ErrorClass: errClass,
		Attempt:    attempt,
		CreatedAt:  time.Now(),
	}
	return a.store.WriteDeadLetter(ctx, rec)
}

// Read returns the most recent dead-letter records for a job (limit <=0 means 100).
func (a *DLQWriterAdapter) Read(ctx context.Context, jobName string, limit int) ([]DLQRecord, error) {
	if limit <= 0 {
		limit = 10000
	}
	recs, err := a.store.ListDeadLetters(ctx, DLQFilter{JobName: jobName, Limit: limit})
	if err != nil {
		return nil, err
	}
	result := make([]DLQRecord, len(recs))
	for i, rec := range recs {
		result[i] = *rec
	}
	return result, nil
}

// ReadFiltered returns dead-letter records matching the filter criteria.
func (a *DLQWriterAdapter) ReadFiltered(ctx context.Context, filter DLQFilter) ([]DLQRecord, error) {
	filter.JobName = filter.JobName
	recs, err := a.store.ListDeadLetters(ctx, filter)
	if err != nil {
		return nil, err
	}
	result := make([]DLQRecord, len(recs))
	for i, rec := range recs {
		result[i] = *rec
	}
	return result, nil
}

// DeleteAll removes all dead-letter records for a job.
func (a *DLQWriterAdapter) DeleteAll(ctx context.Context, jobName string) error {
	return a.store.DeleteAllDeadLetters(ctx, jobName)
}

// DeleteByFilter removes dead-letter records matching the filter and returns the count deleted.
func (a *DLQWriterAdapter) DeleteByFilter(ctx context.Context, filter DLQFilter) (int64, error) {
	return a.store.DeleteDeadLettersByFilter(ctx, filter)
}

// DeleteByID removes a single dead-letter record by ID.
func (a *DLQWriterAdapter) DeleteByID(ctx context.Context, id int64) error {
	return a.store.DeleteDeadLetterByID(ctx, id)
}

// Count returns the number of dead-letter records for a job. Uses COUNT(*) on
// the storage backend (avoids loading up to 100k rows into memory).
func (a *DLQWriterAdapter) Count(ctx context.Context, jobName string) int {
	n, err := a.store.CountDeadLetters(ctx, jobName)
	if err != nil {
		return 0
	}
	return int(n)
}

// ── AuditWriter adapter ──────────────────────────────────────────────

// AuditWriterAdapter bridges storage.Storage for audit logging.
type AuditWriterAdapter struct {
	store Storage
}

func NewAuditWriterAdapter(s Storage) *AuditWriterAdapter {
	return &AuditWriterAdapter{store: s}
}

func (a *AuditWriterAdapter) Write(ctx context.Context, action, method, path, target, remote string) error {
	entry := &AuditEntry{
		Action: action,
		Method: method,
		Path:   path,
		Target: target,
		Remote: remote,
	}
	return a.store.WriteAudit(ctx, entry)
}

func (a *AuditWriterAdapter) List(ctx context.Context, limit int) ([]*AuditEntry, error) {
	return a.store.ListAudit(ctx, limit)
}

// ── PipelineSpecStore adapter ────────────────────────────────────────

// PipelineSpecStore provides YAML spec persistence on top of Storage.
type PipelineSpecStore struct {
	store Storage
}

func NewPipelineSpecStore(s Storage) *PipelineSpecStore {
	return &PipelineSpecStore{store: s}
}

func (p *PipelineSpecStore) Save(ctx context.Context, name, specYAML, status string) error {
	row := &PipelineRow{Name: name, SpecYAML: specYAML, Status: status}
	if err := p.store.SavePipeline(ctx, row); err != nil {
		return err
	}
	_, err := p.store.SavePipelineVersion(ctx, name, specYAML)
	return err
}

func (p *PipelineSpecStore) Get(ctx context.Context, name string) (string, error) {
	row, err := p.store.GetPipeline(ctx, name)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	return row.SpecYAML, nil
}

func (p *PipelineSpecStore) List(ctx context.Context) ([]*PipelineRow, error) {
	return p.store.ListPipelines(ctx)
}

func (p *PipelineSpecStore) Delete(ctx context.Context, name string) error {
	return p.store.DeletePipeline(ctx, name)
}

func (p *PipelineSpecStore) Versions(ctx context.Context, name string) ([]*PipelineVersion, error) {
	return p.store.ListPipelineVersions(ctx, name)
}

// MarshalCheckpointPosition is a helper for serializing checkpoint positions.
func MarshalCheckpointPosition(v any) (json.RawMessage, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint position: %w", err)
	}
	return json.RawMessage(data), nil
}

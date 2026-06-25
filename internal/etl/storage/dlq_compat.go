package storage

import (
	"context"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// DeadLetter mirrors dlq.DeadLetter for the adapter interface.
// Keeping this as a structural alias avoids a circular import.
type DeadLetter struct {
	ID              int64       `json:"id"`
	JobName         string      `json:"job_name"`
	Record          core.Record `json:"record"`
	Error           string      `json:"error"`
	ErrorClass      string      `json:"error_class,omitempty"`
	Timestamp       time.Time   `json:"timestamp"`
	Attempt         int         `json:"attempt"`
	RecordHash      string      `json:"record_hash,omitempty"`
	PipelineVersion int         `json:"pipeline_version,omitempty"`
	DAGNode         string      `json:"dag_node,omitempty"`
}

// DLQCompatWriter implements pipeline.DLQWriter (WriteDLQ) and also provides
// Read/ReadFiltered/Delete/DeleteFiltered/Count for the server's DLQ API handlers.
// It persists dead-letter records to the SQL-backed Storage.
type DLQCompatWriter struct {
	adapter *DLQWriterAdapter
}

func NewDLQCompatWriter(s Storage) *DLQCompatWriter {
	return &DLQCompatWriter{adapter: NewDLQWriterAdapter(s)}
}

// WriteDLQ implements pipeline.DLQWriter.
func (w *DLQCompatWriter) WriteDLQ(ctx context.Context, entry pipeline.DLQEntry) error {
	return w.adapter.WriteEntry(ctx, entry)
}

// Read returns the most recent dead-letter records for a job.
func (w *DLQCompatWriter) Read(ctx context.Context, jobName string, limit int) ([]DeadLetter, error) {
	recs, err := w.adapter.Read(ctx, jobName, limit)
	if err != nil {
		return nil, err
	}
	result := make([]DeadLetter, len(recs))
	for i, rec := range recs {
		result[i] = deadLetterFromRecord(rec)
	}
	return result, nil
}

// ReadByID returns one dead-letter record by primary key.
func (w *DLQCompatWriter) ReadByID(ctx context.Context, jobName string, id int64) (*DeadLetter, error) {
	rec, err := w.adapter.ReadByID(ctx, jobName, id)
	if err != nil || rec == nil {
		return nil, err
	}
	item := deadLetterFromRecord(*rec)
	return &item, nil
}

// Delete removes records. If timestamp is zero, deletes all for the job.
func (w *DLQCompatWriter) Delete(ctx context.Context, jobName string, timestamp time.Time) error {
	if timestamp.IsZero() {
		return w.adapter.DeleteAll(ctx, jobName)
	}
	_, err := w.adapter.DeleteByFilter(ctx, DLQFilter{
		JobName: jobName,
		From:    timestamp,
		Until:   timestamp.Add(time.Second),
	})
	return err
}

// ReadFiltered returns dead-letter records matching the filter criteria.
func (w *DLQCompatWriter) ReadFiltered(ctx context.Context, filter DLQFilter) ([]DeadLetter, error) {
	recs, err := w.adapter.ReadFiltered(ctx, filter)
	if err != nil {
		return nil, err
	}
	result := make([]DeadLetter, len(recs))
	for i, rec := range recs {
		result[i] = deadLetterFromRecord(rec)
	}
	return result, nil
}

// DeleteFiltered removes dead-letter records matching the filter and returns the count.
func (w *DLQCompatWriter) DeleteFiltered(ctx context.Context, filter DLQFilter) (int64, error) {
	return w.adapter.DeleteByFilter(ctx, filter)
}

// Count returns the number of dead-letter records for a job.
func (w *DLQCompatWriter) Count(ctx context.Context, jobName string) int {
	return w.adapter.Count(ctx, jobName)
}

// DeleteAll removes all dead-letter records for a job.
func (w *DLQCompatWriter) DeleteAll(ctx context.Context, jobName string) error {
	return w.adapter.DeleteAll(ctx, jobName)
}

// DeleteByID removes a single dead-letter record by its primary key.
// This is the preferred method for replay cleanup — it targets exactly the
// replayed record, unlike timestamp-based deletion which is imprecise when
// multiple DLQ entries share the same second (P4-10, SV-1).
func (w *DLQCompatWriter) DeleteByID(ctx context.Context, id int64) error {
	return w.adapter.DeleteByID(ctx, id)
}

func deadLetterFromRecord(rec DLQRecord) DeadLetter {
	return DeadLetter{
		ID:              rec.ID,
		JobName:         rec.JobName,
		Record:          rec.Record,
		Error:           rec.Error,
		ErrorClass:      rec.ErrorClass,
		Timestamp:       rec.CreatedAt,
		Attempt:         rec.Attempt,
		RecordHash:      rec.RecordHash,
		PipelineVersion: rec.PipelineVersion,
		DAGNode:         rec.DAGNode,
	}
}

package storage

import (
	"context"
	"time"
)

// RecoveryEvent is a durable recovery boundary for one explicit runtime scope.
// It is not a root-cause incident or a grouping of nearby error messages.
type RecoveryEvent struct {
	ID                 string     `json:"id"`
	ScopeKey           string     `json:"scope_key"`
	PipelineID         string     `json:"pipeline_id"`
	PipelineName       string     `json:"pipeline_name"`
	Endpoint           string     `json:"endpoint"`
	NodeID             string     `json:"node_id,omitempty"`
	ConnectionName     string     `json:"connection_name,omitempty"`
	ConnectionKnown    bool       `json:"connection_known"`
	SpecVersion        int        `json:"spec_version,omitempty"`
	ErrorClass         string     `json:"error_class"`
	FirstError         string     `json:"first_error"`
	State              string     `json:"state"`
	OpenedAt           time.Time  `json:"opened_at"`
	ClosedAt           *time.Time `json:"closed_at,omitempty"`
	AttemptCount       int64      `json:"attempt_count"`
	RecordCount        int64      `json:"record_count"`
	ReplayedRows       int64      `json:"replayed_rows"`
	AbandonedRows      int64      `json:"abandoned_rows"`
	TrackingIncomplete bool       `json:"tracking_incomplete"`
	DLQOpenRows        int64      `json:"dlq_open_rows"`
}

type RecoveryOperation struct {
	DLQRowKey      string     `json:"dlq_row_key,omitempty"`
	ID             string     `json:"id"`
	EventID        string     `json:"recovery_event_id,omitempty"`
	PipelineID     string     `json:"pipeline_id"`
	DLQRowID       int64      `json:"dlq_row_id"`
	RecordHash     string     `json:"record_hash"`
	Kind           string     `json:"kind"`
	State          string     `json:"state"`
	Error          string     `json:"error,omitempty"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	AcknowledgedAt *time.Time `json:"sink_acknowledged_at,omitempty"`
}

type RecoveryFilter struct {
	PipelineID string
	State      string
	Limit      int
	Offset     int
}

// Optional capability keeps third-party Storage implementations compatible.
type RecoveryStore interface {
	ListRecoveryEvents(context.Context, RecoveryFilter) ([]*RecoveryEvent, error)
	GetRecoveryEvent(context.Context, string) (*RecoveryEvent, error)
	ListRecoveryOperations(context.Context, string) ([]*RecoveryOperation, error)
	ListUnassociatedRecoveryOperations(context.Context, string) ([]*RecoveryOperation, error)
	BeginRecoveryOperation(context.Context, string, int64, string) (string, error)
	FinishRecoveryOperation(context.Context, string, string, string) error
	AcknowledgeRecoveryOperation(context.Context, string, time.Time) error
	DeleteDeadLetterAfterReplay(context.Context, string, int64, string) error
}

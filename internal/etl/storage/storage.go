package storage

import (
	"context"
	"encoding/json"
	"time"

	"openetl-go/internal/etl/core"
)

// CheckpointRecord is the storage-layer representation of a checkpoint.
type CheckpointRecord struct {
	JobName   string          `json:"job_name"`
	Source    string          `json:"source"`
	Position  json.RawMessage `json:"position"`
	Timestamp time.Time       `json:"timestamp"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// DLQRecord is the storage-layer representation of a dead-letter entry.
type DLQRecord struct {
	ID         int64       `json:"id"`
	JobName    string      `json:"job_name"`
	Record     core.Record `json:"record"`
	Error      string      `json:"error"`
	ErrorClass string      `json:"error_class,omitempty"`
	Attempt    int         `json:"attempt"`
	CreatedAt  time.Time   `json:"created_at"`
}

// DLQFilter provides SQL-style filtering for dead-letter queries.
type DLQFilter struct {
	JobName       string
	Limit         int
	Offset        int
	From          time.Time
	Until         time.Time
	Contains      string
	ErrorContains string
	ErrorClass    string
}

// AuditEntry is the storage-layer representation of an audit log entry.
type AuditEntry struct {
	ID        int64     `json:"id"`
	Action    string    `json:"action"`
	Method    string    `json:"method,omitempty"`
	Path      string    `json:"path,omitempty"`
	Target    string    `json:"target,omitempty"`
	Remote    string    `json:"remote,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// PipelineRow is the storage-layer representation of a stored pipeline definition.
type PipelineRow struct {
	Name      string    `json:"name"`
	SpecYAML  string    `json:"spec_yaml"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PipelineVersion is a versioned snapshot of a pipeline definition.
type PipelineVersion struct {
	ID        int64     `json:"id"`
	Pipeline  string    `json:"pipeline"`
	Version   int       `json:"version"`
	SpecYAML  string    `json:"spec_yaml"`
	CreatedAt time.Time `json:"created_at"`
}

// RunRecord captures the history of a single pipeline execution.
type RunRecord struct {
	ID             int64      `json:"id"`
	JobName        string     `json:"job_name"`
	Status         string     `json:"status"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	DurationMs     int64      `json:"duration_ms"`
	RecordsRead    int64      `json:"records_read"`
	RecordsWritten int64      `json:"records_written"`
	RecordsFailed  int64      `json:"records_failed"`
	RecordsDLQ     int64      `json:"records_dlq"`
}

// WorkerInfo describes a registered worker node.
type WorkerInfo struct {
	ID            string            `json:"id"`
	Host          string            `json:"host"`
	Port          int               `json:"port"`
	Slots         int               `json:"slots"`
	Status        string            `json:"status"`
	Labels        map[string]string `json:"labels,omitempty"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	RegisteredAt  time.Time         `json:"registered_at"`
}

// TaskAssignment tracks the dispatch of a task to a worker.
type TaskAssignment struct {
	ID         int64      `json:"id"`
	TaskID     string     `json:"task_id"`
	Pipeline   string     `json:"pipeline"`
	WorkerID   string     `json:"worker_id,omitempty"`
	Status     string     `json:"status"`
	AssignedAt *time.Time `json:"assigned_at,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// PluginEntry records an installed extism plugin.
type PluginEntry struct {
	Name        string    `json:"name"`
	Kind        string    `json:"kind"`
	WASMPath    string    `json:"wasm_path"`
	Version     string    `json:"version"`
	Enabled     bool      `json:"enabled"`
	InstalledAt time.Time `json:"installed_at"`
}

// Storage is the abstraction over persistent ETL metadata.
// Implementations must be safe for concurrent use.
type Storage interface {
	// Close releases all underlying resources.
	Close() error

	// Ping checks connectivity to the underlying database.
	Ping() error

	// ── Pipeline definitions ──────────────────────────────────────────

	SavePipeline(ctx context.Context, row *PipelineRow) error
	GetPipeline(ctx context.Context, name string) (*PipelineRow, error)
	ListPipelines(ctx context.Context) ([]*PipelineRow, error)
	DeletePipeline(ctx context.Context, name string) error
	UpdatePipelineStatus(ctx context.Context, name string, status string) error

	// ── Pipeline versions ─────────────────────────────────────────────

	SavePipelineVersion(ctx context.Context, name string, specYAML string) (int, error)
	GetPipelineVersion(ctx context.Context, name string, version int) (*PipelineVersion, error)
	ListPipelineVersions(ctx context.Context, name string) ([]*PipelineVersion, error)

	// ── Checkpoints ───────────────────────────────────────────────────

	SaveCheckpoint(ctx context.Context, rec *CheckpointRecord) error
	LoadCheckpoint(ctx context.Context, jobName string) (*CheckpointRecord, error)
	DeleteCheckpoint(ctx context.Context, jobName string) error
	ListCheckpoints(ctx context.Context) ([]*CheckpointRecord, error)

	// ── Dead letters ──────────────────────────────────────────────────

	WriteDeadLetter(ctx context.Context, rec *DLQRecord) error
	ListDeadLetters(ctx context.Context, filter DLQFilter) ([]*DLQRecord, error)
	CountDeadLetters(ctx context.Context, jobName string) (int64, error)
	DeleteDeadLettersByFilter(ctx context.Context, filter DLQFilter) (int64, error)
	DeleteDeadLetterByID(ctx context.Context, id int64) error
	DeleteAllDeadLetters(ctx context.Context, jobName string) error

	// ── Audit ─────────────────────────────────────────────────────────

	WriteAudit(ctx context.Context, entry *AuditEntry) error
	ListAudit(ctx context.Context, limit int) ([]*AuditEntry, error)

	// ── Run history ───────────────────────────────────────────────────

	RecordRunStart(ctx context.Context, jobName string) (int64, error)
	RecordRunEnd(ctx context.Context, runID int64, status string, read, written, failed, dlq, durationMs int64) error
	ListRunHistory(ctx context.Context, jobName string, limit int) ([]*RunRecord, error)

	// ── Worker registry ───────────────────────────────────────────────

	RegisterWorker(ctx context.Context, info *WorkerInfo) error
	Heartbeat(ctx context.Context, workerID string) error
	ListWorkers(ctx context.Context) ([]*WorkerInfo, error)
	DeregisterWorker(ctx context.Context, workerID string) error

	// ── Task assignments ──────────────────────────────────────────────

	CreateTask(ctx context.Context, task *TaskAssignment) error
	UpdateTask(ctx context.Context, task *TaskAssignment) error
	ListTasks(ctx context.Context, pipeline string) ([]*TaskAssignment, error)

	// ── Plugin registry ───────────────────────────────────────────────

	SavePlugin(ctx context.Context, p *PluginEntry) error
	GetPlugin(ctx context.Context, name string) (*PluginEntry, error)
	ListPlugins(ctx context.Context) ([]*PluginEntry, error)
	DeletePlugin(ctx context.Context, name string) error

	// ── Settings (key-value store for LLM config etc.) ────────────────
	GetSetting(ctx context.Context, key string) (string, error)
	SetSetting(ctx context.Context, key, value string) error
	ListSettings(ctx context.Context) (map[string]string, error)
}

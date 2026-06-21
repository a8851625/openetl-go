package core

import (
	"context"
	"encoding/json"
	"time"
)

// ── Lifecycle Hook Interfaces ────────────────────────────────────────

// HookContext carries pipeline-level context to hooks at each lifecycle point.
// Fields are populated selectively based on which hook is firing.
type HookContext struct {
	PipelineName  string          `json:"pipeline_name"`
	Config        map[string]any  `json:"config,omitempty"`
	Metadata      Metadata        `json:"metadata,omitempty"`
	RecordCount   int             `json:"record_count,omitempty"`
	ErrorMessage  string          `json:"error_message,omitempty"`
	CheckpointPos json.RawMessage `json:"checkpoint_pos,omitempty"`
	Timestamp     time.Time       `json:"timestamp"`
}

// HookKind enumerates the lifecycle hook points.
type HookKind string

const (
	HookOnInit       HookKind = "on_init"
	HookOnPreBatch   HookKind = "on_pre_batch"
	HookOnPostBatch  HookKind = "on_post_batch"
	HookOnError      HookKind = "on_error"
	HookOnCheckpoint HookKind = "on_checkpoint"
	HookOnShutdown   HookKind = "on_shutdown"
)

// LifecycleHook is the interface every hook implementation must satisfy.
// A hook may implement only the methods it cares about; the runner
// type-asserts optional hooks (e.g. PostBatchHook) before calling them.
type LifecycleHook interface {
	Name() string
}

// InitHook fires once after source+sink are opened, before the first record.
type InitHook interface {
	LifecycleHook
	OnInit(ctx context.Context, hctx HookContext) error
}

// PreBatchHook fires before each batch is written to the sink.
type PreBatchHook interface {
	LifecycleHook
	OnPreBatch(ctx context.Context, hctx HookContext) error
}

// PostBatchHook fires after a batch is successfully written to the sink.
type PostBatchHook interface {
	LifecycleHook
	OnPostBatch(ctx context.Context, hctx HookContext) error
}

// ErrorHook fires when a record fails and is routed to DLQ.
type ErrorHook interface {
	LifecycleHook
	OnError(ctx context.Context, hctx HookContext) error
}

// CheckpointHook fires after a checkpoint is saved.
type CheckpointHook interface {
	LifecycleHook
	OnCheckpoint(ctx context.Context, hctx HookContext) error
}

// ShutdownHook fires before the pipeline runner shuts down.
type ShutdownHook interface {
	LifecycleHook
	OnShutdown(ctx context.Context, hctx HookContext) error
}

package telemetry

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Default business-health thresholds (overridable via env on the server side).
const (
	DefaultStaleCheckpointSec = 300
	DefaultHighCDCLagMs       = 60_000
	DefaultWorkerStaleSec     = 30
	DefaultSinkStuckMs        = 120_000
)

// Overall health levels returned by /api/v2/health.
const (
	HealthOK        = "ok"
	HealthDegraded  = "degraded"
	HealthUnhealthy = "unhealthy"
)

// PipelineHealth is the per-pipeline derived status used by API/UI/metrics.
type PipelineHealth string

const (
	PipelineHealthy   PipelineHealth = "healthy"
	PipelineDegraded  PipelineHealth = "degraded"
	PipelineFailed    PipelineHealth = "failed"
	PipelinePaused    PipelineHealth = "paused"
	PipelineScheduled PipelineHealth = "scheduled"
	PipelineCompleted PipelineHealth = "completed"
	PipelineStopped   PipelineHealth = "stopped"
	PipelineStarting  PipelineHealth = "starting"
	PipelineUnknown   PipelineHealth = "unknown"
)

// HealthThresholds controls degraded/unhealthy transitions for running pipelines.
type HealthThresholds struct {
	StaleCheckpointSec int64
	HighCDCLagMs       int64
	WorkerStaleSec     int64
	// SinkStuckMs is reserved for future per-sink stall detection; 0 disables.
	SinkStuckMs int64
}

// DefaultHealthThresholds returns production-friendly defaults matching the UI.
func DefaultHealthThresholds() HealthThresholds {
	return HealthThresholds{
		StaleCheckpointSec: DefaultStaleCheckpointSec,
		HighCDCLagMs:       DefaultHighCDCLagMs,
		WorkerStaleSec:     DefaultWorkerStaleSec,
		SinkStuckMs:        DefaultSinkStuckMs,
	}
}

// PipelineHealthInput is the minimal signal set used to derive pipeline health.
type PipelineHealthInput struct {
	Status               string
	RecordsFailed        int64
	RecordsDLQ           int64
	DLQFileCount         int
	LastError            string
	CheckpointAgeSeconds int64
	CDCLagMs             int64
	SourceReadLatencyMs  float64
	SinkWriteLatencyMs   float64
	CircuitBreakerState  int // 0=closed, 1=open, 2=half_open
}

// DerivePipelineHealth mirrors the UI derivation rules so API/UI/metrics agree.
//
// failed/paused/scheduled/completed/stopped/starting are terminal display states.
// running pipelines become degraded when DLQ/fail/error/lag/checkpoint/CB signals fire.
func DerivePipelineHealth(in PipelineHealthInput, th HealthThresholds) PipelineHealth {
	if th.StaleCheckpointSec <= 0 {
		th.StaleCheckpointSec = DefaultStaleCheckpointSec
	}
	if th.HighCDCLagMs <= 0 {
		th.HighCDCLagMs = DefaultHighCDCLagMs
	}

	s := strings.ToLower(strings.TrimSpace(in.Status))
	switch s {
	case "failed", "error":
		return PipelineFailed
	case "paused":
		return PipelinePaused
	case "scheduled":
		return PipelineScheduled
	case "completed":
		return PipelineCompleted
	case "starting":
		return PipelineStarting
	case "stopped":
		return PipelineStopped
	case "running":
		if in.RecordsFailed > 0 ||
			in.RecordsDLQ > 0 ||
			in.DLQFileCount > 0 ||
			strings.TrimSpace(in.LastError) != "" ||
			(in.CDCLagMs > 0 && in.CDCLagMs > th.HighCDCLagMs) ||
			(in.CheckpointAgeSeconds > th.StaleCheckpointSec) ||
			in.CircuitBreakerState == 1 { // open
			return PipelineDegraded
		}
		return PipelineHealthy
	default:
		return PipelineUnknown
	}
}

// ComponentHealth is one named subsystem entry in /api/v2/health.
type ComponentHealth struct {
	Name    string
	Status  string // ok | degraded | unhealthy | skipped
	Detail  string
	Level   string // ok | degraded | unhealthy (for aggregation)
}

// AggregateHealth folds component + pipeline signals into overall status.
// unhealthy wins over degraded; empty components default to ok.
func AggregateHealth(components []ComponentHealth, pipelineHealth map[string]PipelineHealth) (overall string, reasons []string) {
	overall = HealthOK
	for _, c := range components {
		level := c.Level
		if level == "" {
			level = c.Status
		}
		switch level {
		case HealthUnhealthy:
			overall = HealthUnhealthy
			if c.Detail != "" {
				reasons = append(reasons, fmt.Sprintf("%s: %s", c.Name, c.Detail))
			} else {
				reasons = append(reasons, c.Name+": unhealthy")
			}
		case HealthDegraded:
			if overall != HealthUnhealthy {
				overall = HealthDegraded
			}
			if c.Detail != "" {
				reasons = append(reasons, fmt.Sprintf("%s: %s", c.Name, c.Detail))
			} else {
				reasons = append(reasons, c.Name+": degraded")
			}
		}
	}

	var failed, degraded int
	for _, h := range pipelineHealth {
		switch h {
		case PipelineFailed:
			failed++
		case PipelineDegraded:
			degraded++
		}
	}
	if failed > 0 {
		overall = HealthUnhealthy
		reasons = append(reasons, fmt.Sprintf("%d pipeline(s) failed", failed))
	} else if degraded > 0 {
		if overall != HealthUnhealthy {
			overall = HealthDegraded
		}
		reasons = append(reasons, fmt.Sprintf("%d pipeline(s) degraded", degraded))
	}

	sort.Strings(reasons)
	return overall, reasons
}

// FormatHealthMap flattens components and pipeline health into the legacy
// map[string]string response used by HealthHandler / load balancers.
func FormatHealthMap(overall string, reasons []string, components []ComponentHealth, pipelines map[string]PipelineHealth, extra map[string]string) map[string]string {
	out := map[string]string{
		"status":    overall,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	if len(reasons) > 0 {
		out["reasons"] = strings.Join(reasons, "; ")
	}
	for _, c := range components {
		key := c.Name
		val := c.Status
		if c.Detail != "" {
			val = c.Status + ": " + c.Detail
		}
		out[key] = val
	}
	// Stable pipeline keys.
	names := make([]string, 0, len(pipelines))
	for name := range pipelines {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		out["pipeline_"+name] = string(pipelines[name])
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

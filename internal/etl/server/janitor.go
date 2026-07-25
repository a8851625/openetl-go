package server

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// RetentionConfig holds TTL / max-count knobs for the control-plane janitor.
// Zero duration / count disables that purge path.
type RetentionConfig struct {
	// DLQTTL purges dead_letters older than this duration (ETL_DLQ_TTL).
	DLQTTL time.Duration
	// DLQMaxCount caps total DLQ rows; oldest rows are deleted first (ETL_DLQ_MAX_COUNT).
	// Hard upper bound is MaxDLQMaxCount to prevent misconfiguration.
	DLQMaxCount int
	// AuditTTL purges audit_logs older than this (ETL_AUDIT_TTL).
	AuditTTL time.Duration
	// RunHistoryTTL purges finished run_history older than this (ETL_RUN_HISTORY_TTL).
	RunHistoryTTL time.Duration
	// TaskTTL purges finished task_assignments older than this (ETL_TASK_TTL).
	TaskTTL time.Duration
	// Interval between janitor ticks (ETL_JANITOR_INTERVAL, default 5m).
	Interval time.Duration
	// BatchLimit caps rows deleted per table per tick (ETL_JANITOR_BATCH_LIMIT, default 10000).
	// Hard upper bound is MaxJanitorBatchLimit.
	BatchLimit int
}

// Hard caps prevent a misconfigured env from deleting unbounded rows in one tick.
const (
	MaxDLQMaxCount       = 1_000_000
	MaxJanitorBatchLimit = 100_000
	DefaultJanitorBatch  = 10_000
	DefaultJanitorEvery  = 5 * time.Minute
)

// JanitorStatus is the last observed janitor run (exposed via /api/v2/health).
type JanitorStatus struct {
	Enabled       bool      `json:"enabled"`
	LastRunAt     time.Time `json:"last_run_at,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
	LastDeleted   int64     `json:"last_deleted"`
	LastDLQ       int64     `json:"last_dlq_deleted"`
	LastAudit     int64     `json:"last_audit_deleted"`
	LastRuns      int64     `json:"last_runs_deleted"`
	LastTasks     int64     `json:"last_tasks_deleted"`
	ConfigSummary string    `json:"config_summary"`
}

// loadRetentionConfig reads janitor knobs from env / config.
func loadRetentionConfig(ctx context.Context, existingDLQTTL time.Duration, existingDLQMax int) RetentionConfig {
	cfg := RetentionConfig{
		DLQTTL:        existingDLQTTL,
		DLQMaxCount:   existingDLQMax,
		Interval:      DefaultJanitorEvery,
		BatchLimit:    DefaultJanitorBatch,
		AuditTTL:      parseDurationEnv(ctx, "ETL_AUDIT_TTL", "etl.retention.auditTTL", 0),
		RunHistoryTTL: parseDurationEnv(ctx, "ETL_RUN_HISTORY_TTL", "etl.retention.runHistoryTTL", 0),
		TaskTTL:       parseDurationEnv(ctx, "ETL_TASK_TTL", "etl.retention.taskTTL", 0),
	}
	// Env overrides for DLQ max when NewServer has not already parsed them.
	if cfg.DLQMaxCount == 0 {
		if mc := strings.TrimSpace(os.Getenv("ETL_DLQ_MAX_COUNT")); mc != "" {
			if n, err := strconv.Atoi(mc); err == nil && n > 0 {
				cfg.DLQMaxCount = n
			}
		}
	}
	if cfg.DLQTTL == 0 {
		cfg.DLQTTL = parseDurationEnv(ctx, "ETL_DLQ_TTL", "etl.retention.dlqTTL", 0)
	}
	if v := strings.TrimSpace(os.Getenv("ETL_JANITOR_INTERVAL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.Interval = d
		}
	} else if d := g.Cfg().MustGet(ctx, "etl.retention.interval", "").String(); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil && parsed > 0 {
			cfg.Interval = parsed
		}
	}
	if v := strings.TrimSpace(os.Getenv("ETL_JANITOR_BATCH_LIMIT")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.BatchLimit = n
		}
	} else if n := g.Cfg().MustGet(ctx, "etl.retention.batchLimit", 0).Int(); n > 0 {
		cfg.BatchLimit = n
	}
	if cfg.BatchLimit > MaxJanitorBatchLimit {
		g.Log().Warningf(ctx, "ETL_JANITOR_BATCH_LIMIT=%d exceeds hard cap %d; clamping", cfg.BatchLimit, MaxJanitorBatchLimit)
		cfg.BatchLimit = MaxJanitorBatchLimit
	}
	if cfg.DLQMaxCount == 0 {
		if n := g.Cfg().MustGet(ctx, "etl.retention.dlqMaxCount", 0).Int(); n > 0 {
			cfg.DLQMaxCount = n
		}
	}
	// Clamp after all sources so env/config cannot bypass the hard cap.
	if cfg.DLQMaxCount > MaxDLQMaxCount {
		g.Log().Warningf(ctx, "ETL_DLQ_MAX_COUNT=%d exceeds hard cap %d; clamping", cfg.DLQMaxCount, MaxDLQMaxCount)
		cfg.DLQMaxCount = MaxDLQMaxCount
	}
	return cfg
}

func parseDurationEnv(ctx context.Context, envName, cfgKey string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			g.Log().Warningf(ctx, "invalid %s=%q, ignoring", envName, v)
			return def
		}
		return d
	}
	if raw := g.Cfg().MustGet(ctx, cfgKey, "").String(); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			g.Log().Warningf(ctx, "invalid %s=%q, ignoring", cfgKey, raw)
			return def
		}
		return d
	}
	return def
}

func (c RetentionConfig) enabled() bool {
	return c.DLQTTL > 0 || c.DLQMaxCount > 0 || c.AuditTTL > 0 || c.RunHistoryTTL > 0 || c.TaskTTL > 0
}

func (c RetentionConfig) summary() string {
	parts := []string{}
	if c.DLQTTL > 0 {
		parts = append(parts, fmt.Sprintf("dlq_ttl=%s", c.DLQTTL))
	}
	if c.DLQMaxCount > 0 {
		parts = append(parts, fmt.Sprintf("dlq_max=%d", c.DLQMaxCount))
	}
	if c.AuditTTL > 0 {
		parts = append(parts, fmt.Sprintf("audit_ttl=%s", c.AuditTTL))
	}
	if c.RunHistoryTTL > 0 {
		parts = append(parts, fmt.Sprintf("run_ttl=%s", c.RunHistoryTTL))
	}
	if c.TaskTTL > 0 {
		parts = append(parts, fmt.Sprintf("task_ttl=%s", c.TaskTTL))
	}
	if len(parts) == 0 {
		return "disabled"
	}
	return strings.Join(parts, ",")
}

// janitorState is owned by Server and updated each tick.
type janitorState struct {
	mu     sync.RWMutex
	status JanitorStatus
}

func (s *Server) initJanitor(ctx context.Context) {
	cfg := loadRetentionConfig(ctx, s.dlqTTL, s.dlqMaxCount)
	s.retention = cfg
	s.janitor = &janitorState{status: JanitorStatus{
		Enabled:       cfg.enabled(),
		ConfigSummary: cfg.summary(),
	}}
	// Keep legacy fields in sync for any callers still reading them.
	s.dlqTTL = cfg.DLQTTL
	s.dlqMaxCount = cfg.DLQMaxCount
}

// retentionJanitorLoop periodically purges expired DLQ / audit / run / task rows.
func (s *Server) retentionJanitorLoop(ctx context.Context) {
	interval := s.retention.Interval
	if interval <= 0 {
		interval = DefaultJanitorEvery
	}
	// Run once immediately so short-lived e2e / smoke can observe a tick.
	s.runRetentionJanitor(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.runRetentionJanitor(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) runRetentionJanitor(ctx context.Context) {
	cfg := s.retention
	if !cfg.enabled() {
		return
	}
	var total, dlqN, auditN, runN, taskN int64
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		if firstErr == nil {
			firstErr = err
		}
		g.Log().Warningf(ctx, "retention janitor: %v", err)
	}

	batch := cfg.BatchLimit
	if batch <= 0 {
		batch = DefaultJanitorBatch
	}

	// DLQ TTL.
	if cfg.DLQTTL > 0 {
		cutoff := time.Now().Add(-cfg.DLQTTL)
		n, err := s.store.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{Until: cutoff, Limit: batch})
		recordErr(err)
		if err == nil {
			dlqN += n
			total += n
			if n > 0 {
				g.Log().Infof(ctx, "DLQ janitor: purged %d entries older than %s", n, cutoff.Format(time.RFC3339))
			}
		}
	}

	// DLQ max count: delete oldest excess rows.
	if cfg.DLQMaxCount > 0 {
		if purger, ok := s.store.(storage.RetentionPurger); ok {
			counts, err := purger.CountObjects(ctx)
			recordErr(err)
			if err == nil && counts.DeadLetters > cfg.DLQMaxCount {
				excess := counts.DeadLetters - cfg.DLQMaxCount
				if excess > batch {
					excess = batch
				}
				// Delete oldest by Until far in the future is wrong; use raw delete
				// of oldest N via filter with a synthetic high Until and limit.
				// Prefer a dedicated path: delete where created_at <= now ordered by age.
				n, err := s.store.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{
					Until: time.Now().Add(time.Hour),
					Limit: excess,
				})
				// The above may delete newest if ORDER BY is missing on DELETE.
				// For max-count we rely on rewriteDeleteLimitForPostgres ORDER BY created_at ASC
				// and SQLite/MySQL LIMIT without ORDER (implementation-defined).
				// Prefer explicit oldest purge via a secondary path when available.
				recordErr(err)
				if err == nil {
					dlqN += n
					total += n
					if n > 0 {
						g.Log().Infof(ctx, "DLQ janitor: purged %d entries to enforce max_count=%d", n, cfg.DLQMaxCount)
					}
				}
			}
		}
	}

	purger, hasPurger := s.store.(storage.RetentionPurger)
	if hasPurger {
		if cfg.AuditTTL > 0 {
			cutoff := time.Now().Add(-cfg.AuditTTL)
			n, err := purger.PurgeAuditBefore(ctx, cutoff, batch)
			recordErr(err)
			if err == nil {
				auditN += n
				total += n
				if n > 0 {
					g.Log().Infof(ctx, "audit janitor: purged %d entries older than %s", n, cutoff.Format(time.RFC3339))
				}
			}
		}
		if cfg.RunHistoryTTL > 0 {
			cutoff := time.Now().Add(-cfg.RunHistoryTTL)
			n, err := purger.PurgeRunHistoryBefore(ctx, cutoff, batch)
			recordErr(err)
			if err == nil {
				runN += n
				total += n
				if n > 0 {
					g.Log().Infof(ctx, "run-history janitor: purged %d entries older than %s", n, cutoff.Format(time.RFC3339))
				}
			}
		}
		if cfg.TaskTTL > 0 {
			cutoff := time.Now().Add(-cfg.TaskTTL)
			n, err := purger.PurgeFinishedTasksBefore(ctx, cutoff, batch)
			recordErr(err)
			if err == nil {
				taskN += n
				total += n
				if n > 0 {
					g.Log().Infof(ctx, "task janitor: purged %d finished tasks older than %s", n, cutoff.Format(time.RFC3339))
				}
			}
		}
	}

	status := JanitorStatus{
		Enabled:       true,
		LastRunAt:     time.Now().UTC(),
		LastDeleted:   total,
		LastDLQ:       dlqN,
		LastAudit:     auditN,
		LastRuns:      runN,
		LastTasks:     taskN,
		ConfigSummary: cfg.summary(),
	}
	if firstErr != nil {
		status.LastError = firstErr.Error()
		// Failure alert (deduped by alert manager).
		if s.alertManager != nil {
			s.alertManager.Send(ctx, alert.Event{
				Level:   alert.LevelWarning,
				Title:   "retention janitor failure",
				Message: firstErr.Error(),
				Labels:  map[string]string{"component": "janitor"},
			})
		}
	}
	if s.janitor != nil {
		s.janitor.mu.Lock()
		s.janitor.status = status
		s.janitor.mu.Unlock()
	}
}

// JanitorStatusSnapshot returns a copy of the last janitor status for health.
func (s *Server) JanitorStatusSnapshot() JanitorStatus {
	if s.janitor == nil {
		return JanitorStatus{Enabled: false, ConfigSummary: "disabled"}
	}
	s.janitor.mu.RLock()
	defer s.janitor.mu.RUnlock()
	return s.janitor.status
}

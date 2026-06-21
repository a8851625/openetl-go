package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"openetl-go/internal/etl/alert"
)

// CircuitBreaker tracks consecutive sink-write failures and trips after
// MaxFailures within WindowSec. When tripped, the source should pause reading
// until CooldownSec has elapsed, at which point a half-open probe is allowed.
type CircuitBreaker struct {
	cfg          CircuitBreakerCfg
	mu           sync.Mutex
	failures     []time.Time
	state        breakerState
	trippedAt    time.Time
	lastProbeAt  time.Time
	alertManager *alert.Manager
	pipelineName string
}

type breakerState int

const (
	breakerClosed   breakerState = iota // normal operation
	breakerOpen                         // tripped, rejecting writes
	breakerHalfOpen                     // allowing a probe
)

func NewCircuitBreaker(cfg CircuitBreakerCfg, am *alert.Manager, name string) *CircuitBreaker {
	if cfg.MaxFailures <= 0 {
		cfg.MaxFailures = 5
	}
	if cfg.WindowSec <= 0 {
		cfg.WindowSec = 60
	}
	if cfg.CooldownSec <= 0 {
		cfg.CooldownSec = 30
	}
	return &CircuitBreaker{
		cfg:          cfg,
		alertManager: am,
		pipelineName: name,
	}
}

// Allow returns true if the breaker is closed or half-open (probe allowed).
// Returns false if the breaker is open (source should pause).
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if time.Since(cb.trippedAt) >= time.Duration(cb.cfg.CooldownSec)*time.Second {
			cb.state = breakerHalfOpen
			cb.lastProbeAt = time.Now()
			return true
		}
		return false
	case breakerHalfOpen:
		return true
	}
	return true
}

// RecordSuccess resets the breaker to closed.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = nil
	cb.state = breakerClosed
}

// RecordFailure records a write failure. Returns true if the breaker just tripped.
func (cb *CircuitBreaker) RecordFailure(ctx context.Context, err error) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	window := time.Duration(cb.cfg.WindowSec) * time.Second

	// Prune old failures outside the window
	var recent []time.Time
	for _, t := range cb.failures {
		if now.Sub(t) < window {
			recent = append(recent, t)
		}
	}
	recent = append(recent, now)
	cb.failures = recent

	if cb.state == breakerHalfOpen {
		// Probe failed — re-trip
		cb.tripLocked(now)
		return true
	}

	if len(cb.failures) >= cb.cfg.MaxFailures {
		cb.tripLocked(now)
		if cb.alertManager != nil {
			cb.alertManager.Send(ctx, alert.Event{
				Level:   alert.LevelError,
				Title:   "Circuit breaker tripped",
				Message: fmt.Sprintf("Pipeline %s: sink circuit breaker tripped after %d failures in %ds: %v", cb.pipelineName, len(cb.failures), cb.cfg.WindowSec, err),
				JobName: cb.pipelineName,
			})
		}
		return true
	}
	return false
}

func (cb *CircuitBreaker) tripLocked(at time.Time) {
	cb.state = breakerOpen
	cb.trippedAt = at
}

// IsTripped returns true if the breaker is currently open.
func (cb *CircuitBreaker) IsTripped() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state == breakerOpen
}

// State returns the current state as a string for metrics.
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case breakerClosed:
		return "closed"
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// StateCode returns the current breaker state as an integer for Prometheus:
// 0=closed, 1=open, 2=half_open.
func (cb *CircuitBreaker) StateCode() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case breakerClosed:
		return 0
	case breakerOpen:
		return 1
	case breakerHalfOpen:
		return 2
	}
	return -1
}

// ── Alert Rule Checker ──────────────────────────────────────────────────

// AlertRuleChecker evaluates threshold-based alert rules periodically.
// It runs as a background goroutine within the Runner.
type AlertRuleChecker struct {
	rules        AlertRules
	interval     time.Duration
	alertMgr     *alert.Manager
	pipelineName string
}

func NewAlertRuleChecker(rules AlertRules, am *alert.Manager, name string) *AlertRuleChecker {
	if rules.CheckIntervalSec <= 0 {
		rules.CheckIntervalSec = 30
	}
	return &AlertRuleChecker{
		rules:        rules,
		interval:     time.Duration(rules.CheckIntervalSec) * time.Second,
		alertMgr:     am,
		pipelineName: name,
	}
}

// Check evaluates alert rules against current stats. Returns triggered events.
func (c *AlertRuleChecker) Check(ctx context.Context, stats Stats, metrics MetricsSnapshot, lastRecordAt time.Time) []alert.Event {
	var events []alert.Event
	now := time.Now()

	if c.rules.LagSecondsGt > 0 && metrics.CDCLagMs > 0 {
		lagSec := metrics.CDCLagMs / 1000
		if lagSec > int64(c.rules.LagSecondsGt) {
			events = append(events, alert.Event{
				Level:   alert.LevelWarning,
				Title:   "CDC lag exceeded threshold",
				Message: fmt.Sprintf("Pipeline %s: CDC lag %ds > threshold %ds", c.pipelineName, lagSec, c.rules.LagSecondsGt),
				JobName: c.pipelineName,
			})
		}
	}

	if c.rules.ErrorRateGt > 0 && stats.RecordsRead > 0 {
		errorRate := float64(stats.RecordsFailed) / float64(stats.RecordsRead)
		if errorRate > c.rules.ErrorRateGt {
			events = append(events, alert.Event{
				Level:   alert.LevelWarning,
				Title:   "Error rate exceeded threshold",
				Message: fmt.Sprintf("Pipeline %s: error rate %.2f%% > threshold %.2f%%", c.pipelineName, errorRate*100, c.rules.ErrorRateGt*100),
				JobName: c.pipelineName,
			})
		}
	}

	if c.rules.NoRecordsForMinutes > 0 && !lastRecordAt.IsZero() {
		minutesSince := now.Sub(lastRecordAt).Minutes()
		if minutesSince >= float64(c.rules.NoRecordsForMinutes) {
			events = append(events, alert.Event{
				Level:   alert.LevelWarning,
				Title:   "Pipeline stall detected",
				Message: fmt.Sprintf("Pipeline %s: no records for %.0f minutes (threshold: %d)", c.pipelineName, minutesSince, c.rules.NoRecordsForMinutes),
				JobName: c.pipelineName,
			})
		}
	}

	for _, e := range events {
		c.alertMgr.Send(ctx, e)
	}
	return events
}

// Interval returns the check interval duration.
func (c *AlertRuleChecker) Interval() time.Duration { return c.interval }

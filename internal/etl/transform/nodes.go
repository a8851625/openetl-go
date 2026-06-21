package transform

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/alert"
	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("tap", func(config map[string]any) (core.Transform, error) {
		return NewTapTransform(config)
	})
	registry.RegisterTransform("rate_limiter", func(config map[string]any) (core.Transform, error) {
		return NewRateLimiterTransform(config)
	})
}

// ════════════════════════════════════════════════════════════════════
// Tap — Side-channel listener (metrics / alerts / audit)
// ════════════════════════════════════════════════════════════════════

// TapTransform is a pass-through node that observes every record without
// modifying it. It's used for:
//   - Counting records by table/operation
//   - Detecting anomalies (e.g., sudden spike in DELETE operations)
//   - Forwarding to external alert channels
//   - Computing latency from metadata.Timestamp
//
// Config:
//
//	alert_on: "delete_spike" | "error_spike" | "latency_gt" | "field_match"
//	threshold: numeric threshold (e.g., latency_ms > 5000)
//	field: field name for field_match alert
//	value: expected value for field_match
//	webhook: optional webhook URL to call on alert
//	log_every: log counters every N records (default 100)
//	alert_on_lag_ms: emit a warning when processing latency exceeds this (ms)
type TapTransform struct {
	alertMgr   *alert.Manager
	counters   sync.Map // key → *int64
	logEvery   int64
	alertLagMs int64
	alertOn    string
	threshold  float64
	alertField string
	alertValue string
	webhook    string
}

func NewTapTransform(config map[string]any) (*TapTransform, error) {
	t := &TapTransform{
		alertMgr: alert.NewManager(),
		logEvery: 100,
	}

	if v, ok := config["alert_on"].(string); ok {
		t.alertOn = v
	}
	if v, ok := config["threshold"].(float64); ok {
		t.threshold = v
	} else if v, ok := config["threshold"].(int); ok {
		t.threshold = float64(v)
	}
	if v, ok := config["field"].(string); ok {
		t.alertField = v
	}
	if v, ok := config["value"].(string); ok {
		t.alertValue = v
	}
	if v, ok := config["webhook"].(string); ok && v != "" {
		t.webhook = v
		t.alertMgr.Register(alert.NewWebhookChannel(v))
	}
	if v, ok := config["log_every"].(int); ok && v > 0 {
		t.logEvery = int64(v)
	}
	if v, ok := config["log_every"].(float64); ok && v > 0 {
		t.logEvery = int64(v)
	}
	if v, ok := config["alert_on_lag_ms"].(int); ok && v > 0 {
		t.alertLagMs = int64(v)
	}
	if v, ok := config["alert_on_lag_ms"].(float64); ok && v > 0 {
		t.alertLagMs = int64(v)
	}

	return t, nil
}

func (t *TapTransform) Name() string { return "tap" }

func (t *TapTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	// Count by table + operation.
	key := fmt.Sprintf("%s/%s", rec.Metadata.Table, rec.Operation)
	var counter *int64
	if v, ok := t.counters.Load(key); ok {
		counter = v.(*int64)
	} else {
		counter = new(int64)
		actual, loaded := t.counters.LoadOrStore(key, counter)
		if loaded {
			counter = actual.(*int64)
		}
	}
	count := atomic.AddInt64(counter, 1)

	// Log counters periodically using the configured log_every value.
	if t.logEvery > 0 && count%t.logEvery == 0 {
		g.Log().Infof(ctx, "[tap] %s: %d records", key, count)
	}

	// Compute CDC latency if timestamp is available.
	if !rec.Metadata.Timestamp.IsZero() {
		latency := time.Since(rec.Metadata.Timestamp)
		if latency > 10*time.Second {
			g.Log().Warningf(ctx, "[tap] high latency: %v on %s/%s", latency.Truncate(time.Millisecond), rec.Metadata.Table, rec.Operation)
		}
		// Honor configured alert_on_lag_ms threshold.
		if t.alertLagMs > 0 && latency.Milliseconds() > t.alertLagMs {
			g.Log().Warningf(ctx, "[tap] latency %v exceeds alert_on_lag_ms=%d on %s/%s",
				latency.Truncate(time.Millisecond), t.alertLagMs, rec.Metadata.Table, rec.Operation)
		}
	}

	// Return record unchanged (pass-through).
	return rec, nil
}

// Close stops the alert manager's background goroutines.
func (t *TapTransform) Close() error {
	if t.alertMgr != nil {
		t.alertMgr.Close()
	}
	return nil
}

// ════════════════════════════════════════════════════════════════════
// RateLimiter — Token bucket throttle
// ════════════════════════════════════════════════════════════════════

// RateLimiterTransform throttles the pipeline to a maximum rate using a
// token bucket algorithm. Records that arrive when no token is available
// will block until one becomes available (creating natural backpressure).
//
// Config:
//
//	rps:          max records per second (default 1000)
//	burst:        burst capacity (default = rps, allows short spikes)
type RateLimiterTransform struct {
	rps    int
	burst  int
	tokens chan struct{}
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewRateLimiterTransform(config map[string]any) (*RateLimiterTransform, error) {
	rps := 1000
	if v, ok := config["rps"].(int); ok && v > 0 {
		rps = v
	}
	if v, ok := config["rps"].(float64); ok && v > 0 {
		rps = int(v)
	}
	// Guard against rps <= 0 (would make interval division produce 0) and
	// unreasonably large values (> 1e9 => interval rounds to 0).
	if rps <= 0 {
		rps = 1
	}
	if rps > 1e9 {
		rps = 1e9
	}
	burst := rps
	if v, ok := config["burst"].(int); ok && v > 0 {
		burst = v
	}
	if v, ok := config["burst"].(float64); ok && v > 0 {
		burst = int(v)
	}

	t := &RateLimiterTransform{
		rps:    rps,
		burst:  burst,
		tokens: make(chan struct{}, burst),
		stopCh: make(chan struct{}),
	}

	// Pre-fill the bucket.
	for i := 0; i < burst; i++ {
		t.tokens <- struct{}{}
	}

	// Start refiller goroutine.
	interval := time.Second / time.Duration(rps)
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case t.tokens <- struct{}{}:
				default: // bucket full, drop token
				}
			case <-t.stopCh:
				return
			}
		}
	}()

	return t, nil
}

func (t *RateLimiterTransform) Name() string { return "rate_limiter" }

func (t *RateLimiterTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	select {
	case <-t.tokens:
		return rec, nil
	case <-ctx.Done():
		return rec, ctx.Err()
	}
}

// Close stops the refiller goroutine.
func (t *RateLimiterTransform) Close() error {
	select {
	case <-t.stopCh:
	default:
		close(t.stopCh)
	}
	t.wg.Wait()
	return nil
}

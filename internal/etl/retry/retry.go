package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

type Config struct {
	MaxAttempts     int           `yaml:"max_attempts" json:"max_attempts"`
	InitialInterval time.Duration `yaml:"initial_interval" json:"initial_interval"`
	MaxInterval     time.Duration `yaml:"max_interval" json:"max_interval"`
	Multiplier      float64       `yaml:"multiplier" json:"multiplier"`
}

func DefaultConfig() Config {
	return Config{
		MaxAttempts:     3,
		InitialInterval: time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
	}
}

type IsRetryable func(err error) bool

func DefaultIsRetryable(err error) bool {
	return err != nil
}

func Do(ctx context.Context, cfg Config, isRetryable IsRetryable, fn func() error) error {
	if isRetryable == nil {
		isRetryable = DefaultIsRetryable
	}

	var lastErr error
	interval := cfg.InitialInterval

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryable(err) {
			return err
		}

		if attempt >= cfg.MaxAttempts-1 {
			break
		}

		// Add jitter (up to 25% of interval) to prevent thundering herd
		// when multiple pipelines retry against the same sink simultaneously.
		// Guard against interval <= 0 (PC-4): rand.Int63n(0) panics, which
		// would kill the pipeline via the writeLoop recover instead of
		// retrying. A misconfigured InitialInterval degrades to a 1ms wait.
		jitterBound := int64(interval) / 4
		var wait time.Duration
		switch {
		case jitterBound > 0:
			wait = interval + time.Duration(rand.Int63n(jitterBound))
		case interval > 0:
			wait = interval
		default:
			wait = time.Millisecond
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}

		interval = time.Duration(math.Min(
			float64(interval)*cfg.Multiplier,
			float64(cfg.MaxInterval),
		))
	}

	return lastErr
}

package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDoRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	cfg := Config{MaxAttempts: 3, InitialInterval: time.Millisecond, MaxInterval: time.Millisecond, Multiplier: 1}
	err := Do(context.Background(), cfg, nil, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestDoStopsOnNonRetryable(t *testing.T) {
	attempts := 0
	cfg := Config{MaxAttempts: 3, InitialInterval: time.Millisecond, MaxInterval: time.Millisecond, Multiplier: 1}
	err := Do(context.Background(), cfg, func(error) bool { return false }, func() error {
		attempts++
		return errors.New("permanent")
	})
	if err == nil {
		t.Fatal("Do() error = nil, want error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

// TestDoZeroIntervalNoPanic guards PC-4: a misconfigured InitialInterval of 0
// must not panic on rand.Int63n(0). Previously this killed the pipeline via the
// writeLoop recover on the first transient sink error.
func TestDoZeroIntervalNoPanic(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"zero initial", Config{MaxAttempts: 3, InitialInterval: 0, MaxInterval: time.Millisecond, Multiplier: 2}},
		{"zero everything", Config{MaxAttempts: 3, InitialInterval: 0, MaxInterval: 0, Multiplier: 0}},
		{"sub-nanos truncation", Config{MaxAttempts: 3, InitialInterval: 3 * time.Nanosecond, MaxInterval: time.Millisecond, Multiplier: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			err := Do(context.Background(), tc.cfg, nil, func() error {
				attempts++
				if attempts < tc.cfg.MaxAttempts {
					return errors.New("temporary")
				}
				return nil
			})
			if err != nil {
				t.Fatalf("Do() error = %v", err)
			}
			if attempts != tc.cfg.MaxAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, tc.cfg.MaxAttempts)
			}
		})
	}
}

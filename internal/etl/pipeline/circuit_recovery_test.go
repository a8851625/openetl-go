package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

type recoveryDeadlineSink struct {
	recordingSink
	remaining time.Duration
}

func (s *recoveryDeadlineSink) Write(ctx context.Context, records []core.Record) error {
	deadline, _ := ctx.Deadline()
	s.remaining = time.Until(deadline)
	return s.recordingSink.Write(ctx, records)
}

func TestRunnerCircuitRecoveryKeepsFreshCommitDeadline(t *testing.T) {
	store := newMemoryCPStore()
	r := newCheckpointWriteBatchRunner(t, nil, store, noopDLQ{})
	r.retryConfig.MaxAttempts = 1
	sink := &recoveryDeadlineSink{}
	r.sink = sink
	r.circuitBreaker = NewCircuitBreaker(CircuitBreakerCfg{}, nil, "recovery")
	// The default cooldown is 30s, equal to the batch commit budget. Resume
	// near its end so the test also detects restarting a full cooldown wait.
	r.circuitBreaker.tripLocked(time.Now().Add(-30*time.Second + 50*time.Millisecond))
	r.writeBatch(context.Background(), checkpointTestBatch())
	if sink.remaining < batchCommitTimeout-500*time.Millisecond {
		t.Fatalf("recovered sink got only %s of commit budget", sink.remaining)
	}
	if r.circuitBreaker.StateCode() != 0 {
		t.Fatal("successful probe did not close circuit")
	}
	if !checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("successful probe did not persist checkpoint")
	}
}

func TestRunnerCancelDuringCircuitCooldownDoesNotAdvanceCheckpoint(t *testing.T) {
	store := newMemoryCPStore()
	r := newCheckpointWriteBatchRunner(t, nil, store, noopDLQ{})
	r.retryConfig.MaxAttempts = 1
	sink := &recordingSink{}
	r.sink = sink
	r.circuitBreaker = NewCircuitBreaker(CircuitBreakerCfg{}, nil, "stop")
	r.circuitBreaker.tripLocked(time.Now())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.writeBatch(ctx, checkpointTestBatch())
	if sink.calls != 0 || checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("cancelled cooldown wrote or checkpointed unreadmitted batch")
	}
}

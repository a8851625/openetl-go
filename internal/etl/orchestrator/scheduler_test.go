package orchestrator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// schedulerTestRunner is deliberately inert: these tests exercise scheduler
// registration state, not source/sink execution.
type schedulerTestRunner struct {
	starts   atomic.Int64
	startErr error
}

func (r *schedulerTestRunner) Start(context.Context) error {
	if r.startErr != nil {
		return r.startErr
	}
	r.starts.Add(1)
	return nil
}
func (*schedulerTestRunner) Stop() error                  { return nil }
func (*schedulerTestRunner) Pause() error                 { return nil }
func (*schedulerTestRunner) Resume(context.Context) error { return nil }
func (*schedulerTestRunner) Wait()                        {}
func (*schedulerTestRunner) Done() <-chan struct{}        { return make(chan struct{}) }
func (*schedulerTestRunner) Status() pipeline.Status      { return pipeline.StatusStopped }
func (*schedulerTestRunner) Stats() pipeline.Stats        { return pipeline.Stats{} }
func (*schedulerTestRunner) Duration() time.Duration      { return 0 }
func (*schedulerTestRunner) MetricsSnapshot() pipeline.MetricsSnapshot {
	return pipeline.MetricsSnapshot{}
}
func (*schedulerTestRunner) LogBuffer() *pipeline.LogBuffer { return pipeline.NewLogBuffer(1) }
func (*schedulerTestRunner) Shards() []pipeline.ShardInfo   { return nil }
func (*schedulerTestRunner) IncrementDLQReplay(int64)       {}
func (*schedulerTestRunner) IncrementDLQDelete(int64)       {}
func (*schedulerTestRunner) CircuitBreakerState() int       { return 0 }
func (*schedulerTestRunner) SinkMetrics() []core.SinkMetrics {
	return nil
}
func (*schedulerTestRunner) StateMetrics() []core.StateMetrics {
	return nil
}
func (*schedulerTestRunner) TransformMetrics() []core.TransformMetrics {
	return nil
}

func TestRegisterExecutorFailureLeavesNoSchedulerState(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ScheduleConfig
	}{
		{name: "cron", cfg: &ScheduleConfig{Type: ScheduleCron, Cron: "not a cron"}},
		{name: "periodic", cfg: &ScheduleConfig{Type: SchedulePeriodic, IntervalS: 0}},
		{name: "dependency", cfg: &ScheduleConfig{Type: ScheduleDependency}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScheduler(nil)
			runner := &schedulerTestRunner{}
			if err := s.RegisterExecutor("broken", runner, tt.cfg); err == nil {
				t.Fatal("RegisterExecutor succeeded for invalid schedule")
			}
			if len(s.runners) != 0 || len(s.schedules) != 0 {
				t.Fatalf("failed registration polluted scheduler: runners=%d schedules=%d", len(s.runners), len(s.schedules))
			}
		})
	}
}

func TestValidateScheduleConfigAcceptsFiveAndSixFieldCron(t *testing.T) {
	for _, expression := range []string{"0 0 * * *", "0 0 0 * * *"} {
		if err := ValidateScheduleConfig("pipe", &ScheduleConfig{Type: ScheduleCron, Cron: expression}); err != nil {
			t.Fatalf("ValidateScheduleConfig(%q): %v", expression, err)
		}
	}
}

func TestImmediateRegisterFailureLeavesNoRunnerEntry(t *testing.T) {
	s := NewScheduler(nil)
	s.SetContext(context.Background())
	runner := &schedulerTestRunner{startErr: errors.New("injected start failure")}
	if err := s.RegisterExecutor("once", runner, nil); err == nil {
		t.Fatal("RegisterExecutor succeeded despite runner start failure")
	}
	if len(s.runners) != 0 || len(s.schedules) != 0 {
		t.Fatalf("failed immediate registration polluted scheduler: runners=%d schedules=%d", len(s.runners), len(s.schedules))
	}
}

func TestTriggerPipelineSkipsRunnerStillStopping(t *testing.T) {
	s := NewScheduler(nil)
	s.SetContext(context.Background())
	runner := &schedulerTestRunner{startErr: pipeline.ErrRunnerStopping}

	s.triggerPipeline("still-stopping", runner)
	if got := runner.starts.Load(); got != 0 {
		t.Fatalf("successful starts = %d, want 0", got)
	}
}

func TestPrepareExecutorInjectedFailureLeavesPreviousExecutor(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ScheduleConfig
	}{
		{name: "cron", cfg: &ScheduleConfig{Type: ScheduleCron, Cron: "0 0 * * *"}},
		{name: "periodic", cfg: &ScheduleConfig{Type: SchedulePeriodic, IntervalS: 60}},
		{name: "dependency", cfg: &ScheduleConfig{Type: ScheduleDependency, DependsOn: []string{"upstream"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScheduler(nil)
			s.SetContext(context.Background())
			oldRunner := &schedulerTestRunner{}
			if err := s.RegisterExecutor("pipe", oldRunner, tt.cfg); err != nil {
				t.Fatalf("register old executor: %v", err)
			}
			t.Cleanup(s.StopAll)

			s.SetRegisterFailureInjector(func(string, *ScheduleConfig) error {
				return context.Canceled
			})
			newRunner := &schedulerTestRunner{}
			if replacement, err := s.PrepareExecutor("pipe", newRunner, tt.cfg); err == nil {
				t.Fatal("PrepareExecutor succeeded despite injected failure")
			} else if replacement != nil {
				t.Fatal("failed preparation returned a replacement")
			}
			if got := s.runners["pipe"]; got != oldRunner {
				t.Fatalf("old runner was not preserved: got=%p want=%p", got, oldRunner)
			}
			if _, ok := s.schedules["pipe"]; !ok {
				t.Fatal("old schedule was removed after injected failure")
			}
		})
	}
}

func TestPrepareExecutorKeepsOldExecutorLiveUntilCommit(t *testing.T) {
	s := NewScheduler(nil)
	s.SetContext(context.Background())
	t.Cleanup(s.StopAll)

	oldRunner := &schedulerTestRunner{}
	oldCfg := &ScheduleConfig{Type: ScheduleDependency, DependsOn: []string{"old-upstream"}}
	if err := s.RegisterExecutor("pipe", oldRunner, oldCfg); err != nil {
		t.Fatalf("register old executor: %v", err)
	}
	newRunner := &schedulerTestRunner{}
	replacement, err := s.PrepareExecutor("pipe", newRunner, &ScheduleConfig{
		Type: SchedulePeriodic, IntervalS: 60,
	})
	if err != nil {
		t.Fatalf("prepare executor: %v", err)
	}
	if got := s.runners["pipe"]; got != oldRunner {
		t.Fatalf("prepare changed live runner: got=%p want=%p", got, oldRunner)
	}
	if got := s.schedules["pipe"].Config.Type; got != ScheduleDependency {
		t.Fatalf("prepare changed live schedule: got=%q want=%q", got, ScheduleDependency)
	}

	if returnedOld := replacement.Commit(); returnedOld != oldRunner {
		t.Fatalf("commit returned old runner=%p, want=%p", returnedOld, oldRunner)
	}
	if got := s.runners["pipe"]; got != newRunner {
		t.Fatalf("commit did not install candidate runner: got=%p want=%p", got, newRunner)
	}
	if got := s.schedules["pipe"].Config.Type; got != SchedulePeriodic {
		t.Fatalf("commit schedule type=%q, want=%q", got, SchedulePeriodic)
	}
}

func TestExecutorReplacementRollbackRestoresDeferredExecutor(t *testing.T) {
	s := NewScheduler(nil)
	s.SetContext(context.Background())
	t.Cleanup(s.StopAll)

	oldRunner := &schedulerTestRunner{}
	oldCfg := &ScheduleConfig{Type: SchedulePeriodic, IntervalS: 60}
	if err := s.RegisterExecutor("pipe", oldRunner, oldCfg); err != nil {
		t.Fatalf("register old executor: %v", err)
	}
	newRunner := &schedulerTestRunner{}
	newCfg := &ScheduleConfig{Type: ScheduleDependency, DependsOn: []string{"upstream"}}
	replacement, err := s.PrepareExecutor("pipe", newRunner, newCfg)
	if err != nil {
		t.Fatalf("prepare executor: %v", err)
	}
	returnedOld := replacement.Commit()
	if returnedOld != oldRunner {
		t.Fatalf("returned old runner = %p, want %p", returnedOld, oldRunner)
	}
	if got := s.runners["pipe"]; got != newRunner {
		t.Fatalf("new runner not staged: got=%p want=%p", got, newRunner)
	}
	replacement.Rollback()
	if got := s.runners["pipe"]; got != oldRunner {
		t.Fatalf("rollback runner = %p, want %p", got, oldRunner)
	}
	if got := s.schedules["pipe"].Config.Type; got != SchedulePeriodic {
		t.Fatalf("rollback schedule type = %q, want %q", got, SchedulePeriodic)
	}
}

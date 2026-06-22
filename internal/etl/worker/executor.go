package worker

import (
	"context"
	"fmt"

	"github.com/gogf/gf/v2/frame/g"
	"gopkg.in/yaml.v3"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// ExecutorDeps bundles the dependencies a distributed worker needs to rebuild
// and run a single shard's Runner from the pipeline spec stored in shared
// storage. The worker binary (A11-redo Inc 6) constructs this once at startup.
type ExecutorDeps struct {
	Store     storage.Storage
	CPAdapter core.CheckpointStore
	DLQWriter pipeline.DLQWriter
	AlertMgr  *alert.Manager
}

// ExecuteShard is the real distributed-worker task executor (A11-redo). Given a
// claimed task carrying ShardIndex/ShardTotal, it:
//  1. fetches the pipeline spec from shared storage (GetPipeline);
//  2. builds the single-shard Runner via pipeline.BuildShardRunner — which
//     produces sharding + a checkpoint key IDENTICAL to the inline
//     ParallelRunner path, so a reassigned shard resumes correctly;
//  3. runs it to natural completion (batch) or until ctx cancel (continuous/CDC).
//
// It returns nil only on natural completion (runner reaches StatusCompleted or
// StatusStopped while ctx is still alive). On worker shutdown (ctx cancelled)
// the runner stops with StatusStopped and this returns ctx.Err(); the caller
// (PollLoop) MUST NOT mark the task completed in that case — it leaves the task
// "running" so master.ReassignStaleTasks re-queues it after deregistration.
func ExecuteShard(ctx context.Context, deps ExecutorDeps, task *storage.TaskAssignment) error {
	row, err := deps.Store.GetPipeline(ctx, task.Pipeline)
	if err != nil {
		return fmt.Errorf("load pipeline %s: %w", task.Pipeline, err)
	}
	var spec pipeline.Spec
	if err := yaml.Unmarshal([]byte(row.SpecYAML), &spec); err != nil {
		return fmt.Errorf("unmarshal spec %s: %w", task.Pipeline, err)
	}
	pipeline.ApplyDefaults(&spec)

	runner, err := pipeline.BuildShardRunner(&spec, deps.CPAdapter, deps.DLQWriter, deps.AlertMgr, task.ShardIndex, task.ShardTotal)
	if err != nil {
		return fmt.Errorf("build shard %d/%d for %s: %w", task.ShardIndex, task.ShardTotal, task.Pipeline, err)
	}
	if err := runner.Start(ctx); err != nil {
		return fmt.Errorf("start shard: %w", err)
	}
	runner.Wait()

	// ctx cancelled => the worker is shutting down mid-shard (typical for a
	// continuous/CDC shard, or a batch shard caught at shutdown). Surface it so
	// PollLoop leaves the task reassignable rather than falsely "completed".
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if runner.Status() == pipeline.StatusFailed {
		return fmt.Errorf("shard %s ended failed", task.TaskID)
	}
	g.Log().Infof(ctx, "Worker shard %s completed (status=%s, read=%d written=%d)",
		task.TaskID, runner.Status(), runner.Stats().RecordsRead, runner.Stats().RecordsWritten)
	return nil
}

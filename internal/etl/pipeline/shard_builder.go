package pipeline

import (
	"fmt"

	"openetl-go/internal/etl/alert"
	"openetl-go/internal/etl/core"
)

// BuildShardRunner constructs a single unstarted *Runner for shard `idx` of
// `total` against the given pipeline spec. It is the shared primitive behind:
//
//   - ParallelRunner (standalone mode): builds all N shard runners inline; and
//   - a distributed worker (A11-redo): builds exactly the one shard it was
//     assigned, producing a checkpoint key identical to the inline path so a
//     reassigned shard resumes from the right position.
//
// Correctness rules (A11-redo design review):
//   - spec.Parallelism is NOT nilled. It is copied and its Count forced to
//     `total` so the framework-level NewShardedReader decorator applies for
//     non-native sources (file/redis/...) in pipeline.go. Nilling Parallelism
//     would make every worker read the FULL stream.
//   - The checkpoint store is the shard-scoped "{spec.Name}.shard-{idx}"
//     namespace (NewShardCheckpointStore), matching ParallelRunner's inline
//     instances exactly.
func BuildShardRunner(spec *Spec, cpStore core.CheckpointStore, dlqW DLQWriter, am *alert.Manager, idx, total int) (*Runner, error) {
	if total < 1 {
		total = 1
	}
	if idx < 0 || idx >= total {
		return nil, fmt.Errorf("invalid shard index %d (total %d)", idx, total)
	}

	strategy := ""
	if spec.Parallelism != nil {
		strategy = spec.Parallelism.ShardStrategy
	}

	shardSpec := *spec // shallow copy of value fields

	// Copy Parallelism (don't mutate the caller's spec) and force Count=total.
	// See the rule above: never nil it.
	if spec.Parallelism != nil {
		pc := *spec.Parallelism
		shardSpec.Parallelism = &pc
	} else {
		shardSpec.Parallelism = &ParallelismConfig{}
	}
	shardSpec.Parallelism.Count = total

	// Deep-copy all config maps to prevent cross-shard / cross-worker mutation.
	shardSpec.Source.Config = InjectShardConfig(spec.Source.Config, idx, total, strategy)
	shardSpec.Sink.Config = cloneConfig(spec.Sink.Config)
	shardSpec.Transforms = make([]TransformSpec, len(spec.Transforms))
	for j, tf := range spec.Transforms {
		shardSpec.Transforms[j] = TransformSpec{
			Type:   tf.Type,
			Config: cloneConfig(tf.Config),
		}
	}

	shardCPStore := NewShardCheckpointStore(cpStore, spec.Name, idx)
	runner, err := NewRunner(&shardSpec, shardCPStore, dlqW, am)
	if err != nil {
		return nil, fmt.Errorf("shard-%d/%d: %w", idx, total, err)
	}
	return runner, nil
}

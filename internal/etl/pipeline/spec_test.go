package pipeline

import (
	"os"
	"strings"
	"testing"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestExpandEnvSupportsDefaultsAndMissing(t *testing.T) {
	t.Setenv("ETL_TEST_PASSWORD", "secret")
	out, err := ExpandEnv("password: ${ETL_TEST_PASSWORD}\nhost: ${ETL_TEST_HOST:-localhost}")
	if err != nil {
		t.Fatalf("ExpandEnv() error = %v", err)
	}
	if !strings.Contains(out, "password: secret") || !strings.Contains(out, "host: localhost") {
		t.Fatalf("ExpandEnv() = %q", out)
	}

	_, err = ExpandEnv("password: ${ETL_TEST_MISSING}")
	if err == nil || !strings.Contains(err.Error(), "ETL_TEST_MISSING") {
		t.Fatalf("ExpandEnv() missing err = %v", err)
	}
}

func TestValidateSpecRejectsUnknownPlugins(t *testing.T) {
	spec := &Spec{
		Name:                  "bad",
		Source:                SourceSpec{Type: "missing_source"},
		Sink:                  SinkSpec{Type: "file_sink"},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		BackpressureBuffer:    1,
		Retry:                 &RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
	}
	if err := ValidateSpec(spec); err == nil || !strings.Contains(err.Error(), "unknown source.type") {
		t.Fatalf("ValidateSpec() error = %v", err)
	}
}

func TestApplyDefaultsSetsSourceDefaultSchedule(t *testing.T) {
	spec := &Spec{
		Name:   "cdc-default",
		Source: SourceSpec{Type: "mysql_cdc"},
		Sink:   SinkSpec{Type: "mysql"},
	}
	ApplyDefaults(spec)
	if spec.Schedule == nil || spec.Schedule.Type != ScheduleStreaming {
		t.Fatalf("schedule = %#v, want streaming default", spec.Schedule)
	}

	spec = &Spec{
		Name:   "batch-default",
		Source: SourceSpec{Type: "mysql_batch"},
		Sink:   SinkSpec{Type: "mysql"},
	}
	ApplyDefaults(spec)
	if spec.Schedule == nil || spec.Schedule.Type != ScheduleOnce {
		t.Fatalf("schedule = %#v, want once default", spec.Schedule)
	}
}

func TestValidateSpecRejectsUnsupportedSourceSchedule(t *testing.T) {
	spec := &Spec{
		Name:                  "bad-schedule",
		Source:                SourceSpec{Type: "mysql_cdc"},
		Sink:                  SinkSpec{Type: "mysql"},
		Schedule:              &ScheduleConfig{Type: ScheduleCron, Cron: "0 * * * * *"},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		BackpressureBuffer:    1,
		Retry:                 &RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
	}
	if err := ValidateSpec(spec); err == nil || !strings.Contains(err.Error(), "does not support schedule.type") {
		t.Fatalf("ValidateSpec() error = %v, want unsupported schedule", err)
	}
}

func TestValidateSpecChecksScheduleRequiredFields(t *testing.T) {
	base := func(schedule *ScheduleConfig) *Spec {
		return &Spec{
			Name:                  "schedule-required",
			Source:                SourceSpec{Type: "mysql_batch"},
			Sink:                  SinkSpec{Type: "mysql"},
			Schedule:              schedule,
			BatchSize:             1,
			CheckpointIntervalSec: 1,
			BackpressureBuffer:    1,
			Retry:                 &RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
		}
	}
	cases := []struct {
		name     string
		schedule *ScheduleConfig
		want     string
	}{
		{"cron", &ScheduleConfig{Type: ScheduleCron}, "schedule.cron"},
		{"periodic", &ScheduleConfig{Type: SchedulePeriodic}, "schedule.interval_sec"},
		{"dependency", &ScheduleConfig{Type: ScheduleDependency}, "schedule.depends_on"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSpec(base(tc.schedule))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateSpec() error = %v, want %s", err, tc.want)
			}
		})
	}
}

func TestValidateSpecRejectsSQLStateBackendsForRuntimeState(t *testing.T) {
	spec := validStateSpec()
	spec.Transforms = []TransformSpec{{
		Type: "deduplicate",
		Config: map[string]any{
			"keys":          []any{"id"},
			"state_backend": "sqlite",
		},
	}}
	err := ValidateSpec(spec)
	if err == nil || !strings.Contains(err.Error(), "state_backend=\"sqlite\" is not allowed") {
		t.Fatalf("ValidateSpec() error = %v, want sqlite state backend rejection", err)
	}
}

func TestValidateSpecRejectsRedisStateWithoutRedisConfig(t *testing.T) {
	t.Setenv("ETL_STATE_REDIS_ADDR", "")
	spec := validStateSpec()
	spec.Transforms = []TransformSpec{{
		Type: "deduplicate",
		Config: map[string]any{
			"keys":          []any{"id"},
			"state_backend": "redis",
		},
	}}
	err := ValidateSpec(spec)
	if err == nil || !strings.Contains(err.Error(), "requires etl.state.redis.addr") {
		t.Fatalf("ValidateSpec() error = %v, want Redis config rejection", err)
	}
}

func TestValidateSpecAllowsRedisStateWithLocalRedisConfig(t *testing.T) {
	spec := validStateSpec()
	spec.Transforms = []TransformSpec{{
		Type: "deduplicate",
		Config: map[string]any{
			"keys":             []any{"id"},
			"state_backend":    "redis",
			"state_redis_addr": "redis:6379",
		},
	}}
	if err := ValidateSpec(spec); err != nil {
		t.Fatalf("ValidateSpec() error = %v", err)
	}
}

func TestValidateSpecRejectsEnricherCacheWithoutRedisConfig(t *testing.T) {
	t.Setenv("ETL_STATE_REDIS_ADDR", "")
	spec := validStateSpec()
	spec.Transforms = []TransformSpec{{
		Type: "enricher",
		Config: map[string]any{
			"mode":              "http",
			"url":               "http://example.test/{{.id}}",
			"cache_ttl_seconds": 60,
		},
	}}
	err := ValidateSpec(spec)
	if err == nil || !strings.Contains(err.Error(), "cache_ttl_seconds requires Redis") {
		t.Fatalf("ValidateSpec() error = %v, want Redis cache rejection", err)
	}
}

func TestValidateSpecRejectsSlidingWindowAsUnsupportedSpec(t *testing.T) {
	spec := validStateSpec()
	spec.Transforms = []TransformSpec{{
		Type: "window",
		Config: map[string]any{
			"window_type":         "sliding",
			"state_redis_addr":    "redis:6379",
			"aggregates":          map[string]any{"count": map[string]any{"func": "count"}},
			"window_size_seconds": 60,
		},
	}}
	err := ValidateSpec(spec)
	if err == nil || !strings.Contains(err.Error(), "only tumbling window is part of the production pipeline spec") {
		t.Fatalf("ValidateSpec() error = %v, want sliding window unsupported rejection", err)
	}
}

func TestValidateSpecRejectsArbitraryKeyedStateAndTimerFields(t *testing.T) {
	spec := validStateSpec()
	spec.Transforms = []TransformSpec{{
		Type: "lua",
		Config: map[string]any{
			"script":           "return record",
			"keyed_state":      true,
			"uses_timer":       true,
			"state_backend":    "redis",
			"state_redis_addr": "redis:6379",
		},
	}}
	err := ValidateSpec(spec)
	if err == nil {
		t.Fatal("ValidateSpec() error = nil, want arbitrary state/timer rejection")
	}
	for _, want := range []string{
		"state_backend is only supported by built-in lookup, join, window, and deduplicate",
		"keyed_state is not part of the pipeline spec",
		"uses_timer is not part of the pipeline spec",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ValidateSpec() error = %v, want %q", err, want)
		}
	}
}

func validStateSpec() *Spec {
	return &Spec{
		Name:                  "state-guard",
		Source:                SourceSpec{Type: "file", Config: map[string]any{"path": "/tmp/in.jsonl", "format": "json"}},
		Sink:                  SinkSpec{Type: "mysql", Config: map[string]any{}},
		Schedule:              &ScheduleConfig{Type: ScheduleOnce},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		BackpressureBuffer:    1,
		Retry:                 &RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
	}
}

func TestLoadSpecAppliesDefaultsAndExpandsEnv(t *testing.T) {
	t.Setenv("ETL_TEST_FILE", "/tmp/input.jsonl")
	file, err := os.CreateTemp(t.TempDir(), "spec-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`name: file-test
source:
  type: file
  config:
    path: ${ETL_TEST_FILE}
    format: json
sink:
  type: file_sink
  config:
    output_dir: /tmp/out
`)
	_ = file.Close()

	spec, err := LoadSpec(file.Name())
	if err != nil {
		t.Fatalf("LoadSpec() error = %v", err)
	}
	if spec.BatchSize != 1000 || spec.CheckpointIntervalSec != 30 || spec.BackpressureBuffer != 100 {
		t.Fatalf("defaults not applied: %#v", spec)
	}
	if spec.Source.Config["path"] != "/tmp/input.jsonl" {
		t.Fatalf("env not expanded: %#v", spec.Source.Config)
	}
}

func TestValidateParallelismKafkaWarning(t *testing.T) {
	// Kafka source with parallelism.count > 1 must warn about the partition
	// vs. count relationship and that shard_strategy is ignored.
	spec := &Spec{
		Name:   "kafka-par",
		Source: SourceSpec{Type: "kafka"},
		Sink:   SinkSpec{Type: "file_sink"},
		Parallelism: &ParallelismConfig{
			Count:         4,
			ShardStrategy: "round_robin",
		},
	}
	warnings := ValidateParallelism(spec)
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	joined := strings.Join(warnings, "; ")
	for _, want := range []string{"kafka", "group_id", "partition", "shard_strategy is ignored"} {
		if !strings.Contains(joined, want) {
			t.Errorf("warning missing %q: %s", want, joined)
		}
	}
}

func TestValidateParallelismNoWarningForSingleShard(t *testing.T) {
	// count == 1 (or nil Parallelism) must not emit the kafka warning.
	cases := []*Spec{
		{Name: "a", Source: SourceSpec{Type: "kafka"}, Sink: SinkSpec{Type: "file_sink"}, Parallelism: nil},
		{Name: "b", Source: SourceSpec{Type: "kafka"}, Sink: SinkSpec{Type: "file_sink"}, Parallelism: &ParallelismConfig{Count: 1}},
	}
	for _, spec := range cases {
		if got := ValidateParallelism(spec); len(got) != 0 {
			t.Errorf("spec %s: expected no warnings, got %v", spec.Name, got)
		}
	}
}

func TestValidateParallelismNoWarningForNonKafka(t *testing.T) {
	// mysql_batch with count=4 should not emit the kafka-specific warning.
	spec := &Spec{
		Name:   "mysql-par",
		Source: SourceSpec{Type: "mysql_batch"},
		Sink:   SinkSpec{Type: "file_sink"},
		Parallelism: &ParallelismConfig{
			Count:         4,
			ShardStrategy: "id_range",
		},
	}
	if got := ValidateParallelism(spec); len(got) != 0 {
		t.Errorf("expected no warnings for mysql_batch, got %v", got)
	}
}

func TestParallelismDefaultsMapLegacyFields(t *testing.T) {
	cfg := &ParallelismConfig{Count: 4, ShardStrategy: "id_range", ShardKey: "id"}
	cfg.ApplyDefaults()
	if got := cfg.LogicalShardCount(); got != 4 {
		t.Fatalf("LogicalShardCount() = %d, want 4", got)
	}
	if got := cfg.MaxActiveShardCount(); got != 4 {
		t.Fatalf("MaxActiveShardCount() = %d, want 4", got)
	}
	if cfg.Sharding == nil || cfg.Sharding.Strategy != "id_range" || cfg.Sharding.Key != "id" {
		t.Fatalf("legacy sharding not mapped: %#v", cfg.Sharding)
	}
	if cfg.Execution == nil || cfg.Execution.MaxActiveShards != 4 {
		t.Fatalf("legacy execution not mapped: %#v", cfg.Execution)
	}
}

func TestParallelismDefaultsPreferStructuredFields(t *testing.T) {
	cfg := &ParallelismConfig{
		Count: 2,
		Sharding: &ShardingConfig{
			Strategy:      "pk_mod",
			Key:           "order_id",
			LogicalShards: 16,
		},
		Execution: &ParallelExecutionConfig{MaxActiveShards: 4},
	}
	cfg.ApplyDefaults()
	if got := cfg.LogicalShardCount(); got != 16 {
		t.Fatalf("LogicalShardCount() = %d, want 16", got)
	}
	if got := cfg.MaxActiveShardCount(); got != 4 {
		t.Fatalf("MaxActiveShardCount() = %d, want 4", got)
	}
	if cfg.Count != 4 || cfg.ShardTotal != 16 || cfg.ShardStrategy != "pk_mod" || cfg.ShardKey != "order_id" {
		t.Fatalf("legacy fields not backfilled: %#v", cfg)
	}
}

func TestParallelismShardRangeUsesLogicalShardOwnership(t *testing.T) {
	cfg := &ParallelismConfig{
		Sharding:  &ShardingConfig{LogicalShards: 16},
		Execution: &ParallelExecutionConfig{MaxActiveShards: 4},
	}
	start, end := cfg.ShardRange(7)
	if start != 7 || end != 8 {
		t.Fatalf("ShardRange(7) = [%d,%d), want stable logical range [7,8)", start, end)
	}
	cfg.Execution.MaxActiveShards = 8
	start, end = cfg.ShardRange(7)
	if start != 7 || end != 8 {
		t.Fatalf("ShardRange changed after max_active_shards update: [%d,%d), want [7,8)", start, end)
	}
	start, end = cfg.ShardRange(16)
	if start != 0 || end != 0 {
		t.Fatalf("ShardRange out-of-bounds = [%d,%d), want [0,0)", start, end)
	}
}

func TestValidateParallelismWarnsForReservedSourceConcurrency(t *testing.T) {
	spec := &Spec{
		Name:   "reserved-execution",
		Source: SourceSpec{Type: "mysql_batch"},
		Sink:   SinkSpec{Type: "mysql"},
		Parallelism: &ParallelismConfig{
			Execution: &ParallelExecutionConfig{
				SourceConcurrency: 2,
			},
		},
	}
	got := strings.Join(ValidateParallelism(spec), "; ")
	if !strings.Contains(got, "source_concurrency is reserved") {
		t.Fatalf("ValidateParallelism() = %q, want source_concurrency warning", got)
	}
}

func TestValidateParallelismSourceWarnings(t *testing.T) {
	cases := []struct {
		name string
		spec *Spec
		want string
	}{
		{
			name: "postgres cdc unsupported",
			spec: &Spec{Name: "pg", Source: SourceSpec{Type: "postgres_cdc"}, Sink: SinkSpec{Type: "mysql"}, Parallelism: &ParallelismConfig{Count: 2}},
			want: "does not implement sharding",
		},
		{
			name: "mysql cdc single table",
			spec: &Spec{Name: "mysql-cdc", Source: SourceSpec{Type: "mysql_cdc", Config: map[string]any{"tables": []any{"orders"}}}, Sink: SinkSpec{Type: "mysql"}, Parallelism: &ParallelismConfig{Count: 2}},
			want: "only shards by table list",
		},
		{
			name: "redis round robin",
			spec: &Spec{Name: "redis", Source: SourceSpec{Type: "redis"}, Sink: SinkSpec{Type: "mysql"}, Parallelism: &ParallelismConfig{Count: 2}},
			want: "hash_modulo",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(ValidateParallelism(tc.spec), "; ")
			if !strings.Contains(got, tc.want) {
				t.Fatalf("ValidateParallelism() = %q, want substring %q", got, tc.want)
			}
		})
	}
}

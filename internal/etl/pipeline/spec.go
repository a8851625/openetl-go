package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/state"

	"gopkg.in/yaml.v3"
)

type ScheduleConfig struct {
	Type        string   `yaml:"type" json:"type"`
	Cron        string   `yaml:"cron,omitempty" json:"cron,omitempty"`
	IntervalSec int      `yaml:"interval_sec,omitempty" json:"interval_sec,omitempty"`
	DependsOn   []string `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
}

type SourceSpec struct {
	Type          string         `yaml:"type" json:"type"`
	Connection    string         `yaml:"connection,omitempty" json:"connection,omitempty"`
	ConnectionRef string         `yaml:"connection_ref,omitempty" json:"connection_ref,omitempty"`
	Config        map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

type SinkSpec struct {
	Type          string         `yaml:"type" json:"type"`
	Connection    string         `yaml:"connection,omitempty" json:"connection,omitempty"`
	ConnectionRef string         `yaml:"connection_ref,omitempty" json:"connection_ref,omitempty"`
	Config        map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

type TransformSpec struct {
	Type          string         `yaml:"type" json:"type"`
	Connection    string         `yaml:"connection,omitempty" json:"connection,omitempty"`
	ConnectionRef string         `yaml:"connection_ref,omitempty" json:"connection_ref,omitempty"`
	Config        map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

type RetrySpec struct {
	MaxAttempts       int `yaml:"max_attempts" json:"max_attempts"`
	InitialIntervalMs int `yaml:"initial_interval_ms" json:"initial_interval_ms"`
	MaxIntervalMs     int `yaml:"max_interval_ms" json:"max_interval_ms"`
}

type DLQSpec struct {
	Enable bool     `yaml:"enable" json:"enable"`
	Sink   SinkSpec `yaml:"sink,omitempty" json:"sink,omitempty"`
}

type Spec struct {
	Name                  string             `yaml:"name" json:"name"`
	Source                SourceSpec         `yaml:"source" json:"source"`
	Transforms            []TransformSpec    `yaml:"transforms,omitempty" json:"transforms,omitempty"`
	Sink                  SinkSpec           `yaml:"sink" json:"sink"`
	Schedule              *ScheduleConfig    `yaml:"schedule,omitempty" json:"schedule,omitempty"`
	Retry                 *RetrySpec         `yaml:"retry,omitempty" json:"retry,omitempty"`
	DLQ                   *DLQSpec           `yaml:"dlq,omitempty" json:"dlq,omitempty"`
	BatchSize             int                `yaml:"batch_size" json:"batch_size"`
	FlushIntervalMs       int                `yaml:"flush_interval_ms,omitempty" json:"flush_interval_ms,omitempty"`
	CheckpointIntervalSec int                `yaml:"checkpoint_interval_sec" json:"checkpoint_interval_sec"`
	BackpressureBuffer    int                `yaml:"backpressure_buffer" json:"backpressure_buffer"`
	Tags                  []string           `yaml:"tags,omitempty" json:"tags,omitempty"`
	WorkerSelector        *WorkerSelector    `yaml:"worker_selector,omitempty" json:"worker_selector,omitempty"`
	TableMapping          *TableMapping      `yaml:"table_mapping,omitempty" json:"table_mapping,omitempty"`
	Parallelism           *ParallelismConfig `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
	Hooks                 *HooksSpec         `yaml:"hooks,omitempty" json:"hooks,omitempty"`
	RestartPolicy         *RestartPolicy     `yaml:"restart_policy,omitempty" json:"restart_policy,omitempty"`
	AlertRules            *AlertRules        `yaml:"alert_rules,omitempty" json:"alert_rules,omitempty"`
	CircuitBreaker        *CircuitBreakerCfg `yaml:"circuit_breaker,omitempty" json:"circuit_breaker,omitempty"`
}

// HooksSpec defines lifecycle hooks for a pipeline.
// Each hook is keyed by its lifecycle point (on_init, on_pre_batch, etc.)
type HooksSpec struct {
	OnInit       *HookSpec `yaml:"on_init,omitempty" json:"on_init,omitempty"`
	OnPreBatch   *HookSpec `yaml:"on_pre_batch,omitempty" json:"on_pre_batch,omitempty"`
	OnPostBatch  *HookSpec `yaml:"on_post_batch,omitempty" json:"on_post_batch,omitempty"`
	OnError      *HookSpec `yaml:"on_error,omitempty" json:"on_error,omitempty"`
	OnCheckpoint *HookSpec `yaml:"on_checkpoint,omitempty" json:"on_checkpoint,omitempty"`
	OnShutdown   *HookSpec `yaml:"on_shutdown,omitempty" json:"on_shutdown,omitempty"`
}

// HookSpec defines a single lifecycle hook implementation.
type HookSpec struct {
	// Type: "lua" | "webhook" | "plugin"
	Type string `yaml:"type" json:"type"`
	// Code: inline script (for type=lua)
	Code string `yaml:"code,omitempty" json:"code,omitempty"`
	// Name: plugin name (for type=plugin) or webhook name
	Name string `yaml:"name,omitempty" json:"name,omitempty"`
	// Config: arbitrary key-value config passed to the hook
	Config map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

// RestartPolicy controls automatic pipeline restart on failure.
type RestartPolicy struct {
	Mode              string  `yaml:"mode" json:"mode"` // never | on-failure | always
	MaxRestarts       int     `yaml:"max_restarts" json:"max_restarts"`
	InitialDelayMs    int     `yaml:"initial_delay_ms" json:"initial_delay_ms"`
	MaxDelayMs        int     `yaml:"max_delay_ms" json:"max_delay_ms"`
	BackoffMultiplier float64 `yaml:"backoff_multiplier" json:"backoff_multiplier"`
}

// AlertRules defines threshold-based alerting for a pipeline.
// When any rule is triggered, an alert event is fired.
type AlertRules struct {
	LagSecondsGt        int     `yaml:"lag_seconds_gt,omitempty" json:"lag_seconds_gt,omitempty"`
	ErrorRateGt         float64 `yaml:"error_rate_gt,omitempty" json:"error_rate_gt,omitempty"`
	NoRecordsForMinutes int     `yaml:"no_records_for_minutes,omitempty" json:"no_records_for_minutes,omitempty"`
	CheckIntervalSec    int     `yaml:"check_interval_sec,omitempty" json:"check_interval_sec,omitempty"`
}

// CircuitBreakerCfg configures the circuit breaker for sink writes.
// After MaxFailures consecutive failures within WindowSec, the breaker
// trips and pauses the source to prevent unbounded DLQ growth.
type CircuitBreakerCfg struct {
	MaxFailures int `yaml:"max_failures" json:"max_failures"`
	WindowSec   int `yaml:"window_sec" json:"window_sec"`
	CooldownSec int `yaml:"cooldown_sec" json:"cooldown_sec"`
}

// getConfig returns the Config map for the given hook kind, or nil.
func (h *HooksSpec) getConfig(kind core.HookKind) map[string]any {
	if h == nil {
		return nil
	}
	var hs *HookSpec
	switch kind {
	case core.HookOnInit:
		hs = h.OnInit
	case core.HookOnPreBatch:
		hs = h.OnPreBatch
	case core.HookOnPostBatch:
		hs = h.OnPostBatch
	case core.HookOnError:
		hs = h.OnError
	case core.HookOnCheckpoint:
		hs = h.OnCheckpoint
	case core.HookOnShutdown:
		hs = h.OnShutdown
	}
	if hs == nil {
		return nil
	}
	return hs.Config
}

// WorkerSelector controls which workers can execute this pipeline.
// If empty, any worker (default pool) can run it.
type WorkerSelector struct {
	MatchLabels map[string]string `yaml:"match_labels,omitempty" json:"match_labels,omitempty"`
}

// TableMapping defines source-to-target table name mapping rules.
// Supports glob patterns like "order_*" -> "orders" or regex replacement.
type TableMapping struct {
	// Rules: source_pattern -> target_table
	Rules map[string]string `yaml:"rules,omitempty" json:"rules,omitempty"`
	// RegexPatterns: [{"pattern": "...", "replacement": "..."}]
	RegexPatterns []TableRegexPattern `yaml:"regex,omitempty" json:"regex,omitempty"`
}

type TableRegexPattern struct {
	Pattern     string `yaml:"pattern" json:"pattern"`
	Replacement string `yaml:"replacement" json:"replacement"`
}

// ParallelismConfig controls per-pipeline parallelism (Flink-style).
// N workers each run an independent instance of the pipeline, each with
// its own checkpoint scoped to a shard/partition.
type ParallelismConfig struct {
	// Count is the legacy number of parallel instances (goroutines).
	// For Kafka: should match or be ≤ partition count.
	// For MySQL batch: N ID-range shards are created.
	// Default 1 (no parallelism).
	//
	// Deprecated: use Sharding.LogicalShards + Execution.MaxActiveShards.
	Count int `yaml:"count" json:"count"`

	// ShardStrategy defines how data is split across parallel instances.
	// Supported: "partition" (Kafka), "id_range" (MySQL batch), "round_robin".
	//
	// Deprecated: use Sharding.Strategy.
	ShardStrategy string `yaml:"shard_strategy" json:"shard_strategy"`

	// ShardKey is the field used for sharding (e.g., "id" for id_range).
	//
	// Deprecated: use Sharding.Key.
	ShardKey string `yaml:"shard_key,omitempty" json:"shard_key,omitempty"`

	// ShardTotal is the total number of shards when using id_range.
	// If 0, Count is used. Example: ShardTotal=100, Count=4 → each gets 25 IDs.
	//
	// Deprecated: use Sharding.LogicalShards.
	ShardTotal int `yaml:"shard_total,omitempty" json:"shard_total,omitempty"`

	// Sharding describes stable data ownership. Its LogicalShards value is
	// part of the checkpoint namespace contract and should not be changed just
	// to tune runtime concurrency.
	Sharding *ShardingConfig `yaml:"sharding,omitempty" json:"sharding,omitempty"`

	// Execution describes how many logical shards may run concurrently in this
	// process. SinkConcurrency is enforced across standalone shard runners;
	// TransformWorkers applies to batch-local stateless transform work.
	// SourceConcurrency is parsed for forward compatibility but is not active.
	Execution *ParallelExecutionConfig `yaml:"execution,omitempty" json:"execution,omitempty"`
}

// ShardingConfig controls stable data ownership across restarts and workers.
type ShardingConfig struct {
	Strategy      string `yaml:"strategy,omitempty" json:"strategy,omitempty"`
	Key           string `yaml:"key,omitempty" json:"key,omitempty"`
	LogicalShards int    `yaml:"logical_shards,omitempty" json:"logical_shards,omitempty"`
}

// ParallelExecutionConfig controls current runtime concurrency.
type ParallelExecutionConfig struct {
	MaxActiveShards   int `yaml:"max_active_shards,omitempty" json:"max_active_shards,omitempty"`
	SourceConcurrency int `yaml:"source_concurrency,omitempty" json:"source_concurrency,omitempty"`
	TransformWorkers  int `yaml:"transform_workers,omitempty" json:"transform_workers,omitempty"`
	SinkConcurrency   int `yaml:"sink_concurrency,omitempty" json:"sink_concurrency,omitempty"`
}

// ApplyDefaults sets default values for parallelism config.
func (p *ParallelismConfig) ApplyDefaults() {
	if p == nil {
		return
	}
	legacyCount := p.Count
	if legacyCount <= 0 {
		legacyCount = 1
	}

	if p.Sharding == nil {
		p.Sharding = &ShardingConfig{
			Strategy:      p.ShardStrategy,
			Key:           p.ShardKey,
			LogicalShards: p.ShardTotal,
		}
	}
	if p.Sharding.Strategy == "" {
		p.Sharding.Strategy = p.ShardStrategy
	}
	if p.Sharding.Strategy == "" {
		p.Sharding.Strategy = "round_robin"
	}
	if p.Sharding.Key == "" {
		p.Sharding.Key = p.ShardKey
	}
	if p.Sharding.LogicalShards <= 0 {
		if p.ShardTotal > 0 {
			p.Sharding.LogicalShards = p.ShardTotal
		} else {
			p.Sharding.LogicalShards = legacyCount
		}
	}
	if p.Sharding.LogicalShards <= 0 {
		p.Sharding.LogicalShards = 1
	}

	if p.Execution == nil {
		p.Execution = &ParallelExecutionConfig{MaxActiveShards: legacyCount}
	}
	if p.Execution.MaxActiveShards <= 0 {
		p.Execution.MaxActiveShards = legacyCount
	}
	if p.Execution.MaxActiveShards <= 0 {
		p.Execution.MaxActiveShards = 1
	}

	// Keep legacy fields populated for existing UI/API code and tests.
	p.Count = p.Execution.MaxActiveShards
	p.ShardStrategy = p.Sharding.Strategy
	p.ShardKey = p.Sharding.Key
	p.ShardTotal = p.Sharding.LogicalShards
}

// ShardRange returns the stable logical-shard token range [start, end) for a
// given logical shard index. Runtime concurrency (max_active_shards) must not
// affect this range, otherwise changing execution capacity would change data
// ownership and checkpoint meaning.
func (p *ParallelismConfig) ShardRange(shardIndex int) (int64, int64) {
	if p == nil {
		return 0, 0
	}
	p.ApplyDefaults()
	if p.Sharding.LogicalShards <= 1 || shardIndex < 0 || shardIndex >= p.Sharding.LogicalShards {
		return 0, 0
	}
	start := int64(shardIndex)
	return start, start + 1
}

func (p *ParallelismConfig) LogicalShardCount() int {
	if p == nil {
		return 1
	}
	p.ApplyDefaults()
	return p.Sharding.LogicalShards
}

func (p *ParallelismConfig) MaxActiveShardCount() int {
	if p == nil {
		return 1
	}
	p.ApplyDefaults()
	if p.Execution.MaxActiveShards > p.Sharding.LogicalShards {
		return p.Sharding.LogicalShards
	}
	return p.Execution.MaxActiveShards
}

func (p *ParallelismConfig) SinkConcurrencyLimit() int {
	if p == nil {
		return 0
	}
	p.ApplyDefaults()
	if p.Execution == nil || p.Execution.SinkConcurrency <= 0 {
		return 0
	}
	return p.Execution.SinkConcurrency
}

func (p *ParallelismConfig) TransformWorkerCount() int {
	if p == nil {
		return 1
	}
	p.ApplyDefaults()
	if p.Execution == nil || p.Execution.TransformWorkers <= 1 {
		return 1
	}
	return p.Execution.TransformWorkers
}

func (p *ParallelismConfig) Strategy() string {
	if p == nil {
		return "round_robin"
	}
	p.ApplyDefaults()
	return p.Sharding.Strategy
}

func (p *ParallelismConfig) Key() string {
	if p == nil {
		return ""
	}
	p.ApplyDefaults()
	return p.Sharding.Key
}

// MapTable resolves a source table name to its target table name.
// Returns the original name if no mapping matches.
func (tm *TableMapping) MapTable(sourceTable string) string {
	// Check exact glob rules first
	for pattern, target := range tm.Rules {
		if matchGlob(pattern, sourceTable) {
			return target
		}
	}
	// Check regex patterns
	for _, rp := range tm.RegexPatterns {
		re, err := regexp.Compile(rp.Pattern)
		if err != nil {
			continue
		}
		if re.MatchString(sourceTable) {
			return re.ReplaceAllString(sourceTable, rp.Replacement)
		}
	}
	return sourceTable
}

func matchGlob(pattern, name string) bool {
	matched, err := filepath.Match(pattern, name)
	if err != nil {
		return false
	}
	return matched
}

type SpecFile struct {
	Pipes []Spec `yaml:"pipes" json:"pipes"`
}

func LoadSpecFile(path string) ([]Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec file: %w", err)
	}
	expanded, err := ExpandEnv(string(data))
	if err != nil {
		return nil, fmt.Errorf("expand spec env: %w", err)
	}
	var sf SpecFile
	if err := yaml.Unmarshal([]byte(expanded), &sf); err != nil {
		return nil, fmt.Errorf("parse spec file: %w", err)
	}
	for i := range sf.Pipes {
		ApplyDefaults(&sf.Pipes[i])
		if err := ValidateSpec(&sf.Pipes[i]); err != nil {
			return nil, err
		}
	}
	return sf.Pipes, nil
}

func LoadSpec(path string) (*Spec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec: %w", err)
	}
	expanded, err := ExpandEnv(string(data))
	if err != nil {
		return nil, fmt.Errorf("expand spec env: %w", err)
	}
	var spec Spec
	if err := yaml.Unmarshal([]byte(expanded), &spec); err != nil {
		return nil, fmt.Errorf("parse spec: %w", err)
	}
	ApplyDefaults(&spec)
	if err := ValidateSpec(&spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

func ApplyDefaults(spec *Spec) {
	if spec.Name == "" {
		return
	}
	if spec.BatchSize == 0 {
		spec.BatchSize = 1000
	}
	if spec.CheckpointIntervalSec == 0 {
		spec.CheckpointIntervalSec = 30
	}
	if spec.BackpressureBuffer == 0 {
		spec.BackpressureBuffer = 100
	}
	if spec.FlushIntervalMs == 0 {
		spec.FlushIntervalMs = 1000 // default 1 second
	}
	if spec.Retry == nil {
		spec.Retry = &RetrySpec{
			MaxAttempts:       3,
			InitialIntervalMs: 1000,
			MaxIntervalMs:     30000,
		}
	}
	if spec.Parallelism != nil {
		spec.Parallelism.ApplyDefaults()
	}
	ApplyDefaultSchedule(spec)
}

func ValidateSpec(spec *Spec) error {
	var problems []string
	ApplyDefaultSchedule(spec)
	if strings.TrimSpace(spec.Name) == "" {
		problems = append(problems, "name is required")
	}
	if strings.TrimSpace(spec.Source.Type) == "" && connectionRef(spec.Source.Connection, spec.Source.ConnectionRef) == "" {
		problems = append(problems, "source.type is required")
	} else if strings.TrimSpace(spec.Source.Type) != "" && !registry.HasSource(spec.Source.Type) {
		problems = append(problems, fmt.Sprintf("unknown source.type %q", spec.Source.Type))
	}
	if spec.Source.Config == nil {
		spec.Source.Config = map[string]any{}
	}
	if strings.TrimSpace(spec.Sink.Type) == "" && connectionRef(spec.Sink.Connection, spec.Sink.ConnectionRef) == "" {
		problems = append(problems, "sink.type is required")
	} else if strings.TrimSpace(spec.Sink.Type) != "" && !registry.HasSink(spec.Sink.Type) {
		problems = append(problems, fmt.Sprintf("unknown sink.type %q", spec.Sink.Type))
	}
	if spec.Sink.Config == nil {
		spec.Sink.Config = map[string]any{}
	}
	for i := range spec.Transforms {
		tr := &spec.Transforms[i]
		if strings.TrimSpace(tr.Type) == "" && connectionRef(tr.Connection, tr.ConnectionRef) == "" {
			problems = append(problems, fmt.Sprintf("transforms[%d].type is required", i))
		} else if strings.TrimSpace(tr.Type) != "" && !registry.HasTransform(tr.Type) {
			problems = append(problems, fmt.Sprintf("unknown transforms[%d].type %q", i, tr.Type))
		}
		if tr.Config == nil {
			tr.Config = map[string]any{}
		}
	}
	problems = append(problems, ValidateRuntimeStateRequirements(spec)...)
	if spec.BatchSize <= 0 {
		problems = append(problems, "batch_size must be > 0")
	}
	if spec.CheckpointIntervalSec <= 0 {
		problems = append(problems, "checkpoint_interval_sec must be > 0")
	}
	if spec.BackpressureBuffer <= 0 {
		problems = append(problems, "backpressure_buffer must be > 0")
	}
	if spec.Retry != nil {
		if spec.Retry.MaxAttempts <= 0 {
			problems = append(problems, "retry.max_attempts must be > 0")
		}
		if spec.Retry.InitialIntervalMs <= 0 {
			problems = append(problems, "retry.initial_interval_ms must be > 0")
		}
		if spec.Retry.MaxIntervalMs <= 0 {
			problems = append(problems, "retry.max_interval_ms must be > 0")
		}
	}
	if err := ValidateSourceSchedule(spec); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid pipeline %q: %s", spec.Name, strings.Join(problems, "; "))
	}

	// Block dangerous CDC + non-idempotent sink combinations.
	// These combinations cause silent data duplication on crash recovery
	// because the sink cannot deduplicate replayed records.
	cdcSources := map[string]bool{
		"mysql_cdc": true, "mysql_snapshot_cdc": true, "postgres_cdc": true, "kafka": true,
	}
	nonIdempotentSinks := map[string]bool{
		"file_sink": true, "s3": true,
	}
	if cdcSources[spec.Source.Type] && nonIdempotentSinks[spec.Sink.Type] {
		return fmt.Errorf(
			"unsafe pipeline %q: CDC source %q with non-idempotent sink %q will silently duplicate data on crash recovery; "+
				"use an idempotent sink (mysql/postgres/clickhouse/doris with upsert) or a batch source instead",
			spec.Name, spec.Source.Type, spec.Sink.Type,
		)
	}

	return nil
}

func ValidateRuntimeStateRequirements(spec *Spec) []string {
	if spec == nil {
		return nil
	}
	var problems []string
	for i := range spec.Transforms {
		tr := &spec.Transforms[i]
		problems = append(problems, ValidateTransformRuntimeStateRequirements(i, tr.Type, tr.Config)...)
	}
	return problems
}

func ValidateTransformRuntimeStateRequirements(index int, typ string, config map[string]any) []string {
	var problems []string
	ctx := context.Background()
	transformPath := fmt.Sprintf("transforms[%d]", index)
	if backend, ok := stringConfig(config, "state_backend"); ok {
		if !supportsRuntimeStateBackend(typ) {
			problems = append(problems, fmt.Sprintf("%s.state_backend is only supported by built-in lookup, join, window, and deduplicate transforms; custom keyed state is not part of the pipeline spec", transformPath))
		}
		switch strings.ToLower(backend) {
		case "redis":
			if !redisConfiguredForTransform(ctx, config) {
				problems = append(problems, fmt.Sprintf("%s.state_backend=redis requires etl.state.redis.addr or ETL_STATE_REDIS_ADDR; SQLite/MySQL/PostgreSQL storage is only for checkpoint/metadata", transformPath))
			}
		case "sqlite", "mysql", "postgres", "postgresql":
			problems = append(problems, fmt.Sprintf("%s.state_backend=%q is not allowed for runtime state/cache; use Redis and configure etl.state.redis.addr", transformPath, backend))
		default:
			problems = append(problems, fmt.Sprintf("%s.state_backend=%q is unsupported; only redis is allowed for runtime state/cache", transformPath, backend))
		}
	}
	if backend, ok := stringConfig(config, "cache_backend"); ok {
		switch strings.ToLower(backend) {
		case "redis":
			if !redisConfiguredForTransform(ctx, config) {
				problems = append(problems, fmt.Sprintf("%s.cache_backend=redis requires etl.state.redis.addr or ETL_STATE_REDIS_ADDR; SQL storage cannot be used as cache", transformPath))
			}
		case "sqlite", "mysql", "postgres", "postgresql":
			problems = append(problems, fmt.Sprintf("%s.cache_backend=%q is not allowed; cache backends must be Redis-backed", transformPath, backend))
		default:
			problems = append(problems, fmt.Sprintf("%s.cache_backend=%q is unsupported; only redis is allowed for runtime cache", transformPath, backend))
		}
	}
	if strings.EqualFold(typ, "enricher") {
		if ttl, ok := intConfig(config, "cache_ttl_seconds"); ok && ttl > 0 && !redisConfiguredForTransform(ctx, config) {
			problems = append(problems, fmt.Sprintf("%s.cache_ttl_seconds requires Redis cache configuration; set cache_ttl_seconds=0 or configure etl.state.redis.addr", transformPath))
		}
	}
	if strings.EqualFold(typ, "window") {
		if windowType, ok := stringConfig(config, "window_type"); ok {
			switch strings.ToLower(windowType) {
			case "", "tumbling":
			case "sliding", "session":
				problems = append(problems, fmt.Sprintf("%s.window_type=%q is not supported in this build; only tumbling window is part of the production pipeline spec", transformPath, windowType))
			default:
				problems = append(problems, fmt.Sprintf("%s.window_type=%q is unsupported", transformPath, windowType))
			}
		}
	}
	for _, key := range []string{"keyed_state", "uses_keyed_state", "timer", "uses_timer"} {
		if enabled, ok := boolConfig(config, key); ok && enabled {
			problems = append(problems, fmt.Sprintf("%s.%s is not part of the pipeline spec; arbitrary keyed state and timers are outside the current ETL runtime boundary", transformPath, key))
		}
	}
	return problems
}

func supportsRuntimeStateBackend(typ string) bool {
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "lookup", "join", "window", "deduplicate":
		return true
	default:
		return false
	}
}

func redisConfiguredForTransform(ctx context.Context, config map[string]any) bool {
	return strings.TrimSpace(state.RedisConfigFromMap(ctx, config).Addr) != ""
}

func stringConfig(config map[string]any, key string) (string, bool) {
	if config == nil {
		return "", false
	}
	v, ok := config[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	s = strings.TrimSpace(s)
	return s, s != ""
}

func intConfig(config map[string]any, key string) (int, bool) {
	if config == nil {
		return 0, false
	}
	switch v := config[key].(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case float32:
		return int(v), true
	default:
		return 0, false
	}
}

func boolConfig(config map[string]any, key string) (bool, bool) {
	if config == nil {
		return false, false
	}
	v, ok := config[key]
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

func connectionRef(connection, connectionRef string) string {
	if strings.TrimSpace(connection) != "" {
		return strings.TrimSpace(connection)
	}
	return strings.TrimSpace(connectionRef)
}

func ValidateIdempotency(spec *Spec) []string {
	warnings := CheckIdempotencyCompatibility(spec.Source.Type, spec.Sink.Type, spec.Sink.Config)
	// pre_write idempotency warning (MySQL/PostgreSQL batch sinks)
	warnings = append(warnings, preWriteWarnings(spec)...)
	// increment mode is non-idempotent: replay re-adds (MySQL/PostgreSQL sink)
	warnings = append(warnings, incrementWarnings(spec)...)
	// dependency-schedule downstream recomputation idempotency warning
	warnings = append(warnings, dependencyScheduleWarnings(spec)...)
	return warnings
}

// incrementWarnings surfaces a strong idempotency warning for batch_mode=increment.
// Increment accumulates on each write; a checkpoint reset replays the batch and
// re-adds the same source values, producing inflated totals.
func incrementWarnings(spec *Spec) []string {
	if spec == nil {
		return nil
	}
	sinkType := spec.Sink.Type
	if sinkType != "mysql" && sinkType != "postgres" && sinkType != "postgresql" {
		return nil
	}
	mode, _ := spec.Sink.Config["batch_mode"].(string)
	if mode != "increment" {
		return nil
	}
	tableName, _ := spec.Sink.Config["table"].(string)
	if tableName == "" {
		tableName = "<dynamic>"
	}
	return []string{fmt.Sprintf(
		"%s sink %s uses batch_mode=increment: replay/checkpoint reset will re-add accumulator columns. Only use this mode when the source has a stable dedup key AND checkpoint will not reset, or when downstream reconciliation can detect duplicates; manual reconciliation is required after any reset.",
		sinkType, tableName)}
}

// dependencyScheduleWarnings surfaces idempotency risks for pipelines scheduled
// via `schedule.type: dependency` (Post-Commit Trigger scheme A). When such a
// downstream pipeline re-runs on every upstream completion, it must be
// idempotent: rely on upsert + pk_columns to absorb replays.
func dependencyScheduleWarnings(spec *Spec) []string {
	if spec == nil || spec.Schedule == nil || spec.Schedule.Type != "dependency" {
		return nil
	}
	sinkType := spec.Sink.Type
	switch sinkType {
	case "mysql", "postgres", "postgresql", "doris", "clickhouse":
		batchMode := ""
		if v, ok := spec.Sink.Config["batch_mode"]; ok {
			batchMode, _ = v.(string)
		}
		pkCols := stringSliceConfig(spec.Sink.Config, "pk_columns")
		if (sinkType == "mysql" || sinkType == "postgres" || sinkType == "postgresql") && batchMode != "upsert" {
			return []string{fmt.Sprintf(
				"dependency-scheduled pipeline writes to %s with batch_mode=%q; the downstream re-computation re-runs on every upstream completion, so use batch_mode: upsert with pk_columns to absorb replays",
				sinkType, batchMode)}
		}
		if len(pkCols) == 0 && batchMode == "upsert" {
			return []string{fmt.Sprintf(
				"dependency-scheduled pipeline uses %s upsert without pk_columns; replays of the downstream re-computation cannot be absorbed",
				sinkType)}
		}
	case "file_sink", "s3", "kafka":
		return []string{fmt.Sprintf(
			"dependency-scheduled pipeline writes to append-only sink %s; the downstream re-computation re-runs on every upstream completion and will produce duplicates",
			sinkType)}
	}
	return nil
}

// preWriteWarnings surfaces idempotency risks for configured pre_write actions.
// truncate/truncate_partition/delete are explicit "delete-then-rewrite"
// semantics: on checkpoint reset the pre_write re-runs and the batch is
// rewritten, which is idempotent for batch pipelines but dangerous for CDC.
func preWriteWarnings(spec *Spec) []string {
	if spec == nil {
		return nil
	}
	sinkType := spec.Sink.Type
	if sinkType != "mysql" && sinkType != "postgres" && sinkType != "postgresql" {
		return nil
	}
	raw, ok := spec.Sink.Config["pre_write"]
	if !ok || raw == nil {
		return nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	action, _ := m["action"].(string)
	if action == "" {
		return nil
	}
	tableName, _ := spec.Sink.Config["table"].(string)
	if tableName == "" {
		tableName = "<dynamic>"
	}
	switch action {
	case "delete":
		cond, _ := m["condition"].(string)
		return []string{fmt.Sprintf(
			"pre_write will DELETE FROM %s WHERE %s before each batch; checkpoint reset replays the delete+rewrite (idempotent for batch, not for CDC)",
			tableName, cond)}
	case "truncate":
		return []string{fmt.Sprintf(
			"pre_write will TRUNCATE TABLE %s before each batch; checkpoint reset replays the truncate+rewrite (idempotent for batch, not for CDC)",
			tableName)}
	case "truncate_partition":
		cond, _ := m["condition"].(string)
		return []string{fmt.Sprintf(
			"pre_write will DELETE the target partition of %s (WHERE %s) before each batch; checkpoint reset replays the delete+rewrite (idempotent for batch, not for CDC)",
			tableName, cond)}
	}
	return nil
}

// ValidateParallelism returns warnings about parallelism / shard_strategy
// misconfigurations. These do not block pipeline creation but surface common
// pitfalls before runtime.
//
// Known footguns covered:
//   - kafka source with logical_shards > 1: Kafka shards share one
//     group_id and rely on the consumer-group protocol to distribute topic
//     partitions. If the topic has fewer partitions than logical_shards, the extra
//     shard instances stay idle. shard_strategy is ignored for kafka.
func ValidateParallelism(spec *Spec) []string {
	if spec == nil || spec.Parallelism == nil {
		return nil
	}
	spec.Parallelism.ApplyDefaults()
	logicalShards := spec.Parallelism.LogicalShardCount()
	maxActive := spec.Parallelism.MaxActiveShardCount()
	strategy := spec.Parallelism.Strategy()
	var warnings []string
	if spec.Parallelism.Execution != nil {
		if spec.Parallelism.Execution.SourceConcurrency > 1 {
			warnings = append(warnings,
				"parallelism.execution.source_concurrency is reserved for the staged execution engine and is not active yet; current source concurrency is driven by logical_shards and max_active_shards.")
		}
	}
	if logicalShards <= 1 {
		return warnings
	}
	switch spec.Source.Type {
	case "kafka":
		warnings = append(warnings, fmt.Sprintf(
			"source type %q with logical_shards=%d: shards share the same group_id and Kafka's "+
				"consumer-group protocol assigns topic partitions to them. Ensure the Kafka topic has at "+
				"least %d partitions, otherwise the extra shards will stay idle. shard_strategy is ignored "+
				"for kafka sources.",
			spec.Source.Type, logicalShards, logicalShards))
	case "postgres_cdc":
		warnings = append(warnings,
			"source type \"postgres_cdc\" does not implement sharding; parallel logical shards may duplicate the same replication stream. Keep logical_shards=1 until postgres_cdc exposes a partition-safe strategy.")
	case "mysql_cdc":
		tables := stringSliceConfig(spec.Source.Config, "tables")
		if len(tables) <= 1 {
			warnings = append(warnings,
				"source type \"mysql_cdc\" only shards by table list; a single-table CDC pipeline will not gain data parallelism from logical_shards>1.")
		}
	case "file":
		warnings = append(warnings,
			"source type \"file\" currently uses framework-level line modulo sharding; checkpoint resume for non-native sharding should be treated as at-least-once with possible replay until stable global-position sharding is enabled.")
	case "redis":
		if strategy == "round_robin" || strategy == "line_modulo" {
			warnings = append(warnings,
				"source type \"redis\" should use sharding.strategy: hash_modulo with sharding.key: _key or @key; round_robin/line_modulo does not match Redis key ownership semantics.")
		}
	case "mysql_batch":
		if strategy != "id_range" && strategy != "pk_mod" && strategy != "round_robin" {
			warnings = append(warnings, fmt.Sprintf(
				"source type \"mysql_batch\" ignores sharding.strategy=%q semantics and applies MOD(pk, shard_total)=shard_index; prefer strategy: pk_mod for clarity.", strategy))
		}
	}
	if maxActive < logicalShards && isStreamingSource(spec.Source.Type) {
		warnings = append(warnings, fmt.Sprintf(
			"execution.max_active_shards=%d is lower than sharding.logical_shards=%d for streaming source %q; inactive shards will not consume until a shard lease scheduler is active.",
			maxActive, logicalShards, spec.Source.Type))
	}
	return warnings
}

func stringSliceConfig(config map[string]any, key string) []string {
	if config == nil {
		return nil
	}
	v, ok := config[key]
	if !ok {
		return nil
	}
	switch vv := v.(type) {
	case []string:
		return append([]string(nil), vv...)
	case []any:
		out := make([]string, 0, len(vv))
		for _, item := range vv {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if strings.TrimSpace(vv) == "" {
			return nil
		}
		return []string{vv}
	default:
		return nil
	}
}

func isStreamingSource(sourceType string) bool {
	switch sourceType {
	case "mysql_cdc", "mysql_snapshot_cdc", "postgres_cdc", "kafka", "redis":
		return true
	default:
		return false
	}
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

func ExpandEnv(input string) (string, error) {
	var missing []string
	out := envPattern.ReplaceAllStringFunc(input, func(match string) string {
		parts := envPattern.FindStringSubmatch(match)
		name := parts[1]
		fallback := parts[3]
		if value, ok := os.LookupEnv(name); ok {
			return value
		}
		if parts[2] != "" {
			return fallback
		}
		missing = append(missing, name)
		return match
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("unresolved environment variables: %s", strings.Join(missing, ", "))
	}
	return out, nil
}

// MarshalSpecYAML serializes a Spec back to YAML.
func MarshalSpecYAML(spec *Spec) ([]byte, error) {
	return yaml.Marshal(spec)
}

// SpecChanged returns true if the new spec differs from the old in a meaningful way
// (source type, sink type, table, transforms).
func SpecChanged(old, new *Spec) bool {
	if old == nil || new == nil {
		return true
	}
	if old.Source.Type != new.Source.Type {
		return true
	}
	if old.Sink.Type != new.Sink.Type {
		return true
	}
	if len(old.Transforms) != len(new.Transforms) {
		return true
	}
	for i := range old.Transforms {
		if old.Transforms[i].Type != new.Transforms[i].Type {
			return true
		}
	}
	return false
}

// IsCheckpointIncompatible returns true if the new spec's source configuration
// makes old checkpoints unusable (e.g. different source type, different table).
func IsCheckpointIncompatible(old, new *Spec) bool {
	if old == nil || new == nil {
		return true
	}
	if old.Source.Type != new.Source.Type {
		return true
	}
	// If source table changed, checkpoint is incompatible
	oldTable, _ := old.Source.Config["table"].(string)
	newTable, _ := new.Source.Config["table"].(string)
	if oldTable != "" && newTable != "" && oldTable != newTable {
		return true
	}
	// If source query changed, checkpoint is incompatible
	oldQuery, _ := old.Source.Config["query"].(string)
	newQuery, _ := new.Source.Config["query"].(string)
	if oldQuery != "" && newQuery != "" && oldQuery != newQuery {
		return true
	}
	// If source database changed, checkpoint is incompatible
	oldDB, _ := old.Source.Config["database"].(string)
	newDB, _ := new.Source.Config["database"].(string)
	if oldDB != "" && newDB != "" && oldDB != newDB {
		return true
	}
	return false
}

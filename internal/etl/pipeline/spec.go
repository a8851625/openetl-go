package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"

	"gopkg.in/yaml.v3"
)

type ScheduleConfig struct {
	Type string `yaml:"type" json:"type"`
	Cron string `yaml:"cron,omitempty" json:"cron,omitempty"`
}

type SourceSpec struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

type SinkSpec struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
}

type TransformSpec struct {
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config,omitempty" json:"config,omitempty"`
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
	// Count is the number of parallel instances (goroutines).
	// For Kafka: should match or be ≤ partition count.
	// For MySQL batch: N ID-range shards are created.
	// Default 1 (no parallelism).
	Count int `yaml:"count" json:"count"`

	// ShardStrategy defines how data is split across parallel instances.
	// Supported: "partition" (Kafka), "id_range" (MySQL batch), "round_robin".
	ShardStrategy string `yaml:"shard_strategy" json:"shard_strategy"`

	// ShardKey is the field used for sharding (e.g., "id" for id_range).
	ShardKey string `yaml:"shard_key,omitempty" json:"shard_key,omitempty"`

	// ShardTotal is the total number of shards when using id_range.
	// If 0, Count is used. Example: ShardTotal=100, Count=4 → each gets 25 IDs.
	ShardTotal int `yaml:"shard_total,omitempty" json:"shard_total,omitempty"`
}

// ApplyDefaults sets default values for parallelism config.
func (p *ParallelismConfig) ApplyDefaults() {
	if p.Count <= 0 {
		p.Count = 1
	}
	if p.ShardStrategy == "" {
		p.ShardStrategy = "round_robin"
	}
}

// ShardRange returns the [start, end) range for a given shard index (0-based)
// when using id_range strategy. Returns 0, 0 if not applicable.
func (p *ParallelismConfig) ShardRange(shardIndex int) (int64, int64) {
	if p.Count <= 1 || p.ShardTotal <= 0 {
		return 0, 0
	}
	shardSize := int64(p.ShardTotal / p.Count)
	start := int64(shardIndex) * shardSize
	end := start + shardSize
	if shardIndex == p.Count-1 {
		end = int64(p.ShardTotal)
	}
	return start, end
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
}

func ValidateSpec(spec *Spec) error {
	var problems []string
	if strings.TrimSpace(spec.Name) == "" {
		problems = append(problems, "name is required")
	}
	if strings.TrimSpace(spec.Source.Type) == "" {
		problems = append(problems, "source.type is required")
	} else if !registry.HasSource(spec.Source.Type) {
		problems = append(problems, fmt.Sprintf("unknown source.type %q", spec.Source.Type))
	}
	if spec.Source.Config == nil {
		spec.Source.Config = map[string]any{}
	}
	if strings.TrimSpace(spec.Sink.Type) == "" {
		problems = append(problems, "sink.type is required")
	} else if !registry.HasSink(spec.Sink.Type) {
		problems = append(problems, fmt.Sprintf("unknown sink.type %q", spec.Sink.Type))
	}
	if spec.Sink.Config == nil {
		spec.Sink.Config = map[string]any{}
	}
	for i := range spec.Transforms {
		tr := &spec.Transforms[i]
		if strings.TrimSpace(tr.Type) == "" {
			problems = append(problems, fmt.Sprintf("transforms[%d].type is required", i))
		} else if !registry.HasTransform(tr.Type) {
			problems = append(problems, fmt.Sprintf("unknown transforms[%d].type %q", i, tr.Type))
		}
		if tr.Config == nil {
			tr.Config = map[string]any{}
		}
	}
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

func ValidateIdempotency(spec *Spec) []string {
	return CheckIdempotencyCompatibility(spec.Source.Type, spec.Sink.Type, spec.Sink.Config)
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

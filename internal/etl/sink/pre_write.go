package sink

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// PreWriteAction enumerates the pre-write actions supported by transactional
// SQL sinks (MySQL/PostgreSQL).
//
//   - "delete":            execute `DELETE FROM <table> WHERE <condition>`
//   - "truncate":          execute `TRUNCATE TABLE <table>`
//   - "truncate_partition": execute a partition-scoped truncate/DELETE
//
// pre_write runs once per Write() batch, inside the same transaction and
// BEFORE any data-row insert/upsert/delete. A pre_write failure is classified
// as a data-class error and surfaces to DLQ/retry like any other sink error;
// it does NOT silently skip the batch.
type PreWriteAction string

const (
	PreWriteDelete            PreWriteAction = "delete"
	PreWriteTruncate          PreWriteAction = "truncate"
	PreWriteTruncatePartition PreWriteAction = "truncate_partition"
)

// PreWriteConfig captures a declarative pre-write action.
type PreWriteConfig struct {
	Action     PreWriteAction        `yaml:"action" json:"action"`
	Condition  string                `yaml:"condition,omitempty" json:"condition,omitempty"`
	Params     map[string]any        `yaml:"params,omitempty" json:"params,omitempty"`
	enabled    bool                  `yaml:"-" json:"-"`
	executed   bool                  `yaml:"-" json:"-"`
}

// ParsePreWriteConfig extracts an optional `pre_write` block from sink config.
// Returns a disabled config when the key is absent.
func ParsePreWriteConfig(config map[string]any) (*PreWriteConfig, error) {
	raw, ok := config["pre_write"]
	if !ok || raw == nil {
		return &PreWriteConfig{}, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("pre_write must be a map, got %T", raw)
	}
	cfg := &PreWriteConfig{enabled: true}
	if v, ok := m["action"]; ok {
		cfg.Action = PreWriteAction(fmt.Sprint(v))
	}
	switch cfg.Action {
	case PreWriteDelete, PreWriteTruncate, PreWriteTruncatePartition:
	default:
		return nil, fmt.Errorf("pre_write.action %q is not supported (allowed: delete, truncate, truncate_partition)", cfg.Action)
	}
	if v, ok := m["condition"]; ok {
		cfg.Condition = fmt.Sprint(v)
	}
	if v, ok := m["params"]; ok {
		if pm, ok := v.(map[string]any); ok {
			cfg.Params = pm
		}
	}
	if cfg.Action == PreWriteDelete && strings.TrimSpace(cfg.Condition) == "" {
		return nil, fmt.Errorf("pre_write.action=delete requires a non-empty condition (use truncate to wipe the whole table)")
	}
	if cfg.Action == PreWriteTruncatePartition && strings.TrimSpace(cfg.Condition) == "" {
		return nil, fmt.Errorf("pre_write.action=truncate_partition requires a condition identifying the target partition")
	}
	return cfg, nil
}

// Enabled reports whether pre_write is configured.
func (p *PreWriteConfig) Enabled() bool { return p != nil && p.enabled }

// expandParams resolves ${PROCESSING_DATE} and ${params.xxx} placeholders in
// the condition string. Unknown placeholders are left untouched so that a
// typo surfaces as a SQL error rather than silently matching nothing.
func (p *PreWriteConfig) expandParams() string {
	cond := p.Condition
	cond = strings.ReplaceAll(cond, "${PROCESSING_DATE}", time.Now().UTC().Format("2006-01-02"))
	for k, v := range p.Params {
		cond = strings.ReplaceAll(cond, fmt.Sprintf("${params.%s}", k), fmt.Sprint(v))
	}
	return cond
}

// ExecSQL runs the configured pre-write action against the given transaction.
// tableFQTN is the fully-qualified target table name (`db.table` or
// `schema.table`), already quoted by the caller. For truncate_partition the
// condition is interpreted as a `WHERE` clause applied to a DELETE on the
// target table (portable across MySQL/PostgreSQL).
func (p *PreWriteConfig) ExecSQL(ctx context.Context, tx *sql.Tx, tableFQTN string) error {
	if p == nil || !p.enabled {
		return nil
	}
	if p.executed {
		return nil
	}
	switch p.Action {
	case PreWriteDelete, PreWriteTruncatePartition:
		cond := p.expandParams()
		stmt := fmt.Sprintf("DELETE FROM %s WHERE %s", tableFQTN, cond)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("pre_write %s on %s: %w", p.Action, tableFQTN, err)
		}
	case PreWriteTruncate:
		stmt := fmt.Sprintf("TRUNCATE TABLE %s", tableFQTN)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("pre_write truncate on %s: %w", tableFQTN, err)
		}
	}
	p.executed = true
	return nil
}

// IsDangerousForStreaming reports whether the configured action is unsafe for
// CDC/streaming pipelines. truncate and truncate_partition wipe data and must
// only be used with once/cron/periodic batch pipelines.
func (p *PreWriteConfig) IsDangerousForStreaming() bool {
	if p == nil || !p.enabled {
		return false
	}
	return p.Action == PreWriteTruncate || p.Action == PreWriteTruncatePartition
}

// DescribeForWarning returns a human-readable description for spec validate /
// preflight idempotency warnings.
func (p *PreWriteConfig) DescribeForWarning(table string) string {
	if p == nil || !p.enabled {
		return ""
	}
	switch p.Action {
	case PreWriteDelete:
		return fmt.Sprintf("pre_write will DELETE FROM %s WHERE %s before each batch; checkpoint reset replays the delete+rewrite", table, p.Condition)
	case PreWriteTruncate:
		return fmt.Sprintf("pre_write will TRUNCATE TABLE %s before each batch; checkpoint reset replays the truncate+rewrite", table)
	case PreWriteTruncatePartition:
		return fmt.Sprintf("pre_write will DELETE the target partition of %s (WHERE %s) before each batch; checkpoint reset replays the delete+rewrite", table, p.Condition)
	}
	return ""
}

// PgxExec is the pgx equivalent of ExecSQL. It accepts a function matching
// pgx.Tx.ExecContext so we do not import pgx here (avoiding a cycle).
type pgxExecFunc func(ctx context.Context, sql string, args ...any) error

// ExecPgx runs the configured pre-write action via a pgx-compatible executor.
func (p *PreWriteConfig) ExecPgx(ctx context.Context, exec pgxExecFunc, tableFQTN string) error {
	if p == nil || !p.enabled {
		return nil
	}
	if p.executed {
		return nil
	}
	switch p.Action {
	case PreWriteDelete, PreWriteTruncatePartition:
		cond := p.expandParams()
		stmt := fmt.Sprintf("DELETE FROM %s WHERE %s", tableFQTN, cond)
		if err := exec(ctx, stmt); err != nil {
			return fmt.Errorf("pre_write %s on %s: %w", p.Action, tableFQTN, err)
		}
	case PreWriteTruncate:
		stmt := fmt.Sprintf("TRUNCATE TABLE %s", tableFQTN)
		if err := exec(ctx, stmt); err != nil {
			return fmt.Errorf("pre_write truncate on %s: %w", tableFQTN, err)
		}
	}
	p.executed = true
	return nil
}

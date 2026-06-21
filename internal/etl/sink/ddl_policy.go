package sink

import (
	"context"
	"fmt"

	"openetl-go/internal/etl/core"
)

// DDLPolicy controls how a sink handles OpDDL records.
//   - "reject": return an error (the batch is routed to DLQ by the pipeline)
//   - "ignore": silently drop the DDL record
//   - "apply":  execute the raw DDL via the provided exec function
//
// The default everywhere is "reject" because source-side DDL (e.g. MySQL
// ALTER TABLE) is generally not portable to the target's SQL dialect.
type DDLPolicy string

const (
	DDLPolicyReject DDLPolicy = "reject"
	DDLPolicyIgnore DDLPolicy = "ignore"
	DDLPolicyApply  DDLPolicy = "apply"
)

// ApplyDDLRecords processes OpDDL records according to policy. exec is only
// called when policy is "apply"; it receives the raw DDL statement and the
// affected table name (best-effort, from metadata).
func ApplyDDLRecords(ctx context.Context, ddls []core.Record, policy DDLPolicy, exec func(ctx context.Context, ddl, table string) error) error {
	for _, ddl := range ddls {
		stmt := ddl.Metadata.DDL
		table := ddl.Metadata.Table
		switch policy {
		case DDLPolicyIgnore:
			continue
		case DDLPolicyApply:
			if stmt == "" {
				continue
			}
			if err := exec(ctx, stmt, table); err != nil {
				return fmt.Errorf("apply DDL on %s: %w", table, err)
			}
		default:
			return fmt.Errorf("DDL record on %s rejected by ddl_policy=%q (statement: %q)",
				table, policy, stmt)
		}
	}
	return nil
}

package core

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// ── Schema Management Abstraction ────────────────────────────────────

// SchemaManager handles auto-creation and schema-drift detection for sinks.
// Each database sink (MySQL, Postgres, ClickHouse) implements this interface
// to share a common DDL lifecycle.
//
// Sinks that don't support DDL (Kafka, file, S3) simply don't implement it;
// the pipeline calls EnsureSchema only if the sink implements SchemaManager.
type SchemaManager interface {
	// EnsureSchema is called before each batch write. It:
	//   1. Checks if the target table exists (cached).
	//   2. If auto_create is enabled and table is missing, creates it.
	//   3. If schema_drift is add_columns, adds any missing columns.
	EnsureSchema(ctx context.Context, tableName string, fields []string, fieldValues map[string]any) error
}

// SchemaDriftMode controls behavior when source columns don't exist at the target.
type SchemaDriftMode string

const (
	DriftIgnore  SchemaDriftMode = "ignore"      // silently skip (default)
	DriftFail    SchemaDriftMode = "fail"        // return error
	DriftAddCols SchemaDriftMode = "add_columns" // ALTER TABLE ADD COLUMN
)

// SchemaCache provides a thread-safe cache for table existence and column
// information. Embedded by sink implementations to avoid repeated
// information_schema queries.
type SchemaCache struct {
	mu           sync.RWMutex
	knownTables  map[string]bool            // tableName → exists
	knownColumns map[string]map[string]bool // tableName → set of column names
}

func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		knownTables:  make(map[string]bool),
		knownColumns: make(map[string]map[string]bool),
	}
}

// TableExists returns the cached result, or calls fallback if not cached.
func (c *SchemaCache) TableExists(table string, fallback func() (bool, error)) (bool, error) {
	c.mu.RLock()
	if v, ok := c.knownTables[table]; ok {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()

	exists, err := fallback()
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	c.knownTables[table] = exists
	c.mu.Unlock()
	return exists, nil
}

// MarkTableCreated marks a table as existing in cache.
func (c *SchemaCache) MarkTableCreated(table string, columns []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.knownTables[table] = true
	colSet := make(map[string]bool, len(columns))
	for _, col := range columns {
		colSet[col] = true
	}
	c.knownColumns[table] = colSet
}

// GetColumns returns the cached column set, or calls fallback if not cached.
func (c *SchemaCache) GetColumns(table string, fallback func() (map[string]bool, error)) (map[string]bool, error) {
	c.mu.RLock()
	if v, ok := c.knownColumns[table]; ok {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()

	cols, err := fallback()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.knownColumns[table] = cols
	c.mu.Unlock()
	return cols, nil
}

// InvalidateCache clears cached schema info for a table (e.g. after ALTER TABLE).
func (c *SchemaCache) InvalidateCache(table string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.knownColumns, table)
}

// EnsureSchemaGeneric is a reusable helper that implements the common
// auto_create + schema_drift flow. Sink implementations delegate to this
// after providing their DDL callbacks.
//
// Usage:
//
//	func (s *MySink) EnsureSchema(ctx context.Context, table string, fields []string) error {
//	    return core.EnsureSchemaGeneric(ctx, s.cache, table, fields,
//	        s.autoCreate, core.SchemaDriftMode(s.schemaDrift),
//	        s.tableExists, s.createTable, s.getColumns, s.addColumn)
//	}
func EnsureSchemaGeneric(
	ctx context.Context,
	cache *SchemaCache,
	tableName string,
	fields []string,
	fieldValues map[string]any,
	autoCreate bool,
	driftMode SchemaDriftMode,
	tableExistsFn func(ctx context.Context, table string) (bool, error),
	createTableFn func(ctx context.Context, table string, columns []string, fieldValues map[string]any) error,
	getColumnsFn func(ctx context.Context, table string) (map[string]bool, error),
	addColumnFn func(ctx context.Context, table, column string, fieldValues map[string]any) error,
) error {
	sort.Strings(fields)

	// 1. Check table existence.
	exists, err := cache.TableExists(tableName, func() (bool, error) {
		return tableExistsFn(ctx, tableName)
	})
	if err != nil {
		return err
	}

	if !exists {
		if autoCreate {
			if err := createTableFn(ctx, tableName, fields, fieldValues); err != nil {
				return fmt.Errorf("auto-create table %s: %w", tableName, err)
			}
			cache.MarkTableCreated(tableName, fields)
			return nil
		}
		// auto_create off → let the INSERT fail naturally.
		return nil
	}

	// 2. Table exists — check schema drift.
	if driftMode != DriftAddCols {
		return nil
	}

	existingCols, err := cache.GetColumns(tableName, func() (map[string]bool, error) {
		return getColumnsFn(ctx, tableName)
	})
	if err != nil {
		return nil // best-effort
	}

	for _, col := range fields {
		if !existingCols[col] {
			if err := addColumnFn(ctx, tableName, col, fieldValues); err != nil {
				if driftMode == DriftFail {
					return fmt.Errorf("schema drift: add column %s to %s: %w", col, tableName, err)
				}
				continue
			}
			cache.InvalidateCache(tableName)
		}
	}
	return nil
}

// ── Record Processor Abstraction ─────────────────────────────────────

// RecordProcessor is a middleware that transforms records BEFORE they enter
// the user-visible transform chain. This is for pipeline-level concerns
// like table name mapping, record enrichment, data masking, etc.
//
// Unlike Transform (which is user-configured per-pipeline), RecordProcessors
// are built from the pipeline spec's top-level fields (table_mapping, etc.)
// and run in a fixed order before transforms.
type RecordProcessor interface {
	Process(ctx context.Context, rec Record) (Record, error)
	Name() string
}

// RecordProcessorChain applies processors in sequence.
type RecordProcessorChain []RecordProcessor

func (c RecordProcessorChain) Apply(ctx context.Context, rec Record) (Record, error) {
	var err error
	for _, p := range c {
		rec, err = p.Process(ctx, rec)
		if err != nil {
			return rec, err
		}
	}
	return rec, nil
}

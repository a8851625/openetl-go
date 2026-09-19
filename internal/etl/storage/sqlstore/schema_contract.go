package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// ── Pipeline schema contracts (CH-C2, IT-5/T5.4) ──────────────────────

// SaveSchemaContract persists the contract for (pipeline, spec version),
// replacing any previous row for that version. The write is atomic and
// idempotent.
func (s *Store) SaveSchemaContract(ctx context.Context, c core.SchemaContract) error {
	if c.Pipeline == "" {
		return fmt.Errorf("schema contract pipeline is required")
	}
	if c.Fingerprint == "" {
		return fmt.Errorf("schema contract fingerprint is required")
	}
	colsJSON, err := json.Marshal(c.Columns)
	if err != nil {
		return fmt.Errorf("marshal schema contract columns: %w", err)
	}
	_, err = s.exec(ctx, s.dialect.SchemaContractUpsert(),
		c.Pipeline, c.SpecVersion, c.Fingerprint, string(colsJSON), c.SourceType, c.Database, c.Table)
	return err
}

// LoadSchemaContract returns the contract for the exact (pipeline, version),
// or nil when this version has no stored contract.
func (s *Store) LoadSchemaContract(ctx context.Context, pipeline string, version int64) (*core.SchemaContract, error) {
	row := s.queryRow(ctx, `
SELECT pipeline, version, fingerprint, columns_json, source_type, database, table_name
FROM pipeline_schema_contracts WHERE pipeline = ? AND version = ?`, pipeline, version)
	return scanSchemaContract(row)
}

// LoadLatestSchemaContract returns the newest contract for the pipeline, or
// nil when the pipeline never captured one.
func (s *Store) LoadLatestSchemaContract(ctx context.Context, pipeline string) (*core.SchemaContract, error) {
	row := s.queryRow(ctx, `
SELECT pipeline, version, fingerprint, columns_json, source_type, database, table_name
FROM pipeline_schema_contracts WHERE pipeline = ?
ORDER BY version DESC LIMIT 1`, pipeline)
	return scanSchemaContract(row)
}

// ListSchemaContracts returns every stored contract for the pipeline,
// oldest first (backup/export path).
func (s *Store) ListSchemaContracts(ctx context.Context, pipeline string) ([]core.SchemaContract, error) {
	rows, err := s.query(ctx, `
SELECT pipeline, version, fingerprint, columns_json, source_type, database, table_name
FROM pipeline_schema_contracts WHERE pipeline = ? ORDER BY version ASC`, pipeline)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []core.SchemaContract
	for rows.Next() {
		var c core.SchemaContract
		var colsJSON string
		if err := rows.Scan(&c.Pipeline, &c.SpecVersion, &c.Fingerprint, &colsJSON, &c.SourceType, &c.Database, &c.Table); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(colsJSON), &c.Columns); err != nil {
			return nil, fmt.Errorf("unmarshal schema contract columns for %s v%d: %w", pipeline, c.SpecVersion, err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteSchemaContracts removes every contract row for the pipeline
// (pipeline delete path).
func (s *Store) DeleteSchemaContracts(ctx context.Context, pipeline string) error {
	_, err := s.exec(ctx, `DELETE FROM pipeline_schema_contracts WHERE pipeline = ?`, pipeline)
	return err
}

func scanSchemaContract(row *sql.Row) (*core.SchemaContract, error) {
	var c core.SchemaContract
	var colsJSON string
	err := row.Scan(&c.Pipeline, &c.SpecVersion, &c.Fingerprint, &colsJSON, &c.SourceType, &c.Database, &c.Table)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(colsJSON), &c.Columns); err != nil {
		return nil, fmt.Errorf("unmarshal schema contract columns: %w", err)
	}
	return &c, nil
}

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

type referenceGuardKey struct{ store *Store }

// ConfigureReferenceCipher sets immutable decode policy, not a plaintext cache.
func (s *Store) ConfigureReferenceCipher(cipher *storage.SpecCipher) {
	if cipher != nil {
		s.referenceCipher.Store(cipher)
	}
}

// EnsureReferenceGuard is called under the migration lock by every backend.
// This coordination row is operational metadata and is not wiped/restored as
// user data. UPDATE takes a database write lock across processes and backends.
func (s *Store) EnsureReferenceGuard(ctx context.Context) error {
	_, err := s.exec(ctx, s.dialect.InsertIgnore("connection_reference_guard", "id", "?"), 1)
	return err
}

func (s *Store) withReferenceGuard(ctx context.Context, fn func(context.Context) error) error {
	if guarded, _ := ctx.Value(referenceGuardKey{store: s}).(bool); guarded && s.txFromContext(ctx) != nil {
		return fn(ctx)
	}
	return s.WithTx(ctx, func(txCtx context.Context) error {
		result, err := s.exec(txCtx, "UPDATE connection_reference_guard SET generation=generation+1 WHERE id=1")
		if err != nil {
			return fmt.Errorf("lock connection references: %w", err)
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			return fmt.Errorf("connection reference guard is unavailable")
		}
		return fn(context.WithValue(txCtx, referenceGuardKey{store: s}, true))
	})
}

func (s *Store) currentReadSuffix() string {
	switch s.dialect.(type) {
	case MySQLDialect, PostgresDialect:
		return " FOR UPDATE"
	default:
		return ""
	}
}

func (s *Store) referenceReport(ctx context.Context, name string) (*storage.ConnectionReferenceReport, error) {
	// Locking reads avoid a stale Repeatable Read snapshot when called from
	// an existing transaction. Close rows before issuing connection queries.
	rows, err := s.query(ctx, "SELECT id,name,spec_yaml FROM pipelines ORDER BY id"+s.currentReadSuffix())
	if err != nil {
		return nil, err
	}
	var pipelines []*storage.PipelineRow
	for rows.Next() {
		row := new(storage.PipelineRow)
		if err := rows.Scan(&row.ID, &row.Name, &row.SpecYAML); err != nil {
			rows.Close()
			return nil, err
		}
		pipelines = append(pipelines, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	report := &storage.ConnectionReferenceReport{ConnectionName: name, Known: true,
		References: []storage.ConnectionReference{}, Unreadable: []storage.UnreadableReferenceSpec{}}
	for _, row := range pipelines {
		refs, err := storage.ExtractConnectionReferences(row, s.referenceCipher.Load())
		if err != nil {
			report.Known = false
			report.Unreadable = append(report.Unreadable, storage.UnreadableReferenceSpec{
				PipelineID: row.ID, PipelineName: row.Name, Code: "spec_unreadable"})
			continue
		}
		for _, ref := range refs {
			if ref.ConnectionName == name {
				report.References = append(report.References, ref)
			}
		}
	}
	return report, nil
}

func (s *Store) ConnectionReferences(ctx context.Context, name string) (*storage.ConnectionReferenceReport, error) {
	var report *storage.ConnectionReferenceReport
	err := s.withReferenceGuard(ctx, func(txCtx context.Context) error {
		var err error
		report, err = s.referenceReport(txCtx, name)
		return err
	})
	return report, err
}

func referenceConflict(code, message string, report *storage.ConnectionReferenceReport) error {
	return &storage.ConnectionReferenceConflict{Code: code, Message: message, Report: *report}
}

func (s *Store) validateCurrentReferences(ctx context.Context, row *storage.PipelineRow) error {
	refs, err := storage.ExtractConnectionReferences(row, s.referenceCipher.Load())
	if err != nil {
		if !strings.HasPrefix(row.SpecYAML, "enc:") {
			// Raw legacy metadata can be opaque. It remains repairable, but
			// referenceReport fails closed on it. Runtime validation rejects
			// malformed specs before they reach this raw storage interface.
			return nil
		}
		existing, loadErr := s.GetPipeline(ctx, row.ID)
		if loadErr == nil && existing != nil && existing.SpecYAML == row.SpecYAML {
			return nil // lifecycle repair must not require a missing old key.
		}
		return referenceConflict("spec_unreadable", "cannot validate encrypted pipeline connection references",
			&storage.ConnectionReferenceReport{Known: false, Unreadable: []storage.UnreadableReferenceSpec{{
				PipelineID: row.ID, PipelineName: row.Name, Code: "spec_unreadable"}}})
	}
	for _, ref := range refs {
		var kind, typ string
		err := s.queryRow(ctx, "SELECT kind,type FROM connections WHERE name=?"+s.currentReadSuffix(), ref.ConnectionName).Scan(&kind, &typ)
		if err != nil || kind != ref.Kind || (strings.TrimSpace(ref.Type) != "" && typ != ref.Type) {
			if err != nil && err != sql.ErrNoRows {
				return fmt.Errorf("validate pipeline connection reference: %w", err)
			}
			return referenceConflict("connection_missing_or_incompatible",
				fmt.Sprintf("connection %q is missing or incompatible with %s", ref.ConnectionName, ref.Endpoint),
				&storage.ConnectionReferenceReport{ConnectionName: ref.ConnectionName, Known: true,
					References: []storage.ConnectionReference{ref}, Unreadable: []storage.UnreadableReferenceSpec{}})
		}
	}
	return nil
}

package server

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// Deliberately damaged pre-upgrade metadata must use the offline fidelity
// path. Normal current-spec writes now reject dangling/encrypted unknown refs.
func seedLegacyMetadataFixture(t *testing.T, store storage.Storage, row *storage.PipelineRow) {
	t.Helper()
	storage.EnsurePipelineID(row)
	if row.CreatedAt.IsZero() {
		row.CreatedAt = time.Now().UTC()
	}
	if row.UpdatedAt.IsZero() {
		row.UpdatedAt = row.CreatedAt
	}
	w := storage.UnwrapStorage(store).(interface {
		WithTx(context.Context, func(context.Context) error) error
		WritePipelineFidelity(context.Context, *storage.PipelineRow) error
	})
	if err := w.WithTx(context.Background(), func(ctx context.Context) error {
		return w.WritePipelineFidelity(ctx, row)
	}); err != nil {
		t.Fatal(err)
	}
}

func corruptSavedSpecFixture(t *testing.T, store storage.Storage, id, text string) {
	t.Helper()
	db := storage.UnwrapStorage(store).(interface{ DB() *sql.DB }).DB()
	if _, err := db.Exec("UPDATE pipelines SET spec_yaml=? WHERE id=?", text, id); err != nil {
		t.Fatal(err)
	}
}

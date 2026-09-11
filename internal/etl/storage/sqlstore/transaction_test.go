package sqlstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestTransactionReadsAndRollback(t *testing.T) {
	s, err := sqlite.New(filepath.Join(t.TempDir(), "tx.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	abort := errors.New("abort transaction")
	err = s.WithTx(ctx, func(txCtx context.Context) error {
		if err := s.SavePipeline(txCtx, &storage.PipelineRow{ID: "tx-id", Name: "tx-name", Status: "stopped"}); err != nil {
			return err
		}
		row, err := s.GetPipeline(txCtx, "tx-id")
		if err != nil || row == nil {
			t.Fatalf("queryRow did not see uncommitted write: %+v %v", row, err)
		}
		rows, err := s.ListPipelines(txCtx)
		if err != nil || len(rows) != 1 {
			t.Fatalf("query did not see uncommitted write: %+v %v", rows, err)
		}
		for want := 1; want <= 2; want++ {
			v, err := s.SavePipelineVersion(txCtx, "tx-id", "history")
			if err != nil || v != want {
				t.Fatalf("version allocation did not share transaction: %d %v", v, err)
			}
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	rows, err := s.ListPipelines(ctx)
	if err != nil || len(rows) != 0 {
		t.Fatalf("transaction leaked rows: %+v %v", rows, err)
	}
	versions, err := s.ListPipelineVersions(ctx, "tx-id")
	if err != nil || len(versions) != 0 {
		t.Fatalf("transaction leaked versions: %+v %v", versions, err)
	}
}

func TestTransactionContextIsScopedToStore(t *testing.T) {
	open := func() *sqlite.Store {
		s, err := sqlite.New(filepath.Join(t.TempDir(), "tx.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	a, b := open(), open()
	ctx := context.Background()
	abort := errors.New("rollback a only")
	err := a.WithTx(ctx, func(txCtx context.Context) error {
		if err := b.SavePipeline(txCtx, &storage.PipelineRow{ID: "b", Name: "b", Status: "stopped"}); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	row, err := b.GetPipeline(ctx, "b")
	if err != nil || row == nil {
		t.Fatalf("a's context hijacked a write belonging to b: %+v %v", row, err)
	}
}

func TestTransactionPanicReleasesWriter(t *testing.T) {
	s, err := sqlite.New(filepath.Join(t.TempDir(), "panic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected panic to propagate")
			}
		}()
		_ = s.WithTx(context.Background(), func(ctx context.Context) error {
			if err := s.SavePipeline(ctx, &storage.PipelineRow{ID: "p", Name: "p", Status: "stopped"}); err != nil {
				t.Fatal(err)
			}
			panic("test panic")
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.SavePipeline(ctx, &storage.PipelineRow{ID: "after", Name: "after", Status: "stopped"}); err != nil {
		t.Fatalf("panic left the writer locked: %v", err)
	}
	if row, err := s.GetPipeline(ctx, "p"); err != nil || row != nil {
		t.Fatalf("panic committed its data: %+v %v", row, err)
	}
}

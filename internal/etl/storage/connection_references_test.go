package storage_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func testConnectionReferences(t *testing.T, newStore func(*testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
	defer cleanup()
	ctx := context.Background()
	for _, c := range []*storage.ConnectionEntry{
		{Name: "src", Kind: "source", Type: "demo"},
		{Name: "dim", Kind: "transform", Type: "lookup"},
		{Name: "dst", Kind: "sink", Type: "mysql"},
		{Name: "history-only", Kind: "sink", Type: "file_sink"},
	} {
		if err := s.SaveConnection(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	linear := &storage.PipelineRow{ID: "ref-linear", Name: "same display name", SpecYAML: `
source: {type: demo, connection: src}
transforms: [{type: lookup, connection_ref: dim}]
sink: {type: mysql, connection: dst}
dlq: {sink: {type: mysql, connection_ref: dst}}
`, Status: "stopped"}
	dag := &storage.PipelineRow{ID: "ref-dag", Name: linear.Name, SpecYAML: `
dag:
  nodes:
    - {id: read, kind: source, plugin: demo, connection_ref: src}
    - {id: enrich, kind: lookup, plugin: lookup, connection: dim}
    - {id: write, kind: sink, plugin: mysql, connection_ref: dst}
`, Status: "stopped"}
	for _, row := range []*storage.PipelineRow{linear, dag} {
		if err := s.SavePipeline(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SavePipelineVersion(ctx, linear.ID, "sink: {connection: history-only}"); err != nil {
		t.Fatal(err)
	}
	guard := s.(storage.ConnectionReferenceStore)
	for name, want := range map[string]int{"src": 2, "dim": 2, "dst": 3, "history-only": 0} {
		report, err := guard.ConnectionReferences(ctx, name)
		if err != nil || !report.Known || len(report.References) != want {
			t.Fatalf("%s report = %+v, err=%v, want %d", name, report, err, want)
		}
		for _, ref := range report.References {
			if ref.PipelineID == "ref-dag" && ref.NodeID == "" {
				t.Fatal("DAG reference lost node identity")
			}
		}
	}
	if err := s.DeleteConnection(ctx, "dst"); !errors.Is(err, storage.ErrConnectionReferenceConflict) {
		t.Fatalf("referenced delete = %v", err)
	}
	if err := s.SaveConnection(ctx, &storage.ConnectionEntry{Name: "dst", Kind: "sink", Type: "postgres"}); !errors.Is(err, storage.ErrConnectionReferenceConflict) {
		t.Fatalf("incompatible type change = %v", err)
	}
	if err := s.SaveConnection(ctx, &storage.ConnectionEntry{Name: "dst", Kind: "sink", Type: "mysql", Config: map[string]any{"host": "updated"}}); err != nil {
		t.Fatalf("compatible config update: %v", err)
	}
	if err := s.DeleteConnection(ctx, "history-only"); err != nil {
		t.Fatalf("historical version must not block deletion: %v", err)
	}
	bad := &storage.PipelineRow{ID: "legacy-unreadable", Name: "repair this", SpecYAML: "broken: [", Status: "restore_failed"}
	if err := s.SavePipeline(ctx, bad); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteConnection(ctx, "unused"); !errors.Is(err, storage.ErrConnectionReferenceConflict) {
		t.Fatalf("unreadable legacy spec did not fail closed: %v", err)
	}
	for _, row := range []*storage.PipelineRow{linear, dag, bad} {
		row.SpecYAML = "name: repaired\n"
		if err := s.SavePipeline(ctx, row); err != nil {
			t.Fatalf("repair current spec: %v", err)
		}
	}
	if err := s.DeleteConnection(ctx, "dst"); err != nil {
		t.Fatalf("dereferenced connection did not delete: %v", err)
	}
	// The current/version write must reuse an existing transaction. An outer
	// failure rolls back both its spec and its references and releases the guard.
	tx := s.(interface {
		WithTx(context.Context, func(context.Context) error) error
	})
	abort := errors.New("abort reference transaction")
	err := tx.WithTx(ctx, func(txCtx context.Context) error {
		if err := storage.NewPipelineSpecStore(s).SaveWithID(txCtx, "rolled-back", "rollback", "source: {type: demo, connection: src}", "stopped"); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if row, err := s.GetPipeline(ctx, "rolled-back"); err != nil || row != nil {
		t.Fatalf("outer transaction left current row: %+v, %v", row, err)
	}
}

func TestConnectionReferenceTransactionsAcrossBackends(t *testing.T) {
	for _, backend := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var open func() (storage.Storage, error)
			switch backend {
			case "sqlite":
				path := filepath.Join(t.TempDir(), "refs.db")
				open = func() (storage.Storage, error) { return sqlite.New(path) }
			case "mysql":
				dsn := os.Getenv("MYSQL_DSN")
				if dsn == "" {
					t.Skip("MYSQL_DSN unavailable")
				}
				open = func() (storage.Storage, error) { return mysql.New(dsn) }
			case "postgres":
				dsn := os.Getenv("POSTGRES_DSN")
				if dsn == "" {
					t.Skip("POSTGRES_DSN unavailable")
				}
				open = func() (storage.Storage, error) { return postgres.New(ctx, dsn) }
			}
			a, err := open()
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			b, err := open() // independent SQL pools, including SQLite's writer.
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			for iteration := 0; iteration < 12; iteration++ {
				name := fmt.Sprintf("race-%s-%d", backend, iteration)
				if err := a.SaveConnection(ctx, &storage.ConnectionEntry{Name: name, Kind: "sink", Type: "mysql"}); err != nil {
					t.Fatal(err)
				}
				row := &storage.PipelineRow{ID: name, Name: "race display", SpecYAML: "name: old", Status: "stopped"}
				if iteration%2 == 1 { // alternate new spec creation and update.
					if err := a.SavePipeline(ctx, row); err != nil {
						t.Fatal(err)
					}
				}
				row.SpecYAML = "sink: {type: mysql, connection: " + name + "}"
				start := make(chan struct{})
				var wg sync.WaitGroup
				var saveErr, deleteErr error
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					saveErr = storage.NewPipelineSpecStore(a).SaveWithID(ctx, row.ID, row.Name, row.SpecYAML, row.Status)
				}()
				go func() {
					defer wg.Done()
					<-start
					if iteration%4 >= 2 {
						deleteErr = b.SaveConnection(ctx, &storage.ConnectionEntry{Name: name, Kind: "sink", Type: "postgres"})
					} else {
						deleteErr = b.DeleteConnection(ctx, name)
					}
				}()
				close(start)
				wg.Wait()
				if (saveErr == nil) == (deleteErr == nil) {
					t.Fatalf("expected one commit and one conflict: save=%v delete=%v", saveErr, deleteErr)
				}
				loser := saveErr
				if loser == nil {
					loser = deleteErr
				}
				if !errors.Is(loser, storage.ErrConnectionReferenceConflict) {
					t.Fatalf("race loser is not a reference conflict: %v", loser)
				}
				current, err := a.GetPipeline(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if current != nil && current.SpecYAML == row.SpecYAML {
					conn, err := b.GetConnection(ctx, name)
					if err != nil || conn == nil || conn.Type != "mysql" {
						t.Fatalf("race left dangling reference: conn=%v err=%v", conn, err)
					}
				}
				if err := a.DeletePipeline(ctx, name); err != nil {
					t.Fatal(err)
				}
				if err := b.DeleteConnection(ctx, name); err != nil {
					t.Fatal(err)
				}
			}

			cipher, err := storage.NewSpecCipher("refs", base64.StdEncoding.EncodeToString(make([]byte, 32)), "")
			if err != nil {
				t.Fatal(err)
			}
			name := "encrypted-" + backend
			if err := a.SaveConnection(ctx, &storage.ConnectionEntry{Name: name, Kind: "sink", Type: "mysql"}); err != nil {
				t.Fatal(err)
			}
			specs := storage.NewPipelineSpecStore(a, cipher)
			if err := specs.SaveWithID(ctx, name, name, "sink: {type: mysql, connection: "+name+"}", "stopped"); err != nil {
				t.Fatal(err)
			}
			// A newly opened process has no cached references or decode key.
			fresh, err := open()
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			report, err := fresh.(storage.ConnectionReferenceStore).ConnectionReferences(ctx, name)
			if err != nil || report.Known || len(report.Unreadable) != 1 {
				t.Fatalf("missing-key report = %+v, err=%v", report, err)
			}
			if err := fresh.DeleteConnection(ctx, name); !errors.Is(err, storage.ErrConnectionReferenceConflict) {
				t.Fatalf("encrypted restart reference lost: %v", err)
			}
			storage.NewPipelineSpecStore(fresh, cipher)
			report, err = fresh.(storage.ConnectionReferenceStore).ConnectionReferences(ctx, name)
			if err != nil || !report.Known || len(report.References) != 1 {
				t.Fatalf("keyed restart report = %+v, err=%v", report, err)
			}
			if err := fresh.DeletePipeline(ctx, name); err != nil {
				t.Fatal(err)
			}
			if err := fresh.DeleteConnection(ctx, name); err != nil {
				t.Fatal(err)
			}
		})
	}
}

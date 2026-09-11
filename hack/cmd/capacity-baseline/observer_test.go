package main

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestObservedStoreKeepsTransactionAndError(t *testing.T) {
	st, err := sqlite.New(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	obs := newObserver(st.DB())
	obs.read = func(db *sql.DB) (processSample, error) { return processSample{Pool: poolStats(db)}, nil }
	s := &observedStore{Store: st.Store, observer: obs}
	if _, err := obs.begin(); err != nil {
		t.Fatal(err)
	}
	cp := &storage.CheckpointRecord{JobName: "fixture", Source: "kafka", Position: []byte(`{"offsets":{"0":42}}`), Timestamp: time.Now()}
	if err := s.SaveCheckpoint(context.Background(), cp); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("rollback sentinel")
	err = s.WithTx(context.Background(), func(ctx context.Context) error {
		if err := s.DeleteCheckpoint(ctx, "fixture"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("transaction error changed: %v", err)
	}
	persisted, err := s.LoadCheckpoint(context.Background(), "fixture")
	if err != nil || persisted == nil || string(persisted.Position) != string(cp.Position) {
		t.Fatal("observed store failed to roll back a transaction")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.SaveCheckpoint(ctx, cp); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkpoint error changed: %v", err)
	}
	r, err := obs.end()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Checkpoints) != 2 || r.Checkpoints[0].Failed || !r.Checkpoints[1].Failed {
		t.Fatalf("errors missing: %+v", r.Checkpoints)
	}
	if string(r.EndPositions["fixture"]) != string(cp.Position) {
		t.Fatal("position differs")
	}
}

func TestObservedStorePreservesGenerationFence(t *testing.T) {
	st, err := sqlite.New(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &observedStore{Store: st.Store, observer: newObserver(st.DB())}
	ctx := context.Background()
	row := &storage.PipelineRow{ID: "p", Name: "p", DesiredState: "running", ObservedState: "stopped"}
	if err := s.SavePipeline(ctx, row); err != nil {
		t.Fatal(err)
	}
	generation, err := s.BeginPipelineGeneration(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	adapter := storage.NewCheckpointStoreAdapter(s)
	cp := core.Checkpoint{JobName: "p", Source: "kafka", Position: []byte(`{"offsets":{"0":7}}`), Timestamp: time.Now()}
	fenced := storage.WithCheckpointFence(ctx, "p", generation)
	if err := adapter.Save(fenced, cp); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginPipelineGeneration(ctx, "p"); err != nil {
		t.Fatal(err)
	}
	cp.Position = []byte(`{"offsets":{"0":999}}`)
	if err := adapter.Save(fenced, cp); !errors.Is(err, core.ErrCheckpointFenced) {
		t.Fatalf("stale generation was not fenced: %v", err)
	}
	persisted, err := s.LoadCheckpoint(ctx, "p")
	if err != nil || persisted == nil || string(persisted.Position) != `{"offsets":{"0":7}}` {
		t.Fatal("stale checkpoint replaced durable position")
	}
}

func TestObserverConcurrentWindowAndSnapshot(t *testing.T) {
	o := newObserver(nil)
	o.read = func(*sql.DB) (processSample, error) { return processSample{}, nil }
	if _, err := o.begin(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				cp := &storage.CheckpointRecord{JobName: "fixture", Position: []byte(`{"offsets":{"0":1}}`)}
				o.record(cp, time.Now(), time.Now(), nil)
				cp.Position[0] = 'x'
			}
		}()
	}
	wg.Wait()
	r, err := o.end()
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Checkpoints) != 1000 || r.EndPositions["fixture"][0] != '{' {
		t.Fatal("lost samples or aliased position")
	}
	if _, err := o.end(); err == nil {
		t.Fatal("accepted a second end")
	}
}

func TestRSSUnitsAndMissingData(t *testing.T) {
	rss, peak, err := parseRSS("VmRSS:\t1024 kB\nVmHWM:\t2048 kB\n")
	if err != nil || rss != 1<<20 || peak != 2<<20 {
		t.Fatalf("conversion: %d %d %v", rss, peak, err)
	}
	for _, raw := range []string{"", "VmRSS: 1 MB\nVmHWM: 2 MB\n", "VmRSS: 2 kB\nVmHWM: 1 kB\n"} {
		if _, _, err := parseRSS(raw); err == nil {
			t.Fatal("accepted invalid RSS")
		}
	}
}

func TestVerifierRejectsCorruptionDuplicatesAndMissingPrefix(t *testing.T) {
	v := newVerifier(3)
	bad := expectedRow(1)
	bad.Amount++
	if v.add(bad) == nil {
		t.Fatal("accepted corruption")
	}
	if err := v.add(expectedRow(1)); err != nil {
		t.Fatal(err)
	}
	if v.add(expectedRow(1)) == nil {
		t.Fatal("accepted duplicate")
	}
	if _, err := v.finish(-1); err == nil {
		t.Fatal("accepted incomplete full result")
	}
	if r, err := v.finish(1); err != nil || r.Full {
		t.Fatal("valid prefix not distinguished from full")
	}
	if err := v.add(expectedRow(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := v.finish(1); err == nil {
		t.Fatal("accepted a hole in target prefix")
	}
}

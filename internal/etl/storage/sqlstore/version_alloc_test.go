package sqlstore_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestConcurrentPipelineVersionAllocationIsUnique(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	row := &storage.PipelineRow{
		ID:       "pipe-concurrent",
		Name:     "pipe-concurrent",
		SpecYAML: "name: pipe-concurrent\n",
		Status:   "loaded",
	}
	if err := store.SavePipeline(ctx, row); err != nil {
		t.Fatalf("seed pipeline: %v", err)
	}

	const workers = 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r := &storage.PipelineRow{
				ID:       row.ID,
				Name:     row.Name,
				SpecYAML: "name: pipe-concurrent\nrevision: " + string(rune('a'+i%26)) + "\n",
				Status:   "updated",
			}
			errs <- store.SavePipelineWithVersion(ctx, r, r.SpecYAML)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("SavePipelineWithVersion: %v", err)
		}
	}

	versions, err := store.ListPipelineVersions(ctx, row.ID)
	if err != nil {
		t.Fatalf("ListPipelineVersions: %v", err)
	}
	if len(versions) != workers {
		t.Fatalf("version count = %d, want %d", len(versions), workers)
	}
	seen := map[int]bool{}
	for _, v := range versions {
		if seen[v.Version] {
			t.Fatalf("duplicate version %d", v.Version)
		}
		seen[v.Version] = true
	}
	for want := 1; want <= workers; want++ {
		if !seen[want] {
			t.Fatalf("missing version %d in %#v", want, seen)
		}
	}
}

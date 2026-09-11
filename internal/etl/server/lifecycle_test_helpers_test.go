package server

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func saveLifecycleTestPipeline(t *testing.T, s *Server, id, name, status string) {
	t.Helper()
	if name == "" {
		name = id
	}
	if status == "" {
		status = storage.PipelineDesiredStopped
	}
	if err := s.store.SavePipeline(context.Background(), &storage.PipelineRow{
		ID:       id,
		Name:     name,
		SpecYAML: "name: " + name,
		Status:   status,
	}); err != nil {
		t.Fatalf("SavePipeline(%s): %v", id, err)
	}
}

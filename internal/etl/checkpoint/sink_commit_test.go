package checkpoint

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

type commitMetadataSink struct {
	metadata map[string]any
	err      error
}

func (s commitMetadataSink) Name() string                               { return "commit-test" }
func (s commitMetadataSink) Open(context.Context) error                 { return nil }
func (s commitMetadataSink) Write(context.Context, []core.Record) error { return nil }
func (s commitMetadataSink) Close() error                               { return nil }
func (s commitMetadataSink) SinkCommitMetadata(context.Context) (map[string]any, error) {
	return s.metadata, s.err
}

func TestBuildSinkCommitMetadata(t *testing.T) {
	records := []core.Record{{
		Data:     map[string]any{"id": 7},
		Metadata: core.Metadata{Source: "kafka", Table: "orders", Partition: 2, Offset: 41},
	}}
	meta, err := BuildSinkCommitMetadata(context.Background(), commitMetadataSink{
		metadata: map[string]any{"transaction": "tx-7"},
	}, records, "sink-node")
	if err != nil {
		t.Fatalf("BuildSinkCommitMetadata: %v", err)
	}
	if meta["sink"] != "commit-test" || meta["node"] != "sink-node" || meta["record_count"] != 1 {
		t.Fatalf("metadata = %#v", meta)
	}
	if meta["last_batch_sha256"] == "" {
		t.Fatalf("missing batch digest: %#v", meta)
	}
	native, ok := meta["native"].(map[string]any)
	if !ok || native["transaction"] != "tx-7" {
		t.Fatalf("native metadata = %#v", meta["native"])
	}
}

func TestBuildSinkCommitMetadataFailure(t *testing.T) {
	_, err := BuildSinkCommitMetadata(context.Background(), commitMetadataSink{err: errors.New("commit token unavailable")}, nil, "")
	if err == nil {
		t.Fatal("BuildSinkCommitMetadata = nil error")
	}
}

package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// BuildSinkCommitMetadata describes the sink-acknowledged batch immediately
// preceding a checkpoint. It does not claim a cross-system transaction: the
// source position is still persisted after the sink acknowledgement, so a
// checkpoint-store failure deliberately causes at-least-once replay.
func BuildSinkCommitMetadata(ctx context.Context, sink core.Sink, records []core.Record, node string) (map[string]any, error) {
	payload, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("marshal sink-committed batch: %w", err)
	}
	sum := sha256.Sum256(payload)
	meta := map[string]any{
		"acknowledged":      true,
		"sink":              sink.Name(),
		"record_count":      len(records),
		"last_batch_sha256": hex.EncodeToString(sum[:]),
	}
	if node != "" {
		meta["node"] = node
	}
	if len(records) > 0 {
		last := records[len(records)-1]
		meta["last_record"] = map[string]any{
			"source":    last.Metadata.Source,
			"table":     last.Metadata.Table,
			"partition": last.Metadata.Partition,
			"offset":    last.Metadata.Offset,
		}
	}
	if provider, ok := sink.(core.SinkCommitMetadataProvider); ok {
		native, err := provider.SinkCommitMetadata(ctx)
		if err != nil {
			return nil, fmt.Errorf("sink commit metadata: %w", err)
		}
		if len(native) > 0 {
			meta["native"] = native
		}
	}
	return meta, nil
}

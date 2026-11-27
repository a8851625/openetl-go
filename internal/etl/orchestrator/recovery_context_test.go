package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

type recoveryCaptureDLQ struct{ entries []pipeline.DLQEntry }

func (d *recoveryCaptureDLQ) WriteDLQ(_ context.Context, entry pipeline.DLQEntry) error {
	d.entries = append(d.entries, entry)
	return nil
}
func (*recoveryCaptureDLQ) Close() error { return nil }

func TestDAGRecoveryCaptureUsesActualFailedNode(t *testing.T) {
	writer := &recoveryCaptureDLQ{}
	manager := alert.NewManager()
	defer manager.Close()
	exec := &DAGExecutor{
		spec: &PipelineSpec{Name: "dag-owner", DAG: DAG{Nodes: []*Node{
			{ID: "shape", Kind: KindTransform, Plugin: "identity", ConnectionRef: "shape-connection"},
			{ID: "write", Kind: KindSink, Plugin: "mysql", Connection: "sink-connection"},
		}}},
		dlqWriter: writer, alertMgr: manager,
	}
	for _, node := range []string{"shape", "write", "missing"} {
		if !exec.handleFailed(context.Background(), core.Record{Data: map[string]any{"id": 1}}, errors.New("fixture failure"), node) {
			t.Fatal("durable capture failed")
		}
	}
	for i, connection := range []string{"shape-connection", "sink-connection"} {
		entry := writer.entries[i]
		if entry.JobName != "dag-owner" || entry.DAGNode != entry.RecoveryContext.NodeID || entry.RecoveryContext.Endpoint != "dag_node" || !entry.RecoveryContext.ConnectionKnown || entry.RecoveryContext.ConnectionName != connection {
			t.Fatalf("actual node context=%+v", entry)
		}
	}
	if entry := writer.entries[2]; entry.RecoveryContext.Endpoint != "" || entry.RecoveryContext.ConnectionKnown {
		t.Fatalf("missing node was inferred: %+v", entry)
	}
}

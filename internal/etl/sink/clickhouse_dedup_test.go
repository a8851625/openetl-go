package sink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// CH-C1 (IT-5/T5.1): dedup token derivation is deterministic — the same
// batch identity must always produce the same token, and distinct batches
// must not collide.
func TestDeriveDedupTokenDeterministic(t *testing.T) {
	a := deriveDedupToken("pipe", "mysql-bin.000001:456", "mysql-bin.000001:789", 7, 100)
	b := deriveDedupToken("pipe", "mysql-bin.000001:456", "mysql-bin.000001:789", 7, 100)
	if a != b {
		t.Fatalf("same batch inputs must derive the same token: %s != %s", a, b)
	}
	if len(a) != 64 { // sha256 hex
		t.Fatalf("token must be sha256 hex, got len %d", len(a))
	}

	// Distinct inputs must differ.
	cases := [][5]any{
		{"pipe2", "mysql-bin.000001:456", "mysql-bin.000001:789", uint64(7), 100},
		{"pipe", "mysql-bin.000001:457", "mysql-bin.000001:789", uint64(7), 100},
		{"pipe", "mysql-bin.000001:456", "mysql-bin.000001:790", uint64(7), 100},
		{"pipe", "mysql-bin.000001:456", "mysql-bin.000001:789", uint64(8), 100},
		{"pipe", "mysql-bin.000001:456", "mysql-bin.000001:789", uint64(7), 101},
	}
	for i, c := range cases {
		other := deriveDedupToken(c[0].(string), c[1].(string), c[2].(string), c[3].(uint64), c[4].(int))
		if other == a {
			t.Errorf("case %d: distinct input must derive distinct token", i)
		}
	}

	// Boundary-safety: first/last positions must not be separable by a lone
	// zero byte collision (component separation uses NUL separators).
	x := deriveDedupToken("p", "a\x00b", "c", 1, 1)
	y := deriveDedupToken("p", "a", "b\x00c", 1, 1)
	if x == y {
		t.Error("NUL-separated components must not alias")
	}
}

func TestBatchPositionBoundaries(t *testing.T) {
	mk := func(file string, pos uint32, part int32, off int64) core.Record {
		return core.Record{
			Operation: core.OpInsert,
			Metadata: core.Metadata{
				BinlogFile: file,
				BinlogPos:  pos,
				Partition:  part,
				Offset:     off,
			},
		}
	}
	first, last, ok := batchPosition([]core.Record{mk("f1", 10, 0, 0), mk("f1", 20, 0, 0)})
	if !ok || first != "f1:10" || last != "f1:20" {
		t.Fatalf("binlog positions: first=%q last=%q ok=%v", first, last, ok)
	}

	// Kafka records: partition@offset.
	_, last2, ok2 := batchPosition([]core.Record{{
		Operation: core.OpInsert,
		Metadata:  core.Metadata{Partition: 3, Offset: 99},
	}})
	if !ok2 || last2 != "kafka:3@99" {
		t.Fatalf("kafka position: last=%q ok=%v", last2, ok2)
	}

	// DDL-only or unpositioned batches report ok=false.
	_, _, ok3 := batchPosition([]core.Record{{Operation: core.OpDDL}})
	if ok3 {
		t.Fatal("DDL-only batch must not report a position boundary")
	}
}

func TestClassifyClickHouseWriteError(t *testing.T) {
	// Transport torn mid-flight → transient "ack unknown".
	err := classifyClickHouseWriteError(errors.New("connection reset by peer"))
	ce := core.ClassifiedError{}
	if !errors.As(err, &ce) || ce.Class != core.ErrorClassTransient {
		t.Fatalf("connection reset must classify transient, got %#v", err)
	}
	if !strings.Contains(err.Error(), "ack unknown") {
		t.Fatalf("transport failure must be documented as ack-unknown: %v", err)
	}

	// Server rejected before write → schema class, explicitly not-acked.
	err = classifyClickHouseWriteError(errors.New("Unknown column x"))
	if !errors.As(err, &ce) || ce.Class != core.ErrorClassSchema {
		t.Fatalf("unknown column must classify schema, got %#v", err)
	}
	if !strings.Contains(err.Error(), "not acked") {
		t.Fatalf("pre-write rejection must be documented as not-acked: %v", err)
	}

	// Auth rejection → auth class.
	err = classifyClickHouseWriteError(errors.New("Authentication failed: password is incorrect"))
	if !errors.As(err, &ce) || ce.Class != core.ErrorClassAuth {
		t.Fatalf("auth failure must classify auth, got %#v", err)
	}

	// Unknown shapes default to ack-unknown (replay-safe), never not-acked.
	err = classifyClickHouseWriteError(errors.New("some novel failure"))
	if !errors.As(err, &ce) || ce.Class != core.ErrorClassUnknown {
		t.Fatalf("novel failure must stay unknown, got %#v", err)
	}
	if !strings.Contains(err.Error(), "ack unknown") {
		t.Fatalf("novel failure must be replay-safe: %v", err)
	}
}

func TestSinkCommitMetadataContract(t *testing.T) {
	s := &ClickHouseSink{protocol: "native"}
	s.SetPipelineKey("")
	if k, _ := s.dedupPipelineKey.Load().(string); k != "clickhouse-sink" {
		t.Fatalf("empty pipeline key must fall back, got %q", k)
	}

	// No acknowledged batch → error (checkpoint must not advance).
	if _, err := s.SinkCommitMetadata(context.Background()); err == nil {
		t.Fatal("SinkCommitMetadata without an acknowledged batch must error")
	}

	recs := []core.Record{{
		Operation: core.OpInsert,
		Metadata:  core.Metadata{BinlogFile: "b", BinlogPos: 5},
	}}
	s.beginDedupBatch(recs)
	s.finalizeDedupBatch("p", recs, map[string]int{"t": 1})

	meta, err := s.SinkCommitMetadata(context.Background())
	if err != nil {
		t.Fatalf("acked batch must expose metadata: %v", err)
	}
	if meta["dedup_token"] == "" || meta["protocol"] != "native" {
		t.Fatalf("metadata missing token/protocol: %#v", meta)
	}
	if meta["dedup_mode"] != "record_only" {
		t.Fatalf("unsupported server must record record_only mode, got %v", meta["dedup_mode"])
	}
	if _, ok := meta["tables"].(map[string]any)["t"]; !ok {
		t.Fatalf("table boundary missing: %#v", meta["tables"])
	}

	// A failed Write invalidates the boundary: no stale token may survive.
	s.invalidateDedupBatch()
	if _, err := s.SinkCommitMetadata(context.Background()); err == nil {
		t.Fatal("invalidate must clear retrievable metadata")
	}

	// Token equality across a simulated replay of the same batch identity.
	s2 := &ClickHouseSink{protocol: "http"}
	s2.SetPipelineKey("p")
	s2.beginDedupBatch(recs)
	s2.finalizeDedupBatch("p", recs, map[string]int{"t": 1})
	m2, _ := s2.SinkCommitMetadata(context.Background())
	// batch seq differs (1 vs 1 across sinks) — tokens equal for same seq;
	// here seq increments, so compare determinism instead.
	if m2["dedup_token"] == "" {
		t.Fatal("http protocol must derive tokens too")
	}
	s3 := &ClickHouseSink{protocol: "http"}
	s3.SetPipelineKey("p")
	s3.beginDedupBatch(recs)
	if s3.pendingDedupToken() != s2.pendingDedupToken() {
		t.Fatal("same batch identity must re-derive the same pending token")
	}
}

// Probe fallback: a sink whose queryRowContext reports the setting exposes
// server dedup mode.
type probeRow struct{ fail bool }

func (p probeRow) Scan(...any) error {
	if p.fail {
		return fmt.Errorf("not found")
	}
	return nil
}

func TestDetectInsertDedupSupport(t *testing.T) {
	// queryRowContext is a method on the sink using real connections; the
	// probe path with no connection returns silently (record_only mode).
	s := &ClickHouseSink{}
	s.detectInsertDedupSupport(context.Background())
	if s.insertDedupSupported.Load() {
		t.Fatal("no connection must leave server dedup unsupported")
	}
	if !s.insertDedupProbeDone.Load() {
		t.Fatal("probe must be stamped done to avoid per-write probes")
	}
}

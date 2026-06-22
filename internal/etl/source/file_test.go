package source

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// TestFileCSVCheckpointResume verifies that after a checkpoint is saved,
// reopening the file resumes from the saved byte offset (not the beginning).
func TestFileCSVCheckpointResume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	content := "id,name\n1,alice\n2,bob\n3,carol\n4,dave\n5,eve\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	src, err := NewFileSource(map[string]any{"path": path, "format": "csv"})
	if err != nil {
		t.Fatal(err)
	}

	// Read 3 records then snapshot.
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := reader.ReadBatch(context.Background(), 3)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	cp, err := reader.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	_ = reader.Close()

	// Reopen from checkpoint: should get the remaining 2 records.
	resumed, err := src.Open(context.Background(), &cp)
	if err != nil {
		t.Fatalf("Open with cp: %v", err)
	}
	defer resumed.Close()

	got, err := resumed.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch resumed: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("after resume got %d records, want 2", len(got))
	}
	// Records should be dave (id=4) and eve (id=5).
	if got[0].Data["id"] != "4" {
		t.Errorf("first resumed record id = %v, want '4'", got[0].Data["id"])
	}
	if len(got) >= 2 && got[1].Data["id"] != "5" {
		t.Errorf("second resumed record id = %v, want '5'", got[1].Data["id"])
	}
}

// TestFileJSONCheckpointResume verifies JSON resume from byte offset.
func TestFileJSONCheckpointResume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.jsonl")
	content := `{"id":1,"name":"alice"}
{"id":2,"name":"bob"}
{"id":3,"name":"carol"}
{"id":4,"name":"dave"}
{"id":5,"name":"eve"}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	src, err := NewFileSource(map[string]any{"path": path, "format": "json"})
	if err != nil {
		t.Fatal(err)
	}

	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = reader.ReadBatch(context.Background(), 2)
	cp, err := reader.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	_ = reader.Close()

	resumed, err := src.Open(context.Background(), &cp)
	if err != nil {
		t.Fatalf("Open with cp: %v", err)
	}
	defer resumed.Close()
	got, err := resumed.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadBatch resumed: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("after resume got %d records, want 3", len(got))
	}
	if got[0].Data["id"] != float64(3) {
		t.Errorf("first resumed record id = %v, want 3", got[0].Data["id"])
	}
}

// TestFileCSVNoCheckpoint verifies a fresh start reads everything.
func TestFileCSVNoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte("id,name\n1,a\n2,b\n3,c\n"), 0644); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileSource(map[string]any{"path": path, "format": "csv"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	recs, err := reader.ReadBatch(context.Background(), 100)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 3 {
		t.Errorf("got %d records, want 3", len(recs))
	}
}

// TestFileCheckpointJSONRoundtrip verifies position serializes cleanly.
func TestFileCheckpointJSONRoundtrip(t *testing.T) {
	pos := filePosition{Offset: 42, ByteOffset: 1024, Headers: []string{"id", "name"}}
	data, err := json.Marshal(pos)
	if err != nil {
		t.Fatal(err)
	}
	var decoded filePosition
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Offset != pos.Offset || decoded.ByteOffset != pos.ByteOffset {
		t.Errorf("offset/byte mismatch: got %+v, want %+v", decoded, pos)
	}
	if len(decoded.Headers) != len(pos.Headers) {
		t.Errorf("headers len mismatch: got %d, want %d", len(decoded.Headers), len(pos.Headers))
	}
}

// TestFileCSVHeaderSkipOnResume verifies we don't re-read the header when
// resuming from a non-zero byte offset; headers come from the checkpoint.
func TestFileCSVHeaderSkipOnResume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	content := "id,name\n1,alice\n2,bob\n3,carol\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	src, _ := NewFileSource(map[string]any{"path": path, "format": "csv"})

	// Pretend we have a checkpoint at the byte offset AFTER the header+row 1,
	// WITH the headers persisted so resume uses them.
	headerLen := int64(len("id,name\n")) + int64(len("1,alice\n"))
	cp := core.Checkpoint{
		Position: mustMarshal(filePosition{Offset: 1, ByteOffset: headerLen, Headers: []string{"id", "name"}}),
	}
	reader, err := src.Open(context.Background(), &cp)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	recs, _ := reader.ReadBatch(context.Background(), 10)
	// Should get rows 2 and 3 (bob, carol), NOT the header.
	if len(recs) != 2 {
		t.Errorf("got %d records, want 2", len(recs))
	}
	if len(recs) > 0 && recs[0].Data["id"] != "2" {
		t.Errorf("first record id = %v, want '2' (data=%+v)", recs[0].Data["id"], recs[0].Data)
	}
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// TestFileJSONCheckpointNoCompoundOnRestart (P5-2) guards against the
// byte-offset-compounding regression in jsonReader: byteOffset was seeded with
// the absolute resume offset (should be 0), while Snapshot emitted
// byteOffsetBase+byteOffset — so the stored offset roughly DOUBLED on every
// restart and f.Seek skipped real records. The existing single-resume test does
// NOT catch this (reading is driven by Seek, and one resume happens to land
// correctly); this test restarts TWICE and asserts no skip + no compounding.
func TestFileJSONCheckpointNoCompoundOnRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.jsonl")
	content := `{"id":1}
{"id":2}
{"id":3}
{"id":4}
{"id":5}
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileSource(map[string]any{"path": path, "format": "json"})
	if err != nil {
		t.Fatal(err)
	}

	// Pass 1: read 2 records (id 1,2), snapshot cp1, close.
	r1, err := src.Open(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r1.ReadBatch(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	cp1, err := r1.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = r1.Close()

	// Pass 2: resume from cp1, read 1 record (id 3), snapshot cp2, close.
	r2, err := src.Open(context.Background(), &cp1)
	if err != nil {
		t.Fatal(err)
	}
	recs2, err := r2.ReadBatch(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs2) != 1 || recs2[0].Data["id"] != float64(3) {
		t.Fatalf("pass2: got %v, want id=3", recs2)
	}
	cp2, err := r2.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = r2.Close()

	// cp2 must NOT be ~2x cp1 (the compounding signature of the bug).
	var p1, p2 filePosition
	if err := json.Unmarshal(cp1.Position, &p1); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(cp2.Position, &p2); err != nil {
		t.Fatal(err)
	}
	if p1.ByteOffset > 0 && p2.ByteOffset >= 2*p1.ByteOffset {
		t.Errorf("offset compounded across restart: cp1=%d cp2=%d (cp2 should be ~cp1+1 record, not ~2x cp1)",
			p1.ByteOffset, p2.ByteOffset)
	}

	// Pass 3: resume from cp2 — must read exactly id 4 and 5 (no skip).
	r3, err := src.Open(context.Background(), &cp2)
	if err != nil {
		t.Fatal(err)
	}
	defer r3.Close()
	got, err := r3.ReadBatch(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("pass3: got %d records, want 2 (ids 4,5) — records were skipped by a compounded offset", len(got))
	}
	if got[0].Data["id"] != float64(4) || got[1].Data["id"] != float64(5) {
		t.Errorf("pass3: got ids %v, %v; want 4, 5", got[0].Data["id"], got[1].Data["id"])
	}
}

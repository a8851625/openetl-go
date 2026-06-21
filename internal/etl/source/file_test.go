package source

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"openetl-go/internal/etl/core"
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

package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// The >100k-per-table acceptance and process RSS measurement live in
// hack/e2e-backup-volume.sh. This unit test catches filtering, cursor boundaries,
// and JSON numeric loss cheaply on every normal/race run.
func TestBackupPagingBoundaryPagesExactly(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	fixed := time.Date(2026, 9, 1, 0, 0, 0, 123000000, time.UTC)
	snap := &backup.Snapshot{FormatVersion: 2}
	for i := 0; i < 2000; i++ { // Exactly two pages with repeated timestamps.
		id := int64(i*3 + 1)
		snap.AuditLogs = append(snap.AuditLogs, &storage.AuditEntry{ID: id, Action: fmt.Sprint(i), CreatedAt: fixed})
		snap.RunHistory = append(snap.RunHistory, &storage.RunRecord{ID: id, JobName: "deleted-pipeline", Status: "failed", StartedAt: fixed, RecordsRead: int64(i)})
		snap.Tasks = append(snap.Tasks, &storage.TaskAssignment{ID: id, TaskID: fmt.Sprint(i), Pipeline: "deleted-pipeline", Status: "completed", ShardTotal: 1, FinishedAt: &fixed})
		snap.DeadLetters = append(snap.DeadLetters, &storage.DLQRecord{ID: id, JobName: "deleted-pipeline", CreatedAt: fixed, Record: core.Record{Data: map[string]any{"id": json.Number("9007199254740993"), "sequence": i, "literal": "中'\\<>&"}}})
	}
	if err := backup.Restore(ctx, s, snap, backup.Options{ClearBeforeRestore: true}); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "export.json")
	counts, err := backup.ExportFile(ctx, s, file, backup.Options{})
	if err != nil || counts != backup.CountSnapshot(snap) {
		t.Fatalf("stream export counts=%+v err=%v", counts, err)
	}
	loaded, err := backup.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Counts != counts {
		t.Fatal("serialized counts differ")
	}
	for i, row := range loaded.DeadLetters {
		if row.ID != int64(i*3+1) || row.Record.Data["id"] != json.Number("9007199254740993") || row.Record.Data["sequence"] != json.Number(fmt.Sprint(i)) {
			t.Fatalf("row %d lost ID, precision or content", i)
		}
	}
	dest := openTestStore(t)
	if err := backup.Restore(ctx, dest, loaded, backup.Options{ClearBeforeRestore: true}); err != nil {
		t.Fatal(err)
	}
	assertSnapshotPayload(t, snap, exportForRestoreTest(t, dest))
	var record string
	if err := dest.(*sqlite.Store).DB().QueryRow("SELECT record_json FROM dead_letters WHERE id=1").Scan(&record); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(record, `"id":9007199254740993`) {
		t.Fatal("restored raw JSON rounded an integer business key")
	}
}

type brokenBackupReader struct {
	storage.Storage
	storage.BackupReader
	mode   string
	counts int
}

func (b *brokenBackupReader) CountObjects(ctx context.Context) (storage.ObjectCounts, error) {
	b.counts++
	if b.mode == "initial-count" || (b.mode == "final-count" && b.counts == 2) {
		return storage.ObjectCounts{}, errors.New("injected inventory failure")
	}
	return b.BackupReader.CountObjects(ctx)
}

func (b *brokenBackupReader) ReadBackupPage(ctx context.Context, table string, after *string, limit int) ([]storage.BackupRow, error) {
	if table == "run_history" && b.mode == "read" {
		return nil, errors.New("injected export read failure")
	}
	page, err := b.BackupReader.ReadBackupPage(ctx, table, after, limit)
	if table == "audit_logs" && b.mode == "truncated" && len(page) > 0 {
		page = page[:len(page)-1]
	}
	return page, err
}

func TestStreamingExportDoesNotPublishPartialBackup(t *testing.T) {
	for _, mode := range []string{"initial-count", "final-count", "read", "truncated", "cancelled", "malformed-json", "plaintext"} {
		t.Run(mode, func(t *testing.T) {
			s := openTestStore(t)
			seedControlPlane(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "cancelled":
				cancel()
			case "malformed-json":
				if _, err := s.(*sqlite.Store).DB().Exec("UPDATE dead_letters SET record_json='invalid-json'"); err != nil {
					t.Fatal(err)
				}
			case "plaintext":
				if err := s.SetSetting(ctx, "api_key", "must-never-publish-me"); err != nil {
					t.Fatal(err)
				}
			}
			wrapped := &brokenBackupReader{Storage: s, BackupReader: s.(storage.BackupReader), mode: mode}
			path := filepath.Join(t.TempDir(), "previous.json")
			if err := os.WriteFile(path, []byte("previous successful backup"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := backup.ExportFile(ctx, wrapped, path, backup.Options{}); err == nil {
				t.Fatal("failed/partial backup reported success")
			}
			previous, err := os.ReadFile(path)
			if err != nil || string(previous) != "previous successful backup" {
				t.Fatal("failed export replaced the previous backup")
			}
			files, err := os.ReadDir(filepath.Dir(path))
			if err != nil || len(files) != 1 {
				t.Fatal("failed export left a temporary artifact")
			}
		})
	}
}

type failingExportWriter struct{}

func (failingExportWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestStreamingExportPropagatesWriteFailure(t *testing.T) {
	s := openTestStore(t)
	seedControlPlane(t, s)
	if _, err := backup.ExportJSON(context.Background(), s, failingExportWriter{}, backup.Options{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write error was lost: %v", err)
	}
	var out bytes.Buffer
	if _, err := backup.ExportJSON(context.Background(), s, &out, backup.Options{}); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(out.Bytes()) {
		t.Fatal("stream output is not a complete JSON document")
	}
}

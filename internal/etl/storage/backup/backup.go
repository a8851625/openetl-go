// Package backup provides control-plane metadata backup and restore for the
// three public storage backends (SQLite / MySQL / PostgreSQL).
//
// Backup format (v2, readable with v1 compatibility) is a JSON document covering every table required by
// PR-1.3 acceptance: pipelines, versions, checkpoints, DLQ, audit, runs,
// workers, tasks, plugins, connections and settings. Secrets remain as stored
// (enc:v1 envelopes) — plaintext is never re-encoded into the backup.
package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

const FormatVersion = 2

// Snapshot is the on-disk / portable representation of a control-plane backup.
type Snapshot struct {
	// FormatVersion 2 adds PluginArtifacts (WASM bytes) and fidelity-preserved
	// audit/run-history rows. Version 1 products remain readable.
	FormatVersion   int                        `json:"format_version"`
	CreatedAt       time.Time                  `json:"created_at"`
	Backend         string                     `json:"backend,omitempty"`
	Schema          []storage.SchemaVersionRow `json:"schema_versions,omitempty"`
	Counts          storage.ObjectCounts       `json:"counts"`
	PluginArtifacts map[string]string          `json:"plugin_artifacts,omitempty"` // name -> base64 WASM

	Pipelines   []*storage.PipelineRow      `json:"pipelines"`
	Versions    []*storage.PipelineVersion  `json:"pipeline_versions"`
	Checkpoints []*storage.CheckpointRecord `json:"checkpoints"`
	DeadLetters []*storage.DLQRecord        `json:"dead_letters"`
	AuditLogs   []*storage.AuditEntry       `json:"audit_logs"`
	RunHistory  []*storage.RunRecord        `json:"run_history"`
	Workers     []*storage.WorkerInfo       `json:"workers"`
	Tasks       []*storage.TaskAssignment   `json:"task_assignments"`
	Plugins     []*storage.PluginEntry      `json:"plugins"`
	Connections []*storage.ConnectionEntry  `json:"connections"`
	Settings    map[string]string           `json:"settings"`
}

// Options control export/restore behaviour.
type Options struct {
	// Backend is recorded into the snapshot for diagnostics only.
	Backend string
	// ClearBeforeRestore empties covered tables before inserting rows.
	// When true (recommended for full restore), inventory matches the snapshot.
	ClearBeforeRestore bool
	// PluginsDir is required when restoring plugins: fresh immutable artifact
	// generations are written beneath this directory. Export always packages
	// installed artifacts, using this directory if no WASMPath was recorded.
	PluginsDir string
	// SecretFieldResolver supplies the same descriptor policy as the runtime.
	// A SecretFieldStore input already carries that policy. Raw callers without
	// one retain the legacy name fallback; CLI always supplies descriptors.
	SecretFieldResolver storage.SecretFieldResolver
}

// txRunner is implemented by sqlstore-backed stores for atomic multi-table
// operations (IT-3/T3.4 atomic restore).
type txRunner interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
	WipeControlPlane(ctx context.Context) error
}

// fidelityWriter restores historical rows without invoking runtime lifecycle
// methods or allocating replacement IDs.
type fidelityWriter interface {
	WritePipelineFidelity(context.Context, *storage.PipelineRow) error
	WritePipelineVersionFidelity(context.Context, *storage.PipelineVersion) error
	WriteCheckpointFidelity(context.Context, *storage.CheckpointRecord) error
	WriteDeadLetterFidelity(context.Context, *storage.DLQRecord) error
	WriteAuditFidelity(ctx context.Context, entry *storage.AuditEntry) error
	WriteRunRecordFidelity(ctx context.Context, r *storage.RunRecord) error
	WriteWorkerFidelity(context.Context, *storage.WorkerInfo) error
	WriteTaskFidelity(context.Context, *storage.TaskAssignment) error
	WritePluginFidelity(context.Context, *storage.PluginEntry) error
	WriteConnectionFidelity(context.Context, *storage.ConnectionEntry) error
	AdvanceBackupSequences(context.Context) error
}

// Export builds an in-memory snapshot for small programmatic callers. Use
// ExportFile/ExportJSON for bounded-memory maintenance of large databases.
// Both paths use the same complete traversal, inventory and secret checks.
func Export(ctx context.Context, store storage.Storage, opts Options) (*Snapshot, error) {
	var output bytes.Buffer
	if _, err := ExportJSON(ctx, store, &output, opts); err != nil {
		return nil, err
	}
	return ReadJSON(&output)
}

// CountSnapshot derives ObjectCounts from the snapshot payload.
func CountSnapshot(snap *Snapshot) storage.ObjectCounts {
	if snap == nil {
		return storage.ObjectCounts{}
	}
	return storage.ObjectCounts{
		Pipelines:        len(snap.Pipelines),
		PipelineVersions: len(snap.Versions),
		Checkpoints:      len(snap.Checkpoints),
		DeadLetters:      len(snap.DeadLetters),
		AuditLogs:        len(snap.AuditLogs),
		RunHistory:       len(snap.RunHistory),
		Workers:          len(snap.Workers),
		Tasks:            len(snap.Tasks),
		Plugins:          len(snap.Plugins),
		Connections:      len(snap.Connections),
		Settings:         len(snap.Settings),
	}
}

// WriteJSON encodes a snapshot to w.
func WriteJSON(w io.Writer, snap *Snapshot) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(snap)
}

// WriteFile writes a snapshot to path atomically (temp + rename).
func WriteFile(path string, snap *Snapshot) error {
	return writeAtomicFile(path, func(w io.Writer) error { return WriteJSON(w, snap) })
}

func writeAtomicFile(path string, write func(io.Writer) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".openetl-backup-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

// ReadJSON decodes a snapshot from r.
func ReadJSON(r io.Reader) (*Snapshot, error) {
	var snap Snapshot
	decoder := json.NewDecoder(r)
	decoder.UseNumber() // Preserve JSON integer business keys above 2^53.
	if err := decoder.Decode(&snap); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("backup: expected exactly one JSON snapshot, found trailing data")
	}
	if snap.FormatVersion == 0 {
		snap.FormatVersion = 1
	}
	if snap.FormatVersion != 1 && snap.FormatVersion != FormatVersion {
		return nil, fmt.Errorf("backup: unsupported format_version %d (want %d or legacy 1)", snap.FormatVersion, FormatVersion)
	}
	return &snap, nil
}

// ReadFile loads a snapshot from path.
func ReadFile(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadJSON(f)
}

// Reconcile compares live store counts against a snapshot.
// Returns a multi-line report and ok=false when any critical table differs.
func Reconcile(ctx context.Context, store storage.Storage, snap *Snapshot) (report string, ok bool, err error) {
	if store == nil || snap == nil {
		return "", false, fmt.Errorf("backup: store and snapshot required")
	}
	store = storage.UnwrapStorage(store)
	want := CountSnapshot(snap)
	var got storage.ObjectCounts
	if purger, okp := store.(storage.RetentionPurger); okp {
		got, err = purger.CountObjects(ctx)
		if err != nil {
			return "", false, err
		}
	} else {
		live, err := Export(ctx, store, Options{Backend: "reconcile"})
		if err != nil {
			return "", false, err
		}
		got = CountSnapshot(live)
	}

	type pair struct {
		name     string
		w, g     int
		critical bool
	}
	pairs := []pair{
		{"pipelines", want.Pipelines, got.Pipelines, true},
		{"pipeline_versions", want.PipelineVersions, got.PipelineVersions, true},
		{"checkpoints", want.Checkpoints, got.Checkpoints, true},
		{"dead_letters", want.DeadLetters, got.DeadLetters, true},
		{"audit_logs", want.AuditLogs, got.AuditLogs, true},
		{"run_history", want.RunHistory, got.RunHistory, true},
		{"workers", want.Workers, got.Workers, true},
		{"task_assignments", want.Tasks, got.Tasks, true},
		{"plugins", want.Plugins, got.Plugins, true},
		{"connections", want.Connections, got.Connections, true},
		{"settings", want.Settings, got.Settings, true},
	}
	ok = true
	var b []byte
	for _, p := range pairs {
		line := fmt.Sprintf("%s: want=%d got=%d", p.name, p.w, p.g)
		if p.critical && p.w != p.g {
			ok = false
			line += "  << MISMATCH"
		}
		b = append(b, line...)
		b = append(b, '\n')
	}
	return string(b), ok, nil
}

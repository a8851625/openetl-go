// Package backup provides control-plane metadata backup and restore for the
// three public storage backends (SQLite / MySQL / PostgreSQL).
//
// Backup format (v1) is a JSON document covering every table required by
// PR-1.3 acceptance: pipelines, versions, checkpoints, DLQ, audit, runs,
// workers, tasks, plugins, connections and settings. Secrets remain as stored
// (enc:v1 envelopes) — plaintext is never re-encoded into the backup.
package backup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

const FormatVersion = 1

// Snapshot is the on-disk / portable representation of a control-plane backup.
type Snapshot struct {
	FormatVersion int                        `json:"format_version"`
	CreatedAt     time.Time                  `json:"created_at"`
	Backend       string                     `json:"backend,omitempty"`
	Schema        []storage.SchemaVersionRow `json:"schema_versions,omitempty"`
	Counts        storage.ObjectCounts       `json:"counts"`

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
}

// dbProvider is implemented by sqlstore-backed stores (sqlite/mysql/postgres).
type dbProvider interface {
	DB() *sql.DB
}

// Export builds a Snapshot from a live Storage. Encrypted fields stay encrypted.
func Export(ctx context.Context, store storage.Storage, opts Options) (*Snapshot, error) {
	if store == nil {
		return nil, fmt.Errorf("backup: store is nil")
	}
	snap := &Snapshot{
		FormatVersion: FormatVersion,
		CreatedAt:     time.Now().UTC(),
		Backend:       opts.Backend,
		Settings:      map[string]string{},
	}

	if purger, ok := store.(storage.RetentionPurger); ok {
		if counts, err := purger.CountObjects(ctx); err == nil {
			snap.Counts = counts
		}
		if vers, err := purger.SchemaVersions(ctx); err == nil {
			snap.Schema = vers
		}
	}

	pipes, err := store.ListPipelines(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: list pipelines: %w", err)
	}
	snap.Pipelines = pipes
	for _, p := range pipes {
		if p == nil {
			continue
		}
		vers, err := store.ListPipelineVersions(ctx, p.Name)
		if err != nil {
			vers, err = store.ListPipelineVersions(ctx, p.ID)
		}
		if err != nil {
			return nil, fmt.Errorf("backup: list versions for %q: %w", p.Name, err)
		}
		snap.Versions = append(snap.Versions, vers...)
	}

	cps, err := store.ListCheckpoints(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: list checkpoints: %w", err)
	}
	snap.Checkpoints = cps

	seenDLQ := map[int64]struct{}{}
	for _, p := range pipes {
		if p == nil {
			continue
		}
		for _, key := range []string{p.Name, p.ID} {
			if key == "" {
				continue
			}
			rows, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: key, Limit: 100000})
			if err != nil {
				return nil, fmt.Errorf("backup: list dlq %q: %w", key, err)
			}
			for _, r := range rows {
				if r == nil {
					continue
				}
				if _, ok := seenDLQ[r.ID]; ok {
					continue
				}
				seenDLQ[r.ID] = struct{}{}
				snap.DeadLetters = append(snap.DeadLetters, r)
			}
		}
	}

	audits, err := store.ListAudit(ctx, 100000)
	if err != nil {
		return nil, fmt.Errorf("backup: list audit: %w", err)
	}
	snap.AuditLogs = audits

	seenRun := map[int64]struct{}{}
	for _, p := range pipes {
		if p == nil {
			continue
		}
		for _, key := range []string{p.Name, p.ID} {
			if key == "" {
				continue
			}
			runs, err := store.ListRunHistory(ctx, key, 100000)
			if err != nil {
				return nil, fmt.Errorf("backup: list runs %q: %w", key, err)
			}
			for _, r := range runs {
				if r == nil {
					continue
				}
				if _, ok := seenRun[r.ID]; ok {
					continue
				}
				seenRun[r.ID] = struct{}{}
				snap.RunHistory = append(snap.RunHistory, r)
			}
		}
	}

	workers, err := store.ListWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: list workers: %w", err)
	}
	snap.Workers = workers

	seenTask := map[string]struct{}{}
	tasks, err := store.ListTasks(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("backup: list tasks: %w", err)
	}
	for _, t := range tasks {
		if t == nil {
			continue
		}
		seenTask[t.TaskID] = struct{}{}
		snap.Tasks = append(snap.Tasks, t)
	}
	for _, p := range pipes {
		if p == nil {
			continue
		}
		for _, key := range []string{p.Name, p.ID} {
			if key == "" {
				continue
			}
			more, err := store.ListTasks(ctx, key)
			if err != nil {
				return nil, fmt.Errorf("backup: list tasks %q: %w", key, err)
			}
			for _, t := range more {
				if t == nil {
					continue
				}
				if _, ok := seenTask[t.TaskID]; ok {
					continue
				}
				seenTask[t.TaskID] = struct{}{}
				snap.Tasks = append(snap.Tasks, t)
			}
		}
	}

	plugins, err := store.ListPlugins(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: list plugins: %w", err)
	}
	snap.Plugins = plugins

	conns, err := store.ListConnections(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: list connections: %w", err)
	}
	snap.Connections = conns

	settings, err := store.ListSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup: list settings: %w", err)
	}
	snap.Settings = settings
	snap.Counts = CountSnapshot(snap)
	return snap, nil
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
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := WriteJSON(f, snap); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// ReadJSON decodes a snapshot from r.
func ReadJSON(r io.Reader) (*Snapshot, error) {
	var snap Snapshot
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return nil, err
	}
	if snap.FormatVersion == 0 {
		snap.FormatVersion = FormatVersion
	}
	if snap.FormatVersion != FormatVersion {
		return nil, fmt.Errorf("backup: unsupported format_version %d (want %d)", snap.FormatVersion, FormatVersion)
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

// Restore applies a snapshot into store.
func Restore(ctx context.Context, store storage.Storage, snap *Snapshot, opts Options) error {
	if store == nil {
		return fmt.Errorf("backup: store is nil")
	}
	if snap == nil {
		return fmt.Errorf("backup: snapshot is nil")
	}
	if snap.FormatVersion != 0 && snap.FormatVersion != FormatVersion {
		return fmt.Errorf("backup: unsupported format_version %d", snap.FormatVersion)
	}

	if opts.ClearBeforeRestore {
		if err := clearControlPlane(ctx, store); err != nil {
			return fmt.Errorf("backup: clear before restore: %w", err)
		}
	}

	for _, p := range snap.Pipelines {
		if p == nil {
			continue
		}
		row := *p
		if err := store.SavePipeline(ctx, &row); err != nil {
			return fmt.Errorf("backup: restore pipeline %q: %w", p.Name, err)
		}
	}

	byPipe := map[string][]*storage.PipelineVersion{}
	for _, v := range snap.Versions {
		if v == nil {
			continue
		}
		byPipe[v.Pipeline] = append(byPipe[v.Pipeline], v)
	}
	for pipe, vers := range byPipe {
		for i := 0; i < len(vers); i++ {
			for j := i + 1; j < len(vers); j++ {
				if vers[j].Version < vers[i].Version {
					vers[i], vers[j] = vers[j], vers[i]
				}
			}
		}
		for _, v := range vers {
			if _, err := store.SavePipelineVersion(ctx, pipe, v.SpecYAML); err != nil {
				return fmt.Errorf("backup: restore version %s@%d: %w", pipe, v.Version, err)
			}
		}
	}

	for _, cp := range snap.Checkpoints {
		if cp == nil {
			continue
		}
		rec := *cp
		if err := store.SaveCheckpoint(ctx, &rec); err != nil {
			return fmt.Errorf("backup: restore checkpoint %q: %w", cp.JobName, err)
		}
	}

	for _, d := range snap.DeadLetters {
		if d == nil {
			continue
		}
		rec := *d
		if err := store.WriteDeadLetter(ctx, &rec); err != nil {
			return fmt.Errorf("backup: restore dlq: %w", err)
		}
	}

	for _, a := range snap.AuditLogs {
		if a == nil {
			continue
		}
		entry := *a
		if err := store.WriteAudit(ctx, &entry); err != nil {
			return fmt.Errorf("backup: restore audit: %w", err)
		}
	}

	for _, r := range snap.RunHistory {
		if r == nil {
			continue
		}
		id, err := store.RecordRunStart(ctx, r.JobName)
		if err != nil {
			return fmt.Errorf("backup: restore run start: %w", err)
		}
		status := r.Status
		if status == "" || status == "running" {
			status = "succeeded"
		}
		if err := store.RecordRunEnd(ctx, id, status, r.RecordsRead, r.RecordsWritten, r.RecordsFailed, r.RecordsDLQ, r.DurationMs); err != nil {
			return fmt.Errorf("backup: restore run end: %w", err)
		}
	}

	for _, w := range snap.Workers {
		if w == nil {
			continue
		}
		info := *w
		if err := store.RegisterWorker(ctx, &info); err != nil {
			return fmt.Errorf("backup: restore worker %q: %w", w.ID, err)
		}
	}

	for _, t := range snap.Tasks {
		if t == nil {
			continue
		}
		task := *t
		if err := store.CreateTask(ctx, &task); err != nil {
			return fmt.Errorf("backup: restore task %q: %w", t.TaskID, err)
		}
		if t.Status != "" && t.Status != "pending" {
			upd := *t
			if err := store.UpdateTask(ctx, &upd); err != nil {
				return fmt.Errorf("backup: restore task status %q: %w", t.TaskID, err)
			}
		}
	}

	for _, p := range snap.Plugins {
		if p == nil {
			continue
		}
		plugin := *p
		if err := store.SavePlugin(ctx, &plugin); err != nil {
			return fmt.Errorf("backup: restore plugin %q: %w", p.Name, err)
		}
	}

	for _, c := range snap.Connections {
		if c == nil {
			continue
		}
		conn := *c
		if err := store.SaveConnection(ctx, &conn); err != nil {
			return fmt.Errorf("backup: restore connection %q: %w", c.Name, err)
		}
	}

	for k, v := range snap.Settings {
		if err := store.SetSetting(ctx, k, v); err != nil {
			return fmt.Errorf("backup: restore setting %q: %w", k, err)
		}
	}
	return nil
}

// Reconcile compares live store counts against a snapshot.
// Returns a multi-line report and ok=false when any critical table differs.
func Reconcile(ctx context.Context, store storage.Storage, snap *Snapshot) (report string, ok bool, err error) {
	if store == nil || snap == nil {
		return "", false, fmt.Errorf("backup: store and snapshot required")
	}
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
		{"workers", want.Workers, got.Workers, false},
		{"task_assignments", want.Tasks, got.Tasks, false},
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

// clearControlPlane empties every table covered by backup.
func clearControlPlane(ctx context.Context, store storage.Storage) error {
	// Prefer direct SQL wipe when the store exposes *sql.DB (all three backends).
	if p, ok := store.(dbProvider); ok {
		db := p.DB()
		if db != nil {
			return wipeSQL(ctx, db)
		}
	}
	// Fallback: public API only (cannot fully clear audit/runs without SQL).
	pipes, err := store.ListPipelines(ctx)
	if err != nil {
		return err
	}
	for _, p := range pipes {
		if p == nil {
			continue
		}
		ref := p.ID
		if ref == "" {
			ref = p.Name
		}
		if del, ok := store.(interface {
			DeletePipelineWithCheckpoint(context.Context, string) error
		}); ok {
			_ = del.DeletePipelineWithCheckpoint(ctx, ref)
		} else {
			_ = store.DeletePipeline(ctx, ref)
			_ = store.DeleteCheckpoint(ctx, ref)
		}
		_ = store.DeleteAllDeadLetters(ctx, p.Name)
		_ = store.DeleteAllDeadLetters(ctx, p.ID)
	}
	plugins, _ := store.ListPlugins(ctx)
	for _, p := range plugins {
		if p != nil {
			_ = store.DeletePlugin(ctx, p.Name)
		}
	}
	conns, _ := store.ListConnections(ctx)
	for _, c := range conns {
		if c != nil {
			_ = store.DeleteConnection(ctx, c.Name)
		}
	}
	workers, _ := store.ListWorkers(ctx)
	for _, w := range workers {
		if w != nil {
			_ = store.DeregisterWorker(ctx, w.ID)
		}
	}
	return nil
}

func wipeSQL(ctx context.Context, db *sql.DB) error {
	// Order: children first. _schema_version is intentionally preserved so
	// restore does not re-trigger migrations.
	tables := []string{
		"dead_letters",
		"audit_logs",
		"run_history",
		"task_assignments",
		"checkpoints",
		"pipeline_versions",
		"pipelines",
		"workers",
		"plugins",
		"connections",
		"settings",
		"plugin_state",
	}
	for _, t := range tables {
		if _, err := db.ExecContext(ctx, "DELETE FROM "+t); err != nil {
			// Ignore missing optional tables.
			msg := err.Error()
			if containsAny(msg, "no such table", "doesn't exist", "does not exist") {
				continue
			}
			return fmt.Errorf("wipe %s: %w", t, err)
		}
	}
	return nil
}

func containsAny(s string, parts ...string) bool {
	for _, p := range parts {
		if len(p) > 0 && (len(s) >= len(p)) {
			for i := 0; i+len(p) <= len(s); i++ {
				if s[i:i+len(p)] == p {
					return true
				}
			}
		}
	}
	return false
}

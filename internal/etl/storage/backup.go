package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// BackupManifest describes a logical metadata backup produced by BackupSQLStore.
type BackupManifest struct {
	CreatedAt  time.Time        `json:"created_at"`
	Backend    string           `json:"backend"`
	SchemaNote string           `json:"schema_note"`
	Counts     BackupCounts     `json:"counts"`
	SecretScan SecretScanResult `json:"secret_scan"`
	Path       string           `json:"path,omitempty"`
}

// BackupCounts tallies major control-plane tables.
type BackupCounts struct {
	Pipelines   int `json:"pipelines"`
	Versions    int `json:"versions"`
	Checkpoints int `json:"checkpoints"`
	DLQ         int `json:"dlq"`
	Audit       int `json:"audit"`
	Runs        int `json:"runs"`
	Workers     int `json:"workers"`
	Tasks       int `json:"tasks"`
	Plugins     int `json:"plugins"`
	Connections int `json:"connections"`
	Settings    int `json:"settings"`
}

// SecretScanResult reports whether raw dump text contained known plaintext secrets.
type SecretScanResult struct {
	PlaintextHits int      `json:"plaintext_hits"`
	Samples       []string `json:"samples,omitempty"`
	OK            bool     `json:"ok"`
}

// SQLDumper exposes *sql.DB for logical export.
type SQLDumper interface {
	DB() *sql.DB
	BackendName() string
}

// BackupSQLStore writes a JSONL logical backup of control-plane tables plus manifest.
// Uses SELECT * so minor schema drift across backends does not break export.
func BackupSQLStore(ctx context.Context, d SQLDumper, outDir string, knownPlaintext []string) (*BackupManifest, error) {
	if d == nil || d.DB() == nil {
		return nil, fmt.Errorf("backup: nil store")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	dir := filepath.Join(outDir, "openetl-backup-"+stamp)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := d.DB()
	counts := BackupCounts{}
	tables := []struct {
		name  string
		count *int
	}{
		{"pipelines", &counts.Pipelines},
		{"pipeline_versions", &counts.Versions},
		{"checkpoints", &counts.Checkpoints},
		{"dead_letters", &counts.DLQ},
		{"audit_logs", &counts.Audit},
		{"run_history", &counts.Runs},
		{"workers", &counts.Workers},
		{"task_assignments", &counts.Tasks},
		{"plugins", &counts.Plugins},
		{"connections", &counts.Connections},
		{"settings", &counts.Settings},
	}

	var dumpBuf strings.Builder
	for _, t := range tables {
		n, chunk, err := dumpTableJSONL(ctx, db, "SELECT * FROM "+t.name)
		if err != nil {
			continue
		}
		*t.count = n
		path := filepath.Join(dir, t.name+".jsonl")
		if err := os.WriteFile(path, chunk, 0o600); err != nil {
			return nil, err
		}
		dumpBuf.Write(chunk)
	}
	scan := ScanPlaintextSecrets(dumpBuf.String(), knownPlaintext)
	man := &BackupManifest{
		CreatedAt:  time.Now().UTC(),
		Backend:    d.BackendName(),
		SchemaNote: "logical jsonl export of control-plane tables (SELECT *)",
		Counts:     counts,
		SecretScan: scan,
		Path:       dir,
	}
	b, _ := json.MarshalIndent(man, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0o600); err != nil {
		return nil, err
	}
	return man, nil
}

func dumpTableJSONL(ctx context.Context, db *sql.DB, query string) (int, []byte, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return 0, nil, err
	}
	var out []byte
	n := 0
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return n, out, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			m[c] = normalizeDumpValue(raw[i])
		}
		line, err := json.Marshal(m)
		if err != nil {
			return n, out, err
		}
		out = append(out, line...)
		out = append(out, '\n')
		n++
	}
	return n, out, rows.Err()
}

func normalizeDumpValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	default:
		return t
	}
}

// ScanPlaintextSecrets looks for known secret substrings in dumps.
func ScanPlaintextSecrets(dump string, known []string) SecretScanResult {
	res := SecretScanResult{OK: true}
	for _, k := range known {
		if k == "" || len(k) < 4 {
			continue
		}
		if strings.Contains(dump, k) {
			res.PlaintextHits++
			res.OK = false
			if len(res.Samples) < 5 {
				res.Samples = append(res.Samples, k)
			}
		}
	}
	return res
}

// RetentionPolicy configures janitor cutoffs. Zero means skip that table.
type RetentionPolicy struct {
	RunHistory time.Duration
	AuditLogs  time.Duration
	DLQ        time.Duration
}

// RetentionReport is the result of ApplyRetention.
type RetentionReport struct {
	RunsDeleted  int64     `json:"runs_deleted"`
	AuditDeleted int64     `json:"audit_deleted"`
	DLQDeleted   int64     `json:"dlq_deleted"`
	Cutoff       time.Time `json:"cutoff_base"`
}

// ApplyRetention deletes aged operational rows (not pipelines/checkpoints/secrets).
func ApplyRetention(ctx context.Context, db *sql.DB, now time.Time, p RetentionPolicy) (*RetentionReport, error) {
	if db == nil {
		return nil, fmt.Errorf("retention: nil db")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rep := &RetentionReport{Cutoff: now}
	del := func(query string, cutoff any) int64 {
		res, err := db.ExecContext(ctx, query, cutoff)
		if err != nil {
			return 0
		}
		n, _ := res.RowsAffected()
		return n
	}
	if p.RunHistory > 0 {
		c := now.Add(-p.RunHistory).UTC().Format(time.RFC3339Nano)
		rep.RunsDeleted = del(`DELETE FROM run_history WHERE started_at < ?`, c)
	}
	if p.AuditLogs > 0 {
		c := now.Add(-p.AuditLogs).UTC().Format(time.RFC3339Nano)
		rep.AuditDeleted = del(`DELETE FROM audit_logs WHERE created_at < ?`, c)
	}
	if p.DLQ > 0 {
		c := now.Add(-p.DLQ).UTC().Format(time.RFC3339Nano)
		rep.DLQDeleted = del(`DELETE FROM dead_letters WHERE created_at < ?`, c)
	}
	return rep, nil
}

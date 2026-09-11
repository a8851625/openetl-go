// RA-6 acceptance data, intentionally outside the runtime dependency graph.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlstore"
)

type store interface {
	storage.Storage
	storage.BackupReader
	DB() *sql.DB
}

var ctx = context.Background()
var epoch = time.Date(2026, 9, 1, 2, 3, 4, 123000000, time.UTC)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "volume fixture:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 4 {
		return fmt.Errorf("usage: seed|extend|verify-file|fingerprint BACKEND ROWS [FILE]")
	}
	mode, backend := os.Args[1], os.Args[2]
	n, err := strconv.Atoi(os.Args[3])
	if err != nil || n < 1 {
		return fmt.Errorf("positive row count required")
	}
	if mode == "verify-file" {
		if len(os.Args) != 5 {
			return fmt.Errorf("verify-file requires a path")
		}
		return verifyFile(os.Args[4], n)
	}
	var s store
	var dialect sqlstore.Dialect
	switch backend {
	case "sqlite":
		s, err = sqlite.New(os.Getenv("BACKUP_VOLUME_SQLITE_PATH"))
		dialect = sqlstore.SQLiteDialect{}
	case "mysql":
		s, err = mysql.New(os.Getenv("BACKUP_VOLUME_DSN"))
		dialect = sqlstore.MySQLDialect{}
	case "postgres":
		s, err = postgres.New(ctx, os.Getenv("BACKUP_VOLUME_DSN"))
		dialect = sqlstore.PostgresDialect{}
	default:
		return fmt.Errorf("unknown backend")
	}
	if err != nil {
		return err
	}
	defer s.Close()
	switch mode {
	case "seed", "extend":
		return seed(s, dialect, n, mode == "extend")
	case "fingerprint":
		return fingerprint(s, dialect, n)
	default:
		return fmt.Errorf("unknown mode")
	}
}

func volumeRows(i int) (*storage.DLQRecord, *storage.AuditEntry, *storage.RunRecord) {
	stamp := epoch.Add(time.Duration(i%7) * time.Millisecond) // Deliberately many ties.
	job := []string{"present-id", "present-name", "deleted-pipeline", ""}[i%4]
	key := strconv.FormatInt(9007199254740993+int64(i), 10)
	finish := stamp.Add(time.Duration(i%113) * time.Millisecond)
	dlq := &storage.DLQRecord{
		ID: int64(i * 3), JobName: job, CreatedAt: stamp, Error: fmt.Sprintf("failure %d: 中'\\<>&", i), ErrorClass: "data", Attempt: i%5 + 1,
		RecordHash: fmt.Sprintf("historical-%d", i), PipelineVersion: i%9 + 1, DAGNode: "write-orders",
		Record:          core.Record{Operation: core.OpUpdate, Data: map[string]any{"id": json.Number(key), "sequence": i, "payload": strings.Repeat("v", 256), "literal": "中'\\<>&"}, Metadata: core.Metadata{Table: "orders", Key: key, PrimaryKeyColumns: []string{"id"}}},
		IdentityContext: core.DLQIdentityContext{RawPayload: `{"id":` + key + `}`, PayloadEncoding: core.DLQPayloadEncodingSourceBytes, PrimaryKeyColumns: []string{"id"}, SourceTable: "orders", TargetTable: "ods_orders", ReplayProvenance: core.DLQReplayProvenanceNormalFlow, ReplayState: core.DLQReplayStateSinkAcked, ReplayAttempt: i % 3, SinkAcknowledgedAt: &finish},
	}
	audit := &storage.AuditEntry{ID: int64(i * 5), Action: fmt.Sprintf("operation-%d", i), Method: "POST", Path: fmt.Sprintf("/api/v2/fixture/%d", i), Target: job, Remote: "127.0.0.1", CreatedAt: stamp}
	run := &storage.RunRecord{ID: int64(i * 7), JobName: job, Status: "failed", StartedAt: stamp, FinishedAt: &finish, DurationMs: int64(i % 113), RecordsRead: int64(i), RecordsWritten: int64(i / 2), RecordsFailed: int64(i % 31), RecordsDLQ: int64(i % 17)}
	if i%3 == 0 {
		run.Status, run.FinishedAt = "running", nil
	}
	return dlq, audit, run
}

func seed(s store, dialect sqlstore.Dialect, n int, extend bool) error {
	counts, err := s.CountObjects(ctx)
	if err != nil {
		return err
	}
	from := 1
	if extend {
		marker, err := s.GetSetting(ctx, "volume.fixture")
		if err != nil || marker != "RA-6-isolated-fixture" || counts.AuditLogs != counts.DeadLetters || counts.RunHistory != counts.DeadLetters || counts.DeadLetters >= n {
			return fmt.Errorf("refusing to extend a nonmatching volume fixture")
		}
		from = counts.DeadLetters + 1
	} else {
		if counts != (storage.ObjectCounts{}) {
			return fmt.Errorf("refusing a nonempty volume database: %+v", counts)
		}
		if err := s.SavePipeline(ctx, &storage.PipelineRow{ID: "present-id", Name: "present-name", SpecYAML: "name: volume-fixture", Status: "stopped"}); err != nil {
			return err
		}
		if err := s.SetSetting(ctx, "volume.fixture", "RA-6-isolated-fixture"); err != nil {
			return err
		}
		if _, err := s.SavePipelineVersion(ctx, "deleted-pipeline", "orphan version"); err != nil {
			return err
		}
		if err := s.SaveCheckpoint(ctx, &storage.CheckpointRecord{JobName: "present-id", Source: "file", Position: json.RawMessage(`{"offset":9007199254740993}`), Timestamp: epoch}); err != nil {
			return err
		}
		// Completed tasks for both a present and a deleted pipeline exceed the
		// old dispatch limit. Their content is checked by independent SQL hashes.
		if err := insertBatches(s.DB(), dialect, "task_assignments", "id,task_id,pipeline,shard_index,shard_total,worker_id,status,generation,attempt,lease_expires_at,last_error,required_labels,assigned_at,started_at,finished_at", 1, 2003, func(i int) ([]any, error) {
			job := "deleted-pipeline"
			if i%2 == 0 {
				job = "present-id"
			}
			return []any{i * 11, fmt.Sprintf("task-%d", i), job, i % 3, 3, "fixture-worker", "completed", i, i % 5, epoch, fmt.Sprintf("last-error-%d", i), `{"zone":"中"}`, epoch, epoch, epoch}, nil
		}); err != nil {
			return err
		}
	}
	for _, table := range []string{"dead_letters", "audit_logs", "run_history"} {
		columns := map[string]string{
			"dead_letters": "id,job_name,record_json,error,error_class,identity_context_json,attempt,record_hash,pipeline_version,dag_node,created_at",
			"audit_logs":   "id,action,method,path,target,remote,created_at",
			"run_history":  "id,job_name,status,started_at,finished_at,duration_ms,records_read,records_written,records_failed,records_dlq",
		}[table]
		if err := insertBatches(s.DB(), dialect, table, columns, from, n, func(i int) ([]any, error) {
			d, a, r := volumeRows(i)
			switch table {
			case "dead_letters":
				record, err := json.Marshal(d.Record)
				if err != nil {
					return nil, err
				}
				identity, err := json.Marshal(d.IdentityContext)
				if err != nil {
					return nil, err
				}
				return []any{d.ID, d.JobName, string(record), d.Error, d.ErrorClass, string(identity), d.Attempt, d.RecordHash, d.PipelineVersion, d.DAGNode, d.CreatedAt}, nil
			case "audit_logs":
				return []any{a.ID, a.Action, a.Method, a.Path, a.Target, a.Remote, a.CreatedAt}, nil
			default:
				return []any{r.ID, r.JobName, r.Status, r.StartedAt, r.FinishedAt, r.DurationMs, r.RecordsRead, r.RecordsWritten, r.RecordsFailed, r.RecordsDLQ}, nil
			}
		}); err != nil {
			return err
		}
	}
	fmt.Printf("SEEDED: each of DLQ/audit/run=%d; terminal/orphan tasks=2003\n", n)
	return nil
}

func insertBatches(db *sql.DB, dialect sqlstore.Dialect, table, columns string, from, until int, values func(int) ([]any, error)) error {
	for first := from; first <= until; first += 500 {
		var args []any
		var tuples []string
		for i := first; i < first+500 && i <= until; i++ {
			row, err := values(i)
			if err != nil {
				return err
			}
			tuples = append(tuples, "("+strings.TrimSuffix(strings.Repeat("?,", len(row)), ",")+")")
			args = append(args, row...)
		}
		query := "INSERT INTO " + table + " (" + columns + ") VALUES " + strings.Join(tuples, ",")
		if _, err := db.ExecContext(ctx, dialect.Bind(query), args...); err != nil {
			return fmt.Errorf("seed %s batch %d: %w", table, first, err)
		}
	}
	return nil
}

func verifyFile(path string, n int) error {
	snap, err := backup.ReadFile(path)
	if err != nil {
		return err
	}
	counts := backup.CountSnapshot(snap)
	if counts != snap.Counts || counts.DeadLetters != n || counts.AuditLogs != n || counts.RunHistory != n || counts.Tasks != 2003 || counts.Pipelines != 1 || counts.PipelineVersions != 1 || counts.Checkpoints != 1 || counts.Settings != 1 {
		return fmt.Errorf("volume counts differ: %+v / %+v", counts, snap.Counts)
	}
	for i := 1; i <= n; i++ {
		d, a, r := volumeRows(i)
		for _, pair := range [][2]any{{d, snap.DeadLetters[i-1]}, {a, snap.AuditLogs[i-1]}, {r, snap.RunHistory[i-1]}} {
			want, err := canonical(pair[0])
			if err != nil {
				return err
			}
			got, err := canonical(pair[1])
			if err != nil {
				return err
			}
			if !bytes.Equal(want, got) {
				return fmt.Errorf("%T at sequence %d content differs", pair[0], i)
			}
		}
	}
	fmt.Printf("VERIFIED_FILE: all fields of %d DLQ + %d audit + %d run rows, counts and 2003 retained tasks\n", n, n, n)
	return nil
}

func canonical(value any) ([]byte, error) {
	blob, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(blob))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	var normalize func(any) any
	normalize = func(v any) any {
		switch x := v.(type) {
		case map[string]any:
			for k, v := range x {
				x[k] = normalize(v)
			}
		case []any:
			for i, v := range x {
				x[i] = normalize(v)
			}
		case string:
			if stamp, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return stamp.UTC().Format(time.RFC3339Nano)
			}
		}
		return v
	}
	return json.Marshal(normalize(decoded))
}

// Independent raw SQL traversal compares every column of the history tables,
// avoiding both API filters and the backup reader under test. JSON text is
// canonicalized because whitespace/timezone rendering can differ after restore.
func fingerprint(s store, dialect sqlstore.Dialect, n int) error {
	counts, err := s.CountObjects(ctx)
	if err != nil {
		return err
	}
	if counts.DeadLetters != n || counts.AuditLogs != n || counts.RunHistory != n || counts.Tasks != 2003 {
		return fmt.Errorf("fingerprint row counts differ: %+v", counts)
	}
	result := map[string]any{"counts": counts}
	for _, table := range []string{"pipelines", "pipeline_versions", "checkpoints", "dead_letters", "audit_logs", "run_history", "task_assignments", "settings"} {
		query := "SELECT * FROM " + table + " ORDER BY id"
		if table == "checkpoints" {
			query = "SELECT * FROM checkpoints ORDER BY job_name"
		}
		if table == "settings" {
			key := dialect.SettingKeyColumn()
			query = "SELECT " + key + ",value FROM settings ORDER BY " + key
		}
		rows, err := s.DB().QueryContext(ctx, query)
		if err != nil {
			return err
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return err
		}
		hash := sha256.New()
		count := 0
		for rows.Next() {
			values, ptrs := make([]any, len(cols)), make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return err
			}
			record := map[string]any{}
			for i, col := range cols {
				v := values[i]
				if b, ok := v.([]byte); ok {
					v = string(b)
				}
				if str, ok := v.(string); ok && (strings.HasSuffix(col, "_json") || col == "required_labels" || col == "position") {
					decoder := json.NewDecoder(strings.NewReader(str))
					decoder.UseNumber()
					if err := decoder.Decode(&v); err != nil {
						rows.Close()
						return err
					}
				}
				record[col] = v
			}
			encoded, err := canonical(record)
			if err != nil {
				rows.Close()
				return err
			}
			if _, err := hash.Write(encoded); err != nil {
				rows.Close()
				return err
			}
			_, _ = io.WriteString(hash, "\n")
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		result[table] = map[string]any{"rows": count, "sha256": hex.EncodeToString(hash.Sum(nil))}
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

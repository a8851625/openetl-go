package backup_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

type restoreBackend interface {
	storage.Storage
	SetFailureInjector(func(string) error)
	SavePipelineWithVersion(context.Context, *storage.PipelineRow, string) error
}

// Dedicated DSNs must name empty throwaway databases. The e2e scripts start
// fresh containers; a populated database is refused before any destructive test.
func TestBackupRestoreConformance(t *testing.T) {
	for _, backend := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var s restoreBackend
			var err error
			switch backend {
			case "sqlite":
				s, err = sqlite.New(filepath.Join(t.TempDir(), "restore.db"))
			case "mysql", "postgres":
				dsn := os.Getenv("BACKUP_TEST_" + strings.ToUpper(backend) + "_DSN")
				if dsn == "" {
					if os.Getenv("BACKUP_TEST_REQUIRED") == backend {
						t.Fatal("required backup conformance DSN is missing")
					}
					t.Skip("dedicated BACKUP_TEST_" + strings.ToUpper(backend) + "_DSN is not set")
				}
				if backend == "mysql" {
					s, err = mysql.New(dsn)
				} else {
					s, err = postgres.New(ctx, dsn)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			counts, err := s.(storage.RetentionPurger).CountObjects(ctx)
			if err != nil || counts != (storage.ObjectCounts{}) {
				t.Fatalf("refusing nonempty backup test database: counts=%+v err=%v", counts, err)
			}
			reset := func(t *testing.T) {
				t.Helper()
				s.SetFailureInjector(nil)
				if err := backup.Restore(ctx, s, &backup.Snapshot{FormatVersion: 2}, backup.Options{ClearBeforeRestore: true}); err != nil {
					t.Fatal(err)
				}
			}

			t.Run("id_keyed_and_legacy_history_export", func(t *testing.T) {
				reset(t)
				p := &storage.PipelineRow{ID: "pipeline-42", Name: "orders", Status: "stopped", SpecYAML: "name: orders"}
				for i := 0; i < 2; i++ {
					if err := s.SavePipelineWithVersion(ctx, p, p.SpecYAML); err != nil {
						t.Fatal(err)
					}
				}
				for _, key := range []string{p.Name, "deleted-pipeline"} {
					if _, err := s.SavePipelineVersion(ctx, key, "historical YAML"); err != nil {
						t.Fatal(err)
					}
				}
				snap := exportForRestoreTest(t, s)
				if len(snap.Versions) != 4 {
					t.Fatalf("lost ID-keyed, legacy or orphaned history: %+v", snap.Versions)
				}
				if err := backup.Restore(ctx, s, snap, backup.Options{ClearBeforeRestore: true}); err != nil {
					t.Fatal(err)
				}
				assertSnapshotPayload(t, snap, exportForRestoreTest(t, s))
			})

			for _, wrapped := range []bool{false, true} {
				t.Run(fmt.Sprintf("fidelity_and_rollback/wrapped=%t", wrapped), func(t *testing.T) {
					reset(t)
					var view storage.Storage = s
					if wrapped {
						cipher, err := storage.NewSpecCipher("k1", resolverKey(3), "")
						if err != nil {
							t.Fatal(err)
						}
						view = storage.NewSecretFieldStore(s, cipher)
					}
					snap := fidelityFixture()
					opts := backup.Options{ClearBeforeRestore: true, PluginsDir: t.TempDir()}
					if err := backup.Restore(ctx, view, snap, opts); err != nil {
						t.Fatal(err)
					}
					before := exportForRestoreTest(t, view)
					assertSnapshotPayload(t, snap, before)
					oldPath := before.Plugins[0].WASMPath
					assertRestoredArtifact(t, opts.PluginsDir, oldPath, snap.PluginArtifacts["parser"])
					if snap.Plugins[0].WASMPath != "/old-host/parser.wasm" {
						t.Fatal("restore mutated the input snapshot")
					}

					// Abort after some DELETEs, early INSERTs, and late INSERTs.
					// A failed SQL operation must retain every old row and artifact.
					for _, failAt := range []string{"backup.wipe.pipelines", "backup.restore.checkpoints", "backup.restore.connections"} {
						t.Run(failAt, func(t *testing.T) {
							injected := errors.New("injected restore failure")
							hit := false
							s.SetFailureInjector(func(op string) error {
								if op == failAt {
									hit = true
									return injected
								}
								return nil
							})
							bad := fidelityFixture()
							bad.Pipelines[0].SpecYAML = "replacement that must roll back"
							bad.PluginArtifacts["parser"] = base64.StdEncoding.EncodeToString([]byte("replacement WASM"))
							err := backup.Restore(ctx, view, bad, opts)
							s.SetFailureInjector(nil)
							if !hit || !errors.Is(err, injected) {
								t.Fatalf("failure hook not reached: hit=%t err=%v", hit, err)
							}
							after := exportForRestoreTest(t, view)
							assertSnapshotPayload(t, before, after)
							if after.Plugins[0].WASMPath != oldPath {
								t.Fatal("failed restore switched live artifact path")
							}
							assertRestoredArtifact(t, opts.PluginsDir, oldPath, snap.PluginArtifacts["parser"])
						})
					}
					// Exercise a real backend constraint failure, not only hooks.
					bad := fidelityFixture()
					bad.Versions = append(bad.Versions, bad.Versions[0])
					if err := backup.Restore(ctx, view, bad, opts); err == nil {
						t.Fatal("duplicate historical ID was silently accepted")
					}
					assertSnapshotPayload(t, before, exportForRestoreTest(t, view))
					if err := backup.Restore(ctx, view, snap, opts); err != nil {
						t.Fatalf("retry: %v", err)
					}
					assertSnapshotPayload(t, snap, exportForRestoreTest(t, view))
					assertNextGeneratedIDs(t, s, snap)
				})
			}

			t.Run("legacy_v1_and_unversioned", func(t *testing.T) {
				for _, version := range []int{0, 1} {
					reset(t)
					snap := fidelityFixture()
					snap.FormatVersion = version
					snap.PluginArtifacts = nil
					snap.Pipelines[0].DesiredState = ""
					snap.Pipelines[0].ObservedState = ""
					blob, err := json.Marshal(snap)
					if err != nil {
						t.Fatal(err)
					}
					decoded, err := backup.ReadJSON(strings.NewReader(string(blob)))
					if err != nil {
						t.Fatal(err)
					}
					dir := t.TempDir()
					opts := backup.Options{ClearBeforeRestore: true, PluginsDir: dir}
					if err := backup.Restore(ctx, s, decoded, opts); err == nil {
						t.Fatal("v1 without its original WASM files must fail before mutation")
					}
					if err := os.WriteFile(filepath.Join(dir, "parser.wasm"), []byte("\x00asm\x01\x00\x00\x00"), 0o600); err != nil {
						t.Fatal(err)
					}
					if err := backup.Restore(ctx, s, decoded, opts); err != nil {
						t.Fatalf("v%d: %v", version, err)
					}
					got := exportForRestoreTest(t, s)
					if got.Pipelines[0].DesiredState != "running" || got.Pipelines[0].ObservedState != "failed" {
						t.Fatalf("legacy lifecycle mapping: %+v", got.Pipelines[0])
					}
					if got.Versions[1].ID != 47 || got.Versions[1].Version != 7 {
						t.Fatalf("v1 version gaps/IDs changed: %+v", got.Versions)
					}
				}
			})
		})
	}
}

func fidelityFixture() *backup.Snapshot {
	// Millisecond precision is representable by all three advertised schemas.
	start := time.Date(2026, 8, 7, 12, 13, 14, 123000000, time.UTC)
	end := start.Add(4321 * time.Millisecond)
	return &backup.Snapshot{
		FormatVersion: 2,
		Pipelines:     []*storage.PipelineRow{{ID: "pipeline-42", Name: "orders", SpecYAML: "name: orders\n", Status: "failed", DesiredState: "running", ObservedState: "failed", Generation: 12, RestoreError: "historical recovery error", CreatedAt: start, UpdatedAt: end}},
		Versions: []*storage.PipelineVersion{
			{ID: 41, Pipeline: "pipeline-42", Version: 3, SpecYAML: "first retained YAML", CreatedAt: start},
			{ID: 47, Pipeline: "pipeline-42", Version: 7, SpecYAML: "latest retained YAML", CreatedAt: end},
		},
		Checkpoints: []*storage.CheckpointRecord{{JobName: "pipeline-42", Source: "mysql_cdc", Generation: 12, Position: json.RawMessage(`{"name":"binlog.000123","pos":98765}`), Timestamp: start, UpdatedAt: end}},
		DeadLetters: []*storage.DLQRecord{{ID: 71, JobName: "pipeline-42", Record: core.Record{Operation: core.OpUpdate, Data: map[string]any{"id": "order-23", "amount": 15}, Metadata: core.Metadata{Table: "orders", PrimaryKeyColumns: []string{"id"}}}, Error: "sink unavailable", ErrorClass: "sink", Attempt: 4, RecordHash: "retained-hash", PipelineVersion: 7, DAGNode: "write-orders", CreatedAt: start, IdentityContext: core.DLQIdentityContext{RawPayload: `{"id":"order-23","amount":15}`, PayloadEncoding: core.DLQPayloadEncodingSourceBytes, PrimaryKeyColumns: []string{"id"}, SourceTable: "orders", TargetTable: "ods_orders", ReplayProvenance: core.DLQReplayProvenanceNormalFlow, ReplayState: core.DLQReplayStateSinkAcked, ReplayAttempt: 2, SinkAcknowledgedAt: &end}}},
		AuditLogs:   []*storage.AuditEntry{{ID: 83, Action: "pipeline.start", Method: "POST", Path: "/api/v2/pipelines/orders/start", Target: "pipeline-42", Remote: "127.0.0.1", CreatedAt: start}},
		RunHistory: []*storage.RunRecord{
			{ID: 91, JobName: "pipeline-42", Status: "failed", StartedAt: start, FinishedAt: &end, DurationMs: 4321, RecordsRead: 111, RecordsWritten: 105, RecordsFailed: 6, RecordsDLQ: 4},
			{ID: 96, JobName: "deleted-pipeline", Status: "running", StartedAt: end},
		},
		Workers:         []*storage.WorkerInfo{{ID: "worker-a", Host: "127.0.0.1", Port: 9000, Slots: 3, Status: "offline", Labels: map[string]string{"zone": "a"}, LastHeartbeat: start, RegisteredAt: start}},
		Tasks:           []*storage.TaskAssignment{{ID: 107, TaskID: "task-a", Pipeline: "pipeline-42", WorkerID: "worker-a", Status: "failed", ShardIndex: 1, ShardTotal: 3, Generation: 5, Attempt: 2, LeaseExpiresAt: &end, LastError: "lease expired", RequiredLabels: map[string]string{"zone": "a"}, AssignedAt: &start, StartedAt: &start, FinishedAt: &end}},
		Plugins:         []*storage.PluginEntry{{Name: "parser", Kind: "transform", WASMPath: "/old-host/parser.wasm", Version: "1.2.3", ABI: "openetl:v1", MinRuntimeVersion: "0.2.0", ManifestJSON: `{"name":"parser"}`, ManifestValidated: false, Enabled: true, InstalledAt: start}},
		PluginArtifacts: map[string]string{"parser": base64.StdEncoding.EncodeToString([]byte("\x00asm\x01\x00\x00\x00"))},
		Connections:     []*storage.ConnectionEntry{{Name: "warehouse", Kind: "sink", Type: "jdbc", Config: map[string]any{"dsn": "enc:v1:retained-ciphertext", "batch_size": 123}, LastStatus: "failed", LastError: "network unavailable", LastTestedAt: &start, CreatedAt: start, UpdatedAt: end}},
		Settings:        map[string]string{"llm.api_key": "enc:v1:retained-setting", "ui.language": "zh"},
	}
}

func exportForRestoreTest(t *testing.T, s storage.Storage) *backup.Snapshot {
	t.Helper()
	snap, err := backup.Export(context.Background(), s, backup.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func assertSnapshotPayload(t *testing.T, want, got *backup.Snapshot) {
	t.Helper()
	// Compare every persisted field; only artifact paths change on relocation.
	canonical := func(snap *backup.Snapshot) map[string]any {
		blob, err := json.Marshal(snap)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		if err := json.Unmarshal(blob, &value); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"created_at", "backend", "schema_versions", "counts", "format_version"} {
			delete(value, key)
		}
		for key, items := range value {
			// v1/in-memory snapshots may encode an empty table as null;
			// the streaming writer emits [] (or {} for settings).
			if items == nil {
				if key == "settings" {
					value[key] = map[string]any{}
				} else {
					value[key] = []any{}
				}
				continue
			}
			if rows, ok := items.([]any); ok {
				for _, row := range rows {
					// PostgreSQL timestamptz returns the same instant in the
					// client's timezone. Compare actual timestamps, not offsets.
					fields := row.(map[string]any)
					for _, field := range []string{"created_at", "updated_at", "timestamp", "started_at", "finished_at", "registered_at", "last_heartbeat", "assigned_at", "lease_expires_at", "last_tested_at", "installed_at"} {
						if encoded, ok := fields[field].(string); ok {
							instant, err := time.Parse(time.RFC3339Nano, encoded)
							if err != nil {
								t.Fatal(err)
							}
							fields[field] = instant.UTC().Format(time.RFC3339Nano)
						}
					}
					if key == "plugins" {
						delete(fields, "wasm_path")
					}
				}
				sort.Slice(rows, func(i, j int) bool {
					a, _ := json.Marshal(rows[i])
					b, _ := json.Marshal(rows[j])
					return string(a) < string(b)
				})
			}
		}
		return value
	}
	w, g := canonical(want), canonical(got)
	for table, expected := range w {
		if !reflect.DeepEqual(expected, g[table]) {
			t.Errorf("%s content changed:\nwant=%v\ngot=%v", table, expected, g[table])
		}
	}
}

func assertRestoredArtifact(t *testing.T, root, path, encoded string) {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	if err != nil || filepath.IsAbs(rel) || strings.HasPrefix(rel, "..") {
		t.Fatalf("restored path escapes destination: %q", path)
	}
	b, err := os.ReadFile(path)
	if err != nil || base64.StdEncoding.EncodeToString(b) != encoded {
		t.Fatalf("restored artifact mismatch: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("restored artifact permissions: %v %v", info, err)
	}
}

func assertNextGeneratedIDs(t *testing.T, s storage.Storage, before *backup.Snapshot) {
	t.Helper()
	ctx := context.Background()
	version, err := s.SavePipelineVersion(ctx, "pipeline-42", "new version after restore")
	if err != nil || version != 8 {
		t.Fatalf("next version=%d err=%v", version, err)
	}
	if err := s.WriteDeadLetter(ctx, &storage.DLQRecord{JobName: "pipeline-42", Record: core.Record{Data: map[string]any{"new": true}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteAudit(ctx, &storage.AuditEntry{Action: "after.restore"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordRunStart(ctx, "pipeline-42"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTask(ctx, &storage.TaskAssignment{TaskID: "task-after", Pipeline: "pipeline-42", Status: "pending", ShardTotal: 1}); err != nil {
		t.Fatal(err)
	}
	after := exportForRestoreTest(t, s)
	maxID := func(rows any) int64 {
		v := reflect.ValueOf(rows)
		var max int64
		for i := 0; i < v.Len(); i++ {
			id := v.Index(i).Elem().FieldByName("ID").Int()
			if id > max {
				max = id
			}
		}
		return max
	}
	for _, pair := range [][2]any{{before.Versions, after.Versions}, {before.DeadLetters, after.DeadLetters}, {before.AuditLogs, after.AuditLogs}, {before.RunHistory, after.RunHistory}, {before.Tasks, after.Tasks}} {
		if maxID(pair[1]) <= maxID(pair[0]) {
			t.Errorf("auto-ID sequence did not advance: %T", pair[0])
		}
	}
}

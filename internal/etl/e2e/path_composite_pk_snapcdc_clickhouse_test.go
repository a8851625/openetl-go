//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
)

// TestPathCompositePKSnapshotCDCToClickHouse certifies the IT-6 composite-PK
// snapshot path end to end on real MySQL 8 + ClickHouse:
//
//  1. preflight: snapshot_coverage field issue reports the composite cursor
//     plan (visibility: existing-data gaps are visible before start).
//  2. snapshot: composite-PK table存量 lands complete in ClickHouse with an
//     auto-created ORDER BY (entity_id, language_id) ReplacingMergeTree.
//  3. cdc_update_delete: snapshot row UPDATE overwrites (version merge),
//     DELETE writes a composite-key tombstone (FINAL hides the row).
//  4. restart_recovery: SIGKILL + insert while down + resume from composite
//     snapshot/cdc checkpoint.
//  5. checkpoint_reset: full replay re-reads the composite snapshot and the
//     ReplacingMergeTree absorbs the duplicates (FINAL unchanged).
//
// The pipeline mirrors the production shape from the v0.2.12 incident:
// tables list (no pk_column override), pk_columns_from_metadata sink so the
// composite key derives from record metadata, exactly like CDC records.
func TestPathCompositePKSnapshotCDCToClickHouse(t *testing.T) {
	ctx := context.Background()
	my, err := harness.MySQL(ctx)
	if err != nil {
		harness.Skip(t, "mysql container unavailable: %v", err)
	}
	ch, err := harness.ClickHouse(ctx)
	if err != nil {
		harness.Skip(t, "clickhouse container unavailable: %v", err)
	}
	ns := harness.Namespace(t)
	srcDB, chDB := harness.DBName(ns, "src"), harness.DBName(ns, "ch")
	table := "category_description"
	pipeline := "e2e-" + ns

	rec := harness.NewEvidenceRecorder(t, "mysql_snap_cdc__ch_rmt_composite_pk")
	rec.AddDep("mysql", harness.MySQLImage)
	rec.AddDep("clickhouse", harness.ClickHouseImage)
	rec.AddDep("harness", "e2e")

	// OpenCart-style *_description composite PK: (entity_id, language_id).
	// Only 3 distinct entity_ids so the limit=2 page boundary always falls
	// inside an entity group: the second column must carry the order.
	mustExec(t, my.Exec, fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS %[1]s;
DROP TABLE IF EXISTS %[1]s.%[2]s;
CREATE TABLE %[1]s.%[2]s (
  entity_id INT NOT NULL,
  language_id INT NOT NULL,
  name VARCHAR(128) NOT NULL,
  meta_title VARCHAR(128),
  PRIMARY KEY (entity_id, language_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO %[1]s.%[2]s (entity_id, language_id, name, meta_title) VALUES
  (1, 1, 'Root EN', 'root'),
  (1, 2, 'Root ZH', 'root'),
  (2, 1, 'Cat EN', 'cat'),
  (2, 2, 'Cat ZH', 'cat'),
  (3, 1, 'Deep EN', 'deep'),
  (3, 2, 'Deep ZH', 'deep');`, srcDB, table))

	mustExec(t, ch.Exec, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", chDB))
	mustExec(t, ch.Exec, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", chDB, table))

	srv, err := harness.NewServer(ns)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// No pk_column override: the source must auto-detect the composite PK
	// (the exact production incident shape). Sink derives keys from metadata.
	spec := fmt.Sprintf(`name: "%s"
source:
  type: mysql_snapshot_cdc
  config:
    host: "127.0.0.1"
    port: %d
    user: "%s"
    password: "%s"
    database: "%s"
    tables:
      - "%s"
    limit: 2
    server_id: %d

transforms:
  - type: identity
    config: {}

sink:
  type: clickhouse
  config:
    host: "127.0.0.1"
    port: %d
    user: "%s"
    password: "%s"
    database: "%s"
    pk_columns_from_metadata: true
    version_column: "_version"
    auto_create: true
    schema_drift: "add_columns"
    ddl_policy: "apply"
    source_dialect: "mysql"

batch_size: 2
checkpoint_interval_sec: 1
backpressure_buffer: 20

retry:
  max_attempts: 2
  initial_interval_ms: 100
  max_interval_ms: 500

dlq:
  enable: true
`,
		pipeline, my.Port, harness.MySQLSyncUser, harness.MySQLSyncPassword, srcDB, table, serverIDFor(ns),
		ch.NativePort, harness.CHUser, harness.CHPassword, chDB)
	specPath := filepath.Join(srv.SpecsDir, pipeline+".yaml")
	if err := os.WriteFile(specPath, []byte(spec), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}

	// Case preflight_snapshot_coverage: the plan is visible before start
	// (table resolves a composite cursor, nothing skipped). Validate takes a
	// JSON spec object; the preflight result rides the response.
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}
	specObj := map[string]any{
		"name": pipeline,
		"source": map[string]any{
			"type": "mysql_snapshot_cdc",
			"config": map[string]any{
				"host":      "127.0.0.1",
				"port":      my.Port,
				"user":      harness.MySQLSyncUser,
				"password":  harness.MySQLSyncPassword,
				"database":  srcDB,
				"tables":    []string{table},
				"limit":     2,
				"server_id": serverIDFor(ns),
			},
		},
		"transforms": []any{map[string]any{"type": "identity", "config": map[string]any{}}},
		"sink": map[string]any{
			"type": "clickhouse",
			"config": map[string]any{
				"host":                     "127.0.0.1",
				"port":                     ch.NativePort,
				"user":                     harness.CHUser,
				"password":                 harness.CHPassword,
				"database":                 chDB,
				"pk_columns_from_metadata": true,
				"version_column":           "_version",
				"auto_create":              true,
				"schema_drift":             "add_columns",
				"ddl_policy":               "apply",
				"source_dialect":           "mysql",
			},
		},
		"batch_size":              2,
		"checkpoint_interval_sec": 1,
		"backpressure_buffer":     20,
	}
	payload, _ := json.Marshal(map[string]any{"spec": specObj})
	resp, err := http.Post(srv.APIURL("/api/v2/specs/validate"), "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("validate spec: %v", err)
	}
	defer resp.Body.Close()
	pfBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read validate response: %v", err)
	}
	pf := struct {
		Preflight struct {
			FieldIssues []struct {
				Check   string `json:"check"`
				Message string `json:"message"`
			} `json:"field_issues"`
			Issues []struct {
				Check   string `json:"check"`
				Message string `json:"message"`
			} `json:"issues"`
		} `json:"preflight"`
	}{}
	if err := json.Unmarshal(pfBody, &pf); err != nil {
		t.Fatalf("decode validate response: %v\n%s", err, pfBody)
	}
	foundCoverage := false
	for _, fi := range pf.Preflight.FieldIssues {
		if fi.Check == "snapshot_coverage" {
			foundCoverage = true
			if !strings.Contains(fi.Message, "1 table(s) snapshotted") || !strings.Contains(fi.Message, "1 composite") {
				t.Fatalf("snapshot_coverage message = %q, want composite cursor plan for %s", fi.Message, table)
			}
		}
	}
	for _, iss := range pf.Preflight.Issues {
		if iss.Check == "snapshot-coverage" {
			t.Fatalf("unexpected snapshot-coverage WARN for composite table: %s", iss.Message)
		}
	}
	if !foundCoverage {
		t.Fatalf("validate response lacks snapshot_coverage field issue:\n%s", pfBody)
	}
	t.Logf("case preflight_snapshot_coverage OK: composite plan visible before start")
	rec.AddCheck("preflight_snapshot_coverage", "passed", "")

	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never ran: %v\n%s", err, srv.LogTail())
	}

	waitCH := func(sql, want string) {
		t.Helper()
		if err := ch.WaitValue(sql, want, 90*time.Second); err != nil {
			t.Fatal(err)
		}
	}

	// Case snapshot_composite: all 6 rows land; auto-created table has
	// ORDER BY (entity_id, language_id).
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL", chDB, table), "6")
	waitCH(fmt.Sprintf(
		"SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s' AND engine_full LIKE '%%ReplacingMergeTree%%'",
		chDB, table), "1")
	ordering, err := ch.QueryValue(fmt.Sprintf("SELECT sorting_key FROM system.tables WHERE database = '%s' AND name = '%s'", chDB, table))
	if err != nil {
		t.Fatalf("read sorting key: %v", err)
	}
	if !strings.Contains(ordering, "entity_id") || !strings.Contains(ordering, "language_id") {
		t.Fatalf("sorting key = %q, want composite (entity_id, language_id)", ordering)
	}
	t.Logf("case snapshot_composite OK: 6 rows FINAL, ORDER BY (%s)", ordering)
	rec.AddCheck("snapshot_composite_exhaustive", "passed", "")

	// Case cdc_update_delete: version merge + composite tombstone.
	mustExec(t, my.Exec, fmt.Sprintf(
		"UPDATE %s.%s SET name = 'Root ZH v2' WHERE entity_id = 1 AND language_id = 2; "+
			"DELETE FROM %s.%s WHERE entity_id = 3 AND language_id = 1;",
		srcDB, table, srcDB, table))
	waitCH(fmt.Sprintf(
		"SELECT count() FROM %s.%s FINAL WHERE entity_id = 1 AND language_id = 2 AND name = 'Root ZH v2'",
		chDB, table), "1")
	waitCH(fmt.Sprintf(
		"SELECT count() FROM %s.%s FINAL WHERE entity_id = 3 AND language_id = 1",
		chDB, table), "0")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL", chDB, table), "5")
	t.Logf("case cdc_update_delete OK: UPDATE merged, DELETE tombstoned")
	rec.AddCheck("cdc_composite_update_delete", "passed", "")

	// Case restart_recovery: SIGKILL, write while down, resume.
	if err := srv.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	mustExec(t, my.Exec, fmt.Sprintf(
		"INSERT INTO %s.%s (entity_id, language_id, name) VALUES (4, 1, 'New EN'), (4, 2, 'New ZH');",
		srcDB, table))
	if err := srv.Restart(ctx); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never resumed: %v\n%s", err, srv.LogTail())
	}
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE entity_id = 4", chDB, table), "2")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL", chDB, table), "7")
	t.Logf("case restart_recovery OK: resumed from checkpoint, entity 4 captured")
	rec.AddCheck("restart_recovery", "passed", "")

	// Case checkpoint_reset: full replay re-snapshots the composite table;
	// ReplacingMergeTree absorbs duplicates on FINAL.
	if _, err := srv.Post("/api/v2/pipelines/" + pipeline + "/stop"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if _, err := srv.Post("/api/v2/pipelines/" + pipeline + "/checkpoint/reset"); err != nil {
		t.Fatalf("checkpoint reset: %v", err)
	}
	startPipeline(t, srv, pipeline)
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never restarted: %v\n%s", err, srv.LogTail())
	}
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL", chDB, table), "7")
	waitCH(fmt.Sprintf(
		"SELECT count() FROM %s.%s FINAL WHERE entity_id = 1 AND language_id = 2 AND name = 'Root ZH v2'",
		chDB, table), "1")
	waitCH(fmt.Sprintf(
		"SELECT count() FROM %s.%s FINAL WHERE entity_id = 3 AND language_id = 1",
		chDB, table), "0")
	t.Logf("case checkpoint_reset OK: composite replay absorbed by ReplacingMergeTree")
	rec.AddCheck("checkpoint_reset_composite_replay", "passed", "")

	// Reconciliation: the pipeline is still registered and visible.
	if _, err := srv.Get("/api/v2/pipelines"); err != nil {
		t.Fatalf("pipelines list: %v", err)
	}
	t.Logf("path composite-PK mysql_snapshot_cdc__clickhouse PASS (at-least-once; RMT absorbs replay)")
}

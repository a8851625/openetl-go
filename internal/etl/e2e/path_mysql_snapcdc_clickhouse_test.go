//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
)

// mustQuery runs a ClickHouse query whose failure is fatal.
func mustQuery(t *testing.T, ch *harness.ClickHouseInstance, sql string) string {
	t.Helper()
	v, err := ch.QueryValue(sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return v
}

// TestPathMySQLSnapshotCDCToClickHouse migrates
// hack/e2e-snapshot-cdc-clickhouse.sh (main recommended path:
// mysql snapshot+CDC -> ClickHouse ReplacingMergeTree).
// Cases (same order, same assertions — no relaxation):
//  1. snapshot: initial 5 rows land, FINAL visible
//  2. cdc_update_insert_delete: update id2 / delete id3 / insert id6
//  3. schema_drift: add-column propagates, row 7 carries the new column
//  4. restart_recovery: SIGKILL, row written while down, resume from checkpoint
//  5. checkpoint_reset: replay absorbed by ReplacingMergeTree (FINAL stays 7)
//  6. clickhouse_outage_dlq_replay: outage -> DLQ with classified error ->
//     restore -> replay -> entry deleted
func TestPathMySQLSnapshotCDCToClickHouse(t *testing.T) {
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
	table := "snap_cdc_clickhouse"
	pipeline := "e2e-" + ns

	// Prepare MySQL source table + snapshot rows (script lines 124-139).
	mustExec(t, my.Exec, fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS %[1]s;
DROP TABLE IF EXISTS %[1]s.%[2]s;
CREATE TABLE %[1]s.%[2]s (
  id INT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  status VARCHAR(32),
  amount DECIMAL(12,2),
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO %[1]s.%[2]s (id, name, status, amount) VALUES
  (1, 'Snapshot CH 1', 'active', 10.10),
  (2, 'Snapshot CH 2', 'active', 20.20),
  (3, 'Snapshot CH 3', 'inactive', 30.30),
  (4, 'Snapshot CH 4', 'active', 40.40),
  (5, 'Snapshot CH 5', 'active', 50.50);`, srcDB, table))

	// Prepare ClickHouse target (script lines 142-145).
	mustExec(t, ch.Exec, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", chDB))
	mustExec(t, ch.Exec, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", chDB, table))

	srv, err := harness.NewServer(ns)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// Pipeline spec mirrors testdata/pipes-snapshot-cdc-clickhouse (limit 2,
	// batch 2, checkpoint 1s, retry 2/100/500, auto_create + add_columns).
	spec := fmt.Sprintf(`name: "%s"
source:
  type: mysql_snapshot_cdc
  config:
    host: "127.0.0.1"
    port: %d
    user: "%s"
    password: "%s"
    database: "%s"
    table: "%s"
    pk_column: "id"
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
    table: "%s"
    pk_columns:
      - "id"
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
		ch.NativePort, harness.CHUser, harness.CHPassword, chDB, table)
	specPath := filepath.Join(srv.SpecsDir, pipeline+".yaml")
	if err := os.WriteFile(specPath, []byte(spec), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}

	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never ran: %v\n%s", err, srv.LogTail())
	}

	waitCH := func(sql, want string) {
		t.Helper()
		if err := ch.WaitValue(sql, want, 90*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	waitCheckpointCDC := func() {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			body, err := srv.Get("/api/v2/pipelines/" + pipeline + "/checkpoint")
			if err == nil && strings.Contains(string(body), `"phase":"cdc"`) {
				return
			}
			time.Sleep(time.Second)
		}
		t.Fatalf("checkpoint never reached cdc phase; log tail:\n%s", srv.LogTail())
	}

	// Case snapshot: verify initial snapshot copied.
	// Do not require phase=cdc merely because the producer finished
	// snapshotting: the durable checkpoint stays in snapshot phase until an
	// actual CDC record is sink-acknowledged (script lines 157-162).
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL", chDB, table), "5")
	t.Logf("case snapshot OK: 5 rows via FINAL")

	// Case cdc_update_insert_delete.
	mustExec(t, my.Exec, fmt.Sprintf(
		"UPDATE %s.%s SET amount = 222.22 WHERE id = 2; "+
			"DELETE FROM %s.%s WHERE id = 3; "+
			"INSERT INTO %s.%s (id, name, status, amount) VALUES (6, 'CDC CH 6', 'active', 66.66);",
		srcDB, table, srcDB, table, srcDB, table))
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 2 AND amount = 222.22", chDB, table), "1")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 3", chDB, table), "0")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 6 AND amount = 66.66", chDB, table), "1")
	waitCheckpointCDC()
	t.Logf("case cdc_update_insert_delete OK")

	// Case schema_drift: add-column propagates (script lines 175-182).
	mustExec(t, my.Exec, fmt.Sprintf(
		"ALTER TABLE %s.%s ADD COLUMN loyalty VARCHAR(32); "+
			"INSERT INTO %s.%s (id, name, status, amount, loyalty) VALUES (7, 'CDC CH 7', 'active', 77.77, 'gold');",
		srcDB, table, srcDB, table))
	waitCH(fmt.Sprintf(
		"SELECT count() FROM system.columns WHERE database = '%s' AND table = '%s' AND name = 'loyalty'", chDB, table), "1")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 7 AND loyalty = 'gold'", chDB, table), "1")
	waitCheckpointCDC()
	t.Logf("case schema_drift OK: loyalty add-column propagated")

	// Case restart_recovery from checkpoint (script lines 184-192).
	if err := srv.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	mustExec(t, my.Exec, fmt.Sprintf(
		"INSERT INTO %s.%s (id, name, status, amount, loyalty) VALUES (8, 'Restart CH 8', 'active', 88.88, 'silver');",
		srcDB, table))
	if err := srv.Restart(ctx); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never resumed: %v\n%s", err, srv.LogTail())
	}
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 8 AND loyalty = 'silver'", chDB, table), "1")
	waitCheckpointCDC()
	t.Logf("case restart_recovery OK: id=8 resumed from checkpoint")

	// Case checkpoint_reset: replay absorbed by ReplacingMergeTree
	// (script lines 194-227).
	beforeRawTotal, err := ch.QueryValue(fmt.Sprintf("SELECT count() FROM %s.%s", chDB, table))
	if err != nil {
		t.Fatalf("raw count before reset: %v", err)
	}
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
	// FINAL business-key state must remain correct (no silent loss / no
	// inflated keys).
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL", chDB, table), "7")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 1", chDB, table), "1")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 8 AND loyalty = 'silver'", chDB, table), "1")
	// Prefer observing raw duplicate parts, but accept immediate merge as long
	// as FINAL count stays correct and the pipeline advanced
	// (at-least-once + RMT absorb) — script lines 204-220, note-only.
	atoi := func(s string, def int) int {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return def
		}
		return n
	}
	beforeRawN := atoi(beforeRawTotal, 0)
	rawID1N, rawTotalN := 0, 0
	for i := 0; i < 30; i++ {
		rawID1N = atoi(mustQuery(t, ch, fmt.Sprintf("SELECT count() FROM %s.%s WHERE id = 1", chDB, table)), 0)
		rawTotalN = atoi(mustQuery(t, ch, fmt.Sprintf("SELECT count() FROM %s.%s", chDB, table)), 0)
		if rawID1N >= 2 || rawTotalN > beforeRawN {
			break
		}
		time.Sleep(time.Second)
	}
	if rawID1N < 2 && rawTotalN <= beforeRawN {
		t.Logf("note: raw duplicate parts not observed (likely merged); FINAL absorption still required")
	}
	// Advance the reset run to durable CDC with an acknowledged event.
	mustExec(t, my.Exec, fmt.Sprintf("UPDATE %s.%s SET amount = 111.11 WHERE id = 1;", srcDB, table))
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 1 AND amount = 111.11", chDB, table), "1")
	waitCheckpointCDC()
	t.Logf("case checkpoint_reset OK: FINAL absorbed replay, advanced to durable CDC")

	// Case clickhouse_outage_dlq_replay (script lines 229-262).
	if err := ch.Container.Stop(ctx, nil); err != nil {
		t.Fatalf("stop clickhouse: %v", err)
	}
	mustExec(t, my.Exec, fmt.Sprintf(
		"INSERT INTO %s.%s (id, name, status, amount, loyalty) VALUES (9001, 'DLQ CH 9001', 'active', 900.10, 'replay');",
		srcDB, table))
	dlqBody := pollDLQ(t, srv, pipeline, "9001", 90*time.Second)
	t.Logf("dlq body: %s", dlqBody)
	// The DLQ record must carry a classified sink error, not silence.
	errKeywords := []string{"clickhouse", "connection refused", "broken pipe", "reset by peer", "EOF"}
	if !containsAny(string(dlqBody), errKeywords) {
		t.Fatalf("dlq entry lacks classified clickhouse error: %s", dlqBody)
	}
	dlqID := extractDLQID(t, dlqBody)

	_, _ = srv.Post("/api/v2/pipelines/" + pipeline + "/stop") // script: || true
	if err := ch.Container.Start(ctx); err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	if err := harness.PollUntil(ctx, 90*time.Second, time.Second, "clickhouse ping", ch.Ping); err != nil {
		t.Fatalf("clickhouse did not come back: %v", err)
	}
	replayBody, err := srv.Post(fmt.Sprintf("/api/v2/dlq/%s/%s/replay", pipeline, dlqID))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	t.Logf("replay body: %s", replayBody)
	extractReplayed(t, replayBody)
	waitCH(fmt.Sprintf("SELECT count() FROM %s.%s FINAL WHERE id = 9001 AND loyalty = 'replay'", chDB, table), "1")
	assertDLQEntryGone(t, srv, pipeline, dlqID, "9001")
	t.Logf("case clickhouse_outage_dlq_replay OK")

	// Reconciliation: the pipeline is still registered and visible.
	if _, err := srv.Get("/api/v2/pipelines"); err != nil {
		t.Fatalf("pipelines list: %v", err)
	}
	t.Logf("path matrix mysql_snapshot_cdc__clickhouse PASS (rpo=last durable checkpoint; at-least-once)")
}

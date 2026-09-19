//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
)

// TestPathClickHouseDedupCrashWindowMultiTable extends the CH-C1 crash-window
// conformance to multi-table fan-out (sink table_template): two source tables
// route to two target tables through the same batch, the crash window opens
// after sink ack and before checkpoint commit, and replay must leave exactly
// one row per business key in EACH target table (per-table duplicate
// absorption, no cross-table loss). The envelope's native metadata must carry
// the per-table row map covering both tables.
//
// Deferred from IT-5/T5.2 as a bounded follow-up; native protocol only (the
// single-table test already proves native/http equivalence of the token
// mechanics; this test proves the multi-table boundary).
func TestPathClickHouseDedupCrashWindowMultiTable(t *testing.T) {
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
	orders, users := "mt_orders", "mt_users"
	pipeline := "e2e-" + ns

	rec := harness.NewEvidenceRecorder(t, "ch_dedup_crash_window_multitable")
	rec.AddDep("mysql", harness.MySQLImage)
	rec.AddDep("clickhouse", harness.ClickHouseImage)
	rec.AddDep("harness", "e2e")

	mustExec(t, my.Exec, fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS %[1]s;
DROP TABLE IF EXISTS %[1]s.%[2]s;
DROP TABLE IF EXISTS %[1]s.%[3]s;
CREATE TABLE %[1]s.%[2]s (
  id INT PRIMARY KEY,
  amount DECIMAL(12,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE %[1]s.%[3]s (
  id INT PRIMARY KEY,
  name VARCHAR(128) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO %[1]s.%[2]s (id, amount) VALUES (1, 10.10), (2, 20.20);
INSERT INTO %[1]s.%[3]s (id, name) VALUES (1, 'Alice'), (2, 'Bob');`, srcDB, orders, users))

	mustExec(t, ch.Exec, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", chDB))
	mustExec(t, ch.Exec, fmt.Sprintf("DROP TABLE IF EXISTS %s.ods_%s", chDB, orders))
	mustExec(t, ch.Exec, fmt.Sprintf("DROP TABLE IF EXISTS %s.ods_%s", chDB, users))

	spec := fmt.Sprintf(`name: "%s"
source:
  type: mysql_snapshot_cdc
  config:
    host: "127.0.0.1"
    port: %d
    user: "%s"
    password: "%s"
    database: "%s"
    tables: ["%s", "%s"]
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
    protocol: "native"
    user: "%s"
    password: "%s"
    database: "%s"
    table_template: "ods_{table}"
    pk_columns: ["id"]
    version_column: "_version"
    auto_create: true
    schema_drift: "add_columns"
    ddl_policy: "apply"
    source_dialect: "mysql"

batch_size: 2
checkpoint_interval_sec: 120
backpressure_buffer: 20

retry:
  max_attempts: 2
  initial_interval_ms: 100
  max_interval_ms: 500

dlq:
  enable: true
`,
		pipeline, my.Port, harness.MySQLSyncUser, harness.MySQLSyncPassword, srcDB, orders, users, serverIDFor(ns),
		ch.NativePort, harness.CHUser, harness.CHPassword, chDB)

	srv, err := harness.NewServer(ns)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
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

	// Snapshot of both tables lands (checkpoint interval 120s keeps the
	// crash window open).
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL", chDB, orders), "2")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL", chDB, users), "2")

	// ---- Case 1: envelope native metadata covers BOTH tables. Force a
	// checkpoint via stop and read the per-table row map.
	if _, err := srv.Post("/api/v2/pipelines/" + pipeline + "/stop"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	time.Sleep(2 * time.Second)
	envelope := checkpointNativeMetaTables(t, srv, pipeline)
	tables, _ := envelope["tables"].(map[string]any)
	if len(tables) == 0 {
		t.Fatalf("envelope tables map empty: %#v", envelope)
	}
	// The envelope reports the LAST acknowledged batch; snapshot emits one
	// batch per table (batch_size 2 = one table's rows), so exactly one
	// ods_ table is expected here — but never a foreign table.
	allowed := map[string]bool{"ods_" + orders: true, "ods_" + users: true}
	for k := range tables {
		if !allowed[k] {
			t.Fatalf("envelope tables map has foreign table %q: %#v", k, tables)
		}
	}
	if envelope["dedup_token"] == nil || envelope["dedup_token"].(string) == "" {
		t.Fatalf("checkpoint envelope missing dedup_token: %#v", envelope)
	}
	t.Logf("case ack_token_covers_tables OK: tables=%v", keysOf(tables))
	rec.AddCheck("multitable_ack_token_in_checkpoint", "passed", "")

	// ---- Case 2: crash inside the window with writes to BOTH tables.
	if _, err := srv.Post("/api/v2/pipelines/" + pipeline + "/start"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never resumed: %v\n%s", err, srv.LogTail())
	}
	mustExec(t, my.Exec, fmt.Sprintf(
		"INSERT INTO %s.%s (id, amount) VALUES (3, 30.30); INSERT INTO %s.%s (id, name) VALUES (3, 'Carol');",
		srcDB, orders, srcDB, users))
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL WHERE id = 3", chDB, orders), "1")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL WHERE id = 3", chDB, users), "1")
	if err := srv.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if err := srv.Restart(ctx); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never resumed after crash: %v\n%s", err, srv.LogTail())
	}

	// Per-table FINAL contract: exactly one row per business key in each.
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL", chDB, orders), "3")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL", chDB, users), "3")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL WHERE id = 3 AND amount = 30.30", chDB, orders), "1")
	waitCH(fmt.Sprintf("SELECT count() FROM %s.ods_%s FINAL WHERE id = 3 AND name = 'Carol'", chDB, users), "1")
	t.Logf("case crash_window_replay OK: both tables FINAL=3, per-key duplicates absorbed")
	rec.AddCheck("multitable_crash_window_replay", "passed", "")
}

// checkpointNativeMetaTables is the multi-table variant of the envelope
// reader: returns checkpoint.position.sink_commit.native.
func checkpointNativeMetaTables(t *testing.T, srv *harness.Server, pipeline string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		body, err := srv.Get("/api/v2/pipelines/" + pipeline + "/checkpoint")
		if err == nil {
			var cp struct {
				Checkpoint struct {
					Position struct {
						SinkCommit struct {
							Native map[string]any `json:"native"`
						} `json:"sink_commit"`
					} `json:"position"`
				} `json:"checkpoint"`
			}
			if json.Unmarshal(body, &cp) == nil && cp.Checkpoint.Position.SinkCommit.Native != nil {
				return cp.Checkpoint.Position.SinkCommit.Native
			}
			last = string(body)
		} else {
			last = err.Error()
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("checkpoint envelope never exposed sink native metadata; last: %s", last)
	return nil
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

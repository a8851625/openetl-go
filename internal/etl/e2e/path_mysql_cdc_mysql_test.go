//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
)

// TestPathMySQLCDCMySQLUpsert migrates hack/e2e-path-mysql-cdc-mysql.sh
// (PR-2.2 forced primary path: mysql_cdc -> mysql upsert).
// Cases (same order, same assertions — no relaxation):
//  1. happy: CDC insert x3 + update lands via upsert
//  2. crash_restart: SIGKILL after sink ack, resume from checkpoint, no loss
//  3. checkpoint_reset: full replay absorbed by upsert (one row per key)
//  4. sink_outage_dlq_replay: outage -> DLQ -> restore -> replay -> entry deleted
func TestPathMySQLCDCMySQLUpsert(t *testing.T) {
	ctx := context.Background()
	my, err := harness.MySQL(ctx)
	if err != nil {
		harness.Skip(t, "mysql container unavailable: %v", err)
	}
	ns := harness.Namespace(t)
	srcDB, tgtDB := harness.DBName(ns, "src"), harness.DBName(ns, "tgt")
	table := "path_matrix_customers"
	pipeline := "e2e-" + ns

	// Evidence recorder (IT-1/T1.5): certifies this run on cleanup, bound to
	// the exact source revision and per-case checks.
	rec := harness.NewEvidenceRecorder(t, "mysql_cdc__mysql_upsert")
	rec.AddDep("mysql", harness.MySQLImage)
	rec.AddDep("harness", "e2e")

	// Prepare source/target tables (script lines 134-148).
	mustExec(t, my.Exec, fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS %[1]s;
CREATE DATABASE IF NOT EXISTS %[2]s;
DROP TABLE IF EXISTS %[1]s.%[3]s;
CREATE TABLE %[1]s.%[3]s (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255),
  status VARCHAR(50),
  amount DECIMAL(10,2)
);
DROP TABLE IF EXISTS %[2]s.%[3]s;
CREATE TABLE %[2]s.%[3]s LIKE %[1]s.%[3]s;
GRANT ALL PRIVILEGES ON %[2]s.* TO '%[4]s'@'%%';
FLUSH PRIVILEGES;`, srcDB, tgtDB, table, harness.MySQLSyncUser))

	srv, err := harness.NewServer(ns)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// Write the path matrix pipeline spec (script lines 156-198; batch_size 1,
	// checkpoint 1s, retry 3/100/1000, dlq on — unchanged).
	spec := fmt.Sprintf(`name: "%s"
source:
  type: mysql_cdc
  config:
    host: "127.0.0.1"
    port: %d
    user: "%s"
    password: "%s"
    database: "%s"
    server_id: %d
    tables:
      - "%s"

transforms:
  - type: identity
    config: {}

sink:
  type: mysql
  config:
    host: "127.0.0.1"
    port: %d
    user: "%s"
    password: "%s"
    database: "%s"
    table: "%s"
    batch_mode: "upsert"
    pk_columns:
      - "id"

batch_size: 1
checkpoint_interval_sec: 1
backpressure_buffer: 20

retry:
  max_attempts: 3
  initial_interval_ms: 100
  max_interval_ms: 1000

dlq:
  enable: true
`,
		pipeline, my.Port, harness.MySQLSyncUser, harness.MySQLSyncPassword, srcDB, serverIDFor(ns), table,
		my.Port, harness.MySQLSyncUser, harness.MySQLSyncPassword, tgtDB, table)
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

	wait := func(sql, want string) {
		t.Helper()
		if err := my.WaitValue(sql, want, 90*time.Second); err != nil {
			t.Fatal(err)
		}
	}

	// Case happy: CDC insert + update (emit after pipeline is running).
	mustExec(t, my.Exec, fmt.Sprintf("DELETE FROM %[1]s.%[2]s WHERE id IN (9101,9102,9103,9104,9201); "+
		"INSERT INTO %[1]s.%[2]s (id, name, email, status, amount) VALUES "+
		"(9101, 'Path Alice', 'path-alice@example.com', 'active', 10.10), "+
		"(9102, 'Path Bob', 'path-bob@example.com', 'active', 20.20), "+
		"(9103, 'Path Carol', 'path-carol@example.com', 'active', 30.30);", srcDB, table))
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id IN (9101,9102,9103)", tgtDB, table), "3")
	mustExec(t, my.Exec, fmt.Sprintf("UPDATE %s.%s SET amount=11.11, status='vip' WHERE id=9101;", srcDB, table))
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id=9101 AND amount=11.11 AND status='vip'", tgtDB, table), "1")
	t.Logf("case happy OK: source_count=3 sink_count=3 silent_loss=0")
	rec.AddCheck("happy_path", "passed", "")

	// Case crash_restart: SIGKILL after sink ack, resume from checkpoint.
	if err := srv.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	mustExec(t, my.Exec, fmt.Sprintf(
		"INSERT INTO %s.%s (id, name, email, status, amount) VALUES (9104, 'Path Dave', 'path-dave@example.com', 'active', 40.40);",
		srcDB, table))
	if err := srv.Restart(ctx); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := srv.WaitPipelineRunning(pipeline, 60*time.Second); err != nil {
		t.Fatalf("pipeline never resumed: %v\n%s", err, srv.LogTail())
	}
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id=9104 AND amount=40.40", tgtDB, table), "1")
	// prior rows still present (no silent loss)
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id IN (9101,9102,9103,9104)", tgtDB, table), "4")
	t.Logf("case crash_restart OK: replay_duplicates_absorbed=true silent_loss=0")
	rec.AddCheck("crash_restart_recovery", "passed", "")

	// Case checkpoint_reset: full replay absorbed by upsert.
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
	// CDC from earliest may re-deliver; upsert keeps one row per business key.
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id IN (9101,9102,9103,9104)", tgtDB, table), "4")
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id=9101 AND amount=11.11", tgtDB, table), "1")
	t.Logf("case checkpoint_reset OK: replay_duplicates_absorbed=true silent_loss=0")
	rec.AddCheck("checkpoint_reset_absorption", "passed", "")

	// Case sink_outage_dlq_replay: simulate outage by renaming the target table.
	mustExec(t, my.Exec, fmt.Sprintf("RENAME TABLE %s.%s TO %s.%s_outage_backup;", tgtDB, table, tgtDB, table))
	mustExec(t, my.Exec, fmt.Sprintf(
		"INSERT INTO %s.%s (id, name, email, status, amount) VALUES (9201, 'Path Outage', 'path-outage@example.com', 'active', 99.99);",
		srcDB, table))

	dlqBody := pollDLQ(t, srv, pipeline, "9201", 90*time.Second)
	t.Logf("dlq body: %s", dlqBody)
	dlqID := extractDLQID(t, dlqBody)

	// Restore target table (with prior rows) and replay DLQ.
	mustExec(t, my.Exec, fmt.Sprintf("RENAME TABLE %s.%s_outage_backup TO %s.%s;", tgtDB, table, tgtDB, table))
	_, _ = srv.Post("/api/v2/pipelines/" + pipeline + "/stop") // script: || true
	replayBody, err := srv.Post(fmt.Sprintf("/api/v2/dlq/%s/%s/replay", pipeline, dlqID))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	t.Logf("replay body: %s", replayBody)
	extractReplayed(t, replayBody)
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id=9201 AND amount=99.99", tgtDB, table), "1")
	assertDLQEntryGone(t, srv, pipeline, dlqID, "9201")
	// full business key set still consistent
	wait(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id IN (9101,9102,9103,9104,9201)", tgtDB, table), "5")
	t.Logf("case sink_outage_dlq_replay OK: silent_loss=0")
	rec.AddCheck("sink_outage_dlq_replay", "passed", "")

	// Reconciliation: the pipeline is still registered and visible.
	if _, err := srv.Get("/api/v2/pipelines"); err != nil {
		t.Fatalf("pipelines list: %v", err)
	}
	t.Logf("path matrix mysql_cdc__mysql_upsert PASS (rpo=last durable checkpoint; at-least-once)")
}

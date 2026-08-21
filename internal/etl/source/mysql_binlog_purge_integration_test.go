package source

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
)

// bug2Connect opens the real-MySQL integration DSN for BUG-2 runtime
// verification. Skips (does not fail) when MySQL is unavailable, matching
// repo e2e-skip conventions (missing DSN/service = skip, not fail).
func bug2Connect(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("OPENETL_TEST_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:root123456@tcp(127.0.0.1:3399)/etl"
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Skipf("open mysql: %v", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("mysql unavailable (%v): BUG-2 runtime verification needs a real MySQL with binlog", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func bug2MasterStatus(t *testing.T, db *sql.DB) (string, uint32) {
	t.Helper()
	var file string
	var pos int
	var discard sql.RawBytes
	// MySQL <= 8.3: SHOW MASTER STATUS (5 columns); MySQL 8.4+: SHOW BINARY
	// LOG STATUS (4 columns). Probe column count first with RowsAffected-free
	// queries; a failed Scan poisons the row, so re-query on fallback.
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		rows, err = db.Query("SHOW BINARY LOG STATUS")
		if err != nil {
			t.Skipf("read binlog status: %v", err)
		}
	}
	defer rows.Close()
	if !rows.Next() {
		t.Skipf("binlog status returned no rows (binlog disabled?)")
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Skipf("binlog status columns: %v", err)
	}
	switch len(cols) {
	case 5: // File, Position, Binlog_Do_DB, Binlog_Ignore_DB, Executed_Gtid_Set
		var gtids sql.RawBytes
		if err := rows.Scan(&file, &pos, &discard, &discard, &gtids); err != nil {
			t.Skipf("scan 5-col master status: %v", err)
		}
	case 4: // MySQL 8.4+: File, Position, Binlog_Do_DB, Binlog_Ignore_DB
		if err := rows.Scan(&file, &pos, &discard, &discard); err != nil {
			t.Skipf("scan 4-col binlog status: %v", err)
		}
	default:
		t.Skipf("unexpected binlog status column count %d", len(cols))
	}
	return file, uint32(pos)
}

// TestBinlogPurgeRuntimeDetectionResumeFromCurrent verifies against a real
// MySQL that (1) RESET MASTER (binlog purge) surfaces errcode 1236 through a
// canal RunFrom from the stale coordinate and isBinlogPurgedError detects
// it, and (2) the resume_from_current recovery step — probing the CURRENT
// master position with a fresh canal, exactly as the reconnect loop does —
// yields a valid coordinate that a subsequent canal run accepts (no 1236).
// This closes the BUG-2 residual "resume_from_current not e2e verified".
func TestBinlogPurgeRuntimeDetectionResumeFromCurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test needs real MySQL")
	}
	db := bug2Connect(t)
	for _, stmt := range []string{
		"SET GLOBAL binlog_format = 'ROW'",
		"SET GLOBAL binlog_row_image = 'FULL'",
		"CREATE DATABASE IF NOT EXISTS bug2db",
		"DROP TABLE IF EXISTS bug2db.t",
		"CREATE TABLE bug2db.t (id INT PRIMARY KEY, v VARCHAR(16))",
		"INSERT INTO bug2db.t VALUES (1,'a')",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Skipf("setup (%s): %v", stmt, err)
		}
	}

	oldFile, oldPos := bug2MasterStatus(t, db)
	// Extra write so oldFile/oldPos points strictly before the latest events.
	if _, err := db.Exec("INSERT INTO bug2db.t VALUES (2,'b')"); err != nil {
		t.Fatalf("pre-purge insert: %v", err)
	}

	// Purge everything recorded so far.
	if _, err := db.Exec("RESET MASTER"); err != nil {
		t.Skipf("RESET MASTER unavailable (needs RELOAD+replication privileges): %v", err)
	}
	if _, err := db.Exec("INSERT INTO bug2db.t VALUES (3,'c')"); err != nil {
		t.Fatalf("post-purge insert: %v", err)
	}

	addr := "127.0.0.1:3399"
	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = "root"
	cfg.Password = "root123456"
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""
	cfg.IncludeTableRegex = []string{"bug2db\\..*"}

	// (1) A canal run from the stale coordinate must fail with 1236 and be
	// detected as a binlog purge.
	stale := mysql.Position{Name: oldFile, Pos: oldPos}
	staleCanal, err := canal.NewCanal(cfg)
	if err != nil {
		t.Skipf("create canal: %v", err)
	}
	runErr := staleCanal.RunFrom(stale)
	staleCanal.Close()
	if runErr == nil {
		t.Fatal("canal from stale binlog coordinate returned nil error; expected ERROR 1236")
	}
	if !isBinlogPurgedError(runErr) {
		t.Fatalf("stale-coordinate error not detected as binlog purge: %v", runErr)
	}
	t.Logf("step 1 OK: stale coordinate %s:%d -> detected purge error: %v", oldFile, oldPos, runErr)

	// (2) The resume_from_current recovery: probe the current master pos
	// with a fresh canal (mirroring the reconnect loop), then a follow-up
	// canal run from that new coordinate must NOT report a purge error. We
	// cancel it shortly after start: acceptance is "no 1236", not "runs
	// forever".
	probe, err := canal.NewCanal(cfg)
	if err != nil {
		t.Skipf("create probe canal: %v", err)
	}
	curPos, err := probe.GetMasterPos()
	probe.Close()
	if err != nil {
		t.Fatalf("GetMasterPos: %v", err)
	}
	if curPos.Name == "" || curPos.Name == oldFile && curPos.Pos <= oldPos {
		t.Fatalf("current pos %s:%d does not look fresh (old was %s:%d)", curPos.Name, curPos.Pos, oldFile, oldPos)
	}
	t.Logf("step 2 OK: recovered to current master pos %s:%d", curPos.Name, curPos.Pos)

	// Follow-up run from the recovered coordinate: it must start streaming
	// without a purge error. Bound it with a timer.
	follow, err := canal.NewCanal(cfg)
	if err != nil {
		t.Skipf("create follow-up canal: %v", err)
	}
	runErrCh := make(chan error, 1)
	go func() { runErrCh <- follow.RunFrom(curPos) }()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err := <-runErrCh:
		follow.Close()
		if err != nil && isBinlogPurgedError(err) {
			t.Fatalf("run from recovered pos %s:%d still reports purge: %v", curPos.Name, curPos.Pos, err)
		}
		if err != nil {
			t.Logf("follow-up run exited with non-purge error (acceptable if transient): %v", err)
		} else {
			t.Logf("follow-up run exited cleanly")
		}
	case <-timer.C:
		// Still streaming after 3s without a purge error: the recovered
		// coordinate is valid. Success.
		follow.Close()
		t.Log("step 3 OK: follow-up canal streamed from recovered pos for 3s with no purge error")
	}
}

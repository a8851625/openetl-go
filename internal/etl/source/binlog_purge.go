package source

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/gogf/gf/v2/frame/g"
)

// BinlogPurgedPolicy controls how mysql_cdc / mysql_snapshot_cdc recover when
// the checkpointed binlog position no longer exists on the MySQL server
// (ERROR 1236 "Could not find first log file name in binary log index").
//
// The default (BinlogPurgedFail) stops the pipeline so an operator can decide:
// resuming silently from the current master position would silently drop every
// change between the checkpoint and "now", which is a data-loss event that must
// never happen automatically.
type BinlogPurgedPolicy string

const (
	// BinlogPurgedFail (default): stop the pipeline and surface a fatal error.
	// The operator resets the checkpoint manually after choosing a recovery.
	BinlogPurgedFail BinlogPurgedPolicy = "fail"
	// BinlogPurgedResumeFromCurrent: advance the CDC resume position to the
	// current MySQL master position and continue. All changes between the stale
	// checkpoint and the new position are dropped (explicit RPO loss). Use only
	// when the data path has another recovery source (e.g. re-snapshot, replay).
	BinlogPurgedResumeFromCurrent BinlogPurgedPolicy = "resume_from_current"
	// BinlogPurgedResnapshot: (mysql_snapshot_cdc only) fall back to the snapshot
	// phase, resuming each table from its last known id/string cursor so already-
	// read rows are not re-emitted, then re-enter CDC at the snapshot handoff.
	BinlogPurgedResnapshot BinlogPurgedPolicy = "resnapshot"
)

// parseBinlogPurgedPolicy reads the cdc_on_binlog_purged config key with a
// fail-closed default.
func parseBinlogPurgedPolicy(config map[string]any) BinlogPurgedPolicy {
	v, ok := config["cdc_on_binlog_purged"]
	if !ok {
		// Also honor a global config file value for convenience.
		if cv := g.Cfg().MustGet(context.Background(), "mysql_cdc.cdc_on_binlog_purged", "").String(); cv != "" {
			v = cv
		}
	}
	s, _ := v.(string)
	switch BinlogPurgedPolicy(s) {
	case BinlogPurgedResumeFromCurrent:
		return BinlogPurgedResumeFromCurrent
	case BinlogPurgedResnapshot:
		return BinlogPurgedResnapshot
	default:
		// Unknown / empty -> fail-closed.
		return BinlogPurgedFail
	}
}

// ErrBinlogPurged is the fatal sentinel returned by the canal reconnect loop
// when MySQL reports the checkpointed binlog file no longer exists (ERROR 1236)
// AND the configured policy is BinlogPurgedFail. Pipeline/runner layers can
// errors.Is this to distinguish "retry-worthy transient disconnect" from
// "binlog permanently lost, needs operator".
var ErrBinlogPurged = errors.New("checkpointed binlog position no longer exists on the MySQL server (purged or reset); reset the pipeline checkpoint or change cdc_on_binlog_purged policy")

// isBinlogPurgedError reports whether err is MySQL ERROR 1236, the canonical
// "Could not find first log file name in binary log index" failure raised when
// the requested binlog file has been purged/expired/reset.
func isBinlogPurgedError(err error) bool {
	if err == nil {
		return false
	}
	// go-mysql raises *mysql.MyError with Code == ER_MASTER_FATAL_ERROR_READING_BINLOG (1236).
	var myErr *mysql.MyError
	if errors.As(err, &myErr) {
		return myErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG
	}
	// Fallback: match the user-visible error text (covers wrapped errors from
	// canal.RunFrom and any driver that does not surface the typed MyError).
	msg := err.Error()
	return strings.Contains(msg, "ERROR 1236") ||
		strings.Contains(msg, "Could not find first log file name in binary log index")
}

// binlogPurgedRecovery is the shared decision point for both mysql_cdc and
// mysql_snapshot_cdc reconnect loops. It is called once a 1236 has been
// detected. Returns:
//   - (newPos, nil) when the policy is resume_from_current: the caller must
//     persist newPos as the resume position and continue the CDC loop from it.
//   - (zero, ErrBinlogPurged) when the policy is fail (default) or resnapshot
//     is requested on a plain mysql_cdc source (only snapshot_cdc supports it).
//   - (zero, resnapshot) when the policy is resnapshot and the caller supports
//     it; the caller returns this to its phase machine to re-enter snapshot.
func binlogPurgedRecovery(policy BinlogPurgedPolicy, staleFile string, stalePos uint32, getMasterPos func() (mysql.Position, error)) (mysql.Position, error) {
	switch policy {
	case BinlogPurgedResumeFromCurrent:
		curPos, err := getMasterPos()
		if err != nil {
			return mysql.Position{}, fmt.Errorf("binlog purged recovery (resume_from_current): get current master pos: %w", err)
		}
		return curPos, nil
	default:
		return mysql.Position{}, fmt.Errorf("%w (stale checkpoint %s:%d, policy=%s)", ErrBinlogPurged, staleFile, stalePos, policy)
	}
}

package source

import (
	"errors"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestIsBinlogPurgedError(t *testing.T) {
	// Typed *mysql.MyError with code 1236 (canonical binlog purged error).
	purged := mysql.NewDefaultError(mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG, 1236, "Could not find first log file name in binary log index file")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"typed_1236", purged, true},
		{"wrapped_1236", errors.Join(errors.New("context"), purged), true},
		{"text_1236", errors.New("ERROR 1236 (HY000): Could not find first log file name in binary log index"), true},
		{"text_variant", errors.New("canal disconnected: ERROR 1236 (HY000): some detail"), true},
		{"other_mysql_err", mysql.NewDefaultError(1236, "x"), true}, // code 1236 regardless of msg
		{"connection_refused", errors.New("dial tcp: connection refused"), false},
		{"nil", nil, false},
		{"other_code", mysql.NewDefaultError(1045, "access denied"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBinlogPurgedError(tt.err); got != tt.want {
				t.Errorf("isBinlogPurgedError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestParseBinlogPurgedPolicy(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]any
		want   BinlogPurgedPolicy
	}{
		{"empty_defaults_fail", map[string]any{}, BinlogPurgedFail},
		{"resume_from_current", map[string]any{"cdc_on_binlog_purged": "resume_from_current"}, BinlogPurgedResumeFromCurrent},
		{"resnapshot", map[string]any{"cdc_on_binlog_purged": "resnapshot"}, BinlogPurgedResnapshot},
		{"explicit_fail", map[string]any{"cdc_on_binlog_purged": "fail"}, BinlogPurgedFail},
		{"unknown_fails_closed", map[string]any{"cdc_on_binlog_purged": "nonsense"}, BinlogPurgedFail},
		{"wrong_type_fails_closed", map[string]any{"cdc_on_binlog_purged": 123}, BinlogPurgedFail},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseBinlogPurgedPolicy(tt.config); got != tt.want {
				t.Errorf("parseBinlogPurgedPolicy(%v) = %v, want %v", tt.config, got, tt.want)
			}
		})
	}
}

func TestBinlogPurgedRecoveryResumeFromCurrent(t *testing.T) {
	masterPos := mysql.Position{Name: "mysql-bin.000200", Pos: 999}
	called := 0
	got, err := binlogPurgedRecovery(BinlogPurgedResumeFromCurrent, "mysql-bin.000120", 2789538, func() (mysql.Position, error) {
		called++
		return masterPos, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != masterPos.Name || got.Pos != masterPos.Pos {
		t.Errorf("recovery pos = %s:%d, want %s:%d", got.Name, got.Pos, masterPos.Name, masterPos.Pos)
	}
	if called != 1 {
		t.Errorf("GetMasterPos called %d times, want 1", called)
	}
}

func TestBinlogPurgedRecoveryFail(t *testing.T) {
	_, err := binlogPurgedRecovery(BinlogPurgedFail, "mysql-bin.000120", 2789538, func() (mysql.Position, error) {
		t.Fatal("GetMasterPos should not be called on fail policy")
		return mysql.Position{}, nil
	})
	if !errors.Is(err, ErrBinlogPurged) {
		t.Errorf("fail policy err = %v, want ErrBinlogPurged", err)
	}
}

func TestErrBinlogPurgedIsSentinel(t *testing.T) {
	// Ensure errors.Is works through fmt.Errorf("%w: ...") wrapping.
	_, wrapped := binlogPurgedRecovery(BinlogPurgedFail, "mysql-bin.000120", 100, nil)
	if !errors.Is(wrapped, ErrBinlogPurged) {
		t.Errorf("wrapped error does not match ErrBinlogPurged sentinel")
	}
}

package pipeline

import (
	"strings"
	"testing"
)

func TestLogBufferFormatting(t *testing.T) {
	buf := NewLogBuffer(10)

	// Formatted Infof: should expand args
	buf.Infof("batch %d: %d records", 42, 100)
	// Simple Infof: no args
	buf.Infof("checkpoint saved")

	// Formatted Errorf
	buf.Errorf("Start failed: %v", "connection refused")
	// Formatted Debugf
	buf.Debugf("read=%d written=%d lag=%dms", 500, 498, 250)
	// Formatted Warnf
	buf.Warnf("threshold exceeded: %d > %d", 95, 80)

	snapshot := buf.Snapshot(0)
	if len(snapshot) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(snapshot))
	}

	// Verify formatted entries have the expanded values, not raw format strings
	tests := []struct {
		idx      int
		contains string
		absent   string
	}{
		{0, "batch 42: 100 records", "%d"},
		{1, "checkpoint saved", ""},
		{2, "Start failed: connection refused", "%v"},
		{3, "read=500 written=498 lag=250ms", "%d"},
		{4, "threshold exceeded: 95 > 80", "%d"},
	}

	for _, tt := range tests {
		if tt.idx >= len(snapshot) {
			t.Fatalf("missing entry at index %d", tt.idx)
		}
		msg := snapshot[tt.idx].Message
		if tt.contains != "" && !strings.Contains(msg, tt.contains) {
			t.Errorf("entry[%d]: expected message containing %q, got %q", tt.idx, tt.contains, msg)
		}
		if tt.absent != "" && strings.Contains(msg, tt.absent) {
			t.Errorf("entry[%d]: message should not contain raw format verb %q, got %q", tt.idx, tt.absent, msg)
		}
	}
}

func TestLogBufferOverflow(t *testing.T) {
	buf := NewLogBuffer(3)
	buf.Infof("msg1")
	buf.Infof("msg2")
	buf.Infof("msg3")
	buf.Infof("msg4") // should evict msg1

	snapshot := buf.Snapshot(0)
	if len(snapshot) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snapshot))
	}
	if snapshot[0].Message != "msg2" {
		t.Errorf("oldest entry should be msg2, got %q", snapshot[0].Message)
	}
	if snapshot[2].Message != "msg4" {
		t.Errorf("newest entry should be msg4, got %q", snapshot[2].Message)
	}
}

func TestLogBufferSinceSeq(t *testing.T) {
	buf := NewLogBuffer(10)
	buf.Infof("msg1")
	buf.Infof("msg2")
	buf.Infof("msg3")

	seqAfter2 := buf.LastSeq() - 1
	snapshot := buf.Snapshot(seqAfter2)
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 entry after seq %d, got %d", seqAfter2, len(snapshot))
	}
	if snapshot[0].Message != "msg3" {
		t.Errorf("expected msg3, got %q", snapshot[0].Message)
	}
}

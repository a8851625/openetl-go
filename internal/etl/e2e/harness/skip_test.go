package harness

import (
	"flag"
	"testing"
)

func TestRecordSkipAppendsEvidence(t *testing.T) {
	before := len(RecordedSkips())
	rec := RecordSkip(t.Name(), "mysql container unavailable: boom")
	after := RecordedSkips()

	if len(after) != before+1 {
		t.Fatalf("expected %d records, got %d", before+1, len(after))
	}
	got := after[len(after)-1]
	if got.Test != t.Name() || got.Reason != "mysql container unavailable: boom" {
		t.Errorf("unexpected record: %+v", got)
	}
	if got.At.IsZero() {
		t.Errorf("record timestamp missing")
	}
	if RecordedSkips()[len(RecordedSkips())-1].Test != rec.Test {
		t.Errorf("recorded copy mismatch")
	}
	// RecordedSkips must return a copy — mutating it must not corrupt state.
	after[len(after)-1].Reason = "mutated"
	if RecordedSkips()[len(RecordedSkips())-1].Reason == "mutated" {
		t.Errorf("RecordedSkips exposed internal state")
	}
}

func TestSkipMarksTestSkippedAndRecords(t *testing.T) {
	before := len(RecordedSkips())
	t.Run("child", func(t *testing.T) {
		Skip(t, "intentional %s", "skip")
		t.Errorf("unreachable: Skip must end the test via t.Skip")
	})
	after := RecordedSkips()
	if len(after) != before+1 {
		t.Fatalf("expected one new record, got %d new", len(after)-before)
	}
	last := after[len(after)-1]
	if last.Test != "TestSkipMarksTestSkippedAndRecords/child" {
		t.Errorf("record test name = %q", last.Test)
	}
	if last.Reason != "intentional skip" {
		t.Errorf("record reason = %q", last.Reason)
	}
}

func TestSkipFatalDecisionFollowsStrictFlag(t *testing.T) {
	if skipFatal(true) != true || skipFatal(false) != false {
		t.Fatalf("skipFatal decision is wrong")
	}
	// Assert the DECLARED default, not the runtime value: the CI gate runs the
	// whole tagged binary with -e2e.strict, so *strictMode is legitimately true
	// there. flag.Lookup("e2e.strict").DefValue is invocation-independent.
	f := flag.Lookup("e2e.strict")
	if f == nil || f.DefValue != "false" {
		t.Fatalf("-e2e.strict must be declared defaulting to false (local runs allow skips); got %+v", f)
	}
}

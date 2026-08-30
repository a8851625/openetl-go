package harness

import (
	"flag"
	"fmt"
	"sync"
	"testing"
	"time"
)

// strictMode is bound to the -e2e.strict test flag. In strict mode (CI gate)
// a skip is a failure: the gate must never turn a missing container, DSN or
// credential into silent green (IT-1 spec 交付约束 1).
var strictMode = flag.Bool("e2e.strict", false, "strict gate mode: any skip becomes a test failure")

// SkipRecord is the structured evidence trail of one skipped test.
type SkipRecord struct {
	Test   string    `json:"test"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

var (
	skipMu      sync.Mutex
	skipRecords []SkipRecord
)

// skipFatal is the pure strict-mode decision: in CI gate mode a skip is a
// failure. Split out so the decision is unit-testable without ending the
// calling test (t.Fatalf/t.Skip semantics are stdlib-guaranteed).
func skipFatal(strict bool) bool { return strict }

// Skip records a structured skip and then either fails the test (strict gate
// mode) or marks it skipped (local runs). The reason must name the concrete
// missing condition — "container runtime unavailable", "no DSN", etc.
func Skip(t *testing.T, format string, args ...any) {
	t.Helper()
	reason := fmt.Sprintf(format, args...)
	RecordSkip(t.Name(), reason)
	if skipFatal(*strictMode) {
		t.Fatalf("SKIP forbidden under -e2e.strict: %s", reason)
		return
	}
	t.Skip(reason)
}

// RecordSkip appends a SkipRecord. Split from Skip so the recording path is
// unit-testable without ending the calling test.
func RecordSkip(testName, reason string) SkipRecord {
	rec := SkipRecord{Test: testName, Reason: reason, At: time.Now().UTC()}
	skipMu.Lock()
	skipRecords = append(skipRecords, rec)
	skipMu.Unlock()
	return rec
}

// RecordedSkips returns a copy of all skip records accumulated in this test
// process, for structured evidence output.
func RecordedSkips() []SkipRecord {
	skipMu.Lock()
	defer skipMu.Unlock()
	out := make([]SkipRecord, len(skipRecords))
	copy(out, skipRecords)
	return out
}

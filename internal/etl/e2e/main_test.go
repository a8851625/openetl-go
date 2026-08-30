//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
)

// TestMain owns the shared dependency container lifecycle. With
// TESTCONTAINERS_RYUK_DISABLED=true no reaper container is spawned, so the
// harness terminates what it started once the whole suite finishes.
func TestMain(m *testing.M) {
	code := m.Run()
	harness.CloseAll(context.Background())
	os.Exit(code)
}

// serverIDFor derives a deterministic, per-test MySQL CDC server_id in the
// 40000-59999 range so two suites never fight over the same replica identity.
func serverIDFor(ns string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ns))
	return 40000 + int(h.Sum32()%20000)
}

func mustExec(t *testing.T, exec func(sql string) error, sql string) {
	t.Helper()
	if err := exec(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// pollDLQ polls the DLQ list endpoint until the body contains needle,
// mirroring the shell `grep 9201` loops.
func pollDLQ(t *testing.T, srv *harness.Server, pipeline, needle string, timeout time.Duration) []byte {
	t.Helper()
	url := fmt.Sprintf("/api/v2/dlq/%s?contains=%s&limit=10", pipeline, needle)
	deadline := time.Now().Add(timeout)
	var body []byte
	for time.Now().Before(deadline) {
		b, err := srv.Get(url)
		if err == nil && len(b) > 0 {
			body = b
			if strings.Contains(string(b), needle) {
				return b
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("dlq entry containing %q not found after %s; last body: %s", needle, timeout, string(body))
	return nil
}

var dlqIDRe = regexp.MustCompile(`"id":(\d+)`)

// extractDLQID mirrors the shell `grep -o '"id":[0-9]*' | head -n1`.
func extractDLQID(t *testing.T, body []byte) string {
	t.Helper()
	m := dlqIDRe.FindSubmatch(body)
	if m == nil {
		t.Fatalf("no dlq id in body: %s", string(body))
	}
	return string(m[1])
}

// extractReplayed parses {"replayed":N} and asserts N == 1 (shell `grep '"replayed":1'`).
func extractReplayed(t *testing.T, body []byte) {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("replay response not JSON: %s", string(body))
	}
	if n, ok := out["replayed"].(float64); !ok || n != 1 {
		t.Fatalf("expected replayed:1, got: %s", string(body))
	}
}

// startPipeline POSTs /start, retrying while the previous stop is still in
// flight (409 pipeline_stopping) — the API's documented remediation for that
// transient conflict ("Wait for the previous stop to finish, then retry").
// The shell scripts relied on inter-process latency to never hit it.
func startPipeline(t *testing.T, srv *harness.Server, pipeline string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		body, err := srv.Post("/api/v2/pipelines/" + pipeline + "/start")
		if err == nil {
			return
		}
		if strings.Contains(string(body), "pipeline_stopping") && time.Now().Before(deadline) {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		t.Fatalf("start: %v", err)
	}
}

// containsAny reports whether s contains any of the needles (non-empty).
func containsAny(s string, needles []string) bool {
	for _, n := range needles {
		if n != "" && strings.Contains(s, n) {
			return true
		}
	}
	return false
}

// assertDLQEntryGone mirrors the shell check that the replayed DLQ id was
// deleted from the list.
func assertDLQEntryGone(t *testing.T, srv *harness.Server, pipeline, dlqID, contains string) {
	t.Helper()
	after, err := srv.Get(fmt.Sprintf("/api/v2/dlq/%s?contains=%s&limit=10", pipeline, contains))
	if err != nil {
		t.Fatalf("dlq list after replay: %v", err)
	}
	if strings.Contains(string(after), `"id":`+dlqID) {
		t.Fatalf("replayed DLQ id %s was not deleted: %s", dlqID, string(after))
	}
}

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// recordingDLQRunner counts DLQ mutations so the API-level counter assertions
// are observable. testScheduledRunner supplies the rest of RunnerInterface.
type recordingDLQRunner struct {
	*testScheduledRunner
	mu     sync.Mutex
	replay int64
	delete int64
}

func (r *recordingDLQRunner) IncrementDLQReplay(n int64) {
	r.mu.Lock()
	r.replay += n
	r.mu.Unlock()
}

func (r *recordingDLQRunner) IncrementDLQDelete(n int64) {
	r.mu.Lock()
	r.delete += n
	r.mu.Unlock()
}

func (r *recordingDLQRunner) counters() (replay, deleted int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.replay, r.delete
}

func writeTestDLQ(t *testing.T, s *Server, jobName, errorClass string, data map[string]any) {
	t.Helper()
	if err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName:    jobName,
		Record:     core.Record{Operation: core.OpInsert, Data: data},
		Error:      "forced failure",
		ErrorClass: errorClass,
	}); err != nil {
		t.Fatalf("write dlq: %v", err)
	}
}

// TestDLQDeleteAllReportsRealCount is the regression for the "-1 deleted"
// sentinel: a full purge used to answer {"deleted": -1} and drive the
// dlq_delete_count counter negative (an invalid Prometheus counter).
func TestDLQDeleteAllReportsRealCount(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-delete-all-%d", time.Now().UnixNano())
	runner := &recordingDLQRunner{testScheduledRunner: newTestScheduledRunner()}
	s.mu.Lock()
	s.pipelines[jobName] = runner
	s.pipelineNames[jobName] = jobName
	s.mu.Unlock()

	for i := 0; i < 3; i++ {
		writeTestDLQ(t, s, jobName, "sink", map[string]any{"id": i})
	}

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/api/v2/dlq/"+jobName, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete all: %v", err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if got, _ := body["deleted"].(float64); got != 3 {
		t.Fatalf("deleted = %v, want 3 (no -1 sentinel); body=%#v", body["deleted"], body)
	}
	if _, deleted := runner.counters(); deleted != 3 {
		t.Fatalf("dlq_delete_count delta = %d, want 3 (never negative)", deleted)
	}
}

// TestDLQListSupportsOffsetAndErrorClassFilter covers the two missing query
// capabilities: paging beyond the first page and filtering by the indexed
// error_class column.
func TestDLQListSupportsOffsetAndErrorClassFilter(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-paging-%d", time.Now().UnixNano())
	for i := 0; i < 6; i++ {
		class := "sink"
		if i%2 == 1 {
			class = "schema"
		}
		writeTestDLQ(t, s, jobName, class, map[string]any{"id": i})
		// Distinct timestamps keep the ordering deterministic.
		time.Sleep(2 * time.Millisecond)
	}

	// error_class filter.
	resp, err := http.Get(ts.URL + "/api/v2/dlq/" + jobName + "?error_class=schema&limit=50")
	if err != nil {
		t.Fatalf("get filtered: %v", err)
	}
	defer resp.Body.Close()
	var filtered struct {
		Items []storage.DeadLetter `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&filtered); err != nil {
		t.Fatalf("decode filtered: %v", err)
	}
	if len(filtered.Items) != 3 {
		t.Fatalf("error_class filter returned %d items, want 3", len(filtered.Items))
	}
	for _, item := range filtered.Items {
		if item.ErrorClass != "schema" {
			t.Fatalf("error_class filter leaked %q", item.ErrorClass)
		}
	}

	// Offset paging must return a different page, not the same first page.
	page1 := getDLQIDs(t, ts.URL, jobName, "?limit=2&offset=0")
	page2 := getDLQIDs(t, ts.URL, jobName, "?limit=2&offset=2")
	if len(page1) != 2 || len(page2) != 2 {
		t.Fatalf("page sizes = %d/%d, want 2/2", len(page1), len(page2))
	}
	for _, a := range page1 {
		for _, b := range page2 {
			if a == b {
				t.Fatalf("offset did not page: %v and %v overlap", page1, page2)
			}
		}
	}
}

func getDLQIDs(t *testing.T, baseURL, jobName, query string) []int64 {
	t.Helper()
	resp, err := http.Get(baseURL + "/api/v2/dlq/" + jobName + query)
	if err != nil {
		t.Fatalf("get dlq%s: %v", query, err)
	}
	defer resp.Body.Close()
	var body struct {
		Items []storage.DeadLetter `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode dlq%s: %v", query, err)
	}
	ids := make([]int64, 0, len(body.Items))
	for _, item := range body.Items {
		ids = append(ids, item.ID)
	}
	return ids
}

// TestDLQReplaySerializesConcurrentRequests proves the per-pipeline replay lock
// rejects a second concurrent replay instead of letting both write the same row
// to the sink.
func TestDLQReplaySerializesConcurrentRequests(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-replay-lock-%d", time.Now().UnixNano())
	s.mu.Lock()
	s.specs[jobName] = &pipeline.Spec{
		Name:   jobName,
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "unused"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": t.TempDir(), "format": "jsonl"}},
	}
	s.mu.Unlock()

	lock := s.dlqReplayLock(jobName)
	lock.Lock()
	defer lock.Unlock()

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("post replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("concurrent replay status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(fmt.Sprint(body["error"]), "already running") {
		t.Fatalf("error = %#v, want already-running message", body["error"])
	}
}

// TestDLQReplayRefusesUnsafeDuplicateBoundary closes the hole where replay
// bypassed the allow_unsafe gate that pipeline creation enforces: replay writes
// straight to the sink, so a CDC -> non-idempotent sink pipeline without
// allow_unsafe must not be replayed at all.
func TestDLQReplayRefusesUnsafeDuplicateBoundary(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-unsafe-%d", time.Now().UnixNano())
	s.mu.Lock()
	s.specs[jobName] = &pipeline.Spec{
		Name:   jobName,
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": t.TempDir(), "format": "jsonl"}},
	}
	s.mu.Unlock()
	writeTestDLQ(t, s, jobName, "sink", map[string]any{"id": 1})

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("post replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("replay status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(fmt.Sprint(body["error"]), "allow_unsafe") {
		t.Fatalf("error = %#v, want allow_unsafe refusal", body["error"])
	}
	remaining, err := s.store.ListDeadLetters(context.Background(), storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("refused replay must leave the row intact; remaining = %d", len(remaining))
	}
}

// TestDAGDLQReplayEnforcesMetadataIdentityGate proves DAG replay now applies the
// same identity contract as linear replay. Before this, a DAG row with no
// provable primary key was written to a metadata-PK sink without any check,
// while the repair endpoint refused DAG rows outright.
func TestDAGDLQReplayEnforcesMetadataIdentityGate(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dag-dlq-identity-%d", time.Now().UnixNano())
	outDir := t.TempDir()
	s.mu.Lock()
	s.dagSpecs[jobName] = &orchestrator.PipelineSpec{
		Name: jobName,
		DAG: orchestrator.DAG{
			Nodes: []*orchestrator.Node{
				{ID: "src", Kind: orchestrator.KindSource, Plugin: "file"},
				{ID: "sink-meta", Kind: orchestrator.KindSink, Plugin: "file_sink", Config: map[string]any{
					"output_dir": outDir, "format": "jsonl", "pk_columns_from_metadata": true,
					"pk_columns": []any{"id"},
				}},
			},
			Edges: []*orchestrator.Edge{{From: "src", To: "sink-meta"}},
		},
	}
	s.mu.Unlock()

	// legacy_unknown provenance with no frozen key: replay cannot prove identity.
	if err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName: jobName,
		Record:  core.Record{Operation: core.OpInsert, Data: map[string]any{"name": "no-key"}},
		Error:   "forced DAG failure",
		DAGNode: "sink-meta",
		IdentityContext: core.DLQIdentityContext{
			ReplayProvenance: core.DLQReplayProvenanceLegacyUnknown,
			ReplayState:      core.DLQReplayStateRepairRequired,
		},
	}); err != nil {
		t.Fatalf("write dlq: %v", err)
	}

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("post replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		body, _ := json.Marshal(resp.Status)
		t.Fatalf("DAG replay status = %s, want 409 (identity gate); body=%s", body, readAllString(t, resp))
	}
	remaining, err := s.store.ListDeadLetters(context.Background(), storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("blocked DAG replay must keep the row; remaining = %d", len(remaining))
	}
	if state := remaining[0].IdentityContext.ReplayState; state != core.DLQReplayStateRepairRequired && state != core.DLQReplayStateQuarantined {
		t.Fatalf("replay_state = %s, want repair_required or quarantined", state)
	}
}

// TestDAGDLQIdentityRepairIsSupported proves the repair endpoint no longer
// refuses DAG rows: a row whose historical record carries an explicit primary
// key declaration can be promoted to legacy_verified when it matches the
// reachable metadata-PK sink configuration.
func TestDAGDLQIdentityRepairIsSupported(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dag-dlq-repair-%d", time.Now().UnixNano())
	s.mu.Lock()
	s.dagSpecs[jobName] = &orchestrator.PipelineSpec{
		Name: jobName,
		DAG: orchestrator.DAG{
			Nodes: []*orchestrator.Node{
				{ID: "src", Kind: orchestrator.KindSource, Plugin: "file"},
				{ID: "sink-meta", Kind: orchestrator.KindSink, Plugin: "file_sink", Config: map[string]any{
					"output_dir": t.TempDir(), "format": "jsonl", "pk_columns_from_metadata": true,
					"pk_columns": []any{"id"},
				}},
			},
			Edges: []*orchestrator.Edge{{From: "src", To: "sink-meta"}},
		},
	}
	s.mu.Unlock()

	if err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName: jobName,
		Record: core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": 7, "name": "orders"},
			Metadata: core.Metadata{
				PrimaryKeyColumns: []string{"id"},
				FormatContractID:  core.FormatContractCanalJSONV1,
			},
		},
		Error:           "forced DAG failure",
		DAGNode:         "sink-meta",
		IdentityContext: core.DLQIdentityContext{ReplayProvenance: core.DLQReplayProvenanceLegacyUnknown, ReplayState: core.DLQReplayStateRepairRequired},
	}); err != nil {
		t.Fatalf("write dlq: %v", err)
	}
	items, err := s.store.ListDeadLetters(context.Background(), storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("list dlq: items=%d err=%v", len(items), err)
	}

	body := strings.NewReader(`{"primary_key_columns":["id"]}`)
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/dlq/"+jobName+"/"+strconv.FormatInt(items[0].ID, 10)+"/identity", body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put identity: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("identity repair status = %d, want 200; body=%s", resp.StatusCode, readAllString(t, resp))
	}
	repaired, err := s.store.GetDeadLetterByID(context.Background(), jobName, items[0].ID)
	if err != nil || repaired == nil {
		t.Fatalf("reload repaired row: %v", err)
	}
	if repaired.IdentityContext.ReplayProvenance != core.DLQReplayProvenanceLegacyVerified {
		t.Fatalf("provenance = %s, want legacy_verified", repaired.IdentityContext.ReplayProvenance)
	}
}

// TestPipelinesListExposesCurrentDLQBacklog proves the list endpoint reports the
// CURRENT backlog (and whether the count is known) instead of leaving clients to
// interpret the cumulative stats.records_dlq counter as a backlog.
func TestPipelinesListExposesCurrentDLQBacklog(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-backlog-%d", time.Now().UnixNano())
	runner := &recordingDLQRunner{testScheduledRunner: newTestScheduledRunner()}
	s.mu.Lock()
	s.pipelines[jobName] = runner
	s.pipelineNames[jobName] = jobName
	s.mu.Unlock()

	writeTestDLQ(t, s, jobName, "sink", map[string]any{"id": 1})
	writeTestDLQ(t, s, jobName, "sink", map[string]any{"id": 2})

	resp, err := http.Get(ts.URL + "/api/v2/pipelines")
	if err != nil {
		t.Fatalf("get pipelines: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Pipelines []map[string]any `json:"pipelines"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode pipelines: %v", err)
	}
	var found map[string]any
	for _, p := range body.Pipelines {
		if p["name"] == jobName {
			found = p
			break
		}
	}
	if found == nil {
		t.Fatalf("pipeline %s missing from list: %#v", jobName, body.Pipelines)
	}
	if got, _ := found["dlq_backlog"].(float64); got != 2 {
		t.Fatalf("dlq_backlog = %v, want 2", found["dlq_backlog"])
	}
	if known, _ := found["dlq_backlog_known"].(bool); !known {
		t.Fatalf("dlq_backlog_known = %v, want true", found["dlq_backlog_known"])
	}
}

// TestDLQFilterParsesReplayAndListOptions guards the request parsing contract
// (limit/offset/error_class/from/until) that the UI relies on.
func TestDLQFilterParsesReplayAndListOptions(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.test/api/v2/dlq/p?limit=25&offset=50&error_class=sink&contains=abc&error_contains=boom", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	filter := dlqFilterFromRequest(req)
	if filter.Limit != 25 || filter.Offset != 50 || filter.ErrorClass != "sink" || filter.Contains != "abc" || filter.ErrorContains != "boom" {
		t.Fatalf("parsed filter = %+v", filter)
	}
	s := &Server{}
	storageFilter := s.toStorageFilter("p", filter)
	if storageFilter.Offset != 50 || storageFilter.ErrorClass != "sink" || storageFilter.Limit != 25 {
		t.Fatalf("storage filter = %+v", storageFilter)
	}
}

func readAllString(t *testing.T, resp *http.Response) string {
	t.Helper()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			return sb.String()
		}
	}
}

// TestDLQReplayDrainsOldestFirst proves bulk replay processes the backlog in
// creation order. The previous implementation replayed newest-first, so with a
// partial-replay abort the oldest entries could never be reached.
func TestDLQReplayDrainsOldestFirst(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-replay-order-%d", time.Now().UnixNano())
	outDir := t.TempDir()
	s.mu.Lock()
	s.specs[jobName] = &pipeline.Spec{
		Name:   jobName,
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "unused"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": outDir, "format": "jsonl", "prefix": "order-"}},
	}
	s.mu.Unlock()

	writeTestDLQ(t, s, jobName, "sink", map[string]any{"id": "first"})
	time.Sleep(3 * time.Millisecond)
	writeTestDLQ(t, s, jobName, "sink", map[string]any{"id": "second"})

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("post replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200; body=%s", resp.StatusCode, readAllString(t, resp))
	}

	output := readOutputFiles(t, outDir)
	firstIdx := strings.Index(output, `"first"`)
	secondIdx := strings.Index(output, `"second"`)
	if firstIdx < 0 || secondIdx < 0 {
		t.Fatalf("replay output missing records: %q", output)
	}
	if firstIdx > secondIdx {
		t.Fatalf("replay drained newest-first: %q", output)
	}
}

// TestDLQReplayDryRunReportsIdentityGate proves the preview action is backed by
// a real server decision instead of a client-side guess: it reports how many
// rows would replay and how many are blocked, without writing to the sink or
// mutating DLQ state.
func TestDLQReplayDryRunReportsIdentityGate(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-dry-run-%d", time.Now().UnixNano())
	outDir := t.TempDir()
	s.mu.Lock()
	s.dagSpecs[jobName] = &orchestrator.PipelineSpec{
		Name: jobName,
		DAG: orchestrator.DAG{
			Nodes: []*orchestrator.Node{
				{ID: "src", Kind: orchestrator.KindSource, Plugin: "file"},
				{ID: "sink-meta", Kind: orchestrator.KindSink, Plugin: "file_sink", Config: map[string]any{
					"output_dir": outDir, "format": "jsonl", "pk_columns_from_metadata": true,
					"pk_columns": []any{"id"},
				}},
			},
			Edges: []*orchestrator.Edge{{From: "src", To: "sink-meta"}},
		},
	}
	s.mu.Unlock()

	// One provable row and one legacy_unknown row without a frozen key.
	if err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName: jobName,
		Record: core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": 1},
			Metadata:  core.Metadata{PrimaryKeyColumns: []string{"id"}, FormatContractID: core.FormatContractCanalJSONV1},
		},
		Error:   "x",
		DAGNode: "sink-meta",
	}); err != nil {
		t.Fatalf("write good dlq: %v", err)
	}
	if err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName: jobName,
		Record:  core.Record{Operation: core.OpInsert, Data: map[string]any{"name": "no-key"}},
		Error:   "x",
		DAGNode: "sink-meta",
		IdentityContext: core.DLQIdentityContext{
			ReplayProvenance: core.DLQReplayProvenanceLegacyUnknown,
			ReplayState:      core.DLQReplayStateRepairRequired,
		},
	}); err != nil {
		t.Fatalf("write blocked dlq: %v", err)
	}

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay?dry_run=1", "application/json", nil)
	if err != nil {
		t.Fatalf("post dry run: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dry-run status = %d, want 200; body=%s", resp.StatusCode, readAllString(t, resp))
	}
	var body DLQDryRunResult
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode dry run: %v", err)
	}
	if !body.DryRun {
		t.Fatal("dry_run flag missing from response")
	}
	if body.Replayable != 1 || body.Blocked != 1 {
		t.Fatalf("dry run replayable/blocked = %d/%d, want 1/1; items=%#v", body.Replayable, body.Blocked, body.Items)
	}
	// Nothing may be written or deleted by a preview.
	if entries := readOutputFiles(t, outDir); strings.TrimSpace(entries) != "" {
		t.Fatalf("dry run wrote to the sink: %q", entries)
	}
	remaining, err := s.store.ListDeadLetters(context.Background(), storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil || len(remaining) != 2 {
		t.Fatalf("dry run mutated the DLQ: remaining=%d err=%v", len(remaining), err)
	}
	for _, item := range remaining {
		if item.IdentityContext.ReplayAttempt != 0 {
			t.Fatalf("dry run persisted an attempt on id %d: %+v", item.ID, item.IdentityContext)
		}
	}
}

// TestDLQReplayAppliesTableMapping proves replay reproduces the read-time
// pipeline processors. Without it, a replayed row from a table_mapping pipeline
// is written to the source table name instead of the mapped target.
func TestDLQReplayAppliesTableMapping(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	jobName := fmt.Sprintf("dlq-table-mapping-%d", time.Now().UnixNano())
	outDir := t.TempDir()
	s.mu.Lock()
	s.specs[jobName] = &pipeline.Spec{
		Name:   jobName,
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "unused"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": outDir, "format": "jsonl", "prefix": "mapped-"}},
		TableMapping: &pipeline.TableMapping{
			Rules: map[string]string{"orders": "ods_orders"},
		},
	}
	s.mu.Unlock()

	if err := s.store.WriteDeadLetter(context.Background(), &storage.DLQRecord{
		JobName: jobName,
		Record: core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": 11},
			Metadata:  core.Metadata{Table: "orders", Database: "shop"},
		},
		Error: "forced failure",
	}); err != nil {
		t.Fatalf("write dlq: %v", err)
	}

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("post replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay status = %d, want 200; body=%s", resp.StatusCode, readAllString(t, resp))
	}
	remaining, err := s.store.ListDeadLetters(context.Background(), storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil || len(remaining) != 0 {
		t.Fatalf("replayed row not cleared: remaining=%d err=%v", len(remaining), err)
	}
	// The table-mapping processor preserves the original table in
	// data._source_table; its presence proves replay ran the read-time
	// processors before writing.
	output := readOutputFiles(t, outDir)
	if !strings.Contains(output, `"_source_table":"orders"`) {
		t.Fatalf("replay skipped table_mapping processors: %q", output)
	}
}

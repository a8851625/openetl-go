package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestDLQDeleteByStableID(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	ctx := context.Background()
	jobName := fmt.Sprintf("dlq-id-pipe-%d", time.Now().UnixNano())
	errorClass := fmt.Sprintf("stable-id-test-%d", time.Now().UnixNano())

	for _, id := range []int{1, 2} {
		if err := s.store.WriteDeadLetter(ctx, &storage.DLQRecord{
			JobName:    jobName,
			Record:     core.Record{Operation: core.OpInsert, Data: map[string]any{"id": id}},
			Error:      "forced failure",
			ErrorClass: errorClass,
		}); err != nil {
			t.Fatalf("write dlq %d: %v", id, err)
		}
	}
	items, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, ErrorClass: errorClass, Limit: 10})
	if err != nil {
		t.Fatalf("list dlq: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/api/v2/dlq/"+jobName+"/"+strconv.FormatInt(items[0].ID, 10), nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete by id: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", resp.StatusCode)
	}

	remaining, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, ErrorClass: errorClass, Limit: 10})
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("remaining = %d, want 1", len(remaining))
	}
	if remaining[0].ID == items[0].ID {
		t.Fatalf("deleted id %d still present", items[0].ID)
	}
}

func TestDAGDLQReplayRoutesByNodeContext(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	ctx := context.Background()
	jobName := fmt.Sprintf("dag-dlq-replay-%d", time.Now().UnixNano())
	outDir := t.TempDir()
	s.mu.Lock()
	s.dagSpecs[jobName] = &orchestrator.PipelineSpec{
		Name: jobName,
		DAG: orchestrator.DAG{
			Nodes: []*orchestrator.Node{
				{ID: "src", Kind: orchestrator.KindSource, Plugin: "file"},
				{ID: "add-status", Kind: orchestrator.KindTransform, Plugin: "add_field", Config: map[string]any{"field": "status", "value": "replayed"}},
				{ID: "sink-1", Kind: orchestrator.KindSink, Plugin: "file_sink", Config: map[string]any{"output_dir": outDir, "format": "jsonl", "prefix": "dag-replay-"}},
			},
			Edges: []*orchestrator.Edge{
				{From: "src", To: "add-status"},
				{From: "add-status", To: "sink-1"},
			},
		},
	}
	s.mu.Unlock()

	if err := s.store.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName: jobName,
		Record:  core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1, "status": "direct"}},
		Error:   "forced DAG sink failure",
		DAGNode: "sink-1",
	}); err != nil {
		t.Fatalf("write sink dlq: %v", err)
	}
	if err := s.store.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName: jobName,
		Record:  core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 2}},
		Error:   "forced DAG transform failure",
		DAGNode: "add-status",
	}); err != nil {
		t.Fatalf("write transform dlq: %v", err)
	}
	items, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list dlq: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	var transformID int64
	for _, item := range items {
		if item.DAGNode == "add-status" {
			transformID = item.ID
			break
		}
	}
	if transformID == 0 {
		t.Fatalf("transform DLQ id not found: %#v", items)
	}

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/"+strconv.FormatInt(transformID, 10)+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay by id: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay by id status = %d, want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode replay by id response: %v", err)
	}
	if got, _ := body["replayed"].(float64); got != 1 {
		t.Fatalf("replayed by id = %v, want 1; body=%#v", got, body)
	}

	remaining, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list remaining dlq: %v", err)
	}
	if len(remaining) != 1 || remaining[0].DAGNode != "sink-1" {
		t.Fatalf("remaining = %#v, want only sink DLQ record", remaining)
	}

	resp, err = http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay remaining: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replay remaining status = %d, want 200", resp.StatusCode)
	}
	body = map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode replay remaining response: %v", err)
	}
	if got, _ := body["replayed"].(float64); got != 1 {
		t.Fatalf("replayed remaining = %v, want 1; body=%#v", got, body)
	}

	remaining, err = s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list remaining after replay: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining after replay = %#v, want none", remaining)
	}

	output := readOutputFiles(t, outDir)
	if !strings.Contains(output, `"id":2`) || !strings.Contains(output, `"status":"replayed"`) {
		t.Fatalf("transform replay output = %q, want transformed record", output)
	}
	if !strings.Contains(output, `"id":1`) || !strings.Contains(output, `"status":"direct"`) {
		t.Fatalf("sink replay output = %q, want direct sink record", output)
	}
}

func TestDAGDLQReplayRequiresNodeContext(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	ctx := context.Background()
	jobName := fmt.Sprintf("dag-dlq-missing-node-%d", time.Now().UnixNano())
	s.mu.Lock()
	s.dagSpecs[jobName] = &orchestrator.PipelineSpec{
		Name: jobName,
		DAG: orchestrator.DAG{
			Nodes: []*orchestrator.Node{
				{ID: "src", Kind: orchestrator.KindSource, Plugin: "file"},
				{ID: "sink-1", Kind: orchestrator.KindSink, Plugin: "file_sink", Config: map[string]any{"output_dir": t.TempDir(), "format": "jsonl"}},
			},
			Edges: []*orchestrator.Edge{{From: "src", To: "sink-1"}},
		},
	}
	s.mu.Unlock()

	if err := s.store.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName: jobName,
		Record:  core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}},
		Error:   "legacy DAG failure without node context",
	}); err != nil {
		t.Fatalf("write dlq: %v", err)
	}

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("replay status = %d, want 400", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if !strings.Contains(fmt.Sprint(body["error"]), "dag_node") {
		t.Fatalf("error = %#v, want dag_node context", body)
	}
	remaining, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	if len(remaining) != 1 {
		t.Fatalf("remaining = %d, want 1", len(remaining))
	}
}

func readOutputFiles(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		b.Write(data)
		return nil
	})
	if err != nil {
		t.Fatalf("read output files: %v", err)
	}
	return b.String()
}

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/storage"
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

func TestDAGDLQReplayReturnsExplicitUnsupported(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	ctx := context.Background()
	jobName := fmt.Sprintf("dag-dlq-replay-%d", time.Now().UnixNano())
	s.mu.Lock()
	s.dagSpecs[jobName] = &orchestrator.PipelineSpec{Name: jobName}
	s.mu.Unlock()

	if err := s.store.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName: jobName,
		Record:  core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}},
		Error:   "forced DAG failure",
		DAGNode: "sink-1",
	}); err != nil {
		t.Fatalf("write dlq: %v", err)
	}
	items, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list dlq: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}

	resp, err := http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("replay status = %d, want %d", resp.StatusCode, http.StatusNotImplemented)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if got, _ := body["code"].(string); got != "dag_dlq_replay_unsupported" {
		t.Fatalf("code = %q, want dag_dlq_replay_unsupported; body=%#v", got, body)
	}
	if got, _ := body["supported"].(bool); got {
		t.Fatalf("supported = %v, want false", got)
	}

	remaining, err := s.store.ListDeadLetters(ctx, storage.DLQFilter{JobName: jobName, Limit: 10})
	if err != nil {
		t.Fatalf("list remaining dlq: %v", err)
	}
	if len(remaining) != 1 || remaining[0].ID != items[0].ID {
		t.Fatalf("remaining = %#v, want original DLQ record", remaining)
	}

	resp, err = http.Post(ts.URL+"/api/v2/dlq/"+jobName+"/"+strconv.FormatInt(items[0].ID, 10)+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay by id: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("replay by id status = %d, want %d", resp.StatusCode, http.StatusNotImplemented)
	}
	body = map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode replay by id response: %v", err)
	}
	if got, _ := body["code"].(string); got != "dag_dlq_replay_unsupported" {
		t.Fatalf("replay by id code = %q, want dag_dlq_replay_unsupported; body=%#v", got, body)
	}
}

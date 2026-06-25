package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
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

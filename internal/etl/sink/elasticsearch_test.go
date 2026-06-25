package sink

import (
	"context"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestElasticsearchWriteReturnsMarshalErrors(t *testing.T) {
	s, err := NewElasticsearchSink(map[string]any{
		"hosts": []interface{}{"http://127.0.0.1:1"},
		"index": "orders",
	})
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}

	err = s.Write(context.Background(), []core.Record{
		{
			Data: map[string]any{
				"id":  "order-1",
				"bad": make(chan int),
			},
		},
	})
	if err == nil {
		t.Fatal("Write() = nil error, want document marshal error")
	}
	if !strings.Contains(err.Error(), "elasticsearch marshal document") {
		t.Fatalf("Write() error = %v, want elasticsearch marshal document", err)
	}
}

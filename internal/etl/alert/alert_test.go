package alert

import (
	"context"
	"testing"
	"time"
)

func TestManagerDropsWhenQueueFull(t *testing.T) {
	m := &Manager{
		queue:       make(chan Event, 1),
		dedup:       map[string]time.Time{},
		dedupWindow: time.Hour, // avoid dedup interference across distinct titles
	}
	// Fill the queue without a consumer.
	m.Send(context.Background(), Event{Title: "first", JobName: "p1", Level: LevelError})
	// Second distinct fingerprint should drop.
	m.Send(context.Background(), Event{Title: "second", JobName: "p2", Level: LevelError})
	if got := m.Dropped(); got != 1 {
		t.Fatalf("Dropped()=%d want 1", got)
	}
}

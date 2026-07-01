package sink

import (
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestFileSinkRejectsUnsupportedFormat(t *testing.T) {
	_, err := NewFileSink(map[string]any{"format": "xml"})
	if err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("NewFileSink err = %v, want unsupported format", err)
	}
}

func TestFileSinkNormalizesFormat(t *testing.T) {
	s, err := NewS3Sink(map[string]any{"format": "JSONL"})
	if err != nil {
		t.Fatalf("NewS3Sink: %v", err)
	}
	if s.config.Format != "jsonl" {
		t.Fatalf("format = %q, want jsonl", s.config.Format)
	}
}

func TestFileSinkEncodeRejectsUnsupportedFormat(t *testing.T) {
	s := &FileSink{name: "file_sink", config: FileSinkConfig{Format: "xml"}}
	_, err := s.encode([]core.Record{{Data: map[string]any{"id": 1}}})
	if err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("encode err = %v, want unsupported format", err)
	}
}

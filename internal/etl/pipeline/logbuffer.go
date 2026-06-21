package pipeline

import (
	"fmt"
	"sync"
	"time"
)

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Level     string    `json:"level"`
	Seq       int64     `json:"seq"`
}

type LogBuffer struct {
	mu      sync.Mutex
	entries []LogEntry
	maxSize int
	seq     int64
}

func NewLogBuffer(maxSize int) *LogBuffer {
	if maxSize <= 0 {
		maxSize = 500
	}
	return &LogBuffer{
		entries: make([]LogEntry, 0, maxSize),
		maxSize: maxSize,
	}
}

func (b *LogBuffer) Append(level, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	entry := LogEntry{
		Timestamp: time.Now(),
		Message:   msg,
		Level:     level,
		Seq:       b.seq,
	}
	if len(b.entries) >= b.maxSize {
		b.entries = append(b.entries[1:], entry)
	} else {
		b.entries = append(b.entries, entry)
	}
}

func (b *LogBuffer) Info(msg string) {
	b.Append("INFO", msg)
}

func (b *LogBuffer) Debug(msg string) {
	b.Append("DEBUG", msg)
}

func (b *LogBuffer) Warn(msg string) {
	b.Append("WARN", msg)
}

func (b *LogBuffer) Error(msg string) {
	b.Append("ERROR", msg)
}

func (b *LogBuffer) Infof(format string, args ...any) {
	b.Append("INFO", fmt.Sprintf(format, args...))
}

func (b *LogBuffer) Debugf(format string, args ...any) {
	b.Append("DEBUG", fmt.Sprintf(format, args...))
}

func (b *LogBuffer) Warnf(format string, args ...any) {
	b.Append("WARN", fmt.Sprintf(format, args...))
}

func (b *LogBuffer) Errorf(format string, args ...any) {
	b.Append("ERROR", fmt.Sprintf(format, args...))
}

func (b *LogBuffer) Snapshot(sinceSeq int64) []LogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sinceSeq <= 0 {
		out := make([]LogEntry, len(b.entries))
		copy(out, b.entries)
		return out
	}
	var result []LogEntry
	for _, e := range b.entries {
		if e.Seq > sinceSeq {
			result = append(result, e)
		}
	}
	return result
}

func (b *LogBuffer) LastSeq() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seq
}

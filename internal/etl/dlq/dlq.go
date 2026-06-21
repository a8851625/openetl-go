package dlq

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"openetl-go/internal/etl/core"
)

type DeadLetter struct {
	JobName    string      `json:"job_name"`
	Record     core.Record `json:"record"`
	Error      string      `json:"error"`
	ErrorClass string      `json:"error_class,omitempty"`
	Timestamp  time.Time   `json:"timestamp"`
	Attempt    int         `json:"attempt"`
}

type Writer interface {
	Write(ctx context.Context, dl DeadLetter) error
	Read(ctx context.Context, jobName string, limit int) ([]DeadLetter, error)
	Delete(ctx context.Context, jobName string, timestamp time.Time) error
}

func NewFileWriter(dir string) (Writer, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dlq dir: %w", err)
	}
	return &fileWriter{dir: dir}, nil
}

type fileWriter struct {
	dir   string
	mu    sync.Mutex
	files map[string]*os.File
}

func (w *fileWriter) path(jobName string) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s.jsonl", jobName))
}

func (w *fileWriter) getFile(jobName string) (*os.File, error) {
	if f, ok := w.files[jobName]; ok {
		return f, nil
	}
	f, err := os.OpenFile(w.path(jobName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	w.files[jobName] = f
	return f, nil
}

func (w *fileWriter) Write(ctx context.Context, dl DeadLetter) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	dl.Timestamp = time.Now()
	f, err := w.getFile(dl.JobName)
	if err != nil {
		return fmt.Errorf("open dlq file: %w", err)
	}
	data, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshal dlq: %w", err)
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		delete(w.files, dl.JobName)
		f.Close()
		return fmt.Errorf("write dlq: %w", err)
	}
	// fsync so a crash (OOM/SIGKILL/host failure) doesn't leave the DLQ record
	// stranded in the OS page cache. The DLQ is the last-resort safety net for
	// failed records; an un-fsync'd DLQ only delivers durability on clean
	// shutdowns. The checkpoint store already Syncs (checkpoint.go) — match it
	// (PC-5). DLQ writes are rare (only on failures), so the per-write cost is
	// acceptable.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync dlq: %w", err)
	}
	return nil
}

func (w *fileWriter) Read(ctx context.Context, jobName string, limit int) ([]DeadLetter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.Open(w.path(jobName))
	if err != nil {
		if os.IsNotExist(err) {
			return []DeadLetter{}, nil
		}
		return nil, fmt.Errorf("open dlq file: %w", err)
	}
	defer f.Close()

	items := []DeadLetter{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 10<<20)
	for scanner.Scan() {
		if limit > 0 && len(items) >= limit {
			break
		}
		var dl DeadLetter
		if err := json.Unmarshal(scanner.Bytes(), &dl); err != nil {
			continue
		}
		items = append(items, dl)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan dlq file: %w", err)
	}
	return items, nil
}

func (w *fileWriter) Delete(ctx context.Context, jobName string, timestamp time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := w.path(jobName)
	if timestamp.IsZero() {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove dlq file: %w", err)
		}
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open dlq file: %w", err)
	}
	defer f.Close()

	tmp := path + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open temp dlq file: %w", err)
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 10<<20)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var dl DeadLetter
		if err := json.Unmarshal(line, &dl); err == nil && dl.Timestamp.Equal(timestamp) {
			continue
		}
		line = append(line, '\n')
		if _, err := out.Write(line); err != nil {
			out.Close()
			return fmt.Errorf("write temp dlq file: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		out.Close()
		return fmt.Errorf("scan dlq file: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close temp dlq file: %w", err)
	}
	return os.Rename(tmp, path)
}

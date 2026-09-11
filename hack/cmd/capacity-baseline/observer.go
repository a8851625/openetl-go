package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlstore"
)

// Embed the concrete store, not storage.Storage: transactions, lifecycle,
// fencing, retention and full-table backup interfaces must remain visible.
type observedStore struct {
	*sqlstore.Store
	observer *observer
}

var _ storage.Storage = (*observedStore)(nil)
var _ storage.PipelineLifecycleStore = (*observedStore)(nil)
var _ interface {
	WithTx(context.Context, func(context.Context) error) error
} = (*observedStore)(nil)

func (s *observedStore) SaveCheckpoint(ctx context.Context, cp *storage.CheckpointRecord) error {
	started := time.Now()
	err := s.Store.SaveCheckpoint(ctx, cp)
	finished := time.Now()
	s.observer.record(cp, started, finished, err)
	return err
}

type checkpointSample struct {
	Pipeline   string `json:"pipeline"`
	StartNS    int64  `json:"start_unix_ns"`
	EndNS      int64  `json:"end_unix_ns"`
	DurationNS int64  `json:"duration_ns"`
	Failed     bool   `json:"failed"`
}

type poolSample struct {
	MaxOpen   int   `json:"max_open"`
	Open      int   `json:"open"`
	InUse     int   `json:"in_use"`
	Idle      int   `json:"idle"`
	WaitCount int64 `json:"wait_count"`
	WaitNS    int64 `json:"wait_ns"`
}

func poolStats(db *sql.DB) poolSample {
	s := db.Stats()
	return poolSample{s.MaxOpenConnections, s.OpenConnections, s.InUse, s.Idle, s.WaitCount, int64(s.WaitDuration)}
}

type processSample struct {
	TimeNS           int64            `json:"time_unix_ns"`
	RSSBytes         int64            `json:"rss_bytes"`
	PeakRSSBytes     int64            `json:"peak_rss_bytes"`
	UserCPUSeconds   float64          `json:"user_cpu_seconds"`
	SystemCPUSeconds float64          `json:"system_cpu_seconds"`
	Goroutines       int              `json:"goroutines"`
	Pool             poolSample       `json:"pool"`
	CgroupCPU        map[string]int64 `json:"cgroup_cpu"`
}

func readProcess(db *sql.DB) (processSample, error) {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return processSample{}, err
	}
	rss, peak, err := parseRSS(string(raw))
	if err != nil {
		return processSample{}, err
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return processSample{}, err
	}
	s := processSample{TimeNS: time.Now().UnixNano(), RSSBytes: rss, PeakRSSBytes: peak,
		UserCPUSeconds:   float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6,
		SystemCPUSeconds: float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6,
		Goroutines:       runtime.NumGoroutine(), Pool: poolStats(db), CgroupCPU: map[string]int64{}}
	if raw, err := os.ReadFile("/sys/fs/cgroup/cpu.stat"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 {
				v, err := strconv.ParseInt(fields[1], 10, 64)
				if err != nil {
					return processSample{}, err
				}
				s.CgroupCPU[fields[0]] = v
			}
		}
	}
	return s, nil
}

func parseRSS(raw string) (int64, int64, error) {
	values := map[string]int64{}
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && (fields[0] == "VmRSS:" || fields[0] == "VmHWM:") {
			if len(fields) != 3 || fields[2] != "kB" {
				return 0, 0, errors.New("unexpected /proc RSS unit")
			}
			v, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil || v <= 0 {
				return 0, 0, errors.New("invalid /proc RSS value")
			}
			values[fields[0]] = v * 1024
		}
	}
	rss, peak := values["VmRSS:"], values["VmHWM:"]
	if rss <= 0 || peak < rss {
		return 0, 0, errors.New("missing/inconsistent /proc RSS")
	}
	return rss, peak, nil
}

type observation struct {
	SchemaVersion  int                        `json:"schema_version"`
	Start          processSample              `json:"start"`
	End            processSample              `json:"end"`
	ElapsedSeconds float64                    `json:"elapsed_seconds"`
	StartPositions map[string]json.RawMessage `json:"start_positions"`
	EndPositions   map[string]json.RawMessage `json:"end_positions"`
	Checkpoints    []checkpointSample         `json:"checkpoints"`
	Samples        []processSample            `json:"samples"`
	ObserverNS     int64                      `json:"observer_ns"`
	BoundaryCalls  int                        `json:"boundary_calls_excluded"`
}

type observer struct {
	db        *sql.DB
	mu        sync.Mutex
	active    bool
	startTime time.Time
	last      map[string]json.RawMessage
	data      observation
	read      func(*sql.DB) (processSample, error)
}

func newObserver(db *sql.DB) *observer {
	return &observer{db: db, last: map[string]json.RawMessage{}, read: readProcess}
}

func clonePositions(m map[string]json.RawMessage) map[string]json.RawMessage {
	copy := make(map[string]json.RawMessage, len(m))
	for k, v := range m {
		copy[k] = append(json.RawMessage(nil), v...)
	}
	return copy
}

func (o *observer) record(cp *storage.CheckpointRecord, start, end time.Time, err error) {
	overhead := time.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	key := cp.PipelineID
	if key == "" {
		key = cp.JobName
	}
	if err == nil {
		o.last[key] = append(json.RawMessage(nil), cp.Position...)
	}
	if !o.active {
		return
	}
	if start.Before(o.startTime) {
		o.data.BoundaryCalls++
	} else {
		o.data.Checkpoints = append(o.data.Checkpoints, checkpointSample{
			Pipeline: key, StartNS: start.UnixNano(), EndNS: end.UnixNano(), DurationNS: int64(end.Sub(start)), Failed: err != nil})
	}
	o.data.ObserverNS += int64(time.Since(overhead))
}

func (o *observer) begin() (processSample, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.startTime.IsZero() {
		return processSample{}, errors.New("measurement can only start once per process")
	}
	start, err := o.read(o.db)
	if err != nil {
		return processSample{}, err
	}
	o.startTime = time.Now()
	start.TimeNS = o.startTime.UnixNano()
	o.data = observation{SchemaVersion: 1, Start: start, StartPositions: clonePositions(o.last),
		Checkpoints: []checkpointSample{}, Samples: []processSample{}}
	o.active = true
	return start, nil
}

func (o *observer) sample() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.active {
		return nil
	}
	start := time.Now()
	s, err := o.read(o.db)
	if err != nil {
		return err
	}
	o.data.Samples = append(o.data.Samples, s)
	o.data.ObserverNS += int64(time.Since(start))
	return nil
}

func (o *observer) end() (observation, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.active {
		return observation{}, fmt.Errorf("measurement is not active")
	}
	end, err := o.read(o.db)
	if err != nil {
		return observation{}, err
	}
	o.active = false
	o.data.End = end
	o.data.ElapsedSeconds = time.Since(o.startTime).Seconds()
	o.data.EndPositions = clonePositions(o.last)
	return o.data, nil
}

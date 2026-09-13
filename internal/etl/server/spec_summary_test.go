package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// UI-B.1: the list endpoint exposes a type-level, secret-free spec summary so
// clients render the real topology/mode instead of guessing from tags.

func TestBuildSpecSummaryLinearCDC(t *testing.T) {
	spec := &pipeline.Spec{
		Name: "cdc-orders",
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{
			"host":     "db.internal",
			"password": "super-secret",
		}},
		Transforms: []pipeline.TransformSpec{{Type: "deduplicate"}, {Type: "project"}},
		Sink: pipeline.SinkSpec{Type: "mysql", Config: map[string]any{
			"write_mode": "upsert",
			"host":       "sink.internal",
			"password":   "sink-secret",
		}},
		DLQ: &pipeline.DLQSpec{Enable: true},
	}
	s := buildSpecSummary(spec, nil)
	if s == nil {
		t.Fatal("expected summary")
	}
	if s.Source != "mysql_cdc" || s.SourceMode != "cdc" {
		t.Errorf("source classification: got %s/%s", s.Source, s.SourceMode)
	}
	if s.Sink != "mysql" || s.WriteMode != "upsert" {
		t.Errorf("sink classification: got %s/%s", s.Sink, s.WriteMode)
	}
	if len(s.Transforms) != 2 || s.Transforms[0] != "deduplicate" {
		t.Errorf("transforms: got %v", s.Transforms)
	}
	if !s.HasDLQ {
		t.Error("HasDLQ should be true")
	}
}

func TestBuildSpecSummaryBatchAndSchedule(t *testing.T) {
	spec := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_batch"},
		Sink:   pipeline.SinkSpec{Type: "clickhouse"},
		Schedule: &pipeline.ScheduleConfig{
			Type: "cron", Cron: "0 * * * *",
		},
	}
	s := buildSpecSummary(spec, nil)
	if s.SourceMode != "scheduled" {
		t.Errorf("scheduled batch: got %s", s.SourceMode)
	}
	if s.Schedule != "cron 0 * * * *" {
		t.Errorf("schedule label: got %q", s.Schedule)
	}

	stream := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "kafka"},
		Sink:   pipeline.SinkSpec{Type: "file_sink"},
	}
	s2 := buildSpecSummary(stream, nil)
	if s2.SourceMode != "streaming" {
		t.Errorf("kafka source mode: got %s", s2.SourceMode)
	}

	// ApplyDefaultSchedule materializes {type: once} for nil schedules and
	// {type: streaming} for continuous sources — neither is a real schedule.
	once := &pipeline.Spec{
		Source:   pipeline.SourceSpec{Type: "kafka"},
		Sink:     pipeline.SinkSpec{Type: "file_sink"},
		Schedule: &pipeline.ScheduleConfig{Type: "once"},
	}
	s3 := buildSpecSummary(once, nil)
	if s3.SourceMode != "streaming" {
		t.Errorf("once schedule should classify kafka as streaming, got %s", s3.SourceMode)
	}
	if s3.Schedule != "once" {
		t.Errorf("schedule label for once: got %q", s3.Schedule)
	}

	streamingDefault := &pipeline.Spec{
		Source:   pipeline.SourceSpec{Type: "kafka"},
		Sink:     pipeline.SinkSpec{Type: "file_sink"},
		Schedule: &pipeline.ScheduleConfig{Type: "streaming"},
	}
	s4 := buildSpecSummary(streamingDefault, nil)
	if s4.SourceMode != "streaming" {
		t.Errorf("streaming default should classify kafka as streaming, got %s", s4.SourceMode)
	}
	if s4.Schedule != "streaming" {
		t.Errorf("schedule label for streaming default: got %q", s4.Schedule)
	}
}

func TestBuildSpecSummaryDAG(t *testing.T) {
	dag := &orchestrator.PipelineSpec{
		Name: "wide-table",
		DAG: orchestrator.DAG{Nodes: []*orchestrator.Node{
			{ID: "src", Kind: orchestrator.KindSource, Plugin: "kafka"},
			{ID: "tf", Kind: orchestrator.KindTransform, Plugin: "lookup"},
			{ID: "sink", Kind: orchestrator.KindSink, Plugin: "clickhouse"},
		}},
	}
	s := buildSpecSummary(nil, dag)
	if s == nil {
		t.Fatal("expected summary")
	}
	if s.SourceMode != "dag" {
		t.Errorf("dag mode: got %s", s.SourceMode)
	}
	if len(s.DAGSources) != 1 || s.DAGSources[0] != "kafka" {
		t.Errorf("dag sources: got %v", s.DAGSources)
	}
	if len(s.DAGTransforms) != 1 || s.DAGTransforms[0] != "lookup" {
		t.Errorf("dag transforms: got %v", s.DAGTransforms)
	}
	if len(s.DAGSinks) != 1 || s.DAGSinks[0] != "clickhouse" {
		t.Errorf("dag sinks: got %v", s.DAGSinks)
	}
}

func TestBuildSpecSummaryNilAndUnsafe(t *testing.T) {
	if s := buildSpecSummary(nil, nil); s != nil {
		t.Errorf("nil specs should yield nil, got %+v", s)
	}
	unsafe := &pipeline.Spec{
		AllowUnsafe: true,
		Source:      pipeline.SourceSpec{Type: "file"},
		Sink:        pipeline.SinkSpec{Type: "file_sink"},
	}
	s := buildSpecSummary(unsafe, nil)
	if !s.AllowUnsafe {
		t.Error("AllowUnsafe should propagate")
	}
	if s.SourceMode != "batch" {
		t.Errorf("file source mode: got %s", s.SourceMode)
	}
}

// The summary must never leak config values (secrets live in config maps).
func TestBuildSpecSummaryNoConfigLeak(t *testing.T) {
	spec := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "mysql_cdc", Config: map[string]any{
			"password": "leak-me",
		}},
		Sink: pipeline.SinkSpec{Type: "mysql", Config: map[string]any{
			"password": "leak-me-too",
		}},
	}
	s := buildSpecSummary(spec, nil)
	b := marshalForLeakCheck(t, s)
	for _, secret := range []string{"leak-me", "leak-me-too", "password"} {
		if summaryContains(b, secret) {
			t.Errorf("summary leaked config value %q", secret)
		}
	}
}

func marshalForLeakCheck(t *testing.T, s *specSummary) []byte {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func summaryContains(b []byte, sub string) bool {
	return strings.Contains(string(b), sub)
}

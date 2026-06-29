package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestAIContextPackUsesRuntimeFacts(t *testing.T) {
	pack := buildAIContextPack()
	if pack.Version == "" || len(pack.Components) == 0 {
		t.Fatalf("context pack missing version/components: %#v", pack)
	}
	prompt := pack.SystemPrompt()
	if !strings.Contains(prompt, "OpenETL-Go") || !strings.Contains(prompt, "at-least-once") {
		t.Fatalf("system prompt missing product boundaries:\n%s", prompt)
	}
	if strings.Contains(strings.ToLower(prompt), "flink-like") {
		t.Fatalf("system prompt must not position OpenETL-Go as Flink-like:\n%s", prompt)
	}
	if !strings.Contains(prompt, "source/mysql_cdc") || !strings.Contains(prompt, "sink/doris") || !strings.Contains(prompt, "transform/debezium_cdc") {
		t.Fatalf("system prompt missing runtime descriptors:\n%s", prompt)
	}
	if len(pack.Docs) == 0 {
		t.Fatalf("context pack did not load docs/components summaries")
	}
}

func TestCoreComponentDocsCoverAIContextComponents(t *testing.T) {
	required := []string{
		"source-mysql_batch.md", "source-mysql_cdc.md", "source-mysql_snapshot_cdc.md", "source-kafka.md", "source-file.md", "source-http.md",
		"sink-clickhouse.md", "sink-mysql.md", "sink-postgres.md", "sink-doris.md", "sink-kafka.md", "sink-s3.md", "sink-file_sink.md",
		"transform-lookup.md", "transform-deduplicate.md", "transform-window.md", "transform-flat_map.md", "transform-udtf.md",
		"transform-project.md", "transform-select_fields.md", "transform-type_convert.md", "transform-debezium_cdc.md", "transform-cdc_policy.md",
	}
	dir := filepath.Clean(filepath.Join("..", "..", "..", "docs", "components"))
	for _, name := range required {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read component doc %s: %v", name, err)
		}
		text := string(raw)
		for _, heading := range []string{"## Purpose", "## Config Fields", "## Record Shape", "## Checkpoint, DLQ, Idempotency", "## Example", "## Evidence"} {
			if !strings.Contains(text, heading) {
				t.Fatalf("%s missing heading %q", name, heading)
			}
		}
	}
}

func TestReviewGeneratedSpecFlagsMissingFieldsAndRisks(t *testing.T) {
	spec := &pipeline.Spec{
		Name: "ai-risk",
		Source: pipeline.SourceSpec{
			Type:   "mysql_cdc",
			Config: map[string]any{"host": "mysql", "user": "sync"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": "/tmp/out"},
		},
	}
	review := reviewGeneratedSpec(t.Context(), spec, nil)
	if !hasMissingField(review, "source", "mysql_cdc", "database") || !hasMissingField(review, "source", "mysql_cdc", "tables") {
		t.Fatalf("missing required source fields not reported: %#v", review.MissingFields)
	}
	if !hasRisk(review, "cdc_to_append_sink") {
		t.Fatalf("cdc_to_append_sink risk not reported: %#v", review.RiskFlags)
	}

	spec.Sink = pipeline.SinkSpec{
		Type:   "maxcompute",
		Config: map[string]any{"endpoint": "https://example", "project": "p", "table": "t", "access_key_id": "id", "access_key_secret": "secret"},
	}
	review = reviewGeneratedSpec(t.Context(), spec, nil)
	if !hasRisk(review, "maxcompute_writer_disabled") {
		t.Fatalf("maxcompute writer-disabled risk not reported: %#v", review.RiskFlags)
	}
}

func hasMissingField(review AIGenerationReview, kind, typ, field string) bool {
	for _, item := range review.MissingFields {
		if item.Kind == kind && item.Type == typ && item.Field == field {
			return true
		}
	}
	return false
}

func hasRisk(review AIGenerationReview, code string) bool {
	for _, item := range review.RiskFlags {
		if item.Code == code {
			return true
		}
	}
	return false
}

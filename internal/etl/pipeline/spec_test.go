package pipeline

import (
	"os"
	"strings"
	"testing"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	_ "github.com/a8851625/openetl-go/internal/etl/transform"
)

func TestExpandEnvSupportsDefaultsAndMissing(t *testing.T) {
	t.Setenv("ETL_TEST_PASSWORD", "secret")
	out, err := ExpandEnv("password: ${ETL_TEST_PASSWORD}\nhost: ${ETL_TEST_HOST:-localhost}")
	if err != nil {
		t.Fatalf("ExpandEnv() error = %v", err)
	}
	if !strings.Contains(out, "password: secret") || !strings.Contains(out, "host: localhost") {
		t.Fatalf("ExpandEnv() = %q", out)
	}

	_, err = ExpandEnv("password: ${ETL_TEST_MISSING}")
	if err == nil || !strings.Contains(err.Error(), "ETL_TEST_MISSING") {
		t.Fatalf("ExpandEnv() missing err = %v", err)
	}
}

func TestValidateSpecRejectsUnknownPlugins(t *testing.T) {
	spec := &Spec{
		Name:                  "bad",
		Source:                SourceSpec{Type: "missing_source"},
		Sink:                  SinkSpec{Type: "file_sink"},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		BackpressureBuffer:    1,
		Retry:                 &RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
	}
	if err := ValidateSpec(spec); err == nil || !strings.Contains(err.Error(), "unknown source.type") {
		t.Fatalf("ValidateSpec() error = %v", err)
	}
}

func TestLoadSpecAppliesDefaultsAndExpandsEnv(t *testing.T) {
	t.Setenv("ETL_TEST_FILE", "/tmp/input.jsonl")
	file, err := os.CreateTemp(t.TempDir(), "spec-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.WriteString(`name: file-test
source:
  type: file
  config:
    path: ${ETL_TEST_FILE}
    format: json
sink:
  type: file_sink
  config:
    output_dir: /tmp/out
`)
	_ = file.Close()

	spec, err := LoadSpec(file.Name())
	if err != nil {
		t.Fatalf("LoadSpec() error = %v", err)
	}
	if spec.BatchSize != 1000 || spec.CheckpointIntervalSec != 30 || spec.BackpressureBuffer != 100 {
		t.Fatalf("defaults not applied: %#v", spec)
	}
	if spec.Source.Config["path"] != "/tmp/input.jsonl" {
		t.Fatalf("env not expanded: %#v", spec.Source.Config)
	}
}

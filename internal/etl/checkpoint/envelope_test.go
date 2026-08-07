package checkpoint

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestCheckpointEnvelopeRoundTrip(t *testing.T) {
	source := json.RawMessage(`{"topic":"orders","offsets":{"0":42}}`)
	raw, err := BuildEnvelope(source, map[string]string{"lookup": "1001", "window": "1002"}, map[string]any{"sink": "clickhouse", "version": 7})
	if err != nil {
		t.Fatalf("BuildEnvelope: %v", err)
	}
	env, ok, err := ParseEnvelope(raw)
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	if !ok {
		t.Fatal("ParseEnvelope ok=false, want true")
	}
	if env.Version != 1 || env.DeliveryMode != "at_least_once" {
		t.Fatalf("unexpected envelope metadata: %#v", env)
	}
	if string(env.Source) != string(source) {
		t.Fatalf("source = %s, want %s", env.Source, source)
	}
	if env.State["lookup"] != "1001" || env.State["window"] != "1002" {
		t.Fatalf("state versions = %#v", env.State)
	}
	if env.SinkCommit["sink"] != "clickhouse" {
		t.Fatalf("sink commit = %#v", env.SinkCommit)
	}
}

func TestCheckpointEnvelopeLegacyPosition(t *testing.T) {
	env, ok, err := ParseEnvelope(json.RawMessage(`{"offset":42}`))
	if err != nil {
		t.Fatalf("Parse legacy: %v", err)
	}
	if ok || env != nil {
		t.Fatalf("legacy payload parsed as envelope: ok=%v env=%#v", ok, env)
	}
}

func TestCheckpointEnvelopeRejectsInvalidSourceJSON(t *testing.T) {
	if _, err := BuildEnvelope(json.RawMessage(`not-json`), nil, nil); err == nil {
		t.Fatal("BuildEnvelope accepted invalid source JSON")
	}
}

func TestUnwrapForSourceRejectsCorruptAndUnknownCheckpoint(t *testing.T) {
	cases := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{name: "invalid json", raw: json.RawMessage(`not-json`), want: "not valid JSON"},
		{name: "unknown envelope version", raw: json.RawMessage(`{"version":99,"source":{"offset":1}}`), want: "unsupported checkpoint envelope version"},
		{name: "missing envelope source", raw: json.RawMessage(`{"version":1}`), want: "has no source position"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnwrapForSource(&core.Checkpoint{JobName: "job", Position: tc.raw})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("UnwrapForSource error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestUnwrapForSourceKeepsLegacyPosition(t *testing.T) {
	cp := &core.Checkpoint{JobName: "legacy", Source: "mysql_cdc", Position: json.RawMessage(`{"file":"mysql-bin.000001","pos":42}`)}
	got, err := UnwrapForSource(cp)
	if err != nil {
		t.Fatalf("UnwrapForSource: %v", err)
	}
	if got != cp || string(got.Position) != string(cp.Position) {
		t.Fatalf("legacy checkpoint changed: got=%#v want same pointer/payload", got)
	}
}

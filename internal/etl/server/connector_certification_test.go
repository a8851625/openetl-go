package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/plugin/pluginsystem"
)

type connectorCertificationTarget struct {
	Kind         string
	Type         string
	DocPath      string
	Scripts      []string
	SecretFields []string
	Gates        map[string][]string
}

func TestConnectorCertificationKitProductionSet(t *testing.T) {
	targets := []connectorCertificationTarget{
		{
			Kind: "source", Type: "mysql_batch", DocPath: "docs/components/source-mysql_batch.md",
			Scripts:      []string{"hack/e2e.sh", "hack/e2e-mysql-postgres.sh"},
			SecretFields: []string{"password"},
			Gates:        productionSourceGates("pass", "pass"),
		},
		{
			Kind: "source", Type: "mysql_cdc", DocPath: "docs/components/source-mysql_cdc.md",
			Scripts:      []string{"hack/e2e-cdc-mysql.sh", "hack/e2e-cdc-postgres.sh"},
			SecretFields: []string{"password"},
			Gates:        productionSourceGates("pass", "pass"),
		},
		{
			Kind: "source", Type: "mysql_snapshot_cdc", DocPath: "docs/components/source-mysql_snapshot_cdc.md",
			Scripts:      []string{"hack/e2e-snapshot-cdc.sh", "hack/e2e-snapshot-cdc-clickhouse.sh"},
			SecretFields: []string{"password"},
			Gates:        productionSourceGates("pass", "pass"),
		},
		{
			Kind: "sink", Type: "mysql", DocPath: "docs/components/sink-mysql.md",
			Scripts:      []string{"hack/e2e-cdc-mysql.sh", "hack/e2e-debezium-mysql.sh"},
			SecretFields: []string{"password"},
			Gates:        productionSinkGates("pass", "pass", "pass"),
		},
		{
			Kind: "sink", Type: "clickhouse", DocPath: "docs/components/sink-clickhouse.md",
			Scripts:      []string{"hack/e2e-clickhouse.sh", "hack/e2e-snapshot-cdc-clickhouse.sh"},
			SecretFields: []string{"password"},
			Gates:        productionSinkGates("pass", "pass", "pass"),
		},
		{
			Kind: "source", Type: "kafka", DocPath: "docs/components/source-kafka.md",
			Scripts:      []string{"hack/e2e-kafka.sh", "hack/e2e-kafka-raw-ods.sh"},
			SecretFields: []string{"sasl_password"},
			Gates:        productionSourceGates("partial", "partial"),
		},
		{
			Kind: "sink", Type: "kafka", DocPath: "docs/components/sink-kafka.md",
			Scripts:      []string{"hack/e2e-kafka.sh", "hack/e2e-kafka-raw-ods.sh"},
			SecretFields: []string{"sasl_password"},
			Gates:        productionSinkGates("not_applicable", "partial", "pass"),
		},
		{
			Kind: "source", Type: "file", DocPath: "docs/components/source-file.md",
			Scripts: []string{"hack/e2e.sh"},
			Gates:   productionSourceGates("partial", "partial"),
		},
		{
			Kind: "sink", Type: "file_sink", DocPath: "docs/components/sink-file_sink.md",
			Scripts: []string{"hack/e2e.sh"},
			Gates:   productionSinkGates("not_applicable", "partial", "partial"),
		},
		{
			Kind: "sink", Type: "s3", DocPath: "docs/components/sink-s3.md",
			Scripts:      []string{"hack/e2e-s3-minio.sh"},
			SecretFields: []string{"secret_key"},
			Gates:        productionSinkGates("not_applicable", "partial", "pass"),
		},
	}

	descriptors := connectorDescriptorMap(connectorDescriptors())
	repoRoot := filepath.Clean("../../..")

	for _, target := range targets {
		t.Run(target.Kind+"/"+target.Type, func(t *testing.T) {
			desc, ok := descriptors[target.Kind+":"+target.Type]
			if !ok {
				t.Fatalf("descriptor missing")
			}
			certifyConnectorTarget(t, repoRoot, target, desc)
		})
	}
}

func TestPluginABIV1CertificationDocs(t *testing.T) {
	repoRoot := filepath.Clean("../../..")
	abiDoc := readCertificationDoc(t, repoRoot, "docs/plugin-abi-v1.md")
	required := []string{
		pluginsystem.ABIVersionV1,
		pluginsystem.MinRuntimeVersionV1,
		"Compatibility Matrix",
		"Deprecation Policy",
		"manifest_validated=false",
		"Source and sink plugins must be compiled offline",
	}
	for _, want := range required {
		if !strings.Contains(abiDoc, want) {
			t.Fatalf("docs/plugin-abi-v1.md missing %q", want)
		}
	}

	certDoc := readCertificationDoc(t, repoRoot, "docs/connector-certification.md")
	for _, want := range []string{"Plugin ABI v1 evidence", "Rules For Production Plugins", "TestPluginABIV1CertificationDocs", "feishu-sheet-source"} {
		if !strings.Contains(certDoc, want) {
			t.Fatalf("docs/connector-certification.md missing %q", want)
		}
	}

	sdkBody := readCertificationDoc(t, repoRoot, "web/plugin-sdk/src/index.ts")
	for _, want := range []string{"OPENETL_PLUGIN_ABI", "OPENETL_MIN_RUNTIME_VERSION", "definePluginManifest", "createExtismSourcePlugin"} {
		if !strings.Contains(sdkBody, want) {
			t.Fatalf("web/plugin-sdk/src/index.ts missing %q", want)
		}
	}
}

func TestFeishuSheetSourcePluginSampleCertification(t *testing.T) {
	repoRoot := filepath.Clean("../../..")
	files := []string{
		"web/plugin-sdk/examples/feishu-sheet-source/feishu-sheet-source.ts",
		"web/plugin-sdk/examples/feishu-sheet-source/manifest.json",
		"web/plugin-sdk/examples/feishu-sheet-source/README.md",
		"web/plugin-sdk/examples/feishu-sheet-source/pipeline.example.yaml",
		"web/plugin-sdk/examples/feishu-sheet-source/fixture.test.ts",
	}
	for _, path := range files {
		if _, err := os.Stat(filepath.Join(repoRoot, path)); err != nil {
			t.Fatalf("plugin sample missing %s: %v", path, err)
		}
	}

	manifestBody := readCertificationDoc(t, repoRoot, "web/plugin-sdk/examples/feishu-sheet-source/manifest.json")
	for _, want := range []string{
		pluginsystem.ABIVersionV1,
		pluginsystem.MinRuntimeVersionV1,
		`"kind": "source"`,
		`"read"`,
		"app_id",
		"app_secret",
		"spreadsheet_token",
		"sheet_range",
		"base_url",
		`"secret": true`,
	} {
		if !strings.Contains(manifestBody, want) {
			t.Fatalf("feishu-sheet-source manifest missing %q", want)
		}
	}

	sourceBody := readCertificationDoc(t, repoRoot, "web/plugin-sdk/examples/feishu-sheet-source/feishu-sheet-source.ts")
	for _, want := range []string{"createExtismSourcePlugin", "definePluginManifest", "export const read", "tenant_access_token", "value_range"} {
		if !strings.Contains(sourceBody, want) {
			t.Fatalf("feishu-sheet-source.ts missing %q", want)
		}
	}

	readme := readCertificationDoc(t, repoRoot, "web/plugin-sdk/examples/feishu-sheet-source/README.md")
	for _, want := range []string{"extism-js", "/api/v2/plugins/install", "plugin_feishu-sheet-source", "beta", "dev-only"} {
		if !strings.Contains(readme, want) {
			t.Fatalf("feishu-sheet-source README missing %q", want)
		}
	}

	pipeline := readCertificationDoc(t, repoRoot, "web/plugin-sdk/examples/feishu-sheet-source/pipeline.example.yaml")
	for _, want := range []string{"plugin_feishu-sheet-source", "project", "type_convert", "file_sink"} {
		if !strings.Contains(pipeline, want) {
			t.Fatalf("pipeline.example.yaml missing %q", want)
		}
	}
}

func productionSourceGates(schemaStatus, remoteStatus string) map[string][]string {
	return map[string][]string{
		"registered":           {"pass"},
		"config_schema":        {"pass"},
		"schedule_policy":      {"pass"},
		"schema_introspection": {schemaStatus},
		"checkpoint":           {"pass"},
		"remote_preflight":     {remoteStatus},
		"e2e_evidence":         {"pass"},
	}
}

func productionSinkGates(schemaStatus, replayStatus, remoteStatus string) map[string][]string {
	return map[string][]string{
		"registered":        {"pass"},
		"config_schema":     {"pass"},
		"schema_preflight":  {schemaStatus},
		"replay_absorption": {replayStatus},
		"remote_preflight":  {remoteStatus},
		"e2e_evidence":      {"pass"},
	}
}

func certifyConnectorTarget(t *testing.T, repoRoot string, target connectorCertificationTarget, desc ConnectorDescriptor) {
	t.Helper()
	if !desc.Registered {
		t.Fatalf("registered = false")
	}
	if desc.Maturity != "production" {
		t.Fatalf("maturity = %q, want production", desc.Maturity)
	}
	if len(desc.Fields) == 0 {
		t.Fatalf("config fields are empty")
	}
	for _, secret := range target.SecretFields {
		if !contains(desc.SecretFields, secret) {
			t.Fatalf("secret fields = %#v, want %q", desc.SecretFields, secret)
		}
	}
	for code, allowedStatuses := range target.Gates {
		gate, ok := readinessGate(desc, code)
		if !ok {
			t.Fatalf("readiness gate %q missing; gates=%#v", code, desc.Readiness.Gates)
		}
		if !contains(allowedStatuses, gate.Status) {
			t.Fatalf("gate %s status = %q, want one of %#v", code, gate.Status, allowedStatuses)
		}
		if gate.Status == "partial" && (gate.Evidence == "" || gate.Remediation == "") {
			t.Fatalf("partial gate %s must include evidence and remediation: %#v", code, gate)
		}
	}

	evidenceGate, ok := readinessGate(desc, "e2e_evidence")
	if !ok {
		t.Fatalf("e2e_evidence gate missing")
	}
	docBody := readCertificationDoc(t, repoRoot, target.DocPath)
	if !strings.Contains(docBody, "## Evidence") {
		t.Fatalf("%s missing Evidence section", target.DocPath)
	}
	for _, script := range target.Scripts {
		if _, err := os.Stat(filepath.Join(repoRoot, script)); err != nil {
			t.Fatalf("certification script %s is not available: %v", script, err)
		}
		if !strings.Contains(evidenceGate.Evidence, script) {
			t.Fatalf("descriptor evidence %q does not mention %s", evidenceGate.Evidence, script)
		}
		if !strings.Contains(docBody, script) {
			t.Fatalf("%s does not mention %s", target.DocPath, script)
		}
	}
}

func connectorDescriptorMap(items []ConnectorDescriptor) map[string]ConnectorDescriptor {
	out := make(map[string]ConnectorDescriptor, len(items))
	for _, item := range items {
		out[item.Kind+":"+item.Type] = item
	}
	return out
}

func readinessGate(desc ConnectorDescriptor, code string) (ConnectorReadinessGate, bool) {
	for _, gate := range desc.Readiness.Gates {
		if gate.Code == code {
			return gate, true
		}
	}
	return ConnectorReadinessGate{}, false
}

func readCertificationDoc(t *testing.T, repoRoot, path string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot, path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(body)
}

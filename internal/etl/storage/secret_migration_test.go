package storage_test

import (
	"context"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// TestDetectPlaintextSecretsIsReadOnly verifies detection reports legacy
// plaintext rows (including descriptor-declared dsn) without rewriting them,
// and that remediation is idempotent.
func TestDetectPlaintextSecretsIsReadOnly(t *testing.T) {
	ctx := context.Background()
	wrapped, sf, raw := openSecretFieldStore(t, "k1", testSecretKey(1), "")
	sf.WithSecretFieldResolver(func(kind, typ, field string) bool {
		return (kind == "sink" && typ == "jdbc" && field == "dsn") || field == "password"
	})

	// Simulate a legacy row written before dsn marking existed: plaintext dsn
	// stored directly in the raw backend.
	legacy := &storage.ConnectionEntry{
		Name: "legacy-jdbc",
		Kind: "sink",
		Type: "jdbc",
		Config: map[string]any{
			"dsn": "mysql://root:hunter2@tcp(prod:3306)/db",
		},
	}
	if err := raw.SaveConnection(ctx, legacy); err != nil {
		t.Fatalf("seed legacy connection: %v", err)
	}

	// Detection phase is read-only.
	report, err := sf.DetectPlaintextSecrets(ctx)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	found := false
	for _, f := range report.Connections {
		if f.Name == "legacy-jdbc" && f.Field == "dsn" {
			found = true
		}
	}
	if !found {
		t.Fatalf("legacy plaintext dsn not detected: %+v", report.Connections)
	}
	rawCfg := rawConnectionConfigJSON(t, raw, "legacy-jdbc")
	if !strings.Contains(rawCfg, "hunter2") {
		t.Fatalf("detection must not rewrite rows, but plaintext is gone: %s", rawCfg)
	}

	// Remediation encrypts.
	remediated, err := sf.RemediatePlaintextSecrets(ctx)
	if err != nil {
		t.Fatalf("remediate: %v", err)
	}
	if len(remediated.Connections) == 0 {
		t.Fatalf("remediation report empty")
	}
	rawCfg = rawConnectionConfigJSON(t, raw, "legacy-jdbc")
	if strings.Contains(rawCfg, "hunter2") {
		t.Fatalf("plaintext dsn survived remediation: %s", rawCfg)
	}
	if !strings.Contains(rawCfg, "enc:v1:") {
		t.Fatalf("no envelope after remediation: %s", rawCfg)
	}

	// Idempotent: second detect finds nothing, second remediate is a no-op.
	report2, err := sf.DetectPlaintextSecrets(ctx)
	if err != nil {
		t.Fatalf("detect after remediation: %v", err)
	}
	if len(report2.Connections) != 0 {
		t.Fatalf("expected no findings after remediation, got %+v", report2.Connections)
	}
	if _, err := sf.RemediatePlaintextSecrets(ctx); err != nil {
		t.Fatalf("idempotent remediate: %v", err)
	}
	rawCfg = rawConnectionConfigJSON(t, raw, "legacy-jdbc")
	if strings.Count(rawCfg, "enc:v1:") != 1 {
		t.Fatalf("double encryption detected (envelope count != 1): %s", rawCfg)
	}

	// Values remain usable by runtime callers after remediation.
	got, err := wrapped.GetConnection(ctx, "legacy-jdbc")
	if err != nil {
		t.Fatalf("get after remediation: %v", err)
	}
	if got.Config["dsn"] != "mysql://root:hunter2@tcp(prod:3306)/db" {
		t.Fatalf("decrypted dsn mismatch after remediation: %v", got.Config["dsn"])
	}
}

// TestDetectPlaintextSettings covers the settings side of detection and
// remediation against rows seeded directly into the raw backend (legacy rows
// written before encryption existed).
func TestDetectPlaintextSettings(t *testing.T) {
	ctx := context.Background()
	wrapped, sf, raw := openSecretFieldStore(t, "k1", testSecretKey(1), "")
	// Seed a legacy plaintext secret setting directly in the raw backend.
	if _, err := raw.DB().Exec(`INSERT INTO settings(key, value) VALUES(?, ?)`,
		"alert.smtp_password", "s3cr3t"); err != nil {
		t.Fatalf("seed legacy setting: %v", err)
	}
	if _, err := raw.DB().Exec(`INSERT INTO settings(key, value) VALUES(?, ?)`,
		"ui.theme", "dark"); err != nil {
		t.Fatalf("seed normal setting: %v", err)
	}
	report, err := sf.DetectPlaintextSecrets(ctx)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(report.Settings) != 1 || report.Settings[0].Name != "alert.smtp_password" {
		t.Fatalf("unexpected settings findings: %+v", report.Settings)
	}
	if _, err := sf.RemediatePlaintextSecrets(ctx); err != nil {
		t.Fatalf("remediate: %v", err)
	}
	report2, err := sf.DetectPlaintextSecrets(ctx)
	if err != nil {
		t.Fatalf("detect after: %v", err)
	}
	if len(report2.Settings) != 0 {
		t.Fatalf("settings not remediated: %+v", report2.Settings)
	}
	v, err := wrapped.GetSetting(ctx, "alert.smtp_password")
	if err != nil || v != "s3cr3t" {
		t.Fatalf("setting value lost after remediation: %q err=%v", v, err)
	}
}

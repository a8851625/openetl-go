package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
)

// Ordinary tests run the real command in a subprocess of this test binary.
// The backend e2e scripts set BACKUP_CLI_BINARY to the freshly built executable.
func TestBackupMaintenanceProcess(t *testing.T) {
	if os.Getenv("OPENETL_BACKUP_TEST_PROCESS") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"openetl-go"}, os.Args[i+1:]...)
			break
		}
	}
	Main.Run(context.Background())
	os.Exit(0)
}

func TestBackupMaintenanceCLI(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(config, []byte("etl:\n  profile: production\n  tls:\n    cert: /missing-cert.pem\n    key: /missing-key.pem\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend := os.Getenv("BACKUP_CLI_BACKEND")
	if backend == "" {
		backend = "sqlite"
	}
	binary := os.Getenv("BACKUP_CLI_BINARY")
	var prefix []string
	if binary == "" {
		binary = os.Args[0]
		prefix = []string{"-test.run=^TestBackupMaintenanceProcess$", "--"}
	}
	dataDir := filepath.Join(dir, "data")
	pluginsDir := filepath.Join(dataDir, "plugins")
	base := []string{"--config", config, "--storage", backend, "--data-dir", dataDir, "--plugins-dir", pluginsDir}
	run := func(wantSuccess bool, args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		all := append(append(append([]string{}, prefix...), base...), args...)
		command := exec.CommandContext(ctx, binary, all...)
		for _, env := range os.Environ() {
			if !strings.HasPrefix(env, "ETL_") && !strings.HasPrefix(env, "OPENETL_BACKUP_TEST_PROCESS=") {
				command.Env = append(command.Env, env)
			}
		}
		command.Env = append(command.Env, "OPENETL_BACKUP_TEST_PROCESS=1")
		if dsn := os.Getenv("BACKUP_CLI_DSN"); dsn != "" {
			command.Env = append(command.Env, "ETL_STORAGE_DSN="+dsn)
		}
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("maintenance did not exit; runtime may have started: %v", ctx.Err())
		}
		if (err == nil) != wantSuccess {
			t.Fatalf("maintenance success=%t, want=%t: %v\n%s", err == nil, wantSuccess, err, output)
		}
		return string(output)
	}

	fixed := time.Date(2026, 9, 1, 2, 3, 4, 123000000, time.UTC)
	snapshot := &backup.Snapshot{
		FormatVersion:   2,
		Pipelines:       []*storage.PipelineRow{{ID: "maint-id", Name: "maint", SpecYAML: "name: maint", Status: "running", DesiredState: "running", ObservedState: "running", Generation: 9, CreatedAt: fixed, UpdatedAt: fixed}},
		Versions:        []*storage.PipelineVersion{{ID: 401, Pipeline: "maint-id", Version: 3, SpecYAML: "name: maint", CreatedAt: fixed}, {ID: 409, Pipeline: "maint-id", Version: 7, SpecYAML: "name: maint", CreatedAt: fixed}},
		Checkpoints:     []*storage.CheckpointRecord{{JobName: "maint-id", Source: "file", Position: json.RawMessage(`{"offset":42}`), Generation: 9, Timestamp: fixed, UpdatedAt: fixed}},
		RunHistory:      []*storage.RunRecord{{ID: 701, JobName: "maint-id", Status: "failed", StartedAt: fixed, FinishedAt: &fixed, RecordsRead: 99, RecordsFailed: 4, RecordsDLQ: 4}},
		Plugins:         []*storage.PluginEntry{{Name: "parser", Kind: "transform", WASMPath: "/missing-host/parser.wasm", Version: "1.0.0", InstalledAt: fixed}},
		PluginArtifacts: map[string]string{"parser": base64.StdEncoding.EncodeToString([]byte("\x00asm\x01\x00\x00\x00"))},
		Settings:        map[string]string{"llm.api_key": "enc:v1:maintenance-test-envelope"},
	}
	input := filepath.Join(dir, "input.json")
	if err := backup.WriteFile(input, snapshot); err != nil {
		t.Fatal(err)
	}
	if output := run(true, "--restore-file", input); !strings.Contains(output, "restore complete:") {
		t.Fatalf("missing restore outcome: %s", output)
	}
	outputFile := filepath.Join(dir, "output.json")
	if output := run(true, "--backup-file", outputFile); !strings.Contains(output, "backup complete:") {
		t.Fatalf("missing backup outcome: %s", output)
	}
	loaded, err := backup.ReadFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Versions) != 2 || loaded.Versions[1].ID != 409 || loaded.Versions[1].Version != 7 || loaded.Pipelines[0].Generation != 9 || loaded.RunHistory[0].ID != 701 || !loaded.RunHistory[0].StartedAt.Equal(fixed) {
		t.Fatalf("CLI lost historical content: %+v", loaded)
	}
	if loaded.Settings["llm.api_key"] != snapshot.Settings["llm.api_key"] {
		t.Fatal("CLI changed the stored secret envelope")
	}
	if _, err := os.ReadFile(loaded.Plugins[0].WASMPath); err != nil {
		t.Fatalf("CLI did not restore a usable plugin path: %v", err)
	}
	// Malformed historical data must fail after the clear and leave the
	// original database intact. Check the exact payload after another process.
	snapshot.RunHistory = append(snapshot.RunHistory, snapshot.RunHistory[0])
	if err := backup.WriteFile(input, snapshot); err != nil {
		t.Fatal(err)
	}
	run(false, "--restore-file", input)
	run(true, "--backup-file", outputFile)
	after, err := backup.ReadFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	after.CreatedAt = loaded.CreatedAt
	if !reflect.DeepEqual(after, loaded) {
		t.Fatal("failed subprocess restore changed the original control plane")
	}
	// Invalid CLI usage must never fall through to starting the runtime.
	run(false, "--backup-file", outputFile, "--restore-file", input)
	run(false, "--restore-file", filepath.Join(dir, "missing.json"))
	run(true, "--check-secrets")
	run(false, "--remediate-secrets") // No key: never report a successful remediation.
	run(false, "--check-secrets", "--remediate-secrets")
	run(false, "--check-secrets", "--backup-file", outputFile)
	if backend == "sqlite" {
		run(false, "--backup-file", filepath.Join(dataDir, "etl.db"))
		run(true, "--backup-file", outputFile)
		missingDB := filepath.Join(dir, "typo.db")
		for _, flag := range []string{"--check-secrets", "--remediate-secrets"} {
			run(false, "--sqlite-path", missingDB, flag)
		}
		if _, err := os.Stat(missingDB); !os.IsNotExist(err) {
			t.Fatal("secret maintenance created an empty database for a missing source")
		}
	}
}

package cmd

import (
	"io"
	"strings"
	"testing"
)

func TestRuntimeHelpDocumentsPriorityAndCoreFlags(t *testing.T) {
	help := runtimeHelpText()
	for _, want := range []string{
		"CLI flags > environment variables > config.yaml > built-in defaults",
		"--config PATH",
		"--data-dir DIR",
		"--etl-api-port PORT",
		"--storage TYPE",
		"--api-token TOKEN",
		"--role ROLE",
		"--audit-enabled BOOL",
		"ETL_API_TOKEN",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("help missing %q:\n%s", want, help)
		}
	}
}

func TestParseRuntimeFlags(t *testing.T) {
	opts, err := parseRuntimeFlags([]string{
		"--config", "/etc/openetl/config.yaml",
		"--data-dir", "/var/lib/openetl",
		"--port", "8080",
		"--etl-api-port", "8081",
		"--storage", "mysql",
		"--storage-dsn", "user:pass@tcp(db:3306)/etl",
		"--role", "master",
		"--api-token", "secret",
		"--audit-enabled", "false",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseRuntimeFlags: %v", err)
	}
	if opts.config != "/etc/openetl/config.yaml" || opts.dataDir != "/var/lib/openetl" {
		t.Fatalf("parsed paths = config %q data %q", opts.config, opts.dataDir)
	}
	if opts.port != "8080" || opts.etlAPIPort != "8081" {
		t.Fatalf("parsed ports = %q/%q", opts.port, opts.etlAPIPort)
	}
	if opts.storageType != "mysql" || opts.role != "master" || opts.apiToken != "secret" {
		t.Fatalf("parsed storage/role/token = %q/%q/%q", opts.storageType, opts.role, opts.apiToken)
	}
	if opts.auditEnabled != "false" {
		t.Fatalf("parsed audit-enabled = %q", opts.auditEnabled)
	}
	for _, flagName := range []string{"config", "data-dir", "port", "etl-api-port", "storage", "storage-dsn", "role", "api-token", "audit-enabled"} {
		if !opts.seen[flagName] {
			t.Fatalf("flag %q not marked seen", flagName)
		}
	}
}

func TestValidateRuntimeFlagsRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name string
		opts *runtimeFlags
	}{
		{"role", &runtimeFlags{role: "sidecar"}},
		{"storage", &runtimeFlags{storageType: "oracle"}},
		{"port", &runtimeFlags{port: "99999"}},
		{"worker-slots", &runtimeFlags{workerSlots: "0"}},
		{"audit-enabled", &runtimeFlags{auditEnabled: "maybe"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRuntimeFlags(tc.opts); err == nil {
				t.Fatal("validateRuntimeFlags returned nil, want error")
			}
		})
	}
}

func TestRuntimeAddressHelpers(t *testing.T) {
	if got := joinHostPort("", "8000"); got != ":8000" {
		t.Fatalf("joinHostPort empty host = %q", got)
	}
	if got := joinHostPort("127.0.0.1", "8000"); got != "127.0.0.1:8000" {
		t.Fatalf("joinHostPort host = %q", got)
	}
	host, port, err := splitAddress(":8001")
	if err != nil || host != "" || port != "8001" {
		t.Fatalf("splitAddress(:8001) = %q/%q/%v", host, port, err)
	}
}

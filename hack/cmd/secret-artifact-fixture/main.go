// Test-only fixture for the real portable-backup / vendor-dump security drill.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/server"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

var paths = []struct{ kind, typ string }{{"sink", "jdbc"}, {"transform", "dbt"}, {"transform", "enricher"}, {"transform", "lookup"}}

func testKey(value byte) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string([]byte{value}), 32)))
}

func marker(typ string) string { return "artifact-dsn-secret-" + typ + "-marker" }
func dsn(typ string) string {
	return "postgres://fixture:" + marker(typ) + "@fixture.invalid/warehouse"
}
func needles() []string {
	list := []string{"artifact-nested-password-marker", "artifact-setting-secret-marker", "artifact-old-secret-marker", "artifact-spec-secret-marker", "artifact-control-leak-marker"}
	for _, p := range paths {
		list = append(list, marker(p.typ))
	}
	return list
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 4 {
		return fmt.Errorf("usage: secret-artifact-fixture seed|verify|remember|compare|inject|diagnostic BACKEND ARTIFACT_DIR")
	}
	mode, backend, dir := os.Args[1], os.Args[2], os.Args[3]
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var raw storage.Storage
	var err error
	switch backend {
	case "sqlite":
		raw, err = sqlite.New(os.Getenv("SECRET_SQLITE_PATH"))
	case "mysql":
		raw, err = mysql.New(os.Getenv("SECRET_DSN"))
	case "postgres":
		raw, err = postgres.New(ctx, os.Getenv("SECRET_DSN"))
	default:
		return fmt.Errorf("unknown backend %q", backend)
	}
	if err != nil {
		return err
	}
	defer raw.Close()
	oldCipher, err := storage.NewSpecCipher("old", testKey(17), "")
	if err != nil {
		return err
	}
	newCipher, err := storage.NewSpecCipher("new", testKey(34), "old="+testKey(17))
	if err != nil {
		return err
	}
	wrap := func(cipher *storage.SpecCipher) *storage.SecretFieldStore {
		return storage.NewSecretFieldStore(raw, cipher).(*storage.SecretFieldStore).WithSecretFieldResolver(server.NewDescriptorSecretFieldResolver())
	}
	stateDigest := func() (string, error) {
		connections, err := raw.ListConnections(ctx)
		if err != nil {
			return "", err
		}
		settings, err := raw.ListSettings(ctx)
		if err != nil {
			return "", err
		}
		blob, err := json.Marshal(struct {
			Connections []*storage.ConnectionEntry
			Settings    map[string]string
		}{connections, settings})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", sha256.Sum256(blob)), nil
	}
	switch mode {
	case "seed":
		counts, err := raw.(storage.RetentionPurger).CountObjects(ctx)
		if err != nil || counts != (storage.ObjectCounts{}) {
			return fmt.Errorf("refusing populated fixture database: %+v %v", counts, err)
		}
		for _, p := range paths {
			if err := raw.SaveConnection(ctx, &storage.ConnectionEntry{Name: "fixture-" + p.typ, Kind: p.kind, Type: p.typ, Config: map[string]any{"dsn": dsn(p.typ), "mode": "sql", "nested": map[string]any{"password": "artifact-nested-password-marker"}}}); err != nil {
				return err
			}
		}
		old := wrap(oldCipher)
		if err := old.SaveConnection(ctx, &storage.ConnectionEntry{Name: "sealed-old", Kind: "source", Type: "mysql_batch", Config: map[string]any{"password": "artifact-old-secret-marker"}}); err != nil {
			return err
		}
		if err := raw.SetSetting(ctx, "alert.smtp_password", "artifact-setting-secret-marker"); err != nil {
			return err
		}
		if err := old.SetSetting(ctx, "llm.api_key", "artifact-old-secret-marker"); err != nil {
			return err
		}
		spec := "name: secret-fixture\nsource:\n  type: http\n  config:\n    auth_token: artifact-spec-secret-marker\nsink:\n  type: file\n"
		if err := storage.NewPipelineSpecStore(raw, oldCipher).SaveWithID(ctx, "fixture-pipeline", "secret-fixture", spec, "stopped"); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "needles.txt"), []byte(strings.Join(needles(), "\n")+"\n"), 0o600); err != nil {
			return err
		}
		before, err := stateDigest()
		if err != nil {
			return err
		}
		if _, err := backup.Export(ctx, raw, backup.Options{SecretFieldResolver: server.NewDescriptorSecretFieldResolver()}); err == nil {
			return fmt.Errorf("portable export accepted legacy plaintext secrets")
		}
		after, err := stateDigest()
		if err != nil || before != after {
			return fmt.Errorf("backup preflight mutated the database: %v", err)
		}
		fmt.Println("LEGACY_SEEDED: four DSNs, nested password, settings; existing ciphertext old key; portable preflight rejected without mutation")
	case "verify":
		current := wrap(newCipher)
		for _, p := range paths {
			c, err := current.GetConnection(ctx, "fixture-"+p.typ)
			if err != nil || c == nil || c.Config["dsn"] != dsn(p.typ) {
				return fmt.Errorf("migrated %s DSN no longer readable: %v", p.typ, err)
			}
			nested, _ := c.Config["nested"].(map[string]any)
			if nested["password"] != "artifact-nested-password-marker" {
				return fmt.Errorf("nested secret no longer readable")
			}
			stored, err := raw.GetConnection(ctx, c.Name)
			if err != nil {
				return err
			}
			if !strings.HasPrefix(fmt.Sprint(stored.Config["dsn"]), "enc:v1:new:") {
				return fmt.Errorf("new key not used for %s", p.typ)
			}
		}
		old, err := raw.GetConnection(ctx, "sealed-old")
		if err != nil || old == nil || !strings.HasPrefix(fmt.Sprint(old.Config["password"]), "enc:v1:old:") {
			return fmt.Errorf("untouched old envelope changed: %v", err)
		}
		read, err := current.GetConnection(ctx, "sealed-old")
		if err != nil || read.Config["password"] != "artifact-old-secret-marker" {
			return fmt.Errorf("old key rotation read failed: %v", err)
		}
		row, err := raw.GetPipeline(ctx, "fixture-pipeline")
		if err != nil || row == nil {
			return fmt.Errorf("pipeline missing: %v", err)
		}
		plain, err := newCipher.Decrypt(row.SpecYAML)
		if err != nil || !strings.Contains(plain, "artifact-spec-secret-marker") {
			return fmt.Errorf("pipeline spec old-key read failed: %v", err)
		}
		report, err := current.DetectPlaintextSecrets(ctx)
		if err != nil || len(report.Connections)+len(report.Settings) != 0 {
			return fmt.Errorf("plaintext remains: %v", err)
		}
		fmt.Println("SECRET_FIDELITY_PASS: four DSNs and nested values readable; old ciphertext retained; new key used; no plaintext findings")
	case "remember", "compare":
		digest, err := stateDigest()
		if err != nil {
			return err
		}
		path := filepath.Join(dir, "sealed-state.sha256")
		if mode == "remember" {
			return os.WriteFile(path, []byte(digest), 0o600)
		}
		before, err := os.ReadFile(path)
		if err != nil || string(before) != digest {
			return fmt.Errorf("repeated migration/restore changed sealed rows: %v", err)
		}
		fmt.Println("SEALED_STATE_UNCHANGED")
	case "inject":
		// Simulate a value leak outside declared fields. This still uses the
		// real exporter, so the scanner must reject a real generated product.
		return raw.SetSetting(ctx, "fixture.public.note", "artifact-control-leak-marker")
	case "diagnostic":
		manifest, err := storage.BackupSQLStore(ctx, raw.(storage.SQLDumper), filepath.Join(dir, "clean-jsonl"), needles())
		if err != nil {
			return err
		}
		if !manifest.SecretScan.OK || manifest.Counts.Connections != 5 || manifest.Counts.Pipelines != 1 || manifest.Counts.Versions != 1 || manifest.Counts.Settings != 2 {
			return fmt.Errorf("diagnostic export incomplete or contains plaintext")
		}
		fmt.Println("REAL_JSONL_EXPORT_SCAN_PASS")
	default:
		return fmt.Errorf("unknown fixture mode %q", mode)
	}
	return nil
}

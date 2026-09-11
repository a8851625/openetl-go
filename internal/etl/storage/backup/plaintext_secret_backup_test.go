package backup_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func resolverKey(n byte) string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = n
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// TestBackupProductContainsNoPlaintextDSN is the IT-3 spec acceptance #4
// guard: a connection saved with a descriptor-declared secret dsn must not
// leak that dsn into the portable backup product, whether the export runs
// through the wrapped store or the raw backend.
func TestBackupProductContainsNoPlaintextDSN(t *testing.T) {
	ctx := context.Background()
	raw, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	cipher, err := storage.NewSpecCipher("k1", resolverKey(3), "")
	if err != nil {
		t.Fatalf("NewSpecCipher: %v", err)
	}
	wrapped := storage.NewSecretFieldStore(raw, cipher)
	sfs, ok := wrapped.(*storage.SecretFieldStore)
	if !ok {
		t.Fatalf("expected *SecretFieldStore")
	}
	sfs.WithSecretFieldResolver(func(kind, typ, field string) bool {
		return kind == "sink" && typ == "jdbc" && field == "dsn"
	})

	const dsn = "mysql://backup:topsecret@tcp(10.9.8.7:3306)/warehouse"
	conn := &storage.ConnectionEntry{
		Name: "jdbc-backup",
		Kind: "sink",
		Type: "jdbc",
		Config: map[string]any{
			"dsn":  dsn,
			"host": "10.9.8.7",
		},
	}
	if err := wrapped.SaveConnection(ctx, conn); err != nil {
		t.Fatalf("save connection: %v", err)
	}

	snap, err := backup.Export(ctx, wrapped, backup.Options{Backend: "sqlite"})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	blob, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if strings.Contains(string(blob), "topsecret") {
		t.Fatalf("plaintext dsn leaked into backup product: %.200s", blob)
	}
	// The host (non-secret) may appear; the credential may not.
	if !strings.Contains(string(blob), "enc:v1:") {
		t.Fatalf("expected sealed envelope in backup product: %.200s", blob)
	}

	// Restore into a second store (also wrapped) and confirm the credential
	// round-trips: sealed on disk, decrypted through the runtime view, with no
	// double encryption.
	raw2, err := sqlite.New(filepath.Join(t.TempDir(), "restore.db"))
	if err != nil {
		t.Fatalf("sqlite.New restore: %v", err)
	}
	t.Cleanup(func() { _ = raw2.Close() })
	wrapped2 := storage.NewSecretFieldStore(raw2, cipher)
	sfs2, ok := wrapped2.(*storage.SecretFieldStore)
	if !ok {
		t.Fatalf("expected *SecretFieldStore (restore)")
	}
	sfs2.WithSecretFieldResolver(func(kind, typ, field string) bool {
		return kind == "sink" && typ == "jdbc" && field == "dsn"
	})
	if err := backup.Restore(ctx, wrapped2, snap, backup.Options{ClearBeforeRestore: true}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := wrapped2.GetConnection(ctx, "jdbc-backup")
	if err != nil || got == nil {
		t.Fatalf("get restored connection: %v %v", got, err)
	}
	if got.Config["dsn"] != dsn {
		t.Fatalf("dsn did not round-trip: %v", got.Config["dsn"])
	}
	// No double encryption: exactly one envelope prefix per secret field.
	var rawCfg string
	if err := raw2.DB().QueryRow(`SELECT config_json FROM connections WHERE name=?`, "jdbc-backup").Scan(&rawCfg); err != nil {
		t.Fatalf("raw query: %v", err)
	}
	if strings.Count(rawCfg, "enc:v1:") != 1 {
		t.Fatalf("expected exactly one envelope, got: %s", rawCfg)
	}
}

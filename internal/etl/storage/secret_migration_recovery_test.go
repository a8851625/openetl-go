package storage_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

type interruptedSecretStorage struct {
	storage.Storage
	writes int
}

func (s *interruptedSecretStorage) SaveConnection(ctx context.Context, c *storage.ConnectionEntry) error {
	s.writes++
	if s.writes == 2 {
		return errors.New("simulated interrupted migration")
	}
	return s.Storage.SaveConnection(ctx, c)
}

func TestPlaintextMigrationRequiresKeyAndResumes(t *testing.T) {
	ctx := context.Background()
	_, _, raw := openSecretFieldStore(t, "old", testSecretKey(1), "")
	for i := 0; i < 3; i++ {
		if err := raw.SaveConnection(ctx, &storage.ConnectionEntry{Name: fmt.Sprintf("legacy-%d", i), Kind: "source", Type: "mysql", Config: map[string]any{"password": "migration-secret-marker"}}); err != nil {
			t.Fatal(err)
		}
	}
	withoutKey := storage.NewSecretFieldStore(raw, nil).(*storage.SecretFieldStore)
	if _, err := withoutKey.RemediatePlaintextSecrets(ctx); !errors.Is(err, storage.ErrSpecEncryptionKeyUnavailable) {
		t.Fatalf("migration without key returned %v", err)
	}
	cipher, err := storage.NewSpecCipher("new", testSecretKey(2), "")
	if err != nil {
		t.Fatal(err)
	}
	failing := &interruptedSecretStorage{Storage: raw}
	s := storage.NewSecretFieldStore(failing, cipher).(*storage.SecretFieldStore)
	if _, err := s.RemediatePlaintextSecrets(ctx); err == nil {
		t.Fatal("interrupted migration reported success")
	}
	report, err := s.DetectPlaintextSecrets(ctx)
	if err != nil || len(report.Connections) != 2 {
		t.Fatalf("partial migration=%+v err=%v", report, err)
	}
	if _, err := s.RemediatePlaintextSecrets(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := raw.ListConnections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := json.Marshal(before)
	if strings.Contains(string(first), "migration-secret-marker") {
		t.Fatal("migration left plaintext")
	}
	if _, err := s.RemediatePlaintextSecrets(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := raw.ListConnections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := json.Marshal(after)
	if string(first) != string(second) {
		t.Fatal("idempotent rerun changed sealed rows")
	}
	for _, c := range after {
		read, err := s.GetConnection(ctx, c.Name)
		if err != nil || read.Config["password"] != "migration-secret-marker" {
			t.Fatalf("value changed after resumed migration: %v", err)
		}
	}
}

func TestSecretScanReportDoesNotEchoCredential(t *testing.T) {
	const secret = "sensitive-test-scan-needle"
	report := storage.ScanPlaintextSecrets("data: "+secret, []string{secret})
	if report.OK || report.PlaintextHits != 1 {
		t.Fatal("missing plaintext hit")
	}
	blob, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), secret) {
		t.Fatal("scanner leaked the credential into its report")
	}
}

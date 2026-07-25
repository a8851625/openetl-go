package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestDevelopmentProfileRemainsCompatibleWithoutProductionSecrets(t *testing.T) {
	t.Setenv("ETL_PROFILE", "development")
	t.Setenv("ETL_INSECURE_DEV", "false")
	t.Setenv("ETL_API_TOKEN", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")
	t.Setenv("ETL_TLS_CERT", "")
	t.Setenv("ETL_TLS_KEY", "")
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := NewServer(store, t.TempDir()); err != nil {
		t.Fatalf("development profile rejected legacy defaults: %v", err)
	}
}

func TestProductionProfileFailsClosedWithoutRequiredSecrets(t *testing.T) {
	t.Setenv("ETL_PROFILE", "production")
	t.Setenv("ETL_INSECURE_DEV", "false")
	t.Setenv("ETL_API_TOKEN", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")
	t.Setenv("ETL_TLS_CERT", "")
	t.Setenv("ETL_TLS_KEY", "")
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := NewServer(store, t.TempDir()); err == nil {
		t.Fatal("production profile started without required secrets")
	} else {
		for _, want := range []string{"ETL_API_TOKEN", "ETL_SPEC_ENCRYPTION_KEY", "ETL_TLS_CERT", "ETL_TLS_KEY", "ETL_INSECURE_DEV"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("production error %q missing %q", err, want)
			}
		}
	}
}

func TestProductionProfileExplicitInsecureDevelopmentBypassIsDiagnosable(t *testing.T) {
	t.Setenv("ETL_PROFILE", "production")
	t.Setenv("ETL_INSECURE_DEV", "true")
	t.Setenv("ETL_API_TOKEN", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")
	t.Setenv("ETL_TLS_CERT", "")
	t.Setenv("ETL_TLS_KEY", "")
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg, err := ValidateRuntimeProfile(context.Background(), "standalone")
	if err != nil {
		t.Fatalf("explicit insecure bypass rejected: %v", err)
	}
	if !cfg.InsecureDevelopment || cfg.Name != RuntimeProfileProduction {
		t.Fatalf("profile config = %+v, want production + insecure flag", cfg)
	}
	if _, err := NewServer(store, t.TempDir()); err != nil {
		t.Fatalf("NewServer with explicit bypass: %v", err)
	}
}

func TestProductionProfileAcceptsCompletePinnedRuntimeSecrets(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeTestTLSKeyPair(t, dir)
	t.Setenv("ETL_PROFILE", "production")
	t.Setenv("ETL_INSECURE_DEV", "false")
	t.Setenv("ETL_API_TOKEN", "production-token-0123456789")
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	t.Setenv("ETL_SPEC_ENCRYPTION_KEY_ID", "primary")
	t.Setenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", "")
	t.Setenv("ETL_TLS_CERT", certPath)
	t.Setenv("ETL_TLS_KEY", keyPath)
	t.Setenv("ETL_AUDIT_ENABLED", "true")
	store, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cfg, err := ValidateRuntimeProfile(context.Background(), "standalone")
	if err != nil {
		t.Fatalf("complete production profile rejected: %v", err)
	}
	if cfg.Name != RuntimeProfileProduction || cfg.APIToken == "" {
		t.Fatalf("profile config = %+v", cfg)
	}
	if _, err := NewServer(store, filepath.Join(dir, "pipes")); err != nil {
		t.Fatalf("NewServer complete production profile: %v", err)
	}
}

func writeTestTLSKeyPair(t *testing.T, dir string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

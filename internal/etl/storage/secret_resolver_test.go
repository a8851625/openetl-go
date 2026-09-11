package storage_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// TestDescriptorResolverIsSecretSourceOfTruth verifies the IT-3/T3.1 contract:
// a field marked secret only by descriptor (dsn — not in the legacy pattern
// list) is encrypted when a resolver is present, and the legacy pattern path
// alone leaves it plaintext (proving the resolver is the source of truth, not
// the pattern list).
func TestDescriptorResolverIsSecretSourceOfTruth(t *testing.T) {
	ctx := context.Background()
	wrapped, sf, raw := openSecretFieldStore(t, "k1", testSecretKey(1), "")


	conn := &storage.ConnectionEntry{
		Name: "jdbc-prod",
		Kind: "sink",
		Type: "jdbc",
		Config: map[string]any{
			"dsn":      "mysql://admin:pass@tcp(10.0.0.1:3306)/db",
			"password": "separate-secret",
		},
	}
	if err := wrapped.SaveConnection(ctx, conn); err != nil {
		t.Fatalf("save connection: %v", err)
	}
	rawCfg := rawConnectionConfigJSON(t, raw, "jdbc-prod")
	// Without a resolver the pattern list does not know dsn: the credential
	// stays plaintext. This is the exact gap IT-3/T3.1 closes.
	if !strings.Contains(rawCfg, "mysql://admin:pass@") {
		t.Fatalf("expected plaintext dsn without resolver (pattern list must not silently gain dsn): %s", rawCfg)
	}

	// Now attach a descriptor resolver that knows sink/jdbc has secret dsn.
	sf.WithSecretFieldResolver(func(kind, typ, field string) bool {
		if kind == "sink" && typ == "jdbc" && field == "dsn" {
			return true
		}
		return false
	})
	if err := wrapped.SaveConnection(ctx, conn); err != nil {
		t.Fatalf("re-save connection: %v", err)
	}
	rawCfg = rawConnectionConfigJSON(t, raw, "jdbc-prod")
	if strings.Contains(rawCfg, "mysql://admin:pass@") {
		t.Fatalf("descriptor-declared dsn stayed plaintext after resolver: %s", rawCfg)
	}
	if !strings.Contains(rawCfg, "enc:v1:") {
		t.Fatalf("expected envelope marker in stored config: %s", rawCfg)
	}

	// Decryption still returns usable values for runtime callers.
	got, err := wrapped.GetConnection(ctx, "jdbc-prod")
	if err != nil {
		t.Fatalf("get connection: %v", err)
	}
	if got.Config["dsn"] != "mysql://admin:pass@tcp(10.0.0.1:3306)/db" {
		t.Fatalf("decrypted dsn mismatch: %v", got.Config["dsn"])
	}
}

// TestResolverFalseNegativeIsNotReintroducedByPatterns ensures a field that the
// descriptor resolver says is NOT secret is not encrypted by pattern fallback:
// the resolver wins in both directions.
func TestResolverFalseNegativeIsNotReintroducedByPatterns(t *testing.T) {
	ctx := context.Background()
	wrapped, sf, raw := openSecretFieldStore(t, "k1", testSecretKey(1), "")
	sf.WithSecretFieldResolver(func(kind, typ, field string) bool {
		// Descriptor says sink/jdbc only has dsn as secret.
		return kind == "sink" && typ == "jdbc" && field == "dsn"
	})
	conn := &storage.ConnectionEntry{
		Name: "jdbc-nonsecret",
		Kind: "sink",
		Type: "jdbc",
		Config: map[string]any{
			"dsn":        "mysql://u:p@tcp(h)/db",
			"table_name": "orders_password_like_named_but_not_secret",
		},
	}
	if err := wrapped.SaveConnection(ctx, conn); err != nil {
		t.Fatalf("save: %v", err)
	}
	rawCfg := rawConnectionConfigJSON(t, raw, "jdbc-nonsecret")
	if !strings.Contains(rawCfg, "orders_password_like_named_but_not_secret") {
		t.Fatalf("non-secret field was encrypted despite resolver: %s", rawCfg)
	}
	if !strings.Contains(rawCfg, "enc:v1:") {
		t.Fatalf("secret dsn not encrypted: %s", rawCfg)
	}
}

// TestFallbackSecretWritesCounterIsObservable verifies the fallback counter
// used for WARN visibility increments only when pattern matching decides
// without a resolver.
func TestFallbackSecretWritesCounterIsObservable(t *testing.T) {
	ctx := context.Background()
	wrapped, _, _ := openSecretFieldStore(t, "k1", testSecretKey(1), "")
	before := atomic.LoadInt64(&storage.FallbackSecretWrites)

	conn := &storage.ConnectionEntry{
		Name: "legacy-pattern",
		Kind: "source",
		Type: "custom",
		Config: map[string]any{
			"api_token": "tok-123", // matches pattern without descriptor
		},
	}
	if err := wrapped.SaveConnection(ctx, conn); err != nil {
		t.Fatalf("save: %v", err)
	}
	if after := atomic.LoadInt64(&storage.FallbackSecretWrites); after <= before {
		t.Fatalf("fallback counter not incremented: before=%d after=%d", before, after)
	}
}

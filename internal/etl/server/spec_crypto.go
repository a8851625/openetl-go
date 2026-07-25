package server

import (
	"context"
	"fmt"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// newEncryptedSpecStore is the single construction path for pipeline spec
// persistence used by the control plane. The underlying storage adapter owns
// encryption/decryption so every current/version read follows the same rules.
func newEncryptedSpecStore(store storage.Storage) (*storage.PipelineSpecStore, error) {
	cipher, err := storage.NewSpecCipherFromEnv()
	if err != nil {
		return nil, fmt.Errorf("initialize pipeline spec encryption: %w", err)
	}
	if !cipher.Enabled() {
		g.Log().Warningf(nil,
			"ETL_SPEC_ENCRYPTION_KEY is not set — new pipeline specs and connection/settings secrets will be stored in plaintext; encrypted rows will fail to load until their key is configured")
	}
	return storage.NewPipelineSpecStore(store, cipher), nil
}

// wrapSecretFieldStore applies field-level encryption for connection catalog
// and settings secrets using the same key material as pipeline specs.
func wrapSecretFieldStore(store storage.Storage) (storage.Storage, error) {
	cipher, err := storage.NewSpecCipherFromEnv()
	if err != nil {
		return nil, fmt.Errorf("initialize secret field encryption: %w", err)
	}
	return storage.NewSecretFieldStore(store, cipher), nil
}

// GenerateEncryptionKey generates a random base64-encoded 32-byte AES key for
// ETL_SPEC_ENCRYPTION_KEY.
func GenerateEncryptionKey() (string, error) {
	return storage.GenerateSpecEncryptionKey()
}

// The helpers below retain source compatibility for older package-local
// callers. Runtime code uses storage.PipelineSpecStore directly, so crypto
// failures are returned rather than converted into YAML parse warnings.
func encryptSpec(plaintext string) string {
	cipher, err := storage.NewSpecCipherFromEnv()
	if err != nil {
		return plaintext
	}
	encoded, err := cipher.Encrypt(plaintext)
	if err != nil {
		return plaintext
	}
	return encoded
}

func decryptSpec(stored string) string {
	cipher, err := storage.NewSpecCipherFromEnv()
	if err != nil {
		return stored
	}
	plaintext, err := cipher.Decrypt(stored)
	if err != nil {
		return stored
	}
	return plaintext
}

// EncryptedSpecStore is retained as a compatibility wrapper for package-local
// integrations written before the storage-level adapter was introduced.
type EncryptedSpecStore struct {
	inner *storage.PipelineSpecStore
}

func NewEncryptedSpecStore(inner *storage.PipelineSpecStore) *EncryptedSpecStore {
	if inner != nil {
		if cipher, err := storage.NewSpecCipherFromEnv(); err == nil {
			inner = inner.WithCipher(cipher)
		}
	}
	return &EncryptedSpecStore{inner: inner}
}

func (e *EncryptedSpecStore) Save(ctx context.Context, name, specYAML, status string) error {
	return e.inner.Save(ctx, name, specYAML, status)
}

func (e *EncryptedSpecStore) SaveWithID(ctx context.Context, id, name, specYAML, status string) error {
	return e.inner.SaveWithID(ctx, id, name, specYAML, status)
}

func (e *EncryptedSpecStore) Get(ctx context.Context, name string) (string, error) {
	return e.inner.Get(ctx, name)
}

func (e *EncryptedSpecStore) List(ctx context.Context) ([]*storage.PipelineRow, error) {
	return e.inner.List(ctx)
}

func (e *EncryptedSpecStore) Delete(ctx context.Context, name string) error {
	return e.inner.Delete(ctx, name)
}

func (e *EncryptedSpecStore) Versions(ctx context.Context, name string) ([]*storage.PipelineVersion, error) {
	return e.inner.Versions(ctx, name)
}

package server

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/storage"
)

// specEncryption provides AES-256-GCM encryption for sensitive spec data at rest.
// The encryption key is read from ETL_SPEC_ENCRYPTION_KEY (base64-encoded 32-byte key).
// If the key is not set, encryption is disabled (plaintext storage — with a warning).
var (
	encKey      []byte
	encKeyOnce  sync.Once
	encDisabled bool
)

func initEncryptionKey() {
	encKeyOnce.Do(func() {
		keyB64 := os.Getenv("ETL_SPEC_ENCRYPTION_KEY")
		if keyB64 == "" {
			encDisabled = true
			g.Log().Warningf(nil,
				"ETL_SPEC_ENCRYPTION_KEY is not set — pipeline specs (including credentials) "+
					"will be stored in plaintext. Set this for production deployments.")
			return
		}
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if err != nil || len(key) != 32 {
			g.Log().Errorf(nil, "ETL_SPEC_ENCRYPTION_KEY must be a base64-encoded 32-byte key — encryption disabled")
			encDisabled = true
			return
		}
		encKey = key
	})
}

// encryptSpec encrypts plaintext spec YAML using AES-256-GCM.
// Returns the plaintext unchanged if encryption is disabled.
func encryptSpec(plaintext string) string {
	initEncryptionKey()
	if encDisabled || plaintext == "" {
		return plaintext
	}

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return plaintext
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return plaintext
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return plaintext
	}

	// Prefix "enc:" to mark encrypted content
	encrypted := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return "enc:" + base64.StdEncoding.EncodeToString(encrypted)
}

// decryptSpec decrypts an AES-256-GCM encrypted spec.
// Returns the input unchanged if it's not encrypted (no "enc:" prefix).
func decryptSpec(stored string) string {
	initEncryptionKey()
	if stored == "" || len(stored) < 4 || stored[:4] != "enc:" {
		return stored // not encrypted
	}

	if encDisabled {
		return stored // can't decrypt — key missing
	}

	data, err := base64.StdEncoding.DecodeString(stored[4:])
	if err != nil {
		return stored
	}

	block, err := aes.NewCipher(encKey)
	if err != nil {
		return stored
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return stored
	}

	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return stored
	}

	plaintext, err := gcm.Open(nil, data[:nonceSize], data[nonceSize:], nil)
	if err != nil {
		return stored
	}

	return string(plaintext)
}

// GenerateEncryptionKey generates a random 32-byte AES key encoded as base64.
// Useful for operators to generate a key for ETL_SPEC_ENCRYPTION_KEY.
func GenerateEncryptionKey() (string, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", errors.New("generate key: " + err.Error())
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

// EncryptedSpecStore wraps PipelineSpecStore with AES-256-GCM encryption
// for spec YAML at rest. When encryption is disabled (no key set), it
// passes through unchanged.
type EncryptedSpecStore struct {
	inner *storage.PipelineSpecStore
}

func NewEncryptedSpecStore(inner *storage.PipelineSpecStore) *EncryptedSpecStore {
	return &EncryptedSpecStore{inner: inner}
}

func (e *EncryptedSpecStore) Save(ctx context.Context, name, specYAML, status string) error {
	return e.inner.Save(ctx, name, encryptSpec(specYAML), status)
}

func (e *EncryptedSpecStore) Get(ctx context.Context, name string) (string, error) {
	yaml, err := e.inner.Get(ctx, name)
	if err != nil {
		return "", err
	}
	return decryptSpec(yaml), nil
}

func (e *EncryptedSpecStore) List(ctx context.Context) ([]*storage.PipelineRow, error) {
	return e.inner.List(ctx)
}

func (e *EncryptedSpecStore) Delete(ctx context.Context, name string) error {
	return e.inner.Delete(ctx, name)
}

func (e *EncryptedSpecStore) Versions(ctx context.Context, name string) ([]*storage.PipelineVersion, error) {
	versions, err := e.inner.Versions(ctx, name)
	if err != nil {
		return nil, err
	}
	// Decrypt each version's spec_yaml in place
	for _, v := range versions {
		v.SpecYAML = decryptSpec(v.SpecYAML)
	}
	return versions, nil
}

package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	specEnvelopePrefix         = "enc:"
	specEnvelopeVersion        = "v1"
	defaultSpecEncryptionKeyID = "default"
)

var (
	ErrSpecEncryptionKeyUnavailable = errors.New("pipeline spec encryption key unavailable")
	ErrSpecEncryptionAuthFailed     = errors.New("pipeline spec ciphertext authentication failed")
	ErrSpecEncryptionMalformed      = errors.New("pipeline spec ciphertext is malformed")
	ErrSpecEncryptionVersion        = errors.New("pipeline spec encryption envelope version is unsupported")
)

// SpecCipher encrypts and decrypts pipeline specs stored in SQL metadata.
//
// New writes use enc:v1:<key-id>:<base64(nonce+ciphertext)>. The legacy
// enc:<base64(nonce+ciphertext)> format remains readable so existing
// installations can rotate without rewriting every row before restart.
type SpecCipher struct {
	currentKeyID string
	currentKey   []byte
	keys         map[string][]byte
}

// NewSpecCipherFromEnv loads the current key plus optional previous keys.
//
//   - ETL_SPEC_ENCRYPTION_KEY: base64-encoded 32-byte current AES key
//   - ETL_SPEC_ENCRYPTION_KEY_ID: current key ID (default: "default")
//   - ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS: JSON object or comma-separated
//     key-id=base64 entries used only for decryption during rotation
func NewSpecCipherFromEnv() (*SpecCipher, error) {
	keyID := strings.TrimSpace(os.Getenv("ETL_SPEC_ENCRYPTION_KEY_ID"))
	key := strings.TrimSpace(os.Getenv("ETL_SPEC_ENCRYPTION_KEY"))
	if keyID != "" && key == "" {
		return nil, fmt.Errorf("ETL_SPEC_ENCRYPTION_KEY_ID is set but ETL_SPEC_ENCRYPTION_KEY is missing")
	}
	return NewSpecCipher(
		keyID,
		key,
		strings.TrimSpace(os.Getenv("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS")),
	)
}

// NewSpecCipher builds a spec cipher from explicit configuration. An empty
// current key keeps development/legacy plaintext writes enabled; encrypted
// rows still fail closed when read without their key.
func NewSpecCipher(currentKeyID, currentKeyB64, previousKeys string) (*SpecCipher, error) {
	if currentKeyID == "" {
		currentKeyID = defaultSpecEncryptionKeyID
	}
	if err := validateSpecKeyID(currentKeyID); err != nil {
		return nil, err
	}

	c := &SpecCipher{currentKeyID: currentKeyID, keys: make(map[string][]byte)}
	if currentKeyB64 == "" {
		if previousKeys != "" {
			return nil, fmt.Errorf("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS requires ETL_SPEC_ENCRYPTION_KEY")
		}
		return c, nil
	}

	key, err := decodeSpecKey("ETL_SPEC_ENCRYPTION_KEY", currentKeyB64)
	if err != nil {
		return nil, err
	}
	c.currentKey = key
	c.keys[currentKeyID] = key

	previous, err := parsePreviousSpecKeys(previousKeys)
	if err != nil {
		return nil, err
	}
	for keyID, previousKey := range previous {
		if existing, ok := c.keys[keyID]; ok && !equalBytes(existing, previousKey) {
			return nil, fmt.Errorf("spec encryption key ID %q is configured with multiple values", keyID)
		}
		c.keys[keyID] = previousKey
	}
	return c, nil
}

func (c *SpecCipher) Enabled() bool {
	return c != nil && len(c.currentKey) == 32
}

func (c *SpecCipher) CurrentKeyID() string {
	if c == nil {
		return ""
	}
	return c.currentKeyID
}

func (c *SpecCipher) Encrypt(plaintext string) (string, error) {
	if plaintext == "" || !c.Enabled() {
		return plaintext, nil
	}
	payload, err := encryptSpecPayload(c.currentKey, []byte(plaintext))
	if err != nil {
		return "", fmt.Errorf("encrypt pipeline spec: %w", err)
	}
	return strings.Join([]string{
		"enc",
		specEnvelopeVersion,
		c.currentKeyID,
		base64.StdEncoding.EncodeToString(payload),
	}, ":"), nil
}

func (c *SpecCipher) Decrypt(stored string) (string, error) {
	if stored == "" || !strings.HasPrefix(stored, specEnvelopePrefix) {
		return stored, nil
	}

	rest := strings.TrimPrefix(stored, specEnvelopePrefix)
	// Legacy payloads are just base64 and may coincidentally begin with the
	// letter "v". Treat a value as a structured envelope only when the first
	// separator is present.
	if !strings.Contains(rest, ":") {
		return c.decryptLegacy(rest)
	}

	parts := strings.SplitN(rest, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", fmt.Errorf("%w; restore an intact metadata backup", ErrSpecEncryptionMalformed)
	}
	if parts[0] != specEnvelopeVersion {
		return "", fmt.Errorf("%w: %q; upgrade OpenETL-Go or restore data written by a supported version", ErrSpecEncryptionVersion, parts[0])
	}
	keyID := parts[1]
	key, ok := c.keys[keyID]
	if !ok {
		return "", fmt.Errorf("%w for key ID %q; set ETL_SPEC_ENCRYPTION_KEY with the matching ETL_SPEC_ENCRYPTION_KEY_ID or add the old key to ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS", ErrSpecEncryptionKeyUnavailable, keyID)
	}
	payload, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("%w; restore an intact metadata backup", ErrSpecEncryptionMalformed)
	}
	plaintext, err := decryptSpecPayload(key, payload)
	if err != nil {
		if errors.Is(err, ErrSpecEncryptionMalformed) {
			return "", fmt.Errorf("%w; restore an intact metadata backup", ErrSpecEncryptionMalformed)
		}
		return "", fmt.Errorf("%w for key ID %q; verify the configured key or restore an intact metadata backup", ErrSpecEncryptionAuthFailed, keyID)
	}
	return string(plaintext), nil
}

func (c *SpecCipher) decryptLegacy(encoded string) (string, error) {
	payload, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("%w; restore an intact metadata backup", ErrSpecEncryptionMalformed)
	}
	if c == nil || len(c.keys) == 0 {
		return "", fmt.Errorf("%w for legacy ciphertext; set ETL_SPEC_ENCRYPTION_KEY or ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS with the original key", ErrSpecEncryptionKeyUnavailable)
	}

	keyIDs := make([]string, 0, len(c.keys))
	if c.currentKeyID != "" {
		if _, ok := c.keys[c.currentKeyID]; ok {
			keyIDs = append(keyIDs, c.currentKeyID)
		}
	}
	for keyID := range c.keys {
		if keyID != c.currentKeyID {
			keyIDs = append(keyIDs, keyID)
		}
	}
	if len(keyIDs) > 1 {
		sort.Strings(keyIDs[1:])
	}
	for _, keyID := range keyIDs {
		plaintext, decryptErr := decryptSpecPayload(c.keys[keyID], payload)
		if decryptErr == nil {
			return string(plaintext), nil
		}
		if errors.Is(decryptErr, ErrSpecEncryptionMalformed) {
			return "", fmt.Errorf("%w; restore an intact metadata backup", ErrSpecEncryptionMalformed)
		}
	}
	return "", fmt.Errorf("%w for legacy ciphertext; verify the configured current/previous keys or restore an intact metadata backup", ErrSpecEncryptionAuthFailed)
}

func GenerateSpecEncryptionKey() (string, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", fmt.Errorf("generate spec encryption key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

func encryptSpecPayload(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func decryptSpecPayload(key, payload []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(payload) < gcm.NonceSize() {
		return nil, ErrSpecEncryptionMalformed
	}
	return gcm.Open(nil, payload[:gcm.NonceSize()], payload[gcm.NonceSize():], nil)
}

func parsePreviousSpecKeys(raw string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	if raw == "" {
		return result, nil
	}

	encoded := make(map[string]string)
	if strings.HasPrefix(strings.TrimSpace(raw), "{") {
		if err := json.Unmarshal([]byte(raw), &encoded); err != nil {
			return nil, fmt.Errorf("parse ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS JSON: %w", err)
		}
	} else {
		for _, item := range strings.Split(raw, ",") {
			parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
			if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
				return nil, fmt.Errorf("parse ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS: expected key-id=base64 entries")
			}
			encoded[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	for keyID, value := range encoded {
		if err := validateSpecKeyID(keyID); err != nil {
			return nil, err
		}
		key, err := decodeSpecKey("ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS["+keyID+"]", value)
		if err != nil {
			return nil, err
		}
		result[keyID] = key
	}
	return result, nil
}

func decodeSpecKey(source, encoded string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("%s must be a base64-encoded 32-byte key", source)
	}
	return key, nil
}

func validateSpecKeyID(keyID string) error {
	if keyID == "" {
		return fmt.Errorf("spec encryption key ID must not be empty")
	}
	for _, r := range keyID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("spec encryption key ID %q may contain only letters, digits, '.', '_' or '-'", keyID)
	}
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var different byte
	for i := range a {
		different |= a[i] ^ b[i]
	}
	return different == 0
}

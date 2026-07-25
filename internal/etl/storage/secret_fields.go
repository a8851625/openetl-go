package storage

import (
	"fmt"
	"strings"
)

// secretFieldPatterns match config/setting keys that must be encrypted at rest.
// Keep this list aligned with API masking in the control plane.
var secretFieldPatterns = []string{
	"password", "passwd", "secret", "token", "api_key", "apikey", "credential", "private_key",
}

// IsSecretFieldKey reports whether a config or settings key looks like a secret.
func IsSecretFieldKey(key string) bool {
	lk := strings.ToLower(strings.TrimSpace(key))
	if lk == "" {
		return false
	}
	for _, pat := range secretFieldPatterns {
		if strings.Contains(lk, pat) {
			return true
		}
	}
	return false
}

// EncryptConfigSecrets encrypts secret string fields in a connector config map.
// Non-secret values, empty strings, and already-encrypted envelopes are preserved
// as-is except that encrypted values are re-sealed with the current key on write.
func EncryptConfigSecrets(cipher *SpecCipher, cfg map[string]any) (map[string]any, error) {
	if cfg == nil {
		return nil, nil
	}
	out := make(map[string]any, len(cfg))
	for k, v := range cfg {
		nv, err := encryptConfigValue(cipher, k, v)
		if err != nil {
			return nil, err
		}
		out[k] = nv
	}
	return out, nil
}

// DecryptConfigSecrets decrypts secret string fields that use the field envelope.
// Legacy plaintext secrets remain readable for upgrade compatibility.
func DecryptConfigSecrets(cipher *SpecCipher, cfg map[string]any) (map[string]any, error) {
	if cfg == nil {
		return nil, nil
	}
	out := make(map[string]any, len(cfg))
	for k, v := range cfg {
		nv, err := decryptConfigValue(cipher, k, v)
		if err != nil {
			return nil, err
		}
		out[k] = nv
	}
	return out, nil
}

// EncryptSettingValue encrypts a settings value when the key is secret-bearing.
func EncryptSettingValue(cipher *SpecCipher, key, value string) (string, error) {
	if value == "" || !IsSecretFieldKey(key) || cipher == nil || !cipher.Enabled() {
		return value, nil
	}
	// Re-encrypt so rotation always progresses on write.
	plain := value
	if strings.HasPrefix(value, specEnvelopePrefix) {
		decoded, err := cipher.Decrypt(value)
		if err != nil {
			return "", fmt.Errorf("re-encrypt setting %q: %w", key, err)
		}
		plain = decoded
	}
	return cipher.Encrypt(plain)
}

// DecryptSettingValue decrypts a settings value when it is an envelope.
func DecryptSettingValue(cipher *SpecCipher, key, value string) (string, error) {
	if value == "" || !strings.HasPrefix(value, specEnvelopePrefix) {
		return value, nil
	}
	if cipher == nil {
		return "", fmt.Errorf("%w for setting %q; set ETL_SPEC_ENCRYPTION_KEY (or previous keys) before reading encrypted settings", ErrSpecEncryptionKeyUnavailable, key)
	}
	plain, err := cipher.Decrypt(value)
	if err != nil {
		return "", fmt.Errorf("decrypt setting %q: %w", key, err)
	}
	return plain, nil
}

func encryptConfigValue(cipher *SpecCipher, key string, v any) (any, error) {
	switch vv := v.(type) {
	case map[string]any:
		return EncryptConfigSecrets(cipher, vv)
	case []any:
		items := make([]any, len(vv))
		for i, item := range vv {
			if m, ok := item.(map[string]any); ok {
				enc, err := EncryptConfigSecrets(cipher, m)
				if err != nil {
					return nil, err
				}
				items[i] = enc
			} else {
				items[i] = item
			}
		}
		return items, nil
	case string:
		if !IsSecretFieldKey(key) || vv == "" || cipher == nil || !cipher.Enabled() {
			return vv, nil
		}
		plain := vv
		if strings.HasPrefix(vv, specEnvelopePrefix) {
			decoded, err := cipher.Decrypt(vv)
			if err != nil {
				return nil, fmt.Errorf("re-encrypt connection secret field %q: %w", key, err)
			}
			plain = decoded
		}
		return cipher.Encrypt(plain)
	default:
		return v, nil
	}
}

func decryptConfigValue(cipher *SpecCipher, key string, v any) (any, error) {
	switch vv := v.(type) {
	case map[string]any:
		return DecryptConfigSecrets(cipher, vv)
	case []any:
		items := make([]any, len(vv))
		for i, item := range vv {
			if m, ok := item.(map[string]any); ok {
				dec, err := DecryptConfigSecrets(cipher, m)
				if err != nil {
					return nil, err
				}
				items[i] = dec
			} else {
				items[i] = item
			}
		}
		return items, nil
	case string:
		if vv == "" || !strings.HasPrefix(vv, specEnvelopePrefix) {
			return vv, nil
		}
		if cipher == nil {
			return nil, fmt.Errorf("%w for connection secret field %q; set ETL_SPEC_ENCRYPTION_KEY (or previous keys) before reading encrypted connections", ErrSpecEncryptionKeyUnavailable, key)
		}
		plain, err := cipher.Decrypt(vv)
		if err != nil {
			return nil, fmt.Errorf("decrypt connection secret field %q: %w", key, err)
		}
		return plain, nil
	default:
		return v, nil
	}
}

// ConfigContainsPlaintextSecret reports whether any secret field still holds
// the exact plaintext needle. Used by dump/scanner tests.
func ConfigContainsPlaintextSecret(cfg map[string]any, needle string) bool {
	if needle == "" || cfg == nil {
		return false
	}
	for k, v := range cfg {
		switch vv := v.(type) {
		case map[string]any:
			if ConfigContainsPlaintextSecret(vv, needle) {
				return true
			}
		case []any:
			for _, item := range vv {
				if m, ok := item.(map[string]any); ok && ConfigContainsPlaintextSecret(m, needle) {
					return true
				}
			}
		case string:
			if IsSecretFieldKey(k) && vv == needle {
				return true
			}
		}
	}
	return false
}

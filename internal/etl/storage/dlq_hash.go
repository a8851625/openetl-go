package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// RecordHash returns a stable content hash for a DLQ record payload.
func RecordHash(record any) (string, error) {
	b, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	return RecordHashJSON(b), nil
}

// RecordHashJSON hashes an already-marshaled record payload.
func RecordHashJSON(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/a8851625/openetl-go/internal/etl/server"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func runSecretMaintenance(ctx context.Context, raw storage.Storage, remediate bool, out io.Writer) error {
	cipher, err := storage.NewSpecCipherFromEnv()
	if err != nil {
		return err
	}
	s := storage.NewSecretFieldStore(raw, cipher).(*storage.SecretFieldStore)
	s.WithSecretFieldResolver(server.NewDescriptorSecretFieldResolver())
	var report *storage.PlaintextSecretReport
	if remediate {
		report, err = s.RemediatePlaintextSecrets(ctx)
	} else {
		report, err = s.DetectPlaintextSecrets(ctx)
	}
	if err != nil {
		return err
	}
	// Reports contain identifiers and field names, never credential values.
	if err := json.NewEncoder(out).Encode(report); err != nil {
		return err
	}
	if remediate {
		report, err = s.DetectPlaintextSecrets(ctx)
		if err != nil {
			return err
		}
	}
	if n := len(report.Connections) + len(report.Settings); n != 0 {
		return fmt.Errorf("%d plaintext connection/settings secret fields remain; stop writers and run --remediate-secrets with ETL_SPEC_ENCRYPTION_KEY configured", n)
	}
	return nil
}

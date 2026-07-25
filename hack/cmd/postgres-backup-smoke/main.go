package main

import (
	"context"
	"fmt"
	"os"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
)

func main() {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" || len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: POSTGRES_DSN=... postgres-backup-smoke <outDir>")
		os.Exit(2)
	}
	ctx := context.Background()
	st, err := postgres.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer st.Close()
	secret := "pg-backup-secret-xyz"
	_ = st.SaveConnection(ctx, &storage.ConnectionEntry{
		Name: "probe", Kind: "source", Type: "postgres",
		Config: map[string]any{"password": secret},
	})
	if err := st.SavePipeline(ctx, &storage.PipelineRow{
		ID: "bk1", Name: "bk1", SpecYAML: "name: bk1\n", Status: "stopped",
	}); err != nil {
		panic(err)
	}
	man, err := storage.BackupSQLStore(ctx, st, os.Args[1], []string{secret})
	if err != nil {
		panic(err)
	}
	if man.Counts.Pipelines < 1 {
		panic("no pipelines")
	}
	fmt.Println("PG_BACKUP_OK", man.Path, "pipelines", man.Counts.Pipelines, "secret_hits", man.SecretScan.PlaintextHits)
}

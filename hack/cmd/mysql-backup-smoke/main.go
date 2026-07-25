package main

import (
	"context"
	"fmt"
	"os"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
)

func main() {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" || len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: MYSQL_DSN=... mysql-backup-smoke <outDir>")
		os.Exit(2)
	}
	out := os.Args[1]
	st, err := mysql.New(dsn)
	if err != nil {
		panic(err)
	}
	defer st.Close()
	ctx := context.Background()
	secret := "mysql-backup-secret-xyz"
	if err := st.SaveConnection(ctx, &storage.ConnectionEntry{
		Name: "probe", Kind: "source", Type: "mysql",
		Config: map[string]any{"password": secret},
	}); err != nil {
		fmt.Println("warn SaveConnection:", err)
	}
	if err := st.SavePipeline(ctx, &storage.PipelineRow{
		ID: "bk1", Name: "bk1", SpecYAML: "name: bk1\n", Status: "stopped",
	}); err != nil {
		panic(err)
	}
	man, err := storage.BackupSQLStore(ctx, st, out, []string{secret})
	if err != nil {
		panic(err)
	}
	if man.Counts.Pipelines < 1 {
		panic(fmt.Sprintf("pipelines=%d", man.Counts.Pipelines))
	}
	fmt.Println("MYSQL_BACKUP_OK", man.Path, "pipelines", man.Counts.Pipelines, "secret_hits", man.SecretScan.PlaintextHits)
}

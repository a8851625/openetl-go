package factory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// StoreType constants for supported storage backends.
const (
	StoreTypeSQLite = "sqlite"
	StoreTypeMySQL  = "mysql"
	StoreTypePG     = "postgresql"
)

// NewStore creates a storage.Storage implementation based on config.
// storageType can be "sqlite", "mysql", or "postgresql".
// checkpointDir and dlqDir are used for the default SQLite path and
// for fallback file migration paths.
func NewStore(ctx context.Context, storageType, checkpointDir, dlqDir string) (storage.Storage, error) {
	return newStore(ctx, storageType, checkpointDir, dlqDir, true)
}

// NewMaintenanceStore opens SQL metadata without importing legacy checkpoint
// and DLQ files. An offline backup/restore must not rename or consume those files.
func NewMaintenanceStore(ctx context.Context, storageType, checkpointDir string) (storage.Storage, error) {
	return newStore(ctx, storageType, checkpointDir, "", false)
}

func newStore(ctx context.Context, storageType, checkpointDir, dlqDir string, migrateFiles bool) (storage.Storage, error) {
	switch storageType {
	case StoreTypeSQLite, "":
		dbPath := g.Cfg().MustGet(ctx, "etl.storage.sqlite.path", "").String()
		if dbPath == "" {
			dbPath = filepath.Join(filepath.Dir(checkpointDir), "etl.db")
		}
		if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
			return nil, fmt.Errorf("create db dir: %w", err)
		}
		store, err := sqlite.New(dbPath)
		if err != nil {
			return nil, err
		}
		// Migrate existing file-based data to SQLite
		if migrateFiles {
			migrateFileData(ctx, store, checkpointDir, dlqDir)
		}
		return store, nil

	case StoreTypeMySQL:
		dsn := g.Cfg().MustGet(ctx, "etl.storage.mysql.dsn", "").String()
		if dsn == "" {
			return nil, fmt.Errorf("etl.storage.mysql.dsn is required for mysql storage")
		}
		store, err := mysql.New(dsn)
		if err != nil {
			return nil, err
		}
		if migrateFiles {
			migrateFileData(ctx, store, checkpointDir, dlqDir)
		}
		return store, nil

	case StoreTypePG:
		dsn := g.Cfg().MustGet(ctx, "etl.storage.postgresql.dsn", "").String()
		if dsn == "" {
			return nil, fmt.Errorf("etl.storage.postgresql.dsn is required for postgresql storage")
		}
		store, err := postgres.New(ctx, dsn)
		if err != nil {
			return nil, err
		}
		if migrateFiles {
			migrateFileData(ctx, store, checkpointDir, dlqDir)
		}
		return store, nil

	default:
		return nil, fmt.Errorf("unknown storage type: %s", storageType)
	}
}

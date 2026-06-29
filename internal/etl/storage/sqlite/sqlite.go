package sqlite

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/a8851625/openetl-go/internal/etl/storage/sqlstore"
)

// Store is the SQLite-backed storage implementation. All CRUD/query behavior
// lives in sqlstore.Store; this package only opens SQLite and runs SQLite DDL.
type Store struct {
	*sqlstore.Store
}

// New opens (or creates) a SQLite database at path and runs migrations.
func New(path string) (*Store, error) {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	common := sqlstore.New(db, sqlstore.SQLiteDialect{})
	if err := common.MigrateSQLite(); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{Store: common}, nil
}

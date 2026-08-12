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
//
// SQLite WAL mode allows concurrent readers that never block the single
// writer. To keep checkpoint saves (short writes on the hot path) from
// contending with API/metrics/list SELECT queries, OpenETL routes writes to a
// single-connection pool (sqlite's database-level write lock serializes writes
// anyway) and reads to a small concurrent pool. See BUG-3.
func New(path string) (*Store, error) {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// Writer: a single connection so sqlite's database-wide write lock never
	// surfaces as SQLITE_BUSY between two write transactions from this process.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Reader: a separate connection pool for SELECT queries. WAL readers take a
	// read lock that does not block the writer, so checkpoint saves are not
	// queued behind a long ListPipelines/ListAudit query.
	readDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("open sqlite read pool: %w", err)
	}
	readDB.SetMaxOpenConns(4)
	readDB.SetMaxIdleConns(2)

	common := sqlstore.New(db, sqlstore.SQLiteDialect{})
	common.SetReadDB(readDB)
	if err := common.MigrateSQLite(); err != nil {
		db.Close()
		readDB.Close()
		return nil, err
	}
	return &Store{Store: common}, nil
}

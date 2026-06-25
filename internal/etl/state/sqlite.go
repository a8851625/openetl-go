package state

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore is a durable StateStore implementation backed by SQLite.
// It is intended for standalone deployments and as the reference SQL-backed
// state store before the same contract is wired into other metadata backends.
type SQLiteStore struct {
	db       *sql.DB
	ownsDB   bool
	now      func() time.Time
	migrated bool
}

// NewSQLiteStore opens or creates a SQLite database and initializes state
// storage tables.
func NewSQLiteStore(path string) (*SQLiteStore, error) {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite state store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := NewSQLiteStoreFromDB(db)
	store.ownsDB = true
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// NewSQLiteStoreFromDB adapts an existing SQLite database handle. The caller
// remains responsible for closing db.
func NewSQLiteStoreFromDB(db *sql.DB) *SQLiteStore {
	return &SQLiteStore{db: db, now: time.Now}
}

func (s *SQLiteStore) migrate(ctx context.Context) error {
	if s.migrated {
		return nil
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS etl_state_entries (
			pipeline   TEXT NOT NULL,
			node       TEXT NOT NULL,
			key        TEXT NOT NULL,
			value      BLOB NOT NULL,
			expires_at INTEGER,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (pipeline, node, key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_etl_state_expiry ON etl_state_entries(expires_at)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate sqlite state store: %w", err)
		}
	}
	s.migrated = true
	return nil
}

func (s *SQLiteStore) Get(ctx context.Context, pipeline, node, key string) ([]byte, bool, error) {
	if err := s.migrate(ctx); err != nil {
		return nil, false, err
	}
	var value []byte
	var expiresAt sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT value, expires_at FROM etl_state_entries WHERE pipeline=? AND node=? AND key=?`,
		pipeline, node, key,
	).Scan(&value, &expiresAt)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get state: %w", err)
	}
	if expiresAt.Valid && expiresAt.Int64 <= s.now().UnixNano() {
		if err := s.Delete(ctx, pipeline, node, key); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *SQLiteStore) Set(ctx context.Context, pipeline, node, key string, value []byte, ttl time.Duration) error {
	if err := s.migrate(ctx); err != nil {
		return err
	}
	now := s.now()
	var expiresAt any
	if ttl > 0 {
		expiresAt = now.Add(ttl).UnixNano()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO etl_state_entries (pipeline, node, key, value, expires_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(pipeline, node, key) DO UPDATE SET
		   value=excluded.value,
		   expires_at=excluded.expires_at,
		   updated_at=excluded.updated_at`,
		pipeline, node, key, append([]byte(nil), value...), expiresAt, now.UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("set state: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Delete(ctx context.Context, pipeline, node, key string) error {
	if err := s.migrate(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM etl_state_entries WHERE pipeline=? AND node=? AND key=?`,
		pipeline, node, key)
	if err != nil {
		return fmt.Errorf("delete state: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Snapshot(ctx context.Context, pipeline, node string) (*Snapshot, error) {
	if err := s.migrate(ctx); err != nil {
		return nil, err
	}
	now := s.now()
	if err := s.purgeExpired(ctx, pipeline, node, now); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT key, value, expires_at, updated_at
		 FROM etl_state_entries
		 WHERE pipeline=? AND node=?
		 ORDER BY key`, pipeline, node)
	if err != nil {
		return nil, fmt.Errorf("snapshot state: %w", err)
	}
	defer rows.Close()

	entries := make([]Entry, 0)
	for rows.Next() {
		entry := Entry{Pipeline: pipeline, Node: node}
		var expiresAt sql.NullInt64
		var updatedAt int64
		if err := rows.Scan(&entry.Key, &entry.Value, &expiresAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan state snapshot: %w", err)
		}
		entry.Value = append([]byte(nil), entry.Value...)
		if expiresAt.Valid {
			entry.ExpiresAt = time.Unix(0, expiresAt.Int64).UTC()
		}
		entry.UpdatedAt = time.Unix(0, updatedAt).UTC()
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate state snapshot: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return &Snapshot{
		Pipeline:  pipeline,
		Node:      node,
		Version:   fmt.Sprintf("%d", now.UnixNano()),
		Entries:   entries,
		CreatedAt: now,
	}, nil
}

func (s *SQLiteStore) Restore(ctx context.Context, snap *Snapshot) error {
	if err := s.migrate(ctx); err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin state restore: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM etl_state_entries WHERE pipeline=? AND node=?`,
		snap.Pipeline, snap.Node); err != nil {
		return fmt.Errorf("clear state before restore: %w", err)
	}
	for _, entry := range snap.Entries {
		var expiresAt any
		if !entry.ExpiresAt.IsZero() {
			expiresAt = entry.ExpiresAt.UnixNano()
		}
		updatedAt := entry.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = snap.CreatedAt
		}
		if updatedAt.IsZero() {
			updatedAt = s.now()
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO etl_state_entries (pipeline, node, key, value, expires_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			snap.Pipeline, snap.Node, entry.Key, append([]byte(nil), entry.Value...), expiresAt, updatedAt.UnixNano(),
		); err != nil {
			return fmt.Errorf("restore state entry %q: %w", entry.Key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state restore: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Stats(ctx context.Context, pipeline, node string) (Stats, error) {
	if err := s.migrate(ctx); err != nil {
		return Stats{}, err
	}
	now := s.now()
	if err := s.purgeExpired(ctx, pipeline, node, now); err != nil {
		return Stats{}, err
	}

	var stats Stats
	var updatedAt sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(length(value)), 0), MAX(updated_at)
		 FROM etl_state_entries
		 WHERE pipeline=? AND node=?`, pipeline, node,
	).Scan(&stats.Keys, &stats.Bytes, &updatedAt)
	if err != nil {
		return Stats{}, fmt.Errorf("state stats: %w", err)
	}
	if updatedAt.Valid {
		stats.UpdatedAt = time.Unix(0, updatedAt.Int64).UTC()
	}
	return stats, nil
}

func (s *SQLiteStore) Close() error {
	if s.ownsDB {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStore) purgeExpired(ctx context.Context, pipeline, node string, now time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM etl_state_entries
		 WHERE pipeline=? AND node=? AND expires_at IS NOT NULL AND expires_at <= ?`,
		pipeline, node, now.UnixNano())
	if err != nil {
		return fmt.Errorf("purge expired state: %w", err)
	}
	return nil
}

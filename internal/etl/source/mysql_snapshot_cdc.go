package source

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("mysql_snapshot_cdc", func(config map[string]any) (core.Source, error) {
		return NewMySQLSnapshotCDCSource(config)
	})
}

type MySQLSnapshotCDCSource struct {
	name                   string
	host                   string
	port                   int
	user                   string
	password               string
	database               string
	table                  string
	tables                 []string          // multiple tables for snapshot (after '*' expansion)
	explicitTables         []string          // tables as configured, before '*' expansion
	pkCol                  string            // global fallback pk column (legacy single-table config)
	pkCols                 map[string]string // explicit per-table pk overrides (table -> column)
	skipNoPKTables         bool              // skip tables without a usable snapshot pk instead of failing
	noPKSnapshotMode       string            // "skip" (default) or "hash": how to snapshot tables without a usable pk
	limit                  int
	serverID               uint32
	shardIndex             int
	shardTotal             int
	serverIDBase           uint32
	consistentSnapshotLock bool
	// binlogPurgedPolicy controls recovery when the checkpointed binlog file
	// no longer exists on the server (ERROR 1236). Default fail-closed.
	binlogPurgedPolicy BinlogPurgedPolicy
}

// resolvedPK holds the per-table snapshot key decision made at Open time.
// kind tells runSnapshot how to advance the cursor for that table.
type resolvedPKKind int

const (
	pkKindNone      resolvedPKKind = iota // table has no usable snapshot key; skip snapshot, CDC only
	pkKindNumeric                         // integer-like column: ORDER BY col, numeric cursor, optional MOD sharding
	pkKindOrdered                         // non-numeric but orderable column (e.g. datetime/varchar): ORDER BY col, string cursor
	pkKindComposite                       // composite PRIMARY KEY: row-value (a,b) > (?,?) cursor, no MOD sharding
	pkKindRowHash                         // no usable key: synthesise a content hash key per row
)

// snapshotRowHashColumn is the synthetic key column written for tables that
// have no primary key (no_pk_snapshot=hash). Sinks with
// pk_columns_from_metadata upsert on it, so a re-snapshot is idempotent.
const snapshotRowHashColumn = "_row_hash"

// no_pk_snapshot modes.
const (
	noPKSnapshotSkip = "skip" // default: keyless tables are skipped (CDC only)
	noPKSnapshotHash = "hash" // stream the table and synthesise a content-hash key
)

type resolvedPK struct {
	column string
	kind   resolvedPKKind
	// columns is the ordered composite key column set for pkKindComposite
	// (KEY_COLUMN_USAGE ORDINAL_POSITION order). Empty for other kinds.
	columns []string
}

type snapshotCDCPosition struct {
	Phase    string            `json:"phase"`
	LastID   int64             `json:"last_id"`             // backward-compatible single-table cursor
	LastIDs  map[string]int64  `json:"last_ids,omitempty"`  // per-table numeric snapshot cursors
	LastStrs map[string]string `json:"last_strs,omitempty"` // per-table non-numeric snapshot cursors
	// LastCompositeCursors holds per-table composite snapshot cursors:
	// table -> ordered column-value array (JSON array, one entry per composite
	// key column in ORDINAL_POSITION order). omitempty keeps legacy
	// checkpoints byte-identical after a round trip.
	LastCompositeCursors map[string]json.RawMessage `json:"last_composite_cursors,omitempty"`
	File                 string                     `json:"file"`
	Pos                  uint32                     `json:"pos"`
}

type snapshotCDCReader struct {
	source *MySQLSnapshotCDCSource
	db     *sql.DB
	canal  *canal.Canal

	records chan core.Record
	errors  chan error

	mu    sync.RWMutex
	phase string
	// checkpointPhase/File/Pos and tableLast* represent the last source
	// position that was acknowledged by the runner after durable checkpoint
	// persistence. They must not be used by the snapshot producer as its
	// read-ahead cursor.
	checkpointPhase string
	checkpointFile  string
	checkpointPos   uint32
	lastID          int64
	tableLastIDs    map[string]int64
	// tableLastStr holds string cursors for non-numeric (pkKindOrdered) keys.
	tableLastStr map[string]string
	// tableLastComposite holds durable composite cursors for pkKindComposite
	// keys: table -> ordered column values (one per key column).
	tableLastComposite map[string][]any
	// snapshotRead* are producer-only cursors. They may run ahead of the sink;
	// a restart discards them and reloads tableLast* from the durable checkpoint.
	snapshotReadIDs       map[string]int64
	snapshotReadStr       map[string]string
	snapshotReadComposite map[string][]any
	file                  string
	pos                   uint32
	snapshotHandoffFile   string
	snapshotHandoffPos    uint32
	snapshotHandoffValid  bool
	// resnapshotRequested is set by the CDC reconnect loop when the binlog is
	// purged (ERROR 1236) and the policy is resnapshot; the loop then breaks and
	// the run() outer loop re-enters the snapshot phase from lastIDs/lastStrs.
	resnapshotRequested bool

	// resolvedPKs is the per-table snapshot key decision, populated at Open
	// after the real table list is known. Tables with pkKindNone are skipped
	// during snapshot and only streamed via CDC.
	resolvedPKs map[string]resolvedPK

	done chan struct{}
}

func NewMySQLSnapshotCDCSource(config map[string]any) (*MySQLSnapshotCDCSource, error) {
	s := &MySQLSnapshotCDCSource{name: "mysql_snapshot_cdc", port: 3306, pkCol: "id", limit: 1000, consistentSnapshotLock: true, noPKSnapshotMode: noPKSnapshotSkip}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	if v, ok := config["host"]; ok {
		if vs, ok := v.(string); ok {
			s.host = vs
		}
	}
	if v, ok := config["port"]; ok {
		switch p := v.(type) {
		case int:
			s.port = p
		case float64:
			s.port = int(p)
		}
	}
	if v, ok := config["user"]; ok {
		if vs, ok := v.(string); ok {
			s.user = vs
		}
	}
	if v, ok := config["password"]; ok {
		if vs, ok := v.(string); ok {
			s.password = vs
		}
	}
	if v, ok := config["database"]; ok {
		if vs, ok := v.(string); ok {
			s.database = vs
		}
	}
	if v, ok := config["table"]; ok {
		if vs, ok := v.(string); ok {
			s.table = vs
		}
	}
	s.tables = append(s.tables, readStringSlice(config, "tables")...)
	// Keep the configured table list (before '*' expansion) so resolveTablePKs
	// can distinguish whole-database snapshots from explicit table lists and
	// apply the right policy for tables without a usable snapshot key.
	s.explicitTables = append([]string(nil), s.tables...)
	// If only "tables" is set, use first table as the default single-table.
	if s.table == "" && len(s.tables) > 0 {
		s.table = s.tables[0]
	}
	// If only "table" is set, also populate tables slice.
	if len(s.tables) == 0 && s.table != "" {
		s.tables = []string{s.table}
		s.explicitTables = append([]string(nil), s.tables...)
	}
	if v, ok := config["pk_column"]; ok {
		if vs, ok := v.(string); ok {
			s.pkCol = vs
		}
	}
	// Per-table pk overrides. Accepts YAML map[string]string shaped as
	// {"users": "user_id", "orders": "order_no"}. Decoded shapes from
	// YAML/JSON are map[string]any or map[interface{}]interface{}.
	s.pkCols = readStringMap(config, "pk_columns")
	if v, ok := config["skip_no_pk_tables"]; ok {
		if b, ok := v.(bool); ok {
			s.skipNoPKTables = b
		}
	}
	if v, ok := config["no_pk_snapshot"]; ok {
		if vs, ok := v.(string); ok {
			mode := strings.ToLower(strings.TrimSpace(vs))
			switch mode {
			case "", noPKSnapshotSkip:
				s.noPKSnapshotMode = noPKSnapshotSkip
			case noPKSnapshotHash:
				s.noPKSnapshotMode = noPKSnapshotHash
			default:
				return nil, fmt.Errorf("mysql_snapshot_cdc no_pk_snapshot=%q is unsupported; use skip or hash", vs)
			}
		}
	}
	if v, ok := config["limit"]; ok {
		switch l := v.(type) {
		case int:
			s.limit = l
		case float64:
			s.limit = int(l)
		}
	}
	if v, ok := config["server_id"]; ok {
		switch id := v.(type) {
		case int:
			s.serverID = uint32(id)
		case float64:
			s.serverID = uint32(id)
		case uint32:
			s.serverID = id
		}
	}
	if v, ok := config["server_id_base"]; ok {
		switch id := v.(type) {
		case int:
			s.serverIDBase = uint32(id)
		case float64:
			s.serverIDBase = uint32(id)
		}
	}
	if v, ok := config["consistent_snapshot_lock"]; ok {
		if b, ok := v.(bool); ok {
			s.consistentSnapshotLock = b
		}
	}
	s.binlogPurgedPolicy = parseBinlogPurgedPolicy(config)
	if v, ok := config["shard_index"]; ok {
		switch si := v.(type) {
		case int:
			s.shardIndex = si
		case float64:
			s.shardIndex = int(si)
		}
	}
	if v, ok := config["shard_total"]; ok {
		switch st := v.(type) {
		case int:
			s.shardTotal = st
		case float64:
			s.shardTotal = int(st)
		}
	}
	// Default server_id: when sharding, base+shard; otherwise random per-instance.
	if s.serverID == 0 {
		if s.shardTotal > 0 && s.serverIDBase > 0 {
			s.serverID = s.serverIDBase + uint32(s.shardIndex)
		} else {
			s.serverID = deriveServerID(s.name)
		}
	}
	if s.host == "" || s.user == "" || s.database == "" || len(s.tables) == 0 {
		return nil, fmt.Errorf("mysql_snapshot_cdc requires host, user, database, and table or tables")
	}
	return s, nil
}

func (s *MySQLSnapshotCDCSource) Name() string { return s.name }

func (s *MySQLSnapshotCDCSource) Describe(ctx context.Context) (core.SchemaInfo, error) {
	table, ok := singleDescribableMySQLTable(s.tables)
	if !ok {
		return core.SchemaInfo{}, nil
	}
	db, err := openMySQLSchemaDB(ctx, s.user, s.password, s.host, s.port, s.database)
	if err != nil {
		return core.SchemaInfo{}, err
	}
	defer db.Close()
	return describeMySQLTableSchema(ctx, db, s.database, table, nil)
}

func (s *MySQLSnapshotCDCSource) listTables(ctx context.Context, db *sql.DB) ([]string, error) {
	return listMySQLTables(ctx, db, s.database)
}

func listMySQLTables(ctx context.Context, db *sql.DB, database string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name`,
		database,
	)
	if err != nil {
		return nil, fmt.Errorf("list mysql tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

// resolveTablePKs determines the snapshot key for every table in s.tables.
// Priority per table:
//  1. explicit per-table override in s.pkColumns
//  2. global s.pkCol (legacy single-table config; applied to all tables)
//  3. auto-detected single-column PRIMARY KEY from information_schema
//  4. none -> pkKindNone (snapshot skipped for this table, CDC still applies)
//
// A table whose resolved key is not an integer type is classified as
// pkKindOrdered (string cursor, no MOD sharding). Tables explicitly listed
// but lacking any usable key are rejected unless skip_no_pk_tables=true,
// because silently dropping a user-requested table from the snapshot would
// hide a configuration mistake. Tables discovered via tables=["*"] that lack
// a usable key are skipped with a warning, since whole-database snapshots
// routinely include junction/log tables without a single-column PK.
func (s *MySQLSnapshotCDCSource) resolveTablePKs(ctx context.Context, db *sql.DB) (map[string]resolvedPK, error) {
	out := make(map[string]resolvedPK, len(s.tables))
	wholeDB := len(s.explicitTables) == 1 && s.explicitTables[0] == "*"
	for _, t := range s.tables {
		rpk, err := s.resolveOnePK(ctx, db, t)
		if err != nil {
			return nil, err
		}
		if rpk.kind == pkKindNone && s.noPKSnapshotMode == noPKSnapshotHash {
			// The operator explicitly asked for content-hash snapshotting, so a
			// keyless table is no longer a configuration error: it is snapshotted
			// with a synthetic _row_hash key instead of being skipped.
			rpk = resolvedPK{kind: pkKindRowHash}
		}
		if rpk.kind == pkKindNone && !wholeDB && !s.skipNoPKTables {
			return nil, fmt.Errorf("mysql_snapshot_cdc: table %s has no usable primary key (single or composite) and no pk_column override; set pk_columns or skip_no_pk_tables=true", t)
		}
		out[t] = rpk
	}
	return out, nil
}

// resolveOnePK resolves the snapshot key for a single table.
func (s *MySQLSnapshotCDCSource) resolveOnePK(ctx context.Context, db *sql.DB, table string) (resolvedPK, error) {
	return resolveSnapshotTableKey(ctx, db, s.database, table, s.pkCol, s.pkCols)
}

// resolveSnapshotTableKey is the package-level snapshot-key resolution used
// both by the live source and by ProbeSnapshotCoverage so preflight reports
// exactly what the runtime will do (no classification drift).
func resolveSnapshotTableKey(ctx context.Context, db *sql.DB, database, table, pkCol string, pkCols map[string]string) (resolvedPK, error) {
	// 1. explicit per-table override
	if col, ok := pkCols[table]; ok && col != "" {
		return classifySnapshotPK(ctx, db, database, table, col)
	}
	// 2. global pkCol fallback (legacy)
	if pkCol != "" {
		// Confirm the column exists on this table; if it does not, fall through
		// to auto-detection so a wrong global default does not abort a
		// whole-database snapshot at the first table without an `id` column.
		if exists, err := columnExists(ctx, db, database, table, pkCol); err != nil {
			return resolvedPK{}, err
		} else if exists {
			return classifySnapshotPK(ctx, db, database, table, pkCol)
		}
	}
	// 3. auto-detect single-column PRIMARY KEY; composite falls to step 3.5
	pk, err := detectSingleColumnPK(ctx, db, database, table)
	if err != nil {
		return resolvedPK{}, err
	}
	if pk != "" {
		ct, _ := columnType(ctx, db, database, table, pk)
		return resolvedPK{column: pk, kind: pkKindForType(ct)}, nil
	}
	// 3.5 auto-detect composite PRIMARY KEY (row-value comparison cursor)
	cols, err := detectCompositePK(ctx, db, database, table)
	if err != nil {
		return resolvedPK{}, err
	}
	if len(cols) > 1 {
		ok, err := compositeCursorColumnTypes(ctx, db, database, table, cols)
		if err != nil {
			return resolvedPK{}, err
		}
		if ok {
			// column keeps the first key column for log/compat surfaces; the
			// composite cursor always uses the full ordered column set.
			return resolvedPK{column: cols[0], columns: cols, kind: pkKindComposite}, nil
		}
		g.Log().Warningf(ctx, "mysql_snapshot_cdc: table %s has a composite primary key (%s) but not every column is NOT NULL and orderable; snapshot will be skipped and CDC will still capture changes", table, strings.Join(cols, ","))
	}
	return resolvedPK{kind: pkKindNone}, nil
}

// classifySnapshotPK validates that the chosen column exists and decides
// numeric vs ordered cursor based on its declared column_type.
func classifySnapshotPK(ctx context.Context, db *sql.DB, database, table, col string) (resolvedPK, error) {
	ct, err := columnType(ctx, db, database, table, col)
	if err != nil {
		return resolvedPK{}, err
	}
	return resolvedPK{column: col, kind: pkKindForType(ct)}, nil
}

func pkKindForType(columnType string) resolvedPKKind {
	t := strings.ToLower(strings.TrimSpace(columnType))
	// Strip length/type modifiers: "int(11) unsigned" -> "int unsigned".
	if i := strings.IndexByte(t, '('); i >= 0 {
		t = t[:i]
	}
	t = strings.TrimSpace(t)
	// Drop trailing qualifiers so "bigint unsigned" (the column_type MySQL
	// reports for an unsigned bigint without a display width) is classified
	// the same as "bigint". Without this, an `id bigint unsigned` primary key
	// falls through to the string-cursor (pkKindOrdered) snapshot path, which
	// disables MOD sharding and stores the cursor as a string.
	if i := strings.Index(t, " "); i >= 0 {
		t = t[:i]
	}
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "bit":
		return pkKindNumeric
	}
	return pkKindOrdered
}

// detectSingleColumnPK reads information_schema.columns (column_key='PRI',
// ordinal_position order) and returns the single PK column name, or "" when
// the table has no PK or a composite PK.
func detectSingleColumnPK(ctx context.Context, db *sql.DB, database, table string) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_key = 'PRI'
		ORDER BY ordinal_position`, database, table)
	if err != nil {
		return "", fmt.Errorf("detect primary key for %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(cols) == 1 {
		return cols[0], nil
	}
	return "", nil
}

// detectCompositePK reads information_schema.KEY_COLUMN_USAGE for the PRIMARY
// constraint and returns the ordered composite key columns in
// ORDINAL_POSITION order. Returns nil when the table has no PRIMARY
// constraint or only a single-column one.
func detectCompositePK(ctx context.Context, db *sql.DB, database, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT kcu.column_name
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.table_constraints tc
		  ON tc.constraint_schema = kcu.constraint_schema
		 AND tc.table_schema = kcu.table_schema
		 AND tc.table_name = kcu.table_name
		 AND tc.constraint_name = kcu.constraint_name
		WHERE kcu.table_schema = ? AND kcu.table_name = ?
		  AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position`, database, table)
	if err != nil {
		return nil, fmt.Errorf("detect composite primary key for %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) <= 1 {
		return nil, nil
	}
	return cols, nil
}

// compositeCursorColumnTypes returns whether every composite key column is
// NOT NULL and orderable (numeric family / string family / date-time family)
// so the row-value cursor (a,b) > (?,?) yields a stable total order.
// Strings must be non-binary to sort deterministically under the connection
// collation; binary types are rejected to pkKindNone.
func compositeCursorColumnTypes(ctx context.Context, db *sql.DB, database, table string, cols []string) (bool, error) {
	for _, col := range cols {
		var isNullable string
		var dataType string
		err := db.QueryRowContext(ctx, `
			SELECT is_nullable, data_type FROM information_schema.columns
			WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
			database, table, col).Scan(&isNullable, &dataType)
		if err != nil {
			return false, fmt.Errorf("read composite key column %s.%s.%s: %w", database, table, col, err)
		}
		if strings.ToUpper(strings.TrimSpace(isNullable)) != "NO" {
			return false, nil
		}
		if !compositeCursorOrderable(dataType) {
			return false, nil
		}
	}
	return true, nil
}

// compositeCursorOrderable reports whether a declared data_type supports a
// deterministic row-value comparison cursor.
func compositeCursorOrderable(dataType string) bool {
	t := strings.ToLower(strings.TrimSpace(dataType))
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"char", "varchar", "binary", "varbinary",
		"date", "datetime", "timestamp":
		return true
	}
	return false
}

func columnExists(ctx context.Context, db *sql.DB, database, table, col string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
		database, table, col).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("check column %s on %s.%s: %w", col, database, table, err)
	}
	return n > 0, nil
}

func columnType(ctx context.Context, db *sql.DB, database, table, col string) (string, error) {
	var ct sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT column_type FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
		database, table, col).Scan(&ct)
	if err != nil {
		return "", fmt.Errorf("read column_type for %s.%s.%s: %w", database, table, col, err)
	}
	return ct.String, nil
}

// tableColumnTypes returns the MySQL COLUMN_TYPE for every column of a table in
// a single information_schema query. The returned map is attached to each
// snapshot record's Metadata.ColumnTypes so downstream sinks (direct or via a
// kafka hop) can auto-create target tables from the real source schema instead
// of guessing from sample values.
func tableColumnTypes(ctx context.Context, q interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}, database, table string) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT column_name, column_type FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?`, database, table)
	if err != nil {
		return nil, fmt.Errorf("read column_types for %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	out := make(map[string]string)
	for rows.Next() {
		var name string
		var ct sql.NullString
		if err := rows.Scan(&name, &ct); err != nil {
			return nil, fmt.Errorf("scan column_type for %s.%s: %w", database, table, err)
		}
		out[name] = ct.String
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column_types for %s.%s: %w", database, table, err)
	}
	return out, nil
}

func (s *MySQLSnapshotCDCSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	if err := s.ValidateCheckpoint(ctx, cp); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=Local&timeout=10s&readTimeout=300s", s.user, s.password, s.host, s.port, s.database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("connect mysql (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	if len(s.tables) == 1 && s.tables[0] == "*" {
		tables, err := s.listTables(ctx, db)
		if err != nil {
			db.Close()
			return nil, err
		}
		s.tables = tables
		if len(s.tables) == 0 {
			db.Close()
			return nil, fmt.Errorf("mysql_snapshot_cdc found no tables in database %s", s.database)
		}
		s.table = s.tables[0]
	}

	// Resolve a snapshot key for every table after the real table list is
	// known. Each table may have a different PK (or none), so we cannot rely
	// on a single global pkCol across a whole-database snapshot.
	resolved, err := s.resolveTablePKs(ctx, db)
	if err != nil {
		db.Close()
		return nil, err
	}

	reader := &snapshotCDCReader{
		source:          s,
		db:              db,
		records:         make(chan core.Record, 1024),
		errors:          make(chan error, 64),
		phase:           "snapshot",
		checkpointPhase: "snapshot",
		tableLastIDs:    make(map[string]int64),
		tableLastStr:    make(map[string]string),
		snapshotReadIDs: make(map[string]int64),
		snapshotReadStr: make(map[string]string),
		resolvedPKs:     resolved,
		done:            make(chan struct{}),
	}

	if cp == nil {
		// Fresh runs set the CDC handoff position inside runSnapshot while the
		// snapshot transaction is opened. Checkpointed runs restore it below.
		reader.file = "master"
		reader.pos = 0
	}

	if cp != nil {
		pos, err := decodeSnapshotCDCCheckpointPosition(cp.Position)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("decode mysql_snapshot_cdc checkpoint: %w", err)
		}
		if pos.Phase == "" {
			pos.Phase = "snapshot"
		}
		if pos.Phase != "snapshot" && pos.Phase != "cdc" {
			db.Close()
			return nil, fmt.Errorf("mysql_snapshot_cdc checkpoint has unsupported phase %q", pos.Phase)
		}
		if pos.Phase == "cdc" && (pos.File == "" || pos.Pos == 0) {
			db.Close()
			return nil, fmt.Errorf("mysql_snapshot_cdc CDC checkpoint is missing the binlog position")
		}
		reader.phase = pos.Phase
		reader.checkpointPhase = pos.Phase
		reader.lastID = pos.LastID
		reader.tableLastIDs = cloneIntCursorMap(pos.LastIDs)
		if pos.LastID != 0 && s.table != "" {
			if _, exists := reader.tableLastIDs[s.table]; !exists {
				reader.tableLastIDs[s.table] = pos.LastID
			}
		}
		if len(pos.LastStrs) > 0 {
			reader.tableLastStr = cloneStringCursorMap(pos.LastStrs)
			// Migration: older releases misclassified `bigint unsigned`/`int unsigned`
			// primary keys as pkKindOrdered and checkpointed their progress as
			// string cursors in last_strs. pkKindForType now classifies them as
			// numeric, so without a bridge the snapshot would restart those tables
			// from id=0 and re-emit every row.
			migrateStringCursorsToNumeric(reader.tableLastIDs, pos.LastStrs)
		}
		if len(pos.LastCompositeCursors) > 0 {
			// Composite cursors are stored as JSON arrays of per-column values.
			// Restore them typed: numbers as float64 (exact for int53-range keys;
			// larger values keep their JSON number form), strings as strings.
			// A malformed entry drops only that table's cursor (its snapshot
			// replays from the beginning — correct under at-least-once) while the
			// error is surfaced in the log.
			restored := make(map[string][]any, len(pos.LastCompositeCursors))
			for table, raw := range pos.LastCompositeCursors {
				var arr []json.RawMessage
				if err := json.Unmarshal(raw, &arr); err != nil || len(arr) == 0 {
					g.Log().Warningf(ctx, "mysql_snapshot_cdc: ignoring malformed composite cursor for table %s: %v", table, err)
					continue
				}
				tuple := make([]any, len(arr))
				for i, el := range arr {
					var s string
					if err := json.Unmarshal(el, &s); err == nil {
						tuple[i] = s
						continue
					}
					var f float64
					if err := json.Unmarshal(el, &f); err == nil {
						tuple[i] = f
						continue
					}
					tuple[i] = nil
				}
				// A null element cannot be expressed back into the (a,b) > (?,?)
				// cursor; drop the table cursor so the snapshot replays it.
				hasNil := false
				for _, v := range tuple {
					if v == nil {
						hasNil = true
						break
					}
				}
				if hasNil {
					g.Log().Warningf(ctx, "mysql_snapshot_cdc: composite cursor for table %s contains a null element; replaying this table from the beginning", table)
					continue
				}
				restored[table] = tuple
			}
			reader.tableLastComposite = restored
		}
		if pos.File != "" {
			reader.file = pos.File
			reader.checkpointFile = pos.File
		}
		if pos.Pos != 0 {
			reader.pos = pos.Pos
			reader.checkpointPos = pos.Pos
		}
		if pos.Phase == "snapshot" {
			if pos.File == "" || pos.Pos == 0 {
				db.Close()
				return nil, fmt.Errorf("mysql_snapshot_cdc snapshot checkpoint is missing the CDC handoff position")
			}
			reader.snapshotHandoffFile = pos.File
			reader.snapshotHandoffPos = pos.Pos
			reader.snapshotHandoffValid = true
		}
	}

	c, err := s.newCanal(reader)
	if err != nil {
		db.Close()
		return nil, err
	}
	reader.setCanal(c)
	reader.snapshotReadIDs = cloneIntCursorMap(reader.tableLastIDs)
	reader.snapshotReadStr = cloneStringCursorMap(reader.tableLastStr)
	reader.snapshotReadComposite = cloneCompositeCursorMap(reader.tableLastComposite)

	go reader.run(ctx)
	return reader, nil
}

func (s *MySQLSnapshotCDCSource) newCanal(reader *snapshotCDCReader) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.host, s.port)
	cfg.User = s.user
	cfg.Password = s.password
	cfg.Flavor = "mysql"
	cfg.ServerID = s.serverID
	cfg.Dump.ExecutionPath = ""
	if len(s.tables) > 0 {
		regexes := make([]string, 0, len(s.tables))
		for _, t := range s.tables {
			if t == "*" {
				regexes = append(regexes, fmt.Sprintf("%s\\..*", s.database))
			} else {
				regexes = append(regexes, fmt.Sprintf("%s\\.%s", s.database, regexp.QuoteMeta(t)))
			}
		}
		cfg.IncludeTableRegex = regexes
	} else {
		cfg.IncludeTableRegex = []string{fmt.Sprintf("%s\\.%s", s.database, regexp.QuoteMeta(s.table))}
	}
	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("create canal (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	c.SetEventHandler(&snapshotCDCHandler{reader: reader})
	return c, nil
}

func (r *snapshotCDCReader) run(ctx context.Context) {
	defer close(r.records)
	defer close(r.errors)
	defer r.db.Close()
	defer close(r.done)
	defer r.closeCanal()

	// Outer loop supports binlog-purged resnapshot recovery (BUG-2): when the
	// CDC reconnect loop detects ERROR 1236 and the policy is resnapshot, it
	// resets phase to "snapshot" and breaks; this loop re-enters runSnapshot
	// from the last per-table cursors and then re-enters CDC at the new handoff.
	for {
		if r.getPhase() != "cdc" {
			if err := r.runSnapshot(ctx); err != nil {
				select {
				case r.errors <- err:
				case <-ctx.Done():
				}
				return
			}
			r.setPhase("cdc")
		}
		// The canal used to capture the snapshot handoff is no longer needed once
		// the snapshot transaction has finished. Release it before opening the CDC
		// stream; Close() is idempotent and also unblocks an in-flight RunFrom.
		r.closeCanal()

		curFile, curPos := r.getDurableBinlogPos()
		if curFile == "" || curPos == 0 {
			select {
			case r.errors <- fmt.Errorf("mysql_snapshot_cdc has no durable CDC resume position"):
			case <-ctx.Done():
			}
			return
		}
		pos := mysql.Position{Name: curFile, Pos: curPos}
		g.Log().Infof(ctx, "Starting snapshot+CDC stream from %s:%d", pos.Name, pos.Pos)

		// CDC reconnect loop with exponential backoff. The cdcLoop label lets
		// the binlog-purged resnapshot policy break out to re-enter snapshot.
	cdcLoop:
		for {
			backoff := time.Second
			const maxBackoff = 30 * time.Second
			if ctx.Err() != nil {
				return
			}
			// Reopen from the last acknowledged checkpoint. The handler's runtime
			// position may be many events ahead of the sink and must never become a
			// recovery boundary merely because canal disconnected.
			curFile, curPos := r.getDurableBinlogPos()
			if curFile == "" || curPos == 0 {
				select {
				case r.errors <- fmt.Errorf("mysql_snapshot_cdc has no durable CDC resume position"):
				case <-ctx.Done():
					return
				}
				return
			}
			runPos := mysql.Position{Name: curFile, Pos: curPos}
			c, err := r.source.newCanal(r)
			if err != nil {
				runErr := err
				g.Log().Warningf(ctx, "mysql_snapshot_cdc canal create failed: %v; reconnecting in %s", runErr, backoff)
				select {
				case r.errors <- fmt.Errorf("create canal: %w", runErr):
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				continue
			}
			r.setCanal(c)
			runErr := c.RunFrom(runPos)
			c.Close()
			r.clearCanal(c)
			if ctx.Err() != nil {
				return
			}
			if runErr == nil {
				// Canal exited cleanly.
				return
			}

			// BUG-2: binlog purged / ERROR 1236 is not transient. The checkpointed
			// file no longer exists; retrying it loops forever. Apply the policy.
			if isBinlogPurgedError(runErr) {
				staleFile, stalePos := runPos.Name, runPos.Pos
				g.Log().Errorf(ctx, "mysql_snapshot_cdc binlog purged (ERROR 1236): checkpoint %s:%d no longer exists; policy=%s", staleFile, stalePos, r.source.binlogPurgedPolicy)
				switch r.source.binlogPurgedPolicy {
				case BinlogPurgedResnapshot:
					// Fall back to snapshot: re-read each table from its last cursor.
					// CDC events between the stale checkpoint and the new snapshot
					// handoff are NOT captured (RPO gap); the snapshot catches the
					// current full state. Break out of the CDC loop; the run() outer
					// loop sees phase reset to "snapshot" and re-enters runSnapshot.
					r.requestResnapshot()
					g.Log().Warningf(ctx, "mysql_snapshot_cdc binlog purged: falling back to snapshot phase from last cursors (RPO gap for changes since stale checkpoint)")
					break cdcLoop
				case BinlogPurgedResumeFromCurrent:
					probe, perr := r.source.newCanal(r)
					if perr == nil {
						curPos, gerr := probe.GetMasterPos()
						probe.Close()
						if gerr == nil {
							r.advanceResumePos(curPos.Name, uint32(curPos.Pos))
							g.Log().Warningf(ctx, "mysql_snapshot_cdc resuming from current master pos %s:%d; changes between stale checkpoint and now are DROPPED", curPos.Name, curPos.Pos)
							backoff = time.Second
							continue
						}
						runErr = fmt.Errorf("binlog purged recovery: get master pos: %w", gerr)
					} else {
						runErr = fmt.Errorf("binlog purged recovery: create probe canal: %w", perr)
					}
				}
				// fail (default) or probe failure: fatal.
				select {
				case r.errors <- fmt.Errorf("%w: %v", ErrBinlogPurged, runErr):
				case <-ctx.Done():
				}
				return
			}

			g.Log().Warningf(ctx, "mysql_snapshot_cdc canal exited: %v; reconnecting in %s", runErr, backoff)
			select {
			case r.errors <- fmt.Errorf("canal disconnected: %w", runErr):
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
		// Reached only on resnapshot break (the CDC loop's only non-return exit):
		// re-enter the outer loop, which sees phase reset to "snapshot" and
		// re-runs runSnapshot from last cursors.
		r.mu.RLock()
		wantResnap := r.resnapshotRequested
		r.mu.RUnlock()
		if wantResnap {
			r.mu.Lock()
			r.resnapshotRequested = false
			r.mu.Unlock()
			continue
		}
		return
	}
}

// runSnapshot performs a consistent snapshot of the source table.
// It wraps all SELECT queries in a single REPEATABLE READ transaction,
// ensuring a consistent MVCC snapshot across all pages. Combined with
// recording the binlog position BEFORE starting the snapshot, this guarantees
// no duplicates or gaps at the snapshot→CDC handoff.
func (r *snapshotCDCReader) runSnapshot(ctx context.Context) error {
	// Start a consistent snapshot transaction and anchor the CDC resume
	// position while writes are blocked. FLUSH TABLES WITH READ LOCK is
	// connection-scoped, so use a dedicated sql.Conn until UNLOCK.
	var tx *sql.Tx
	var err error
	var conn *sql.Conn
	if r.source.consistentSnapshotLock {
		conn, err = r.db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("snapshot get connection: %w", err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
			return fmt.Errorf("flush tables with read lock: %w", err)
		}
		locked := true
		defer func() {
			if locked {
				_, _ = conn.ExecContext(context.Background(), "UNLOCK TABLES")
			}
		}()

		startPos, err := r.snapshotStartPosition()
		if err != nil {
			return fmt.Errorf("get snapshot handoff position under lock: %w", err)
		}
		r.setRuntimeBinlogPos(startPos.Name, uint32(startPos.Pos))

		tx, err = conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return fmt.Errorf("begin snapshot tx under lock: %w", err)
		}
		if err := r.primeSnapshotReadView(ctx, tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("unlock tables after snapshot tx begin: %w", err)
		}
		locked = false
	} else {
		startPos, err := r.snapshotStartPosition()
		if err != nil {
			return fmt.Errorf("get snapshot handoff position: %w", err)
		}
		r.setRuntimeBinlogPos(startPos.Name, uint32(startPos.Pos))
		tx, err = r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return fmt.Errorf("begin snapshot tx: %w", err)
		}
	}
	defer tx.Rollback()

	for _, tableName := range r.source.tables {
		rpk, ok := r.resolvedPKs[tableName]
		if !ok || rpk.kind == pkKindNone {
			// No usable snapshot key for this table (no primary key at all, or a
			// composite key whose columns cannot form a stable cursor): skip
			// historical snapshot; the CDC phase still covers it (binlog is
			// key-agnostic). Composite keys that DO form a stable cursor are no
			// longer skipped — see the pkKindComposite branch in snapshotTable.
			g.Log().Warningf(ctx, "mysql_snapshot_cdc: skipping snapshot for table %s (no primary key usable as snapshot cursor); CDC phase will still capture changes", tableName)
			continue
		}
		if rpk.kind == pkKindRowHash {
			if err := r.snapshotTableRowHash(ctx, tx, tableName); err != nil {
				return err
			}
			continue
		}
		if err := r.snapshotTable(ctx, tx, tableName, rpk); err != nil {
			return err
		}
	}
	return nil
}

func (r *snapshotCDCReader) snapshotStartPosition() (mysql.Position, error) {
	r.mu.Lock()
	if r.snapshotHandoffValid {
		pos := mysql.Position{Name: r.snapshotHandoffFile, Pos: r.snapshotHandoffPos}
		r.mu.Unlock()
		return pos, nil
	}
	r.mu.Unlock()
	c := r.getCanal()
	if c == nil {
		// The initial snapshot reuses the canal created by Open. That canal is
		// deliberately closed before entering CDC, so an automatic resnapshot
		// after ERROR 1236 must create a fresh one before capturing its new
		// handoff. Keep it registered on the reader so Close can interrupt it
		// and the outer run loop can release it after the snapshot transaction.
		var err error
		c, err = r.source.newCanal(r)
		if err != nil {
			return mysql.Position{}, fmt.Errorf("reopen canal for snapshot handoff: %w", err)
		}
		r.setCanal(c)
	}
	pos, err := c.GetMasterPos()
	if err != nil {
		return mysql.Position{}, err
	}
	if pos.Name == "" || pos.Pos == 0 {
		return mysql.Position{}, fmt.Errorf("mysql_snapshot_cdc returned an invalid CDC handoff position %q:%d", pos.Name, pos.Pos)
	}
	r.installSnapshotHandoff(pos.Name, uint32(pos.Pos))
	return pos, nil
}

// requestResnapshot invalidates the previous snapshot/CDC ordering anchor.
// The next runSnapshot call must capture a new master position; reusing the
// original handoff would make later snapshot rows compare older than CDC rows
// already written to a versioned sink.
func (r *snapshotCDCReader) requestResnapshot() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resnapshotRequested = true
	r.phase = "snapshot"
	r.snapshotHandoffFile = ""
	r.snapshotHandoffPos = 0
	r.snapshotHandoffValid = false
}

// installSnapshotHandoff switches the in-memory checkpoint candidate into a
// new snapshot generation. The persisted checkpoint is unchanged until the
// runner successfully writes and acknowledges a record from this generation.
func (r *snapshotCDCReader) installSnapshotHandoff(file string, pos uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshotHandoffFile = file
	r.snapshotHandoffPos = pos
	r.snapshotHandoffValid = true
	r.checkpointPhase = "snapshot"
	r.checkpointFile = file
	r.checkpointPos = pos
	r.file = file
	r.pos = pos
}

func (r *snapshotCDCReader) setRuntimeBinlogPos(file string, pos uint32) {
	r.mu.Lock()
	r.file = file
	r.pos = pos
	r.mu.Unlock()
}

// snapshotTable pages through one table's historical rows using its resolved
// snapshot key. Numeric keys use a numeric cursor (and optional MOD sharding);
// ordered non-numeric keys (datetime/varchar) use a string > cursor without
// sharding, since MOD is only defined for integers.
func (r *snapshotCDCReader) snapshotTable(ctx context.Context, tx *sql.Tx, tableName string, rpk resolvedPK) error {
	handoffFile, handoffPos, handoffValid := r.getSnapshotHandoffPosition()
	if !handoffValid {
		return fmt.Errorf("snapshot source-order handoff position is unavailable for %s", tableName)
	}
	// Fetch the source column types once per table so every snapshot record
	// carries real schema metadata; downstream sinks (direct or via a kafka
	// hop) can then auto-create target tables from the source schema instead
	// of guessing from sample values.
	colTypes, colTypeErr := tableColumnTypes(ctx, tx, r.source.database, tableName)
	if colTypeErr != nil {
		// Non-fatal: sinks fall back to value inference when ColumnTypes is nil.
		g.Log().Warningf(ctx, "snapshot_cdc: read column types for %s.%s failed (sinks will infer from values): %v", r.source.database, tableName, colTypeErr)
		colTypes = nil
	}
	for {
		var query string
		var args []any
		if rpk.kind == pkKindNumeric {
			lastID := r.getSnapshotReadID(tableName)
			if r.source.shardTotal > 1 {
				query = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` > ? AND MOD(`%s`, %d) = %d ORDER BY `%s` LIMIT %d",
					tableName, rpk.column, rpk.column,
					r.source.shardTotal, r.source.shardIndex,
					rpk.column, r.source.limit)
			} else {
				query = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` > ? ORDER BY `%s` LIMIT %d",
					tableName, rpk.column, rpk.column, r.source.limit)
			}
			args = []any{lastID}
		} else if rpk.kind == pkKindComposite {
			// Row-value comparison cursor: WHERE (a,b) > (?,?) ORDER BY a,b.
			// MySQL 8 optimizes this form with the composite PRIMARY index (row
			// constructor optimization). Parameter order must equal the ORDER BY
			// column order. MOD sharding is not defined for composite keys: if a
			// shard config is present we degrade to unsharded (sharding is a
			// performance option, not a correctness one) and say so once.
			if r.source.shardTotal > 1 {
				g.Log().Infof(ctx, "mysql_snapshot_cdc: table %s has a composite primary key; MOD sharding is disabled for this table and the snapshot reads all shard rows", tableName)
			}
			lastVals := r.getSnapshotReadComposite(tableName, len(rpk.columns))
			query, args = compositeCursorQuery(tableName, rpk.columns, lastVals, r.source.limit)
		} else {
			// pkKindOrdered: string-typed cursor (datetime/varchar/char).
			lastStr := r.getSnapshotReadStr(tableName)
			query = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` > ? ORDER BY `%s` LIMIT %d",
				tableName, rpk.column, rpk.column, r.source.limit)
			args = []any{lastStr}
		}
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("snapshot query %s: %w", tableName, err)
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return fmt.Errorf("snapshot get columns %s: %w", tableName, err)
		}
		count := 0
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return fmt.Errorf("snapshot scan %s: %w", tableName, err)
			}
			data := map[string]any{}
			for i, col := range cols {
				data[col] = normalizeValue(values[i])
			}
			if rpk.kind == pkKindComposite {
				// Advance the composite read cursor to this row's key tuple.
				// Rows are read in composite ORDER BY order, so the last row of
				// the batch carries the greatest tuple; assert per-column value
				// presence to fail loudly rather than silently mispaging.
				tuple := make([]any, len(rpk.columns))
				for ci, colName := range rpk.columns {
					v, ok := data[colName]
					if !ok || v == nil {
						rows.Close()
						return fmt.Errorf("snapshot composite cursor %s.%s is missing or NULL", tableName, colName)
					}
					tuple[ci] = v
				}
				r.setSnapshotReadComposite(tableName, tuple)
			} else if v, ok := data[rpk.column]; ok {
				if rpk.kind == pkKindNumeric {
					nid, parseErr := parseSnapshotID(v)
					if parseErr != nil {
						rows.Close()
						return fmt.Errorf("snapshot cursor %s.%s: %w", tableName, rpk.column, parseErr)
					}
					if nid > r.getSnapshotReadID(tableName) {
						r.setSnapshotReadID(tableName, nid)
					}
				} else {
					if v == nil {
						rows.Close()
						return fmt.Errorf("snapshot cursor %s.%s is NULL", tableName, rpk.column)
					}
					if cs := cursorString(v); cs > r.getSnapshotReadStr(tableName) {
						r.setSnapshotReadStr(tableName, cs)
					}
				}
			}
			cursorKind := core.SourceCursorOrdered
			if rpk.kind == pkKindNumeric {
				cursorKind = core.SourceCursorNumeric
			}
			cursor := ""
			if rpk.kind == pkKindComposite {
				// Render the composite tuple as the record cursor so replay
				// inspection shows the real paging position.
				cursor = compositeTupleCursorString(rpk.columns, data)
			} else if value, ok := data[rpk.column]; ok && value != nil {
				cursor = cursorString(value)
			}
			rec := core.Record{
				Operation: core.OpInsert,
				Data:      data,
				Metadata: core.Metadata{
					Source:              r.source.name,
					SourceType:          core.SourceTypeMySQLSnapshotCDC,
					SourcePhase:         core.SourcePhaseSnapshot,
					Database:            r.source.database,
					Table:               tableName,
					Timestamp:           time.Now(),
					SnapshotHandoffFile: handoffFile,
					SnapshotHandoffPos:  handoffPos,
					Offset:              numericOffset(rpk, data),
					Cursor:              cursor,
					CursorKind:          cursorKind,
					Key:                 snapshotMetadataKey(rpk, data),
					PrimaryKeyColumns:   snapshotPrimaryKeyColumns(rpk),
					ColumnTypes:         colTypes,
				},
			}
			select {
			case r.records <- rec:
			case <-ctx.Done():
				rows.Close()
				return ctx.Err()
			}
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if count == 0 {
			break
		}
	}
	return nil
}

// snapshotTableRowHash streams a table that has no usable primary key and
// synthesises a deterministic content-hash key for every row.
//
// Why a hash key: without a key the sink cannot upsert, so a re-snapshot (or a
// restart mid-table) would duplicate every row. Hashing the row content makes
// the write idempotent under any sink that upserts on the declared primary key
// (pk_columns_from_metadata). The trade-offs are explicit and documented:
//
//   - two rows with identical column values collapse into one, because they are
//     indistinguishable by content;
//   - the scan is not resumable: an interrupted table restarts from the first
//     row (correct, because the write is idempotent, but slower on huge tables);
//   - the read is one streaming SELECT inside the snapshot transaction, so the
//     undo log is held for the duration of the table read.
func (r *snapshotCDCReader) snapshotTableRowHash(ctx context.Context, tx *sql.Tx, tableName string) error {
	handoffFile, handoffPos, handoffValid := r.getSnapshotHandoffPosition()
	if !handoffValid {
		return fmt.Errorf("snapshot source-order handoff position is unavailable for %s", tableName)
	}
	colTypes, colTypeErr := tableColumnTypes(ctx, tx, r.source.database, tableName)
	if colTypeErr != nil {
		g.Log().Warningf(ctx, "snapshot_cdc: read column types for %s.%s failed (sinks will infer from values): %v", r.source.database, tableName, colTypeErr)
		colTypes = nil
	}
	g.Log().Infof(ctx, "mysql_snapshot_cdc: table %s has no usable primary key; snapshotting with a synthetic %s key (no_pk_snapshot=hash)", tableName, snapshotRowHashColumn)

	rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT * FROM `%s`", tableName))
	if err != nil {
		return fmt.Errorf("snapshot query %s: %w", tableName, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("snapshot get columns %s: %w", tableName, err)
	}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("snapshot scan %s: %w", tableName, err)
		}
		data := make(map[string]any, len(cols)+1)
		for i, col := range cols {
			data[col] = normalizeValue(values[i])
		}
		rowHash := snapshotRowHash(cols, data)
		data[snapshotRowHashColumn] = rowHash
		rec := core.Record{
			Operation: core.OpInsert,
			Data:      data,
			Metadata: core.Metadata{
				Source:              r.source.name,
				SourceType:          core.SourceTypeMySQLSnapshotCDC,
				SourcePhase:         core.SourcePhaseSnapshot,
				Database:            r.source.database,
				Table:               tableName,
				Timestamp:           time.Now(),
				SnapshotHandoffFile: handoffFile,
				SnapshotHandoffPos:  handoffPos,
				Cursor:              rowHash,
				CursorKind:          core.SourceCursorOrdered,
				Key:                 metadataKeyJSON(snapshotRowHashColumn, data),
				PrimaryKeyColumns:   []string{snapshotRowHashColumn},
				ColumnTypes:         colTypes,
			},
		}
		select {
		case r.records <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

// snapshotRowHash derives the synthetic key from the row content: column names
// are sorted so the hash does not depend on information_schema ordering, and a
// NULL marker keeps "NULL" distinguishable from the empty string.
func snapshotRowHash(cols []string, data map[string]any) string {
	ordered := make([]string, 0, len(cols))
	for _, c := range cols {
		if c == snapshotRowHashColumn {
			continue
		}
		ordered = append(ordered, c)
	}
	sort.Strings(ordered)
	var b strings.Builder
	for _, c := range ordered {
		b.WriteString(c)
		b.WriteByte('=')
		switch v := data[c].(type) {
		case nil:
			b.WriteString("\x00NULL")
		case []byte:
			b.WriteString(hex.EncodeToString(v))
		case string:
			b.WriteString(v)
		case time.Time:
			b.WriteString(v.UTC().Format(time.RFC3339Nano))
		default:
			b.WriteString(fmt.Sprint(v))
		}
		b.WriteByte('\x1f')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func (r *snapshotCDCReader) primeSnapshotReadView(ctx context.Context, tx *sql.Tx) error {
	if len(r.source.tables) == 0 {
		return nil
	}
	// Prime the consistent read view against the first table that has a
	// usable snapshot key. The query only needs to establish the MVCC read
	// view inside the REPEATABLE READ transaction; any qualifying table works.
	for _, tableName := range r.source.tables {
		rpk, ok := r.resolvedPKs[tableName]
		if !ok || rpk.kind == pkKindNone || rpk.kind == pkKindRowHash {
			// pkKindRowHash has no cursor column, so it cannot prime the read
			// view; the fallback SELECT 1 below anchors the snapshot instead.
			continue
		}
		query := fmt.Sprintf("SELECT `%s` FROM `%s` ORDER BY `%s` LIMIT 1", rpk.column, tableName, rpk.column)
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("prime snapshot read view %s: %w", tableName, err)
		}
		return rows.Close()
	}
	// No table has a usable snapshot key; still prime the read view with a
	// trivial statement so the transaction's consistent snapshot is anchored.
	rows, err := tx.QueryContext(ctx, "SELECT 1")
	if err != nil {
		return fmt.Errorf("prime snapshot read view: %w", err)
	}
	return rows.Close()
}

func parseSnapshotID(v any) (int64, error) {
	switch id := v.(type) {
	case int:
		return int64(id), nil
	case int8:
		return int64(id), nil
	case int16:
		return int64(id), nil
	case int32:
		return int64(id), nil
	case int64:
		return id, nil
	case uint:
		if uint64(id) > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("unsigned value %d overflows int64", id)
		}
		return int64(id), nil
	case uint8:
		return int64(id), nil
	case uint16:
		return int64(id), nil
	case uint32:
		return int64(id), nil
	case uint64:
		if id > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("unsigned value %d overflows int64", id)
		}
		return int64(id), nil
	case float32:
		f := float64(id)
		if f != f || f < float64(-1<<63) || f >= float64(1<<63) || f != float64(int64(f)) {
			return 0, fmt.Errorf("non-integer numeric value %v", id)
		}
		return int64(f), nil
	case float64:
		if id != id || id < float64(-1<<63) || id >= float64(1<<63) || id != float64(int64(id)) {
			return 0, fmt.Errorf("non-integer numeric value %v", id)
		}
		return int64(id), nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %q: %w", id, err)
		}
		return parsed, nil
	case []byte:
		return parseSnapshotID(string(id))
	default:
		return 0, fmt.Errorf("unsupported numeric value type %T", v)
	}
}

// parseStrictIntCursor reports whether a string cursor carries an integer
// value. It is used to migrate legacy string-cursor checkpoints (written
// when an unsigned integer PK was misclassified as ordered) into numeric
// cursors so the snapshot resumes instead of replaying the table.
func parseStrictIntCursor(s string) (int64, bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// migrateStringCursorsToNumeric carries integer-valued string cursors forward
// as numeric cursors. A numeric entry already present for a table wins, so a
// correctly-checkpointed numeric cursor is never overwritten by a stale
// string value.
func migrateStringCursorsToNumeric(numeric map[string]int64, strs map[string]string) {
	for table, strCursor := range strs {
		if strCursor == "" {
			continue
		}
		if _, hasNumeric := numeric[table]; hasNumeric {
			continue
		}
		if n, ok := parseStrictIntCursor(strCursor); ok {
			numeric[table] = n
		}
	}
}

func cloneIntCursorMap(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStringCursorMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneCompositeCursorMap(in map[string][]any) map[string][]any {
	out := make(map[string][]any, len(in))
	for k, v := range in {
		tuple := make([]any, len(v))
		copy(tuple, v)
		out[k] = tuple
	}
	return out
}

// marshalCompositeCursors renders durable composite cursors into the JSON
// array form used by snapshotCDCPosition.LastCompositeCursors. nil/empty
// yields nil so omitempty keeps legacy checkpoints byte-identical.
func marshalCompositeCursors(in map[string][]any) map[string]json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(in))
	for table, tuple := range in {
		b, err := json.Marshal(tuple)
		if err != nil {
			continue
		}
		out[table] = b
	}
	return out
}

func (r *snapshotCDCReader) getSnapshotReadID(table string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.snapshotReadIDs != nil {
		if v, ok := r.snapshotReadIDs[table]; ok {
			return v
		}
	}
	return r.getTableLastIDLocked(table)
}

func (r *snapshotCDCReader) setSnapshotReadID(table string, id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshotReadIDs == nil {
		r.snapshotReadIDs = make(map[string]int64)
	}
	r.snapshotReadIDs[table] = id
}

func (r *snapshotCDCReader) getSnapshotReadStr(table string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.snapshotReadStr != nil {
		return r.snapshotReadStr[table]
	}
	return ""
}

func (r *snapshotCDCReader) setSnapshotReadStr(table, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshotReadStr == nil {
		r.snapshotReadStr = make(map[string]string)
	}
	r.snapshotReadStr[table] = value
}

// compositeCursorQuery builds the row-value comparison page query for a
// composite snapshot cursor. Parameter order equals the ORDER BY column
// order (the composite PRIMARY index order); a mismatch would silently skip
// or replay rows, so both halves are derived from the same cols slice.
func compositeCursorQuery(tableName string, cols []string, lastVals []any, limit int) (string, []any) {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = "`" + c + "`"
	}
	placeholders := make([]string, len(cols))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("SELECT * FROM `%s` WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
		tableName,
		strings.Join(quoted, ","), strings.Join(placeholders, ","),
		strings.Join(quoted, ","), limit)
	if lastVals == nil {
		// First page: no cursor yet. An all-minimum sentinel would break on
		// negative/generic types; instead page with a plain ORDER BY prefix.
		query = fmt.Sprintf("SELECT * FROM `%s` ORDER BY %s LIMIT %d",
			tableName, strings.Join(quoted, ","), limit)
		return query, nil
	}
	return query, lastVals
}

// getSnapshotReadComposite returns the producer read cursor tuple for a
// composite-key table. A nil return means "start from the beginning".
// Restored legacy checkpoints whose composite cursor is absent also yield nil.
func (r *snapshotCDCReader) getSnapshotReadComposite(table string, colCount int) []any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if vals, ok := r.snapshotReadComposite[table]; ok && len(vals) == colCount {
		return vals
	}
	if vals, ok := r.tableLastComposite[table]; ok && len(vals) == colCount {
		return vals
	}
	return nil
}

func (r *snapshotCDCReader) setSnapshotReadComposite(table string, tuple []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshotReadComposite == nil {
		r.snapshotReadComposite = make(map[string][]any)
	}
	r.snapshotReadComposite[table] = tuple
}

// compositeTupleGreater reports whether tuple a is strictly greater than
// tuple b under composite ORDER BY semantics (compare element-wise; the
// first difference decides). Each element is compared with its own kind:
// integers exactly, floats numerically, strings/time lexicographically via
// cursorString. Mixed kinds within one column cannot occur (the column has a
// fixed declared type); a defensive kind mismatch falls back to string form.
func compositeTupleGreater(a, b []any) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		c := compositeCompareValues(a[i], b[i])
		if c > 0 {
			return true
		}
		if c < 0 {
			return false
		}
	}
	return false
}

// compositeCompareValues compares two composite cursor elements of the same
// column type: -1, 0, or 1. Integer kinds compare exactly (no float64
// precision loss for large bigint/unsigned keys); textual and temporal kinds
// compare lexicographically in cursorString form, matching how the ORDER BY
// collation ordered the pages.
func compositeCompareValues(a, b any) int {
	if an, aok := compositeAsInt(a); aok {
		if bn, bok := compositeAsInt(b); bok {
			switch {
			case an < bn:
				return -1
			case an > bn:
				return 1
			}
			return 0
		}
	}
	if au, aok := compositeAsUint(a); aok {
		if bu, bok := compositeAsUint(b); bok {
			switch {
			case au < bu:
				return -1
			case au > bu:
				return 1
			}
			return 0
		}
	}
	if af, aok := compositeAsFloat(a); aok {
		if bf, bok := compositeAsFloat(b); bok {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			}
			return 0
		}
	}
	as, bs := cursorString(a), cursorString(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	}
	return 0
}

// compositeAsInt reports whether v is a signed integer Go kind and its exact
// value.
func compositeAsInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}

// compositeAsUint reports whether v is an unsigned integer Go kind and its
// exact value.
func compositeAsUint(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint64:
		return x, true
	}
	return 0, false
}

// compositeAsFloat reports whether v is a floating Go kind and its value.
// DECIMAL columns arrive as []byte/string and compare via the string path.
func compositeAsFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// compositeTupleCursorString renders a composite row's key tuple into a
// human-inspectable cursor string (JSON array of per-column cursor forms).
func compositeTupleCursorString(cols []string, data map[string]any) string {
	tuple := make([]any, 0, len(cols))
	for _, c := range cols {
		v, ok := data[c]
		if !ok || v == nil {
			return ""
		}
		tuple = append(tuple, cursorString(v))
	}
	b, err := json.Marshal(tuple)
	if err != nil {
		return ""
	}
	return string(b)
}

// snapshotMetadataKey builds Metadata.Key for a snapshot row: composite keys
// use the multi-column JSON object (same helper/format as the CDC phase so
// downstream RMT merges match across the handoff boundary); other kinds keep
// the single-column form.
func snapshotMetadataKey(rpk resolvedPK, data map[string]any) string {
	if rpk.kind == pkKindComposite {
		return metadataKeyJSONMulti(rpk.columns, data)
	}
	return metadataKeyJSON(rpk.column, data)
}

// snapshotPrimaryKeyColumns declares the record's primary key columns:
// every composite key column (ordered) for pkKindComposite, the single
// column otherwise.
func snapshotPrimaryKeyColumns(rpk resolvedPK) []string {
	if rpk.kind == pkKindComposite {
		return append([]string(nil), rpk.columns...)
	}
	return []string{rpk.column}
}

func (r *snapshotCDCReader) getTableLastIDLocked(table string) int64 {
	if r.tableLastIDs != nil {
		if v, ok := r.tableLastIDs[table]; ok {
			return v
		}
	}
	if r.source != nil && table == r.source.table {
		return r.lastID
	}
	return 0
}

// cursorString renders a snapshot cell value into its lexicographic cursor
// form for pkKindOrdered columns (datetime/varchar/char). We rely on the
// driver returning time.Time for datetime/timestamp and string/[]byte for
// textual columns; both must sort consistently under string comparison in
// the same collation the DB used for ORDER BY.
func cursorString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		// Keep the driver's local wall-clock representation. The MySQL
		// connection is configured with loc=Local, and converting to UTC here
		// would make the next `WHERE col > ?` cursor differ from the ORDER BY
		// value around non-UTC deployments.
		// Use fixed-width microseconds so lexical comparison remains
		// chronological when a value has trailing zero fractional digits.
		return x.Format("2006-01-02 15:04:05.000000")
	case fmt.Stringer:
		return x.String()
	}
	return fmt.Sprint(v)
}

// numericOffset returns the record Offset for snapshot rows: the numeric PK
// value for pkKindNumeric tables (preserves legacy single-table cursor
// semantics), 0 otherwise. Offset is only consumed for checkpoint replay of
// snapshot cursors; string cursors are persisted via tableLastStr.
func numericOffset(rpk resolvedPK, data map[string]any) int64 {
	if rpk.kind != pkKindNumeric {
		return 0
	}
	if v, ok := data[rpk.column]; ok {
		if id, err := parseSnapshotID(v); err == nil {
			return id
		}
	}
	return 0
}

// metadataKeyJSON builds a JSON object representation of the primary key for a
// record, e.g. {"id":123} or {"audit_log_id":99}. This is the format Debezium
// and OpenETL's kafka sink encode into Metadata.Key, and the format doris/
// elasticsearch sinks with pk_columns_from_metadata expect when reading back
// from Kafka. The key is always emitted (even for deletes, where Data may be
// sparse) so downstream sinks can identify rows by PK. An empty pkColumn or a
// missing value yields "" (no key), which sinks treat as "no PK derivation".
func metadataKeyJSON(pkColumn string, data map[string]any) string {
	if pkColumn == "" {
		return ""
	}
	v, ok := data[pkColumn]
	if !ok || v == nil {
		return ""
	}
	b, err := json.Marshal(map[string]any{pkColumn: v})
	if err != nil {
		return ""
	}
	return string(b)
}

// metadataKeyJSONMulti builds a per-table primary-key JSON object from the
// given column names and row map, e.g. {"session_id":"abc"} or composite
// {"tenant_id":3,"id":42}. Empty pkColumns or a row missing every PK column
// yields "" (no key). Used by CDC sources (mysql_cdc, mysql_snapshot_cdc CDC
// phase) so downstream sinks with pk_columns_from_metadata can identify rows
// even on DELETE events where Data may be sparse.
func metadataKeyJSONMulti(pkColumns []string, row map[string]any) string {
	if len(pkColumns) == 0 {
		return ""
	}
	obj := make(map[string]any, len(pkColumns))
	haveAny := false
	for _, col := range pkColumns {
		v, ok := row[col]
		if !ok || v == nil {
			continue
		}
		obj[col] = v
		haveAny = true
	}
	if !haveAny {
		return ""
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	return string(b)
}

func (r *snapshotCDCReader) Read(ctx context.Context) (core.Record, error) {
	records := r.records
	errors := r.errors
	for records != nil || errors != nil {
		select {
		case rec, ok := <-records:
			if !ok {
				// Drain any pending error after the record stream has closed;
				// a closed error channel must not win a select while snapshot
				// rows are still buffered.
				records = nil
				continue
			}
			return rec, nil
		case err, ok := <-errors:
			if !ok {
				errors = nil
				continue
			}
			if err != nil {
				return core.Record{}, err
			}
		case <-ctx.Done():
			return core.Record{}, ctx.Err()
		}
	}
	return core.Record{}, io.EOF
}

func (r *snapshotCDCReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	batch := make([]core.Record, 0, n)
	for i := 0; i < n; i++ {
		rec, err := r.Read(ctx)
		if err != nil {
			if len(batch) > 0 {
				return batch, nil
			}
			return nil, err
		}
		batch = append(batch, rec)
	}
	return batch, nil
}

func (r *snapshotCDCReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	return r.checkpointFromRecords(ctx, nil)
}

func (r *snapshotCDCReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	return r.CheckpointForRecords(ctx, []core.Record{rec})
}

// CheckpointForRecords builds a candidate from the records that actually
// reached the sink boundary. Producer read-ahead cursors are intentionally not
// consulted; they may be many pages ahead of the durable sink position.
func (r *snapshotCDCReader) CheckpointForRecords(ctx context.Context, records []core.Record) (core.Checkpoint, error) {
	return r.checkpointFromRecords(ctx, records)
}

func (r *snapshotCDCReader) checkpointFromRecords(_ context.Context, records []core.Record) (core.Checkpoint, error) {
	r.mu.RLock()
	phase := r.checkpointPhase
	file := r.checkpointFile
	pos := r.checkpointPos
	lastID := r.lastID
	lastIDs := cloneIntCursorMap(r.tableLastIDs)
	lastStrs := cloneStringCursorMap(r.tableLastStr)
	lastComposite := cloneCompositeCursorMap(r.tableLastComposite)
	r.mu.RUnlock()
	if phase == "" {
		phase = "snapshot"
	}

	if len(records) == 0 {
		return marshalSnapshotCheckpoint(snapshotCDCPosition{
			Phase: phase, LastID: lastID, LastIDs: lastIDs, LastStrs: lastStrs,
			LastCompositeCursors: marshalCompositeCursors(lastComposite), File: file, Pos: pos,
		})
	}
	if r.source == nil {
		return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc source is unavailable while building checkpoint")
	}

	for _, rec := range records {
		if rec.Metadata.BinlogFile != "" {
			if rec.Metadata.BinlogPos == 0 {
				return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc CDC record %s has an empty binlog position", rec.Metadata.BinlogFile)
			}
			phase = "cdc"
			file = rec.Metadata.BinlogFile
			pos = rec.Metadata.BinlogPos
			continue
		}

		if phase == "cdc" {
			// Snapshot records must precede CDC records in the source channel. A
			// later snapshot record would make the handoff boundary ambiguous.
			return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot record arrived after CDC phase")
		}
		tableName := sourceSnapshotTable(rec)
		rpk := r.resolvedPKs[tableName]
		if rpk.kind == pkKindNumeric {
			value, ok := rec.Data[rpk.column]
			if !ok {
				return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot record for %s is missing cursor column %s", tableName, rpk.column)
			}
			id, err := parseSnapshotID(value)
			if err != nil {
				return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot cursor %s.%s: %w", tableName, rpk.column, err)
			}
			if current, ok := lastIDs[tableName]; !ok || id > current {
				lastIDs[tableName] = id
			}
			if tableName == r.source.table && lastIDs[tableName] > lastID {
				lastID = lastIDs[tableName]
			}
		} else if rpk.kind == pkKindOrdered {
			value, ok := rec.Data[rpk.column]
			if !ok {
				return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot record for %s is missing cursor column %s", tableName, rpk.column)
			}
			if value == nil {
				return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot cursor %s.%s is NULL", tableName, rpk.column)
			}
			cursor := cursorString(value)
			if current, ok := lastStrs[tableName]; !ok || cursor > current {
				lastStrs[tableName] = cursor
			}
		} else if rpk.kind == pkKindRowHash {
			// Keyless tables have no cursor to advance: every snapshot re-entry
			// re-reads the whole table, and the write is idempotent because the
			// destination upserts on the synthetic content key. The checkpoint
			// therefore only needs the handoff anchor asserted below.
			if _, ok := rec.Data[snapshotRowHashColumn]; !ok {
				return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot record for keyless table %s is missing the synthetic %s key", tableName, snapshotRowHashColumn)
			}
		} else if rpk.kind == pkKindComposite {
			tuple := make([]any, len(rpk.columns))
			for ci, colName := range rpk.columns {
				value, ok := rec.Data[colName]
				if !ok {
					return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot record for %s is missing cursor column %s", tableName, colName)
				}
				if value == nil {
					return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot cursor %s.%s is NULL", tableName, colName)
				}
				tuple[ci] = value
			}
			if current, ok := lastComposite[tableName]; !ok || compositeTupleGreater(tuple, current) {
				lastComposite[tableName] = tuple
			}
		} else {
			// Legacy unit/in-memory readers may not have resolvedPKs. Preserve
			// the historical numeric offset fallback for those single-table
			// positions; real Open() readers always resolve a key.
			if rec.Metadata.Offset != 0 {
				lastIDs[tableName] = rec.Metadata.Offset
				if tableName == r.source.table && rec.Metadata.Offset > lastID {
					lastID = rec.Metadata.Offset
				}
			} else {
				return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc cannot determine snapshot cursor for table %s", tableName)
			}
		}
		phase = "snapshot"
		if file == "" || pos == 0 {
			return core.Checkpoint{}, fmt.Errorf("mysql_snapshot_cdc snapshot handoff position is unavailable")
		}
	}

	return marshalSnapshotCheckpoint(snapshotCDCPosition{
		Phase: phase, LastID: lastID, LastIDs: lastIDs, LastStrs: lastStrs,
		LastCompositeCursors: marshalCompositeCursors(lastComposite), File: file, Pos: pos,
	})
}

// AckCheckpoint applies a snapshot/CDC cursor only after the checkpoint store
// has durably saved it. The producer's read-ahead maps remain untouched so a
// failed checkpoint cannot make the current reader skip rows.
func (r *snapshotCDCReader) AckCheckpoint(_ context.Context, cp core.Checkpoint) error {
	if len(cp.Position) == 0 || !json.Valid(cp.Position) {
		return fmt.Errorf("mysql_snapshot_cdc checkpoint position is not valid JSON")
	}
	var pos snapshotCDCPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		return fmt.Errorf("decode mysql_snapshot_cdc checkpoint: %w", err)
	}
	if pos.Phase != "snapshot" && pos.Phase != "cdc" {
		return fmt.Errorf("mysql_snapshot_cdc checkpoint has unsupported phase %q", pos.Phase)
	}
	if pos.Phase == "snapshot" && (pos.File == "" || pos.Pos == 0) {
		return fmt.Errorf("mysql_snapshot_cdc snapshot checkpoint is missing the CDC handoff position")
	}
	if pos.Phase == "cdc" && (pos.File == "" || pos.Pos == 0) {
		return fmt.Errorf("mysql_snapshot_cdc CDC checkpoint is missing the binlog position")
	}
	r.mu.Lock()
	r.checkpointPhase = pos.Phase
	r.checkpointFile = pos.File
	r.checkpointPos = pos.Pos
	r.lastID = pos.LastID
	r.tableLastIDs = cloneIntCursorMap(pos.LastIDs)
	if pos.LastID != 0 && r.source != nil && r.source.table != "" {
		if _, exists := r.tableLastIDs[r.source.table]; !exists {
			r.tableLastIDs[r.source.table] = pos.LastID
		}
	}
	r.tableLastStr = cloneStringCursorMap(pos.LastStrs)
	if len(pos.LastCompositeCursors) > 0 {
		// Ack path mirrors Open's restore typing. Reuse the same decode loop
		// by decoding through the canonical JSON position helper.
		restored := make(map[string][]any, len(pos.LastCompositeCursors))
		for table, raw := range pos.LastCompositeCursors {
			var arr []json.RawMessage
			if err := json.Unmarshal(raw, &arr); err != nil || len(arr) == 0 {
				continue
			}
			tuple := make([]any, len(arr))
			valid := true
			for i, el := range arr {
				var s string
				if err := json.Unmarshal(el, &s); err == nil {
					tuple[i] = s
					continue
				}
				var f float64
				if err := json.Unmarshal(el, &f); err == nil {
					tuple[i] = f
					continue
				}
				valid = false
				break
			}
			if valid {
				restored[table] = tuple
			}
		}
		r.tableLastComposite = restored
	}
	if pos.Phase == "snapshot" {
		r.snapshotHandoffFile = pos.File
		r.snapshotHandoffPos = pos.Pos
		r.snapshotHandoffValid = true
	}
	r.mu.Unlock()
	return nil
}

func sourceSnapshotTable(rec core.Record) string {
	if rec.Data != nil {
		if v, ok := rec.Data["_source_table"].(string); ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	return rec.Metadata.Table
}

func marshalSnapshotCheckpoint(pos snapshotCDCPosition) (core.Checkpoint, error) {
	data, err := json.Marshal(pos)
	if err != nil {
		return core.Checkpoint{}, fmt.Errorf("marshal mysql_snapshot_cdc checkpoint: %w", err)
	}
	return core.Checkpoint{Source: "mysql_snapshot_cdc", Position: data, Timestamp: time.Now()}, nil
}

func (r *snapshotCDCReader) Close() error {
	if r.db != nil {
		_ = r.db.Close()
	}
	r.closeCanal()
	return nil
}

func (r *snapshotCDCReader) getPhase() string      { r.mu.RLock(); defer r.mu.RUnlock(); return r.phase }
func (r *snapshotCDCReader) setPhase(phase string) { r.mu.Lock(); defer r.mu.Unlock(); r.phase = phase }

func (r *snapshotCDCReader) getBinlogPos() (string, uint32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.file, r.pos
}

func (r *snapshotCDCReader) getDurableBinlogPos() (string, uint32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.checkpointFile, r.checkpointPos
}

func (r *snapshotCDCReader) getSnapshotHandoffPosition() (string, uint32, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotHandoffFile, r.snapshotHandoffPos,
		r.snapshotHandoffValid && r.snapshotHandoffFile != "" && r.snapshotHandoffPos != 0
}

// advanceResumePos atomically moves both the durable checkpoint position and
// the producer runtime cursor to a new binlog position. It is used by the
// binlog-purged recovery path (resume_from_current / resnapshot handoff) so
// the next canal.RunFrom starts from the advanced position instead of the
// stale one that triggered ERROR 1236.
func (r *snapshotCDCReader) advanceResumePos(file string, pos uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkpointFile = file
	r.checkpointPos = pos
	r.file = file
	r.pos = pos
}

func (r *snapshotCDCReader) getCanal() *canal.Canal {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.canal
}

func (r *snapshotCDCReader) setCanal(c *canal.Canal) {
	r.mu.Lock()
	r.canal = c
	r.mu.Unlock()
}

func (r *snapshotCDCReader) clearCanal(c *canal.Canal) {
	r.mu.Lock()
	if r.canal == c {
		r.canal = nil
	}
	r.mu.Unlock()
}

func (r *snapshotCDCReader) closeCanal() {
	r.mu.Lock()
	c := r.canal
	r.canal = nil
	r.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

type snapshotCDCHandler struct {
	canal.DummyEventHandler
	reader *snapshotCDCReader
}

func (h *snapshotCDCHandler) OnRow(e *canal.RowsEvent) error {
	file, pos := h.reader.getBinlogPos()
	// Source-side column metainfo (canal reads information_schema itself):
	// lets auto_create sinks rebuild real source types instead of guessing
	// from sample values / name hints (BUG-6). Mirrors mysql_cdc OnRow so the
	// CDC phase carries the same declared types the snapshot phase fills via
	// tableColumnTypes.
	colTypes := map[string]string{}
	for _, col := range e.Table.Columns {
		raw := col.RawType
		if raw == "" {
			continue
		}
		if col.IsUnsigned && !strings.Contains(raw, "unsigned") {
			raw += " unsigned"
		}
		colTypes[col.Name] = raw
	}
	// Derive primary-key column names for this CDC event so the per-row key
	// JSON object can be built for downstream sinks using
	// pk_columns_from_metadata. Priority:
	//   1. the resolvedPKs map (built at Open from information_schema /
	//      explicit overrides) — authoritative when the snapshot resolved a
	//      non-empty key for this table;
	//   2. the canal row event's own PK columns (handles composite keys and
	//      tables that were skipped during snapshot, e.g. skip_no_pk_tables,
	//      or added via DDL after Open).
	// Canal reports composite PKs as a multi-element index slice, so use the
	// multi-column helper rather than the old single-column fallback that
	// silently dropped keys for any table whose PK was not a single column.
	var pkCols []string
	if rpk, ok := h.reader.resolvedPKs[e.Table.Name]; ok && (rpk.kind == pkKindComposite && len(rpk.columns) > 0 || rpk.column != "") {
		if rpk.kind == pkKindComposite {
			pkCols = append([]string(nil), rpk.columns...)
		} else {
			pkCols = []string{rpk.column}
		}
	} else {
		pkCols = pkColumnNames(e.Table)
	}
	for i := 0; i < len(e.Rows); i++ {
		row := e.Rows[i]
		rec := core.Record{Metadata: core.Metadata{
			Source:            h.reader.source.name,
			SourceType:        core.SourceTypeMySQLSnapshotCDC,
			SourcePhase:       core.SourcePhaseCDC,
			Database:          h.reader.source.database,
			Table:             e.Table.Name,
			Timestamp:         time.Now(),
			BinlogFile:        file,
			BinlogPos:         pos,
			PrimaryKeyColumns: append([]string(nil), pkCols...),
			ColumnTypes:       colTypes,
		}}
		switch e.Action {
		case canal.InsertAction:
			rec.Operation = core.OpInsert
			rec.Data = rowToMap(e.Table.Columns, row)
		case canal.UpdateAction:
			rec.Operation = core.OpUpdate
			rec.Before = rowToMap(e.Table.Columns, row)
			i++
			if i < len(e.Rows) {
				rec.Data = rowToMap(e.Table.Columns, e.Rows[i])
			}
		case canal.DeleteAction:
			rec.Operation = core.OpDelete
			rec.Data = rowToMap(e.Table.Columns, row)
		}
		// Emit the primary key as a JSON object so downstream sinks reading
		// this record via Kafka (doris/clickhouse/es with
		// pk_columns_from_metadata) can derive the key columns without a static
		// pk_columns config. Uses the after-image (rec.Data) which always
		// carries the PK for insert/update; for delete, rec.Data is the deleted
		// row which also carries the PK. Prefer the before-image on update for
		// stable identity when present.
		keyRow := rec.Data
		if rec.Operation == core.OpUpdate && len(rec.Before) > 0 {
			keyRow = rec.Before
		}
		rec.Metadata.Key = metadataKeyJSONMulti(pkCols, keyRow)
		select {
		case h.reader.records <- rec:
		case <-h.reader.done:
			return fmt.Errorf("reader closed")
		}
	}
	return nil
}

func (h *snapshotCDCHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	h.reader.mu.Lock()
	defer h.reader.mu.Unlock()
	h.reader.file = nextPos.Name
	h.reader.pos = uint32(nextPos.Pos)
	return nil
}

func (h *snapshotCDCHandler) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	h.reader.mu.Lock()
	defer h.reader.mu.Unlock()
	h.reader.file = string(e.NextLogName)
	h.reader.pos = uint32(e.Position)
	return nil
}

// OnDDL captures DDL events during the CDC phase.
func (h *snapshotCDCHandler) OnDDL(header *replication.EventHeader, p mysql.Position, e *replication.QueryEvent) error {
	ddl := string(e.Query)
	if ddl == "" {
		return nil
	}
	h.reader.mu.Lock()
	h.reader.file = p.Name
	h.reader.pos = uint32(p.Pos)
	h.reader.mu.Unlock()

	rec := core.Record{
		Operation: core.OpDDL,
		Metadata: core.Metadata{
			Source:      h.reader.source.name,
			SourceType:  core.SourceTypeMySQLSnapshotCDC,
			SourcePhase: core.SourcePhaseCDC,
			Database:    h.reader.source.database,
			Table:       extractDDLTable(ddl),
			Timestamp:   time.Now(),
			BinlogFile:  p.Name,
			BinlogPos:   uint32(p.Pos),
			DDL:         ddl,
		},
	}
	select {
	case h.reader.records <- rec:
	case <-h.reader.done:
		return fmt.Errorf("reader closed")
	}
	return nil
}

func (h *snapshotCDCHandler) String() string { return "MySQLSnapshotCDCHandler" }

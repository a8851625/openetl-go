package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
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
	limit                  int
	serverID               uint32
	shardIndex             int
	shardTotal             int
	serverIDBase           uint32
	consistentSnapshotLock bool
}

// resolvedPK holds the per-table snapshot key decision made at Open time.
// kind tells runSnapshot how to advance the cursor for that table.
type resolvedPKKind int

const (
	pkKindNone    resolvedPKKind = iota // table has no usable snapshot key; skip snapshot, CDC only
	pkKindNumeric                       // integer-like column: ORDER BY col, numeric cursor, optional MOD sharding
	pkKindOrdered                       // non-numeric but orderable column (e.g. datetime/varchar): ORDER BY col, string cursor
)

type resolvedPK struct {
	column string
	kind   resolvedPKKind
}

type snapshotCDCPosition struct {
	Phase    string            `json:"phase"`
	LastID   int64             `json:"last_id"`             // backward-compatible single-table cursor
	LastIDs  map[string]int64  `json:"last_ids,omitempty"`  // per-table numeric snapshot cursors
	LastStrs map[string]string `json:"last_strs,omitempty"` // per-table non-numeric snapshot cursors
	File     string            `json:"file"`
	Pos      uint32            `json:"pos"`
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
	// snapshotRead* are producer-only cursors. They may run ahead of the sink;
	// a restart discards them and reloads tableLast* from the durable checkpoint.
	snapshotReadIDs      map[string]int64
	snapshotReadStr      map[string]string
	file                 string
	pos                  uint32
	snapshotHandoffFile  string
	snapshotHandoffPos   uint32
	snapshotHandoffValid bool

	// resolvedPKs is the per-table snapshot key decision, populated at Open
	// after the real table list is known. Tables with pkKindNone are skipped
	// during snapshot and only streamed via CDC.
	resolvedPKs map[string]resolvedPK

	done chan struct{}
}

func NewMySQLSnapshotCDCSource(config map[string]any) (*MySQLSnapshotCDCSource, error) {
	s := &MySQLSnapshotCDCSource{name: "mysql_snapshot_cdc", port: 3306, pkCol: "id", limit: 1000, consistentSnapshotLock: true}
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
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name`,
		s.database,
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
		if rpk.kind == pkKindNone && !wholeDB && !s.skipNoPKTables {
			return nil, fmt.Errorf("mysql_snapshot_cdc: table %s has no single-column primary key and no pk_column override; set pk_columns or skip_no_pk_tables=true", t)
		}
		out[t] = rpk
	}
	return out, nil
}

// resolveOnePK resolves the snapshot key for a single table.
func (s *MySQLSnapshotCDCSource) resolveOnePK(ctx context.Context, db *sql.DB, table string) (resolvedPK, error) {
	// 1. explicit per-table override
	if col, ok := s.pkCols[table]; ok && col != "" {
		return s.classifyPK(ctx, db, table, col)
	}
	// 2. global pkCol fallback (legacy)
	if s.pkCol != "" {
		// Confirm the column exists on this table; if it does not, fall through
		// to auto-detection so a wrong global default does not abort a
		// whole-database snapshot at the first table without an `id` column.
		if exists, err := columnExists(ctx, db, s.database, table, s.pkCol); err != nil {
			return resolvedPK{}, err
		} else if exists {
			return s.classifyPK(ctx, db, table, s.pkCol)
		}
	}
	// 3. auto-detect single-column PRIMARY KEY
	pk, err := detectSingleColumnPK(ctx, db, s.database, table)
	if err != nil {
		return resolvedPK{}, err
	}
	if pk == "" {
		return resolvedPK{kind: pkKindNone}, nil
	}
	return s.classifyPKByName(ctx, db, table, pk)
}

// classifyPK validates that the chosen column exists and decides numeric vs
// ordered cursor based on its declared column_type.
func (s *MySQLSnapshotCDCSource) classifyPK(ctx context.Context, db *sql.DB, table, col string) (resolvedPK, error) {
	ct, err := columnType(ctx, db, s.database, table, col)
	if err != nil {
		return resolvedPK{}, err
	}
	return resolvedPK{column: col, kind: pkKindForType(ct)}, nil
}

// classifyPKByName is like classifyPK but the caller has already confirmed
// the column exists (it came from the PK detection query); we only need its
// type. Falls back to numeric if the type lookup fails for any reason.
func (s *MySQLSnapshotCDCSource) classifyPKByName(ctx context.Context, db *sql.DB, table, col string) (resolvedPK, error) {
	ct, _ := columnType(ctx, db, s.database, table, col)
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

func detectSingleColumnPK(ctx context.Context, db *sql.DB, database, table string) (string, error) {
	// A snapshot cursor needs a single-column key to page with WHERE col > ?.
	// Composite primary keys cannot be expressed as one ordered cursor, so we
	// return "" and let the caller skip the snapshot for that table.
	//
	// We read from information_schema.columns (column_key='PRI') rather than
	// joining key_column_usage to table_constraints: on MySQL 8.0 the latter
	// JOIN can yield duplicate rows for the PRIMARY constraint because
	// constraint_name 'PRIMARY' is not unique across schemas/databases, which
	// would make a real single-column PK look like a composite one.
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

	// CDC reconnect loop with exponential backoff.
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
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
			// No usable snapshot key for this table: skip historical snapshot,
			// it will still be covered by the CDC phase (binlog is key-agnostic).
			g.Log().Warningf(ctx, "mysql_snapshot_cdc: skipping snapshot for table %s (no single-column primary key or pk_column configured); CDC phase will still capture changes", tableName)
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
		return mysql.Position{}, fmt.Errorf("canal is unavailable")
	}
	pos, err := c.GetMasterPos()
	if err != nil {
		return mysql.Position{}, err
	}
	if pos.Name == "" || pos.Pos == 0 {
		return mysql.Position{}, fmt.Errorf("mysql_snapshot_cdc returned an invalid CDC handoff position %q:%d", pos.Name, pos.Pos)
	}
	r.mu.Lock()
	r.snapshotHandoffFile = pos.Name
	r.snapshotHandoffPos = uint32(pos.Pos)
	r.snapshotHandoffValid = true
	if r.checkpointFile == "" || r.checkpointPos == 0 {
		r.checkpointFile = pos.Name
		r.checkpointPos = uint32(pos.Pos)
	}
	r.mu.Unlock()
	return pos, nil
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
			if v, ok := data[rpk.column]; ok {
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
			rec := core.Record{Operation: core.OpInsert, Data: data, Metadata: core.Metadata{Source: r.source.name, Database: r.source.database, Table: tableName, Timestamp: time.Now(), Offset: numericOffset(rpk, data)}}
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

func (r *snapshotCDCReader) primeSnapshotReadView(ctx context.Context, tx *sql.Tx) error {
	if len(r.source.tables) == 0 {
		return nil
	}
	// Prime the consistent read view against the first table that has a
	// usable snapshot key. The query only needs to establish the MVCC read
	// view inside the REPEATABLE READ transaction; any qualifying table works.
	for _, tableName := range r.source.tables {
		rpk, ok := r.resolvedPKs[tableName]
		if !ok || rpk.kind == pkKindNone {
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
	r.mu.RUnlock()
	if phase == "" {
		phase = "snapshot"
	}

	if len(records) == 0 {
		return marshalSnapshotCheckpoint(snapshotCDCPosition{
			Phase: phase, LastID: lastID, LastIDs: lastIDs, LastStrs: lastStrs, File: file, Pos: pos,
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
		Phase: phase, LastID: lastID, LastIDs: lastIDs, LastStrs: lastStrs, File: file, Pos: pos,
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
	for i := 0; i < len(e.Rows); i++ {
		row := e.Rows[i]
		rec := core.Record{Metadata: core.Metadata{Source: h.reader.source.name, Database: h.reader.source.database, Table: e.Table.Name, Timestamp: time.Now(), BinlogFile: file, BinlogPos: pos}}
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
			Source:     h.reader.source.name,
			Database:   h.reader.source.database,
			Table:      extractDDLTable(ddl),
			Timestamp:  time.Now(),
			BinlogFile: p.Name,
			BinlogPos:  uint32(p.Pos),
			DDL:        ddl,
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

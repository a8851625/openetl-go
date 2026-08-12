package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("mysql_cdc", func(config map[string]any) (core.Source, error) {
		return NewMySQLCDCSource(config)
	})
}

type mysqlCDCRecordReader struct {
	source  *MySQLCDCSource
	records chan core.Record
	errors  chan error

	mu          sync.RWMutex
	lastPosName string
	lastPos     uint32
	lastGTID    string
	done        chan struct{}
}

type MySQLCDCSource struct {
	name         string
	host         string
	port         int
	user         string
	password     string
	database     string
	tables       []string
	serverID     uint32
	loc          *time.Location
	shardIndex   int
	shardTotal   int
	serverIDBase uint32
	enableGTID   bool
	startFrom    string // "timestamp" or "binlog:file:pos" or "gtid:..."
	// binlogPurgedPolicy controls recovery when the checkpointed binlog file
	// no longer exists on the server (ERROR 1236). Default fail-closed.
	binlogPurgedPolicy BinlogPurgedPolicy
}

func NewMySQLCDCSource(config map[string]any) (*MySQLCDCSource, error) {
	s := &MySQLCDCSource{name: "mysql_cdc"}

	if v, ok := config["name"]; ok {
		s.name = v.(string)
	}
	if v, ok := config["host"]; ok {
		s.host = v.(string)
	} else {
		v, _ := g.Cfg().Get(context.Background(), "mysql_cdc.host")
		s.host = v.String()
	}
	if v, ok := config["port"]; ok {
		switch p := v.(type) {
		case int:
			s.port = p
		case float64:
			s.port = int(p)
		default:
			s.port = 3306
		}
	} else {
		s.port = 3306
	}
	if v, ok := config["user"]; ok {
		s.user = v.(string)
	}
	if v, ok := config["password"]; ok {
		s.password = v.(string)
	}
	if v, ok := config["database"]; ok {
		s.database = v.(string)
	}
	s.tables = append(s.tables, readStringSlice(config, "tables")...)
	if v, ok := config["server_id"]; ok {
		switch id := v.(type) {
		case int:
			s.serverID = uint32(id)
		case float64:
			s.serverID = uint32(id)
		case uint32:
			s.serverID = id
		default:
			s.serverID = 0
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
	if v, ok := config["enable_gtid"]; ok {
		if b, ok := v.(bool); ok {
			s.enableGTID = b
		}
	}
	if v, ok := config["start_from"]; ok {
		s.startFrom = v.(string)
	}
	s.binlogPurgedPolicy = parseBinlogPurgedPolicy(config)

	// Default server_id: when sharding, base+shard; otherwise random per-instance
	// to avoid collisions with multiple consumers on the same MySQL.
	if s.serverID == 0 {
		if s.shardTotal > 0 && s.serverIDBase > 0 {
			s.serverID = s.serverIDBase + uint32(s.shardIndex)
		} else {
			// Generate a stable pseudo-random server_id based on hostname+pid.
			// Avoids hard-coded 1001 which collides across instances.
			s.serverID = deriveServerID(s.name)
		}
	}

	// Resolve timezone without mutating global time.Local.
	if tz := os.Getenv("TZ"); tz != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			s.loc = loc
		} else if tz == "CST-8" {
			s.loc = time.FixedZone("CST", 8*3600)
		}
	}
	if s.loc == nil {
		s.loc = time.Local
	}

	// Apply table partitioning across shards: shard i processes tables
	// [i, i+total, i+2*total, ...]. With a single table this is a no-op
	// (every shard sees the same table) — for that case users should use
	// a parallel PK-sharded approach via mysql_snapshot_cdc.
	if s.shardTotal > 1 && len(s.tables) > 0 {
		partitioned := make([]string, 0, len(s.tables)/s.shardTotal+1)
		for i := s.shardIndex; i < len(s.tables); i += s.shardTotal {
			partitioned = append(partitioned, s.tables[i])
		}
		s.tables = partitioned
	}

	if s.database == "" {
		return nil, fmt.Errorf("mysql_cdc source: database is required")
	}
	if s.host == "" {
		return nil, fmt.Errorf("mysql_cdc source: host is required")
	}
	if s.user == "" {
		return nil, fmt.Errorf("mysql_cdc source: user is required")
	}

	return s, nil
}

func (s *MySQLCDCSource) Name() string { return s.name }

func (s *MySQLCDCSource) Describe(ctx context.Context) (core.SchemaInfo, error) {
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

func (s *MySQLCDCSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	if err := s.ValidateCheckpoint(ctx, cp); err != nil {
		return nil, err
	}
	reader := &mysqlCDCRecordReader{
		source:  s,
		records: make(chan core.Record, 1024),
		errors:  make(chan error, 64),
		done:    make(chan struct{}),
	}

	reader.lastPosName = "master"
	if cp != nil {
		pos, err := decodeMySQLCDCCheckpointPosition(cp.Position)
		if err != nil {
			return nil, fmt.Errorf("decode mysql_cdc checkpoint: %w", err)
		}
		if pos.File != "" {
			g.Log().Infof(ctx, "Resuming from checkpoint: %s:%d", pos.File, pos.Pos)
			reader.lastPosName = pos.File
			reader.lastPos = pos.Pos
		}
		if pos.GTID != "" {
			reader.lastGTID = pos.GTID
		}
	}

	// Historical backfill: if start_from is configured and no checkpoint
	// exists, parse the start position. Supports:
	//   start_from: "binlog:mysql-bin.000003:12345" (explicit file:pos)
	//   start_from: "gtid:3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
	// Timestamp-based lookup is intentionally rejected until we implement a
	// real timestamp→binlog resolver; silently starting from current master pos
	// would lose data for users expecting historical replay.
	hasCheckpoint := cp != nil && len(cp.Position) > 0
	if !hasCheckpoint && s.startFrom != "" {
		if strings.HasPrefix(s.startFrom, "gtid:") {
			reader.lastGTID = strings.TrimPrefix(s.startFrom, "gtid:")
			g.Log().Infof(ctx, "start_from GTID: %s", reader.lastGTID)
		} else if strings.HasPrefix(s.startFrom, "binlog:") {
			parts := strings.SplitN(strings.TrimPrefix(s.startFrom, "binlog:"), ":", 2)
			if len(parts) == 2 {
				reader.lastPosName = parts[0]
				fmt.Sscanf(parts[1], "%d", &reader.lastPos)
				g.Log().Infof(ctx, "start_from binlog: %s:%d", reader.lastPosName, reader.lastPos)
			}
		} else {
			return nil, fmt.Errorf("mysql_cdc start_from %q is unsupported; use binlog:<file>:<pos> or gtid:<set>", s.startFrom)
		}
	}

	go func() {
		defer close(reader.records)
		defer close(reader.errors)

		backoff := time.Second
		const maxBackoff = 30 * time.Second
		for {
			if ctx.Err() != nil {
				return
			}

			c, runErr := s.startCanalOnce(ctx, reader)
			if c != nil {
				c.Close()
			}

			if ctx.Err() != nil {
				return
			}
			if runErr == nil {
				// Canal exited cleanly (e.g. context cancelled). Treat as done.
				return
			}

			// BUG-2: binlog purged / ERROR 1236 is not a transient disconnect.
			// The checkpointed file no longer exists; retrying the same position
			// loops forever. Apply the configured recovery policy instead.
			if isBinlogPurgedError(runErr) {
				staleFile := reader.lastPosName
				stalePos := reader.lastPos
				g.Log().Errorf(ctx, "mysql_cdc binlog purged (ERROR 1236): checkpoint %s:%d no longer exists on server; policy=%s", staleFile, stalePos, s.binlogPurgedPolicy)
				switch s.binlogPurgedPolicy {
				case BinlogPurgedResumeFromCurrent:
					// Advance the resume position to the current master position.
					// This silently drops every change between the stale checkpoint
					// and now (explicit RPO loss); log it loudly.
					probe, perr := canal.NewCanal(s.canalConfig())
					if perr == nil {
						curPos, gerr := probe.GetMasterPos()
						probe.Close()
						if gerr == nil {
							reader.mu.Lock()
							reader.lastPosName = curPos.Name
							reader.lastPos = curPos.Pos
							reader.mu.Unlock()
							g.Log().Warningf(ctx, "mysql_cdc resuming from current master pos %s:%d; changes between stale checkpoint and now are DROPPED", curPos.Name, curPos.Pos)
							backoff = time.Second // reset backoff after position advance
							continue
						}
						runErr = fmt.Errorf("binlog purged recovery: get master pos: %w", gerr)
					} else {
						runErr = fmt.Errorf("binlog purged recovery: create probe canal: %w", perr)
					}
				}
				// fail (default) or resnapshot (unsupported on plain mysql_cdc): fatal.
				select {
				case reader.errors <- fmt.Errorf("%w: %v", ErrBinlogPurged, runErr):
				case <-ctx.Done():
				}
				return
			}

			// Report the error but do not terminate; we will retry.
			g.Log().Warningf(ctx, "mysql_cdc canal exited: %v; reconnecting in %s", runErr, backoff)
			select {
			case reader.errors <- fmt.Errorf("canal disconnected: %w", runErr):
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
	}()

	return reader, nil
}

// canalConfig builds the canal configuration shared by the CDC loop and the
// binlog-purged recovery probe (GetMasterPos needs a live canal).
func (s *MySQLCDCSource) canalConfig() *canal.Config {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.host, s.port)
	cfg.User = s.user
	cfg.Password = s.password
	cfg.Flavor = "mysql"
	cfg.ServerID = s.serverID
	cfg.Dump.ExecutionPath = ""
	for _, table := range s.tables {
		if table == "*" {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex,
				fmt.Sprintf("%s\\..*", s.database))
		} else {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex,
				fmt.Sprintf("%s\\.%s", s.database, regexp.QuoteMeta(table)))
		}
	}
	return cfg
}

// startCanalOnce creates a fresh canal, attaches the event handler, and runs it
// until it exits (error or context cancel). Returns the canal so the caller can
// close it. The caller must NOT use the returned canal after Close.
func (s *MySQLCDCSource) startCanalOnce(ctx context.Context, reader *mysqlCDCRecordReader) (*canal.Canal, error) {
	cfg := s.canalConfig()

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("create canal (host %s:%d, db %s, serverID %d): %w", s.host, s.port, s.database, s.serverID, err) // P5-15: WHERE context
	}
	c.SetEventHandler(&mysqlCDCHandler{reader: reader})

	// Read the latest known position/GTID from the reader (updated by
	// OnRotate/OnXID/OnPosSynced and checkpoints).
	reader.mu.RLock()
	lastGTID := reader.lastGTID
	lastPosName := reader.lastPosName
	lastPos := reader.lastPos
	reader.mu.RUnlock()

	runErr := func() error {
		if lastGTID != "" {
			g.Log().Infof(ctx, "Starting CDC from GTID: %s", lastGTID)
			gtidSet, err := mysql.ParseGTIDSet("mysql", lastGTID)
			if err == nil {
				return c.StartFromGTID(gtidSet)
			}
			g.Log().Warningf(ctx, "Failed to parse GTID %q, falling back to file:pos", lastGTID)
		}

		pos, err := c.GetMasterPos()
		if err != nil {
			return fmt.Errorf("get master pos: %w", err)
		}
		if lastPosName == "master" && lastPos == 0 {
			reader.mu.Lock()
			reader.lastPosName = pos.Name
			reader.lastPos = pos.Pos
			reader.mu.Unlock()
		} else if lastPosName != "master" && lastPos > 0 {
			pos = mysql.Position{Name: lastPosName, Pos: lastPos}
		}
		g.Log().Infof(ctx, "Starting CDC from %s:%d", pos.Name, pos.Pos)
		return c.RunFrom(pos)
	}()

	return c, runErr
}

type mysqlCDCPosition struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
	GTID string `json:"gtid,omitempty"`
}

func (r *mysqlCDCRecordReader) Read(ctx context.Context) (core.Record, error) {
	select {
	case rec, ok := <-r.records:
		if !ok {
			return core.Record{}, io.EOF
		}
		return rec, nil
	case err, ok := <-r.errors:
		if !ok {
			return core.Record{}, io.EOF
		}
		return core.Record{}, err
	case <-ctx.Done():
		return core.Record{}, ctx.Err()
	}
}

func (r *mysqlCDCRecordReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	var batch []core.Record
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

func (r *mysqlCDCRecordReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	pos := mysqlCDCPosition{
		File: r.lastPosName,
		Pos:  r.lastPos,
		GTID: r.lastGTID,
	}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{
		Source:    "mysql_cdc",
		Position:  data,
		Timestamp: time.Now(),
	}, nil
}

func (r *mysqlCDCRecordReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	r.mu.RLock()
	pos := mysqlCDCPosition{File: r.lastPosName, Pos: r.lastPos, GTID: r.lastGTID}
	r.mu.RUnlock()
	if rec.Metadata.BinlogFile != "" {
		pos.File = rec.Metadata.BinlogFile
		pos.Pos = rec.Metadata.BinlogPos
	}
	if rec.Metadata.Gtid != "" {
		pos.GTID = rec.Metadata.Gtid
	}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "mysql_cdc", Position: data, Timestamp: time.Now()}, nil
}

func (r *mysqlCDCRecordReader) Close() error {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
	return nil
}

type mysqlCDCHandler struct {
	canal.DummyEventHandler
	reader *mysqlCDCRecordReader
}

func (h *mysqlCDCHandler) OnRow(e *canal.RowsEvent) error {
	tableName := e.Table.Name
	now := time.Now()

	// Source-side column metainfo (canal library reads information_schema
	// itself): lets auto_create sinks rebuild real source types instead of
	// guessing from sample values / name hints.
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

	// Primary-key column names for this table (canal fills e.Table.PKColumns
	// with column indexes from information_schema). Used to build a per-row
	// JSON key object so downstream sinks with pk_columns_from_metadata can
	// identify rows on DELETE/UPDATE events where Data may be sparse. Without
	// this, CDC DELETE records reach the sink with an empty Metadata.Key and
	// pk_columns_from_metadata falls back to the static pk_columns (often
	// "id"), producing "delete record missing primary-key column" errors.
	pkCols := pkColumnNames(e.Table)

	h.reader.mu.RLock()
	file := h.reader.lastPosName
	pos := h.reader.lastPos
	gtid := h.reader.lastGTID
	h.reader.mu.RUnlock()

	for i := 0; i < len(e.Rows); i++ {
		row := e.Rows[i]
		rec := core.Record{
			Metadata: core.Metadata{
				Source:      h.reader.source.name,
				Database:    h.reader.source.database,
				Table:       tableName,
				Timestamp:   now,
				BinlogFile:  file,
				BinlogPos:   pos,
				Gtid:        gtid,
				ColumnTypes: colTypes,
			},
		}

		switch e.Action {
		case canal.InsertAction:
			rec.Operation = core.OpInsert
			rec.Data = rowToMap(e.Table.Columns, row)
			rec.Metadata.Key = metadataKeyJSONMulti(pkCols, rec.Data)
		case canal.UpdateAction:
			rec.Operation = core.OpUpdate
			if i%2 == 0 {
				rec.Before = rowToMap(e.Table.Columns, row)
				i++
				if i < len(e.Rows) {
					rec.Data = rowToMap(e.Table.Columns, e.Rows[i])
				}
			} else {
				rec.Data = rowToMap(e.Table.Columns, row)
			}
			// Prefer the before-image for the key (stable identity); fall back
			// to the after-image when no before is present.
			keyRow := rec.Before
			if len(keyRow) == 0 {
				keyRow = rec.Data
			}
			rec.Metadata.Key = metadataKeyJSONMulti(pkCols, keyRow)
		case canal.DeleteAction:
			rec.Operation = core.OpDelete
			rec.Data = rowToMap(e.Table.Columns, row)
			rec.Metadata.Key = metadataKeyJSONMulti(pkCols, rec.Data)
		}

		select {
		case h.reader.records <- rec:
		case <-h.reader.done:
			return fmt.Errorf("reader closed")
		}
	}

	return nil
}

func (h *mysqlCDCHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	h.reader.mu.Lock()
	defer h.reader.mu.Unlock()
	h.reader.lastPosName = nextPos.Name
	h.reader.lastPos = uint32(nextPos.Pos)
	return nil
}

func (h *mysqlCDCHandler) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	h.reader.mu.Lock()
	defer h.reader.mu.Unlock()
	h.reader.lastPosName = string(e.NextLogName)
	h.reader.lastPos = uint32(e.Position)
	return nil
}

// OnGTID captures GTID events for HA failover support.
func (h *mysqlCDCHandler) OnGTID(header *replication.EventHeader, e mysql.BinlogGTIDEvent) error {
	return nil
}

// OnPosSynced is the canonical position+GTID sync callback. We use this
// to capture both file:pos and GTID in one place.
func (h *mysqlCDCHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	h.reader.mu.Lock()
	defer h.reader.mu.Unlock()
	h.reader.lastPosName = pos.Name
	h.reader.lastPos = uint32(pos.Pos)
	if set != nil {
		h.reader.lastGTID = set.String()
	}
	return nil
}

// OnDDL captures DDL events (ALTER TABLE, CREATE TABLE, DROP COLUMN, etc.)
// and emits them as OpDDL records. Sinks with auto_apply_ddl enabled will
// execute the DDL statement on the target; others will see it in metadata
// and can log or ignore it.
func (h *mysqlCDCHandler) OnDDL(header *replication.EventHeader, p mysql.Position, e *replication.QueryEvent) error {
	ddl := string(e.Query)
	if ddl == "" {
		return nil
	}
	h.reader.mu.Lock()
	h.reader.lastPosName = p.Name
	h.reader.lastPos = uint32(p.Pos)
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

// extractDDLTable attempts to extract the table name from a DDL statement
// for metadata tagging. Returns empty string if it can't be parsed.
func extractDDLTable(ddl string) string {
	ddl = strings.TrimSpace(ddl)
	if len(ddl) < 6 {
		return ""
	}
	// Look for patterns: TABLE `name`, TABLE name, TABLE db.name
	upper := strings.ToUpper(ddl)
	for _, keyword := range []string{"ALTER TABLE", "CREATE TABLE", "DROP TABLE", "RENAME TABLE"} {
		idx := strings.Index(upper, keyword)
		if idx < 0 {
			continue
		}
		rest := ddl[idx+len(keyword):]
		rest = strings.TrimSpace(rest)
		if rest == "" {
			return ""
		}
		// Check for IF NOT EXISTS / IF EXISTS
		restUpper := strings.ToUpper(rest)
		if strings.HasPrefix(restUpper, "IF NOT EXISTS") {
			rest = rest[len("IF NOT EXISTS"):]
			rest = strings.TrimSpace(rest)
		} else if strings.HasPrefix(restUpper, "IF EXISTS") {
			rest = rest[len("IF EXISTS"):]
			rest = strings.TrimSpace(rest)
		}
		// Extract table name (may be backtick-quoted, or bare, or db.table)
		if strings.HasPrefix(rest, "`") {
			end := strings.Index(rest[1:], "`")
			if end >= 0 {
				return rest[1 : 1+end]
			}
		}
		// Bare name: take up to whitespace or comma or parenthesis or semicolon
		endIdx := len(rest)
		for i, ch := range rest {
			if ch == ' ' || ch == '\t' || ch == '\n' || ch == '(' || ch == ',' || ch == ';' {
				endIdx = i
				break
			}
		}
		name := rest[:endIdx]
		// Strip db. prefix
		if dot := strings.LastIndex(name, "."); dot >= 0 {
			name = name[dot+1:]
		}
		return name
	}
	return ""
}

func (h *mysqlCDCHandler) String() string { return "MySQLCDCHandler" }

func rowToMap(columns []schema.TableColumn, row []interface{}) map[string]any {
	m := make(map[string]any, len(columns))
	for i, col := range columns {
		if i < len(row) {
			m[col.Name] = row[i]
		}
	}
	return m
}

// deriveServerID produces a pseudo-random MySQL replication server_id in the
// valid range [1, 2^32-1] from the pipeline name. This avoids the previous
// hard-coded 1001 that caused collisions when multiple pipeline instances
// connected to the same MySQL as replicas.
func deriveServerID(name string) uint32 {
	var h uint32 = 2166136261 // FNV-1a 32-bit offset basis
	for i := 0; i < len(name); i++ {
		h ^= uint32(name[i])
		h *= 16777619
	}
	if h == 0 {
		h = 1
	}
	return h
}

// pkColumnNames returns the primary-key column names for a canal table, derived
// from the schema.Table.PKColumns index slice. Returns nil when the table has
// no declared primary key (canal could not introspect one).
func pkColumnNames(tbl *schema.Table) []string {
	if tbl == nil || len(tbl.PKColumns) == 0 {
		return nil
	}
	cols := make([]string, 0, len(tbl.PKColumns))
	for _, idx := range tbl.PKColumns {
		if idx >= 0 && idx < len(tbl.Columns) {
			cols = append(cols, tbl.Columns[idx].Name)
		}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
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
	tables                 []string // multiple tables for snapshot
	pkCol                  string
	limit                  int
	serverID               uint32
	shardIndex             int
	shardTotal             int
	serverIDBase           uint32
	consistentSnapshotLock bool
}

type snapshotCDCPosition struct {
	Phase   string           `json:"phase"`
	LastID  int64            `json:"last_id"`            // backward-compatible single-table cursor
	LastIDs map[string]int64 `json:"last_ids,omitempty"` // per-table snapshot cursors
	File    string           `json:"file"`
	Pos     uint32           `json:"pos"`
}

type snapshotCDCReader struct {
	source *MySQLSnapshotCDCSource
	db     *sql.DB
	canal  *canal.Canal

	records chan core.Record
	errors  chan error

	mu           sync.RWMutex
	phase        string
	lastID       int64
	tableLastIDs map[string]int64
	file         string
	pos          uint32

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
	if v, ok := config["tables"]; ok {
		if tables, ok := v.([]interface{}); ok {
			for _, t := range tables {
				if ts, ok := t.(string); ok {
					s.tables = append(s.tables, ts)
				}
			}
		}
	}
	// If only "tables" is set, use first table as the default single-table.
	if s.table == "" && len(s.tables) > 0 {
		s.table = s.tables[0]
	}
	// If only "table" is set, also populate tables slice.
	if len(s.tables) == 0 && s.table != "" {
		s.tables = []string{s.table}
	}
	if v, ok := config["pk_column"]; ok {
		if vs, ok := v.(string); ok {
			s.pkCol = vs
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

func (s *MySQLSnapshotCDCSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
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

	reader := &snapshotCDCReader{
		source:       s,
		db:           db,
		records:      make(chan core.Record, 1024),
		errors:       make(chan error, 64),
		phase:        "snapshot",
		tableLastIDs: make(map[string]int64),
		done:         make(chan struct{}),
	}

	c, err := s.newCanal(reader)
	if err != nil {
		db.Close()
		return nil, err
	}
	reader.canal = c

	if cp == nil {
		// Fresh runs set the CDC handoff position inside runSnapshot while the
		// snapshot transaction is opened. Checkpointed runs restore it below.
		reader.file = "master"
		reader.pos = 0
	}

	if cp != nil {
		var pos snapshotCDCPosition
		if err := json.Unmarshal(cp.Position, &pos); err == nil {
			reader.phase = pos.Phase
			reader.lastID = pos.LastID
			if len(pos.LastIDs) > 0 {
				reader.tableLastIDs = pos.LastIDs
			} else if pos.LastID > 0 && s.table != "" {
				reader.tableLastIDs[s.table] = pos.LastID
			}
			if pos.File != "" {
				reader.file = pos.File
			}
			if pos.Pos != 0 {
				reader.pos = pos.Pos
			}
		}
	}

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
	if r.canal != nil {
		defer r.canal.Close()
	}

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

	pos := mysql.Position{Name: r.file, Pos: r.pos}
	g.Log().Infof(ctx, "Starting snapshot+CDC stream from %s:%d", pos.Name, pos.Pos)

	// CDC reconnect loop with exponential backoff.
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		// Re-read latest known file/pos each iteration (updated by handler).
		curFile, curPos := r.getBinlogPos()
		curPos32 := uint32(curPos)
		runPos := mysql.Position{Name: curFile, Pos: curPos32}
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
		r.mu.Lock()
		r.canal = c
		r.mu.Unlock()
		runErr := c.RunFrom(runPos)
		c.Close()
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

		startPos, err := r.canal.GetMasterPos()
		if err != nil {
			return fmt.Errorf("get master pos under snapshot lock: %w", err)
		}
		r.mu.Lock()
		r.file = startPos.Name
		r.pos = uint32(startPos.Pos)
		r.mu.Unlock()

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
		startPos, err := r.canal.GetMasterPos()
		if err != nil {
			return fmt.Errorf("get master pos: %w", err)
		}
		r.mu.Lock()
		r.file = startPos.Name
		r.pos = uint32(startPos.Pos)
		r.mu.Unlock()
		tx, err = r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return fmt.Errorf("begin snapshot tx: %w", err)
		}
	}
	defer tx.Rollback()

	for _, tableName := range r.source.tables {
		for {
			lastID := r.getTableLastID(tableName)
			var query string
			if r.source.shardTotal > 1 {
				query = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` > ? AND MOD(`%s`, %d) = %d ORDER BY `%s` LIMIT %d",
					tableName, r.source.pkCol, r.source.pkCol,
					r.source.shardTotal, r.source.shardIndex,
					r.source.pkCol, r.source.limit)
			} else {
				query = fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` > ? ORDER BY `%s` LIMIT %d",
					tableName, r.source.pkCol, r.source.pkCol, r.source.limit)
			}
			rows, err := tx.QueryContext(ctx, query, lastID)
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
				rowLastID := lastID
				if id, ok := data[r.source.pkCol]; ok {
					rowLastID = normalizeSnapshotID(id)
					if rowLastID > lastID {
						r.setTableLastID(tableName, rowLastID)
						lastID = rowLastID
					}
				}
				rec := core.Record{Operation: core.OpInsert, Data: data, Metadata: core.Metadata{Table: tableName, Timestamp: time.Now(), Offset: rowLastID}}
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
	}
	return nil
}

func (r *snapshotCDCReader) primeSnapshotReadView(ctx context.Context, tx *sql.Tx) error {
	if len(r.source.tables) == 0 {
		return nil
	}
	tableName := r.source.tables[0]
	query := fmt.Sprintf("SELECT `%s` FROM `%s` ORDER BY `%s` LIMIT 1", r.source.pkCol, tableName, r.source.pkCol)
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("prime snapshot read view %s: %w", tableName, err)
	}
	return rows.Close()
}

func normalizeSnapshotID(v any) int64 {
	switch id := v.(type) {
	case int:
		return int64(id)
	case int8:
		return int64(id)
	case int16:
		return int64(id)
	case int32:
		return int64(id)
	case int64:
		return id
	case uint:
		return int64(id)
	case uint8:
		return int64(id)
	case uint16:
		return int64(id)
	case uint32:
		return int64(id)
	case uint64:
		if id > uint64(^uint64(0)>>1) {
			return 0
		}
		return int64(id)
	case float64:
		return int64(id)
	case float32:
		return int64(id)
	}
	return 0
}

func (r *snapshotCDCReader) getTableLastID(table string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.tableLastIDs != nil {
		if v, ok := r.tableLastIDs[table]; ok {
			return v
		}
	}
	if table == r.source.table {
		return r.lastID
	}
	return 0
}

func (r *snapshotCDCReader) setTableLastID(table string, id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tableLastIDs == nil {
		r.tableLastIDs = make(map[string]int64)
	}
	r.tableLastIDs[table] = id
	if table == r.source.table {
		r.lastID = id
	}
}

func (r *snapshotCDCReader) Read(ctx context.Context) (core.Record, error) {
	select {
	case rec, ok := <-r.records:
		if !ok {
			return core.Record{}, fmt.Errorf("snapshot cdc stream closed")
		}
		return rec, nil
	case err := <-r.errors:
		return core.Record{}, err
	case <-ctx.Done():
		return core.Record{}, ctx.Err()
	}
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
	r.mu.RLock()
	defer r.mu.RUnlock()
	lastIDs := make(map[string]int64, len(r.tableLastIDs))
	for k, v := range r.tableLastIDs {
		lastIDs[k] = v
	}
	pos := snapshotCDCPosition{Phase: r.phase, LastID: r.lastID, LastIDs: lastIDs, File: r.file, Pos: r.pos}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "mysql_snapshot_cdc", Position: data, Timestamp: time.Now()}, nil
}

func (r *snapshotCDCReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	phase := r.getPhase()
	if rec.Metadata.Offset > 0 && phase != "cdc" {
		r.setTableLastID(rec.Metadata.Table, rec.Metadata.Offset)
	}
	r.mu.RLock()
	lastIDs := make(map[string]int64, len(r.tableLastIDs))
	for k, v := range r.tableLastIDs {
		lastIDs[k] = v
	}
	pos := snapshotCDCPosition{Phase: r.phase, LastID: r.lastID, LastIDs: lastIDs, File: r.file, Pos: r.pos}
	r.mu.RUnlock()
	if rec.Metadata.BinlogFile != "" {
		pos.Phase = "cdc"
		pos.File = rec.Metadata.BinlogFile
		pos.Pos = rec.Metadata.BinlogPos
	}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "mysql_snapshot_cdc", Position: data, Timestamp: time.Now()}, nil
}

func (r *snapshotCDCReader) Close() error {
	if r.db != nil {
		_ = r.db.Close()
	}
	r.canal.Close()
	return nil
}

func (r *snapshotCDCReader) getPhase() string      { r.mu.RLock(); defer r.mu.RUnlock(); return r.phase }
func (r *snapshotCDCReader) setPhase(phase string) { r.mu.Lock(); defer r.mu.Unlock(); r.phase = phase }

func (r *snapshotCDCReader) getBinlogPos() (string, uint32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.file, r.pos
}

type snapshotCDCHandler struct {
	canal.DummyEventHandler
	reader *snapshotCDCReader
}

func (h *snapshotCDCHandler) OnRow(e *canal.RowsEvent) error {
	file, pos := h.reader.getBinlogPos()
	for i := 0; i < len(e.Rows); i++ {
		row := e.Rows[i]
		rec := core.Record{Metadata: core.Metadata{Table: e.Table.Name, Timestamp: time.Now(), BinlogFile: file, BinlogPos: pos}}
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

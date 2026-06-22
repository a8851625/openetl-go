package source

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("postgres_cdc", func(config map[string]any) (core.Source, error) {
		return NewPostgresCDCSource(config)
	})
}

type PostgresCDCSource struct {
	name            string
	host            string
	port            int
	user            string
	password        string
	database        string
	tables          []string
	slotName        string
	sslmode         string
	enableSnapshot  bool
	dropSlotOnClose bool
}

func NewPostgresCDCSource(config map[string]any) (*PostgresCDCSource, error) {
	s := &PostgresCDCSource{
		name:     "postgres_cdc",
		slotName: "etl_slot",
		port:     5432,
		sslmode:  "prefer",
	}
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
		s.database = v.(string)
	}
	if v, ok := config["slot_name"]; ok {
		if vs, ok := v.(string); ok {
			s.slotName = vs
		}
	}
	if v, ok := config["sslmode"]; ok {
		if vs, ok := v.(string); ok {
			s.sslmode = vs
		}
	}
	if v, ok := config["tables"]; ok {
		if tbls, ok := v.([]interface{}); ok {
			for _, t := range tbls {
				if ts, ok := t.(string); ok {
					s.tables = append(s.tables, ts)
				}
			}
		}
	}
	if v, ok := config["enable_snapshot"]; ok {
		if b, ok := v.(bool); ok {
			s.enableSnapshot = b
		}
	}
	if v, ok := config["drop_slot_on_close"]; ok {
		if b, ok := v.(bool); ok {
			s.dropSlotOnClose = b
		}
	}
	if s.host == "" || s.user == "" || s.database == "" {
		return nil, fmt.Errorf("postgres_cdc requires host, user, database")
	}
	return s, nil
}

func (s *PostgresCDCSource) Name() string { return s.name }

func (s *PostgresCDCSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		s.user, s.password, s.host, s.port, s.database, s.sslmode)

	replConn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("connect postgres (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}

	reader := &pgCDCReader{
		source:   s,
		replConn: replConn,
		records:  make(chan core.Record, 1024),
		errors:   make(chan error, 64),
		catalog:  newPGCatalog(),
		done:     make(chan struct{}),
		phase:    "cdc",
		ctx:      ctx,
	}

	if cp != nil && len(cp.Position) > 0 && string(cp.Position) != "null" {
		var pos pgPosition
		if json.Unmarshal(cp.Position, &pos) == nil && pos.LSN != "" {
			reader.lsn = pos.LSN
			reader.committedLsn = pos.LSN
		}
		if pos.Phase != "" {
			reader.phase = pos.Phase
		}
	} else if s.enableSnapshot {
		// Fresh start with snapshot enabled: begin in snapshot phase.
		reader.phase = "snapshot"
	}

	// Open snapshot DB if configured.
	if s.enableSnapshot && reader.phase != "cdc" {
		snapConnStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			s.user, s.password, s.host, s.port, s.database, s.sslmode)
		snapDB, err := sql.Open("pgx", snapConnStr)
		if err != nil {
			replConn.Close(context.Background())
			return nil, fmt.Errorf("open snapshot db (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
		}
		if err := snapDB.PingContext(ctx); err != nil {
			snapDB.Close()
			replConn.Close(context.Background())
			return nil, fmt.Errorf("ping snapshot db (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
		}
		reader.snapshotDB = snapDB
	}

	go reader.run(ctx)
	return reader, nil
}

type pgCDCReader struct {
	source       *PostgresCDCSource
	replConn     *pgconn.PgConn
	records      chan core.Record
	errors       chan error
	mu           sync.Mutex
	lsn          string
	committedLsn string
	catalog      *pgCatalog
	done         chan struct{}
	phase        string
	snapshotDB   *sql.DB
	ctx          context.Context
	isClosed     bool
}

// pgCatalog holds relation metadata learned from RELATION messages.
type pgCatalog struct {
	mu       sync.RWMutex
	rel2name map[uint32]string
	rel2cols map[uint32][]pgColumnInfo
}

type pgColumnInfo struct {
	Name    string
	TypeOID uint32
}

func newPGCatalog() *pgCatalog {
	return &pgCatalog{
		rel2name: map[uint32]string{},
		rel2cols: map[uint32][]pgColumnInfo{},
	}
}

func (c *pgCatalog) setRelation(relID uint32, tableName string, cols []pgColumnInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rel2name[relID] = tableName
	c.rel2cols[relID] = cols
}

func (c *pgCatalog) tableName(relID uint32) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if n, ok := c.rel2name[relID]; ok {
		return n
	}
	return fmt.Sprintf("rel_%d", relID)
}

func (c *pgCatalog) columns(relID uint32) []pgColumnInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if cols, ok := c.rel2cols[relID]; ok {
		return cols
	}
	return nil
}

func (r *pgCDCReader) run(ctx context.Context) {
	defer close(r.records)
	defer close(r.errors)
	defer r.replConn.Close(ctx)
	defer close(r.done)
	if r.snapshotDB != nil {
		defer r.snapshotDB.Close()
	}

	// Phase 1: Snapshot (if enabled and not yet done).
	if r.phase != "cdc" && r.source.enableSnapshot {
		if err := r.runSnapshot(ctx); err != nil {
			select {
			case r.errors <- fmt.Errorf("snapshot: %w", err):
			case <-ctx.Done():
			}
			return
		}
		r.phase = "cdc"
	}

	// Phase 2: CDC.
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		r.source.user, r.source.password, r.source.host, r.source.port, r.source.database, r.source.sslmode)

	setupConn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		select {
		case r.errors <- fmt.Errorf("setup connect: %w", err):
		case <-ctx.Done():
		}
		return
	}
	defer setupConn.Close(ctx)

	if err := r.setupPublication(ctx, setupConn); err != nil {
		r.errors <- fmt.Errorf("setup publication: %w", err)
		return
	}
	if err := r.setupSlot(ctx, setupConn); err != nil {
		r.errors <- fmt.Errorf("setup slot: %w", err)
		return
	}

	startLSN := "0/0"
	r.mu.Lock()
	if r.lsn != "" {
		startLSN = r.lsn
	}
	r.mu.Unlock()

	startCmd := fmt.Sprintf(
		"START_REPLICATION SLOT %s LOGICAL %s (proto_version '1', publication_names 'etl_pub')",
		r.source.slotName, startLSN,
	)
	if err := r.replConn.Exec(ctx, startCmd).Close(); err != nil {
		r.errors <- fmt.Errorf("start replication: %w", err)
		return
	}

	// Receive loop with reconnection on transient errors.
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		msg, err := r.replConn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || r.isClosedNow() {
				return
			}
			select {
			case r.errors <- fmt.Errorf("receive: %w", err):
			default:
			}
			// Reconnect: create a new replication connection and resume
			// from the last known LSN.
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			case <-r.done:
				return
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			r.replConn.Close(ctx)
			replConn2, connErr := pgconn.Connect(ctx, connStr)
			if connErr != nil {
				select {
				case r.errors <- fmt.Errorf("reconnect: %w", connErr):
				default:
				}
				continue
			}
			r.replConn = replConn2

			r.mu.Lock()
			resumeLSN := r.lsn
			r.mu.Unlock()
			if resumeLSN == "" {
				resumeLSN = "0/0"
			}
			resumeCmd := fmt.Sprintf(
				"START_REPLICATION SLOT %s LOGICAL %s (proto_version '1', publication_names 'etl_pub')",
				r.source.slotName, resumeLSN,
			)
			if startErr := r.replConn.Exec(ctx, resumeCmd).Close(); startErr != nil {
				select {
				case r.errors <- fmt.Errorf("resume replication: %w", startErr):
				default:
				}
				continue
			}
			backoff = time.Second
			continue
		}
		backoff = time.Second
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}
			switch m.Data[0] {
			case 'w':
				r.handleWALData(ctx, m.Data[1:])
			case 'k':
				r.sendStandbyStatus(m.Data[1:])
			}
		case *pgproto3.CopyDone:
			return
		case *pgproto3.ErrorResponse:
			r.errors <- fmt.Errorf("postgres error: %s", m.Message)
			return
		}
	}
}

func (r *pgCDCReader) isClosedNow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.isClosed
}

func (r *pgCDCReader) setupPublication(ctx context.Context, conn *pgconn.PgConn) error {
	exists := false
	result := conn.ExecParams(ctx,
		"SELECT 1 FROM pg_publication WHERE pubname='etl_pub'",
		nil, nil, nil, nil)
	for result.NextRow() {
		exists = true
	}
	result.Close()

	if exists {
		return nil
	}

	tableList := ""
	for i, t := range r.source.tables {
		if i > 0 {
			tableList += ", "
		}
		tableList += "public." + t
	}
	if len(r.source.tables) > 0 {
		return conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION etl_pub FOR TABLE %s", tableList)).Close()
	}
	return conn.Exec(ctx, "CREATE PUBLICATION etl_pub FOR ALL TABLES").Close()
}

func (r *pgCDCReader) setupSlot(ctx context.Context, conn *pgconn.PgConn) error {
	exists := false
	result := conn.ExecParams(ctx,
		"SELECT 1 FROM pg_replication_slots WHERE slot_name=$1",
		[][]byte{[]byte(r.source.slotName)}, nil, nil, nil)
	for result.NextRow() {
		exists = true
	}
	result.Close()

	if exists {
		return nil
	}

	return conn.Exec(ctx, fmt.Sprintf(
		"SELECT pg_create_logical_replication_slot('%s', 'pgoutput')",
		r.source.slotName,
	)).Close()
}

func (r *pgCDCReader) handleWALData(ctx context.Context, data []byte) {
	// XLogData payload (after the outer CopyData 'w' byte is stripped by caller):
	//   dataStart(8) + dataEnd(8) + timeline(4) + walPayload
	// The walPayload is a stream of pgoutput messages (B/I/U/D/R/C/...).
	if len(data) < 20 {
		return
	}
	frameLSN := binary.BigEndian.Uint64(data[0:8])
	frameLSNStr := fmt.Sprintf("%X/%X", frameLSN>>32, frameLSN&0xFFFFFFFF)
	r.mu.Lock()
	r.lsn = frameLSNStr
	r.mu.Unlock()

	data = data[20:]
	for len(data) >= 1 {
		switch data[0] {
		case 'B':
			data = r.skipTxBlock(data[1:])
		case 'C':
			// Commit: record the latest received LSN. The durable/acked LSN is
			// advanced only from CheckpointForRecord after downstream write and
			// checkpoint succeed.
			data = r.handleCommitMsg(data[1:], frameLSNStr)
		case 'R':
			data = r.parseRelationMsg(data[1:])
		case 'I':
			data = r.parseInsertMsg(data[1:], frameLSNStr)
		case 'U':
			data = r.parseUpdateMsg(data[1:], frameLSNStr)
		case 'D':
			data = r.parseDeleteMsg(data[1:], frameLSNStr)
		case 'T':
			// TRUNCATE — not yet mapped to an OpDelete batch. Log and skip
			// so unrecognised messages don't halt the stream.
			data = r.skipTruncateMsg(data[1:])
		default:
			// Unknown pgoutput message type. pgoutput messages do not carry a
			// uniform length prefix, so we cannot safely skip this byte and
			// continue (advancing data would mis-parse the rest of the frame;
			// not advancing would loop forever on the same byte). Stop parsing
			// this frame and surface it at ERROR level — a future PG message
			// type we don't handle must not be silently dropped (P5-7).
			g.Log().Errorf(r.ctx, "postgres_cdc: unknown pgoutput message type '%c' (0x%x) — aborting frame parse; remaining messages in this frame are skipped. Update the parser to handle this message type.", data[0], data[0])
			return
		}
	}
}

func (r *pgCDCReader) handleCommitMsg(data []byte, frameLSN string) []byte {
	// Commit message body: flags(1) + lsn_end(8) + lsn_commit(8) + xid(4)
	if len(data) >= 21 {
		endLSN := binary.BigEndian.Uint64(data[1:9])
		commitLSN := fmt.Sprintf("%X/%X", endLSN>>32, endLSN&0xFFFFFFFF)
		r.mu.Lock()
		r.lsn = commitLSN
		r.mu.Unlock()
	} else {
		r.mu.Lock()
		r.lsn = frameLSN
		r.mu.Unlock()
	}
	return r.skipTxBlock(data)
}

func (r *pgCDCReader) skipTxBlock(data []byte) []byte {
	if len(data) < 25 {
		return data[:0]
	}
	return data[25:]
}

// skipTruncateMsg skips a TRUNCATE pgoutput message.
// Format: flags(1) + n_relations(4) + for each: relID(4) + option_bits(1).
func (r *pgCDCReader) skipTruncateMsg(data []byte) []byte {
	if len(data) < 5 {
		return data[:0]
	}
	nRels := binary.BigEndian.Uint32(data[1:5])
	skip := 5 + int(nRels)*5
	if len(data) < skip {
		return data[:0]
	}
	g.Log().Warningf(r.ctx, "postgres_cdc: TRUNCATE on %d table(s) — not yet mapped to target DELETE; rows still exist in sink", nRels)
	return data[skip:]
}

func (r *pgCDCReader) parseRelationMsg(data []byte) []byte {
	if len(data) < 10 {
		return data[:0]
	}
	pos := 0
	relID := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	ns, pos := readNullString(data, pos)
	tableName, pos := readNullString(data, pos)
	_ = ns

	if pos+1 >= len(data) {
		return data[:0]
	}
	pos++
	numCols := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	var cols []pgColumnInfo
	for i := 0; i < numCols && pos < len(data); i++ {
		if pos+1 >= len(data) {
			break
		}
		pos++
		colName, nextPos := readNullString(data, pos)
		pos = nextPos
		if pos+8 > len(data) {
			break
		}
		typeOID := binary.BigEndian.Uint32(data[pos:])
		pos += 4
		pos += 4
		cols = append(cols, pgColumnInfo{Name: colName, TypeOID: typeOID})
	}

	r.catalog.setRelation(relID, tableName, cols)
	return data[pos:]
}

func (r *pgCDCReader) parseInsertMsg(data []byte, lsn string) []byte {
	if len(data) < 7 {
		return data[:0]
	}
	pos := 0
	relID := binary.BigEndian.Uint32(data[pos:])
	pos += 4
	if pos >= len(data) || data[pos] != 'N' {
		return data[:0]
	}
	pos++
	if pos+2 > len(data) {
		return data[:0]
	}
	numCols := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	tableName := r.catalog.tableName(relID)
	cols := r.catalog.columns(relID)

	dataMap := make(map[string]any, numCols)
	for i := 0; i < numCols && pos < len(data); i++ {
		if pos >= len(data) {
			break
		}
		colType := data[pos]
		pos++
		if colType == 'n' {
			if i < len(cols) {
				dataMap[cols[i].Name] = nil
			}
			continue
		}
		if pos+4 > len(data) {
			break
		}
		valLen := int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if valLen < 0 || pos+int(valLen) > len(data) {
			break
		}
		val := data[pos : pos+int(valLen)]
		pos += int(valLen)

		colName := fmt.Sprintf("col_%d", i)
		var typeOID uint32
		if i < len(cols) {
			colName = cols[i].Name
			typeOID = cols[i].TypeOID
		}
		dataMap[colName] = decodeColumnValue(val, typeOID)
	}

	r.sendRecord(core.Record{
		Operation: core.OpInsert,
		Data:      dataMap,
		Metadata: core.Metadata{
			Source:    r.source.name,
			Table:     tableName,
			Timestamp: time.Now(),
			LSN:       lsn,
		},
	})
	return data[pos:]
}

// sendRecord safely sends a record to the pipeline, respecting context
// cancellation and close signals. Used by all WAL message handlers.
func (r *pgCDCReader) sendRecord(rec core.Record) {
	r.mu.Lock()
	if r.isClosed {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	select {
	case r.records <- rec:
	case <-r.done:
	case <-r.ctx.Done():
	}
}

func (r *pgCDCReader) parseUpdateMsg(data []byte, lsn string) []byte {
	if len(data) < 7 {
		return data[:0]
	}
	pos := 0
	relID := binary.BigEndian.Uint32(data[pos:])
	pos += 4
	if pos >= len(data) || (data[pos] != 'K' && data[pos] != 'O') {
		return data[:0]
	}
	tupleType := data[pos]
	pos++
	if pos+2 > len(data) {
		return data[:0]
	}
	numCols := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	tableName := r.catalog.tableName(relID)
	cols := r.catalog.columns(relID)

	var beforeMap map[string]any
	if tupleType == 'O' {
		beforeMap = make(map[string]any, numCols)
		for i := 0; i < numCols && pos < len(data); i++ {
			if pos >= len(data) {
				break
			}
			colType := data[pos]
			pos++
			if colType == 'n' {
				if i < len(cols) {
					beforeMap[cols[i].Name] = nil
				}
				continue
			}
			if pos+4 > len(data) {
				break
			}
			valLen := int32(binary.BigEndian.Uint32(data[pos:]))
			pos += 4
			if valLen < 0 || pos+int(valLen) > len(data) {
				break
			}
			val := data[pos : pos+int(valLen)]
			pos += int(valLen)

			if i < len(cols) {
				beforeMap[cols[i].Name] = decodeColumnValue(val, cols[i].TypeOID)
			}
		}
	}

	if pos >= len(data) {
		return data[:0]
	}
	nextTupleType := data[pos]
	pos++
	if nextTupleType != 'K' && nextTupleType != 'N' {
		return data[pos:]
	}
	if nextTupleType == 'N' && tupleType == 'K' {
		rec := core.Record{
			Operation: core.OpDelete,
			Metadata:  core.Metadata{Source: r.source.name, Table: tableName, Timestamp: time.Now(), LSN: lsn},
		}
		r.sendRecord(rec)
		return data[pos:]
	}
	if pos+2 > len(data) {
		return data[:0]
	}
	numNewCols := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	dataMap := make(map[string]any, numNewCols)
	for i := 0; i < numNewCols && pos < len(data); i++ {
		if pos >= len(data) {
			break
		}
		colType := data[pos]
		pos++
		if colType == 'n' {
			if i < len(cols) {
				dataMap[cols[i].Name] = nil
			}
			continue
		}
		if pos+4 > len(data) {
			break
		}
		valLen := int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if valLen < 0 || pos+int(valLen) > len(data) {
			break
		}
		val := data[pos : pos+int(valLen)]
		pos += int(valLen)

		colName := fmt.Sprintf("col_%d", i)
		var typeOID uint32
		if i < len(cols) {
			colName = cols[i].Name
			typeOID = cols[i].TypeOID
		}
		dataMap[colName] = decodeColumnValue(val, typeOID)
	}

	rec := core.Record{
		Operation: core.OpUpdate,
		Data:      dataMap,
		Before:    beforeMap,
		Metadata:  core.Metadata{Source: r.source.name, Table: tableName, Timestamp: time.Now(), LSN: lsn},
	}
	r.sendRecord(rec)
	return data[pos:]
}

func (r *pgCDCReader) parseDeleteMsg(data []byte, lsn string) []byte {
	if len(data) < 7 {
		return data[:0]
	}
	pos := 0
	relID := binary.BigEndian.Uint32(data[pos:])
	pos += 4
	if pos >= len(data) || (data[pos] != 'K' && data[pos] != 'O') {
		return data[:0]
	}
	pos++
	if pos+2 > len(data) {
		return data[:0]
	}
	numCols := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	tableName := r.catalog.tableName(relID)
	cols := r.catalog.columns(relID)

	dataMap := make(map[string]any, numCols)
	for i := 0; i < numCols && pos < len(data); i++ {
		if pos >= len(data) {
			break
		}
		colType := data[pos]
		pos++
		if colType == 'n' {
			if i < len(cols) {
				dataMap[cols[i].Name] = nil
			}
			continue
		}
		if pos+4 > len(data) {
			break
		}
		valLen := int32(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if valLen < 0 || pos+int(valLen) > len(data) {
			break
		}
		val := data[pos : pos+int(valLen)]
		pos += int(valLen)

		if i < len(cols) {
			dataMap[cols[i].Name] = decodeColumnValue(val, cols[i].TypeOID)
		}
	}

	r.sendRecord(core.Record{
		Operation: core.OpDelete,
		Data:      dataMap,
		Metadata:  core.Metadata{Source: r.source.name, Table: tableName, Timestamp: time.Now(), LSN: lsn},
	})
	return data[pos:]
}

func decodeColumnValue(raw []byte, typeOID uint32) any {
	s := string(raw)
	switch typeOID {
	case 16: // bool
		return s == "t"
	case 20: // int8
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return s
		}
		return v
	case 21: // int2
		v, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return s
		}
		return int16(v)
	case 23: // int4
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return s
		}
		return int32(v)
	case 700: // float4
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return s
		}
		return float32(v)
	case 701: // float8
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return s
		}
		return v
	case 1700: // numeric/decimal
		return parsePGNumeric(raw)
	case 1082: // date
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return s
		}
		return t
	case 1083: // time
		t, err := time.Parse("15:04:05", s)
		if err != nil {
			t2, err2 := time.Parse("15:04:05.999999", s)
			if err2 != nil {
				return s
			}
			return t2
		}
		return t
	case 1114: // timestamp
		t, err := time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			ts, err2 := time.Parse("2006-01-02 15:04:05.999999", s)
			if err2 != nil {
				return s
			}
			return ts
		}
		return t
	case 1184: // timestamptz
		layouts := []string{
			"2006-01-02 15:04:05-07",
			"2006-01-02 15:04:05.999999-07",
			time.RFC3339,
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, s); err == nil {
				return t
			}
		}
		return s
	case 114, 3802: // json, jsonb
		var result any
		if err := json.Unmarshal(raw, &result); err != nil {
			return s
		}
		return result
	case 2950: // uuid
		return s
	case 17: // bytea
		return fmt.Sprintf("\\x%x", raw)
	case 25, 1043: // text, varchar
		return s
	case 1007, 1005, 1009, 1015, 1016: // _int4, _int2, _text, _varchar, _int8
		return parsePGArray(s)
	default:
		return s
	}
}

// parsePGNumeric decodes PostgreSQL NUMERIC (OID 1700) binary format.
func parsePGNumeric(raw []byte) any {
	if len(raw) < 8 {
		return string(raw)
	}
	nDigits := binary.BigEndian.Uint16(raw[0:2])
	weight := int16(binary.BigEndian.Uint16(raw[2:4]))
	sign := binary.BigEndian.Uint16(raw[4:6])
	_ = binary.BigEndian.Uint16(raw[6:8]) // dscale

	if len(raw) < 8+int(nDigits)*2 {
		return string(raw)
	}

	var sb strings.Builder
	if sign == 0x4000 {
		sb.WriteByte('-')
	}
	intGroups := int(weight) + 1
	for i := 0; i < intGroups; i++ {
		var val uint16
		if i < int(nDigits) {
			val = binary.BigEndian.Uint16(raw[8+i*2 : 8+i*2+2])
		}
		if i > 0 {
			sb.WriteString(fmt.Sprintf("%04d", val))
		} else {
			sb.WriteString(fmt.Sprintf("%d", val))
		}
	}
	if int(nDigits) > intGroups {
		sb.WriteByte('.')
		for i := intGroups; i < int(nDigits); i++ {
			val := binary.BigEndian.Uint16(raw[8+i*2 : 8+i*2+2])
			sb.WriteString(fmt.Sprintf("%04d", val))
		}
	}
	result := strings.TrimRight(sb.String(), "0")
	if strings.HasSuffix(result, ".") {
		result += "0"
	}
	if f, err := strconv.ParseFloat(result, 64); err == nil {
		return f
	}
	return result
}

// parsePGArray parses a PostgreSQL text array like "{a,b,c}".
func parsePGArray(s string) []any {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []any{}
	}
	var result []any
	var current strings.Builder
	inQuotes := false
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if ch == '"' {
			inQuotes = !inQuotes
		} else if ch == ',' && !inQuotes {
			result = append(result, strings.Trim(current.String(), `"`))
			current.Reset()
		} else {
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		result = append(result, strings.Trim(current.String(), `"`))
	}
	return result
}

func (r *pgCDCReader) sendStandbyStatus(data []byte) {
	if len(data) < 17 {
		return
	}
	// Acknowledge only up to the last durably committed LSN (records that
	// have been delivered and checkpointed downstream). This prevents PG
	// from advancing the slot's restart_lsn past WAL we still need.
	// If we have no committed LSN yet (e.g. before the first commit), fall
	// back to the server-provided keepalive LSN so idle connections don't
	// time out, but this is safe because no consumer progress is claimed.
	r.mu.Lock()
	committed := r.committedLsn
	r.mu.Unlock()

	var lsnBytes []byte
	if committed != "" {
		if b, ok := parseLSNToBytes(committed); ok {
			lsnBytes = b
		}
	}
	if lsnBytes == nil {
		lsnBytes = data[1:9]
	}

	status := make([]byte, 25)
	status[0] = 'r'
	copy(status[1:9], lsnBytes)   // last WAL byte written
	copy(status[9:17], lsnBytes)  // last WAL byte flushed
	copy(status[17:25], lsnBytes) // last WAL byte applied
	cd := &pgproto3.CopyData{Data: status}
	encoded, err := cd.Encode(nil)
	if err != nil {
		return
	}
	_ = r.replConn.Frontend().SendUnbufferedEncodedCopyData(encoded)
}

// parseLSNToBytes converts a PostgreSQL LSN string "HILO/LOLO" (hex) into an
// 8-byte big-endian representation. Returns false on parse error.
func parseLSNToBytes(lsn string) ([]byte, bool) {
	parts := strings.SplitN(lsn, "/", 2)
	if len(parts) != 2 {
		return nil, false
	}
	hi, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return nil, false
	}
	lo, err := strconv.ParseUint(parts[1], 16, 64)
	if err != nil {
		return nil, false
	}
	combined := (hi << 32) | lo
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, combined)
	return b, true
}

func readNullString(data []byte, off int) (string, int) {
	if off >= len(data) {
		return "", off
	}
	end := off
	for end < len(data) && data[end] != 0 {
		end++
	}
	return string(data[off:end]), end + 1
}

func (r *pgCDCReader) Read(ctx context.Context) (core.Record, error) {
	select {
	case rec, ok := <-r.records:
		if !ok {
			return core.Record{}, fmt.Errorf("pg cdc closed")
		}
		return rec, nil
	case err := <-r.errors:
		return core.Record{}, err
	case <-ctx.Done():
		return core.Record{}, ctx.Err()
	}
}

func (r *pgCDCReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	var batch []core.Record
	for i := 0; i < n; i++ {
		rec, err := r.Read(ctx)
		if err != nil {
			return batch, err
		}
		batch = append(batch, rec)
	}
	return batch, nil
}

func (r *pgCDCReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	r.mu.Lock()
	lsn := r.committedLsn
	phase := r.phase
	r.mu.Unlock()
	pos := pgPosition{LSN: lsn, Phase: phase}
	data, err := json.Marshal(pos)
	if err != nil {
		return core.Checkpoint{}, err
	}
	return core.Checkpoint{Source: r.source.name, Position: data, Timestamp: time.Now()}, nil
}

func (r *pgCDCReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	// Prefer the record's own LSN (per-record safe resume point).
	lsn := rec.Metadata.LSN
	phase := "cdc"
	r.mu.Lock()
	if lsn != "" {
		r.committedLsn = lsn
	} else {
		lsn = r.committedLsn
		phase = r.phase
	}
	r.mu.Unlock()
	pos := pgPosition{LSN: lsn, Phase: phase}
	data, err := json.Marshal(pos)
	if err != nil {
		return core.Checkpoint{}, err
	}
	return core.Checkpoint{Source: r.source.name, Position: data, Timestamp: time.Now()}, nil
}

// runSnapshot performs an initial full-table read for each configured table.
// This reads current data before CDC begins, similar to mysql_snapshot_cdc.
func (r *pgCDCReader) runSnapshot(ctx context.Context) error {
	tables := r.source.tables
	if len(tables) == 0 {
		return nil
	}
	for _, t := range tables {
		// Quote table name properly: if it contains a dot, split schema.table.
		tableRef := t
		if dotIdx := strings.Index(t, "."); dotIdx >= 0 {
			schema := t[:dotIdx]
			tableName := t[dotIdx+1:]
			tableRef = fmt.Sprintf(`"%s"."%s"`, schema, tableName)
		} else {
			tableRef = fmt.Sprintf(`"public"."%s"`, t)
		}
		query := fmt.Sprintf(`SELECT * FROM %s`, tableRef)
		rows, err := r.snapshotDB.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("snapshot %s: %w", t, err)
		}
		cols, colsErr := rows.Columns()
		if colsErr != nil {
			rows.Close()
			return fmt.Errorf("snapshot %s: get columns: %w", t, colsErr)
		}
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				fmt.Printf("[WARN] pg_cdc: snapshot scan error on %s: %v (row skipped)\n", t, err)
				continue
			}
			data := map[string]any{}
			for i, col := range cols {
				data[col] = normalizeValue(values[i])
			}
			rec := core.Record{
				Operation: core.OpInsert,
				Data:      data,
				Metadata: core.Metadata{
					Table:     t,
					Timestamp: time.Now(),
				},
			}
			select {
			case r.records <- rec:
			case <-ctx.Done():
				rows.Close()
				return ctx.Err()
			case <-r.done:
				rows.Close()
				return nil
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("snapshot %s: rows iteration: %w", t, err)
		}
		rows.Close()
	}
	r.phase = "cdc"
	return nil
}

func (r *pgCDCReader) Close() error {
	r.mu.Lock()
	r.isClosed = true
	r.mu.Unlock()
	close(r.done)

	// Only drop the replication slot when explicitly requested via
	// `drop_slot_on_close: true`. Dropping unconditionally on every Close
	// loses WAL needed for resumable CDC and causes silent data loss on
	// pipeline restart.
	if r.source.dropSlotOnClose {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			r.source.user, r.source.password, r.source.host, r.source.port,
			r.source.database, r.source.sslmode)
		if cleanupConn, err := pgconn.Connect(cleanupCtx, connStr); err == nil {
			_ = cleanupConn.Exec(cleanupCtx,
				fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", r.source.slotName)).Close()
			cleanupConn.Close(cleanupCtx)
		}
		cancel()
	}

	return r.replConn.Close(context.Background())
}

type pgPosition struct {
	LSN   string `json:"lsn"`
	Phase string `json:"phase,omitempty"`
}

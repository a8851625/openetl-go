package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
)

func init() {
	registry.RegisterSink("doris", func(config map[string]any) (core.Sink, error) {
		return NewDorisSink(config)
	})
}

// Doris source-order (replay-ordering) modes. The default stays "off" so
// existing pipelines keep the legacy last-write-wins behaviour: enabling
// source_order requires a table contract that existing Doris tables cannot
// acquire retroactively (see validateSourceOrderStatement).
const (
	dorisVersionModeOff         = "off"
	dorisVersionModeSourceOrder = "source_order"

	dorisSequenceTypeLargeInt = "largeint"
	dorisSequenceTypeBigInt   = "bigint"

	dorisDefaultSequenceColumn = "_version"

	// dorisDeleteSignColumn is reserved by Doris for the merge-on-write delete
	// marker. This sink never writes it: on Doris 2.1.11 a Stream Load payload
	// carrying __DORIS_DELETE_SIGN is accepted (Status=Success) but does NOT
	// delete the row — neither with the columns, hidden_columns nor
	// partial_columns headers, nor via CSV, and merge_type=DELETE is a no-op
	// as well. Deletes therefore stay on the MySQL-protocol DELETE path, whose
	// implicit delete-sign row inherits the deleted row's sequence value.
	dorisDeleteSignColumn = "__DORIS_DELETE_SIGN"
)

// DorisSink writes records to Apache Doris via Stream Load (HTTP) or MySQL
// protocol (INSERT). It supports auto-create, schema drift, batch upsert
// (via Doris Unique Key model), and DELETE operations.
//
// Doris speaks MySQL wire protocol on port 9030 (FE), and exposes a high-
// performance Stream Load HTTP API on port 8030 (FE). This sink uses:
//
//   - MySQL protocol for DDL (CREATE TABLE, ALTER TABLE) and DELETE
//   - Stream Load for high-throughput batch INSERT/UPSERT (default)
//   - MySQL protocol INSERT as a fallback write mode
//
// Config:
//
//	host:          Doris FE host
//	port:          MySQL protocol port (default 9030)
//	http_port:     Stream Load HTTP port (default 8030)
//	user:          Doris user (default "root")
//	password:      Doris password
//	database:      Target database name
//	table:         Target table name (can be overridden per-record via metadata.table)
//	write_mode:    "stream_load" (default) or "insert"
//	batch_mode:    "insert" (default) or "upsert" — both use INSERT; Doris
//	               Unique Key model handles dedup automatically
//	pk_columns:    Primary key columns (used for DELETE and auto-create key model)
//	stream_load_timeout_sec: Stream Load HTTP timeout (default 30)
//	stream_load_format: "json" (default) or "csv"
//	auto_create:   Auto-create target table if missing
//	schema_drift:  "ignore" | "fail" | "add_columns"
//	insert_chunk_size: Rows per INSERT statement for insert mode (default 500)
//	version_mode:  "off" (default) or "source_order" — see dorisVersionMode* constants
//	sequence_column: numeric column carrying core.SourceOrder().Version (default "_version")
//	sequence_type: "largeint" (default) or "bigint"
type DorisSink struct {
	name     string
	host     string
	port     int
	httpPort int
	user     string
	password string
	database string
	table    string
	// tableTemplate, when set (e.g. "ods_{table}"), resolves the target table
	// per record from record metadata instead of using the static table. This
	// lets a single sink fan out a mixed-topic/multi-table stream (e.g. kafka
	// source consuming one topic with format=envelope) into multiple Doris
	// tables. {table} and {db} are substituted from record metadata.
	tableTemplate string

	writeMode string // "stream_load" | "insert"
	batchMode string // "insert" | "upsert" (informational; Doris UK model dedupes)
	pkColumns []string
	// pkColumnsFromMetadata derives per-table key columns from the JSON
	// Debezium/Kafka key carried in core.Metadata.Key. This is required for
	// multi-table CDC upsert/delete flows where one static pk_columns list
	// cannot describe every routed table.
	pkColumnsFromMetadata bool

	streamLoadTimeout time.Duration
	streamLoadFormat  string
	streamLoadScheme  string
	tlsSkipVerify     bool

	insertChunkSize int

	autoCreate             bool
	schemaDrift            string
	ddLPolicy              DDLPolicy
	allowMixedCDCNonAtomic bool

	// versionMode is "off" (default, legacy last-write-wins) or
	// "source_order". source_order injects core.SourceOrder().Version into
	// sequenceColumn so Doris ignores records that arrive after a newer
	// version of the same key was already applied (verified on Doris 2.1.11;
	// see docs/iterations/IT-7-doris-source-order/findings.md).
	versionMode string
	// sequenceColumn is the Doris column holding the source-order version.
	// It must be declared at CREATE TABLE time as
	// PROPERTIES("function_column.sequence_col"="<col>") — Doris 2.1.11
	// cannot add that property to an existing table.
	sequenceColumn string
	// sequenceType is the declared type of sequenceColumn. "largeint"
	// (default) holds the full uint64 source-order domain; "bigint" is an
	// opt-in that must reject values above math.MaxInt64 because Doris
	// silently wraps them to negative numbers.
	sequenceType string
	// validatedSourceOrderTables caches tables whose source_order contract has
	// already been verified, so multi-table fanout validates each routed table
	// once per process instead of on every batch.
	validatedSourceOrderTables sync.Map

	// ackMu guards pendingLabels/ackedTables: the deterministic Stream Load
	// labels of the batch currently being written.
	ackMu         sync.Mutex
	pendingLabels map[string]string
	batchSeq      uint64
	// dorisCommit holds the last fully acknowledged batch so the runner can
	// persist its boundary in the checkpoint envelope.
	dorisCommit atomic.Pointer[dorisCommitState]

	db           *sql.DB
	httpClient   *http.Client
	schemaCache  *core.SchemaCache
	sinkCounters // P4-20: per-sink write metrics (SK-4)
	tableMetrics *tableMetricsSet
}

func NewDorisSink(config map[string]any) (*DorisSink, error) {
	s := &DorisSink{
		name:              "doris",
		port:              9030,
		httpPort:          8030,
		user:              "root",
		writeMode:         "stream_load",
		batchMode:         "insert",
		streamLoadTimeout: 30 * time.Second,
		streamLoadFormat:  "json",
		streamLoadScheme:  "http",
		insertChunkSize:   500,
		versionMode:       dorisVersionModeOff,
		sequenceColumn:    dorisDefaultSequenceColumn,
		sequenceType:      dorisSequenceTypeLargeInt,
	}
	s.tableMetrics = newTableMetricsSet()
	sequenceColumnProvided := false
	if v, ok := config["name"]; ok {
		s.name = v.(string)
	}
	if v, ok := config["host"]; ok {
		s.host = v.(string)
	}
	if v, ok := config["port"]; ok {
		switch p := v.(type) {
		case int:
			s.port = p
		case float64:
			s.port = int(p)
		}
	}
	if v, ok := config["http_port"]; ok {
		switch p := v.(type) {
		case int:
			s.httpPort = p
		case float64:
			s.httpPort = int(p)
		}
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
	if v, ok := config["table"]; ok {
		s.table = v.(string)
	}
	if v, ok := config["table_template"]; ok {
		if vs, ok := v.(string); ok {
			s.tableTemplate = vs
		}
	}
	if v, ok := config["write_mode"]; ok {
		s.writeMode = v.(string)
	}
	if v, ok := config["batch_mode"]; ok {
		s.batchMode = v.(string)
	}
	s.pkColumns = append(s.pkColumns, stringSliceConfig(config, "pk_columns")...)
	if v, ok := config["pk_columns_from_metadata"]; ok {
		if b, ok := v.(bool); ok {
			s.pkColumnsFromMetadata = b
		}
	}
	if v, ok := config["stream_load_timeout_sec"]; ok {
		switch t := v.(type) {
		case int:
			s.streamLoadTimeout = time.Duration(t) * time.Second
		case float64:
			s.streamLoadTimeout = time.Duration(t) * time.Second
		}
	}
	if v, ok := config["stream_load_format"]; ok {
		if vs, ok := v.(string); ok && (vs == "json" || vs == "csv") {
			s.streamLoadFormat = vs
		}
	}
	if v, ok := config["stream_load_scheme"]; ok {
		if vs, ok := v.(string); ok && (vs == "http" || vs == "https") {
			s.streamLoadScheme = vs
		}
	}
	if v, ok := config["https"]; ok {
		if b, ok := v.(bool); ok && b {
			s.streamLoadScheme = "https"
		}
	}
	if v, ok := config["tls_skip_verify"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsSkipVerify = b
		}
	}
	if v, ok := config["insert_chunk_size"]; ok {
		switch cs := v.(type) {
		case int:
			s.insertChunkSize = cs
		case float64:
			s.insertChunkSize = int(cs)
		}
	}
	if s.insertChunkSize <= 0 {
		s.insertChunkSize = 500
	}
	if v, ok := config["auto_create"]; ok {
		if b, ok := v.(bool); ok {
			s.autoCreate = b
		}
	}
	if v, ok := config["schema_drift"]; ok {
		if vs, ok := v.(string); ok {
			s.schemaDrift = vs
		}
	}
	if s.schemaDrift == "" {
		s.schemaDrift = "ignore"
	}
	if v, ok := config["ddl_policy"]; ok {
		if vs, ok := v.(string); ok {
			s.ddLPolicy = DDLPolicy(vs)
		}
	}
	if s.ddLPolicy == "" {
		s.ddLPolicy = DDLPolicyReject
	}
	if v, ok := config["allow_mixed_cdc_non_atomic"]; ok {
		if b, ok := v.(bool); ok {
			s.allowMixedCDCNonAtomic = b
		}
	}
	if v, ok := config["version_mode"]; ok {
		if vs, ok := v.(string); ok {
			s.versionMode = strings.ToLower(strings.TrimSpace(vs))
		}
	}
	if s.versionMode == "" {
		s.versionMode = dorisVersionModeOff
	}
	if v, ok := config["sequence_column"]; ok {
		if vs, ok := v.(string); ok {
			s.sequenceColumn = strings.TrimSpace(vs)
			sequenceColumnProvided = true
		}
	}
	if s.sequenceColumn == "" && !sequenceColumnProvided {
		s.sequenceColumn = dorisDefaultSequenceColumn
	}
	if v, ok := config["sequence_type"]; ok {
		if vs, ok := v.(string); ok {
			s.sequenceType = strings.ToLower(strings.TrimSpace(vs))
		}
	}
	if s.sequenceType == "" {
		s.sequenceType = dorisSequenceTypeLargeInt
	}
	if err := s.validateSourceOrderConfig(sequenceColumnProvided); err != nil {
		return nil, err
	}
	s.schemaCache = core.NewSchemaCache()
	return s, nil
}

// dorisQuotedPairPattern matches the quoted "name" = "value" pairs that
// SHOW CREATE TABLE exposes in the PROPERTIES block.
var dorisQuotedPairPattern = regexp.MustCompile(`"([^"]+)"\s*=\s*"([^"]*)"`)

func (s *DorisSink) Name() string { return s.name }

// sourceOrderEnabled reports whether version_mode=source_order is active.
func (s *DorisSink) sourceOrderEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(s.versionMode), dorisVersionModeSourceOrder)
}

// effectiveSequenceColumn resolves the configured sequence column, defaulting
// to _version (the same name ClickHouse uses) when unset.
func (s *DorisSink) effectiveSequenceColumn() string {
	if col := strings.TrimSpace(s.sequenceColumn); col != "" {
		return col
	}
	return dorisDefaultSequenceColumn
}

// effectiveSequenceType resolves the configured sequence column type.
func (s *DorisSink) effectiveSequenceType() string {
	if typ := strings.ToLower(strings.TrimSpace(s.sequenceType)); typ != "" {
		return typ
	}
	return dorisSequenceTypeLargeInt
}

// dorisSequenceColumnDDLType is the Doris column type written by auto-create.
// LARGEINT is the default because Doris BIGINT silently wraps values above
// math.MaxInt64 to negative numbers, which would make a newer event look older.
func (s *DorisSink) dorisSequenceColumnDDLType() string {
	if s.effectiveSequenceType() == dorisSequenceTypeBigInt {
		return "BIGINT"
	}
	return "LARGEINT"
}

// validateSourceOrderConfig rejects configuration combinations that cannot
// honour the source-order contract. It is a no-op unless version_mode is
// source_order, so the legacy ("off") path keeps its historical leniency.
func (s *DorisSink) validateSourceOrderConfig(sequenceColumnProvided bool) error {
	switch mode := strings.ToLower(strings.TrimSpace(s.versionMode)); mode {
	case "", dorisVersionModeOff:
		return nil
	case dorisVersionModeSourceOrder:
	default:
		return fmt.Errorf("doris sink version_mode %q is not supported; set it to off (default, last-write-wins) or source_order (requires a connector-owned numeric source position)", s.versionMode)
	}

	switch typ := s.effectiveSequenceType(); typ {
	case dorisSequenceTypeLargeInt, dorisSequenceTypeBigInt:
	default:
		return fmt.Errorf("doris sink sequence_type %q is not supported; use largeint (default, holds the full uint64 source-order domain) or bigint (opt-in; values above math.MaxInt64 are rejected instead of being wrapped)", s.sequenceType)
	}

	if sequenceColumnProvided && strings.TrimSpace(s.sequenceColumn) == "" {
		return fmt.Errorf("doris sink sequence_column must not be blank when set; use a numeric column name such as %q", dorisDefaultSequenceColumn)
	}
	sequenceColumn := s.effectiveSequenceColumn()
	if strings.EqualFold(sequenceColumn, dorisDeleteSignColumn) {
		return fmt.Errorf("doris sink sequence_column must not be %q: that name is reserved by Doris for the merge-on-write delete marker", dorisDeleteSignColumn)
	}
	for _, key := range s.pkColumns {
		if normalizeIdentifier(key) == normalizeIdentifier(sequenceColumn) {
			return fmt.Errorf("doris sink sequence_column %q collides with pk_columns entry %q; the sequence column must not be part of the UNIQUE KEY", sequenceColumn, key)
		}
	}

	// source_order relies on the Doris engine comparing sequence values. The
	// MySQL-protocol INSERT fallback and the informational batch_mode default
	// ("insert") both skip the UNIQUE KEY/merge-on-write contract checks, so
	// require the combination that actually enforces them.
	if s.writeMode == "insert" {
		return fmt.Errorf("doris sink version_mode=source_order requires write_mode=stream_load: the MySQL INSERT fallback bypasses Doris sequence handling, so ordered writes could silently regress")
	}
	if s.batchMode != "upsert" {
		return fmt.Errorf("doris sink version_mode=source_order requires batch_mode=upsert: upsert enables the UNIQUE KEY + merge-on-write contract validation that source_order depends on")
	}
	return nil
}

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *DorisSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *DorisSink) ValidateSchema(ctx context.Context, schema core.SchemaInfo) error {
	if len(schema.Columns) == 0 || s.table == "" {
		return nil
	}
	exists, err := s.tableExists(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate doris schema: check table %s.%s: %w", s.database, s.table, err)
	}
	if !exists {
		if s.autoCreate {
			if s.batchMode == "upsert" && len(s.pkColumns) == 0 && !s.pkColumnsFromMetadata && !schemaHasColumn(schema, "id") {
				return fmt.Errorf("schema validation failed for doris %s.%s: batch_mode=upsert with auto_create requires pk_columns, pk_columns_from_metadata, or an id column for a stable Doris UNIQUE KEY model", s.database, s.table)
			}
			return nil
		}
		return fmt.Errorf("schema validation failed for doris %s.%s: target table does not exist; enable auto_create or create a Doris UNIQUE KEY table first", s.database, s.table)
	}
	target, err := s.getExistingColumnInfo(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate doris schema: read columns for %s.%s: %w", s.database, s.table, err)
	}
	if err := validateSchemaCompatibility(schema, target, schemaValidationOptions{
		targetName:     fmt.Sprintf("doris %s.%s", s.database, s.table),
		allowMissing:   s.schemaDrift == string(core.DriftAddCols),
		missingRemedy:  "enable schema_drift=add_columns or add the columns manually",
		allowTypeSync:  s.schemaDrift == dorisSchemaDriftSync,
		typeSyncRemedy: "enable schema_drift=sync (add_columns + ALTER ... MODIFY COLUMN widening) or add a transform/type_convert before the sink",
	}); err != nil {
		return err
	}
	checkUniqueKey := s.batchMode == "upsert" || len(s.pkColumns) > 0
	if checkUniqueKey || s.sourceOrderEnabled() {
		if s.pkColumnsFromMetadata && len(s.pkColumns) == 0 {
			// The concrete UNIQUE KEY is resolved from each record's metadata
			// during Write; a static preflight table cannot validate it here.
			return nil
		}
		if checkUniqueKey && len(s.pkColumns) == 0 {
			return fmt.Errorf("schema validation failed for doris %s.%s: batch_mode=upsert requires pk_columns so checkpoint/DLQ replay targets a stable Doris UNIQUE KEY", s.database, s.table)
		}
		// One SHOW CREATE TABLE covers both the UNIQUE KEY and the
		// source_order contract checks.
		createStmt, err := s.showCreateTable(ctx, s.table)
		if err != nil {
			return fmt.Errorf("validate doris table contract for %s.%s: %w", s.database, s.table, err)
		}
		if checkUniqueKey {
			if err := s.validateUniqueKeyStatement(createStmt, s.table); err != nil {
				return err
			}
		}
		if s.sourceOrderEnabled() {
			if err := s.validateSourceOrderStatement(createStmt, s.table); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *DorisSink) Open(ctx context.Context) error {
	// MySQL protocol connection (for DDL + DELETE + fallback INSERT)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=Local&interpolateParams=true",
		s.user, s.password, s.host, s.port, s.database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect doris (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping doris (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	s.db = db

	// HTTP client for Stream Load
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if s.tlsSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	s.httpClient = &http.Client{
		Timeout:   s.streamLoadTimeout,
		Transport: transport,
	}
	return nil
}

func (s *DorisSink) Write(ctx context.Context, records []core.Record) (err error) {
	s.beginDorisBatch()
	defer func() {
		if err != nil {
			s.recordError()
			s.invalidateDorisBatch() // no verifiable commit boundary after a failure
		}
	}() // P5-12: count write failures
	if len(records) == 0 {
		return nil
	}
	start := time.Now()

	// Separate DDL records from data records.
	var ddlRecords, dataRecords []core.Record
	for _, rec := range records {
		if rec.Operation == core.OpDDL {
			ddlRecords = append(ddlRecords, rec)
		} else {
			dataRecords = append(dataRecords, rec)
		}
	}
	originalHasWrites := false
	originalHasDeletes := false
	for _, record := range dataRecords {
		if record.Operation == core.OpDelete {
			originalHasDeletes = true
		} else {
			originalHasWrites = true
		}
	}

	pkColumnsByTable := map[string][]string{}
	if s.pkColumnsFromMetadata {
		validationStarted := time.Now()
		validation, validationErr := validateMetadataPKBatch(s.name, dataRecords, s.pkColumns, func(record core.Record) (metadataPKTarget, error) {
			table, resolveErr := s.resolveTable(record)
			return metadataPKTarget{Database: s.database, Table: table}, resolveErr
		})
		if validationErr != nil {
			recordMetadataPKFailure(s.tableMetrics, validationErr, time.Since(validationStarted))
			return validationErr
		}
		pkColumnsByTable = validation.ColumnsByTable
		dataRecords, _ = expandMetadataPKKeyChanges(dataRecords, validation)
	}

	// Validate data identity and target routing before DDL so a malformed row
	// cannot leave a schema-only side effect.
	if err := ApplyDDLRecords(ctx, ddlRecords, s.ddLPolicy, func(ctx context.Context, ddl, table string) error {
		if err := validateDorisApplyDDL(ddl); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("execute DDL %q: %w", ddl, err)
		}
		s.schemaCache.InvalidateCache(table)
		return nil
	}); err != nil {
		return err
	}

	if len(dataRecords) == 0 {
		return nil
	}

	// Compact by (table, PK) in source order so mixed CDC batches on the same
	// key do not get reordered by the write/delete phase split.
	dataRecords = CompactRecordsByPK(dataRecords, func(table string) []string {
		if pk, ok := pkColumnsByTable[table]; ok {
			return pk
		}
		// CompactRecordsByPK receives the source metadata table. With a
		// configured static destination, that metadata may be empty (or may be
		// intentionally ignored), while pkColumnsByTable is keyed by the
		// resolved target table.
		if s.table != "" {
			if pk, ok := pkColumnsByTable[s.table]; ok {
				return pk
			}
		}
		if len(s.pkColumns) > 0 {
			return s.pkColumns
		}
		return []string{"id"}
	})

	// Auto-create missing tables and handle schema drift before writing.
	if err := s.ensureTablesAndColumns(ctx, dataRecords, pkColumnsByTable); err != nil {
		return err
	}

	// version_mode=source_order depends on a table-level contract that cannot be
	// added after the fact, so verify every routed table before the first write
	// instead of degrading silently to last-write-wins. This also covers
	// table_template fanout, where ValidateSchema intentionally skips the
	// single-table preflight.
	if err := s.ensureSourceOrderTables(ctx, dataRecords); err != nil {
		return err
	}

	// Inject the connector-owned source position into the sequence column so a
	// late-arriving older record cannot overwrite newer state. Deletes keep the
	// native DELETE path, which is already order-aware once the table carries a
	// sequence column (see findings.md, T7.1 fact 2/part 4).
	dataRecords, err = s.prepareSourceOrderRecords(dataRecords, pkColumnsByTable)
	if err != nil {
		return err
	}

	// Separate deletes from inserts/upserts.
	var deletes, writes []core.Record
	for _, rec := range dataRecords {
		if rec.Operation == core.OpDelete {
			deletes = append(deletes, rec)
		} else {
			writes = append(writes, rec)
		}
	}
	// A key-changing UPDATE necessarily expands to write-new then delete-old.
	// That sequence is safe for at-least-once replay (a crash can leave a stale
	// old row until retry, but cannot silently lose the new row). Continue to
	// require the explicit opt-in for an originally mixed write/delete batch.
	if originalHasWrites && originalHasDeletes && !s.allowMixedCDCNonAtomic {
		return fmt.Errorf("doris sink refuses mixed write/delete CDC batch because Stream Load and MySQL DELETE are not atomic together; set allow_mixed_cdc_non_atomic=true to accept at-least-once non-atomic semantics")
	}

	// Apply writes before deletes so mixed CDC batches preserve source order for
	// delete-after-update cases. Deletes are still isolated into one bulk phase.
	if len(writes) > 0 {
		switch s.writeMode {
		case "insert":
			if err := s.writeViaInsert(ctx, writes); err != nil {
				return err
			}
		default:
			if err := s.writeViaStreamLoad(ctx, writes); err != nil {
				return err
			}
		}
	}
	if len(deletes) > 0 {
		if err := s.batchDeleteRecords(ctx, deletes, pkColumnsByTable); err != nil {
			return err
		}
	}
	// Publish the acknowledged boundary: table -> rows, plus the Stream Load
	// labels recorded per sub-batch. The runner persists this in the checkpoint
	// envelope so a crash-window replay is deduplicated by Doris.
	tableRows := make(map[string]int, 4)
	for _, rec := range writes {
		table, resolveErr := s.resolveTable(rec)
		if resolveErr != nil {
			return resolveErr
		}
		tableRows[table] += 1
	}
	protocol := "stream_load"
	switch {
	case s.writeMode == "insert":
		protocol = "insert"
	case len(writes) == 0:
		protocol = "delete_only"
	}
	s.batchSeq++
	s.finalizeDorisBatch(s.batchSeq, len(records), tableRows, protocol)

	s.recordMetrics(len(records), time.Since(start))
	return nil
}

// pkColumnsByTable applies the shared complete-identity contract before Doris
// schema or row operations. Ordinary metadata-driven records never fall back
// to static pk_columns or id.
func (s *DorisSink) pkColumnsByTable(records []core.Record) (map[string][]string, error) {
	if !s.pkColumnsFromMetadata {
		return map[string][]string{}, nil
	}
	started := time.Now()
	validation, err := validateMetadataPKBatch(s.name, records, s.pkColumns, func(record core.Record) (metadataPKTarget, error) {
		table, resolveErr := s.resolveTable(record)
		return metadataPKTarget{Database: s.database, Table: table}, resolveErr
	})
	if err != nil {
		recordMetadataPKFailure(s.tableMetrics, err, time.Since(started))
		return nil, err
	}
	return validation.ColumnsByTable, nil
}

// applyDDL executes a DDL statement on the Doris target via MySQL protocol
// and invalidates the schema cache.
func (s *DorisSink) applyDDL(ctx context.Context, ddlRec core.Record) error {
	ddl := ddlRec.Metadata.DDL
	if ddl == "" {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("execute DDL %q: %w", ddl, err)
	}
	// Invalidate schema cache.
	s.schemaCache.InvalidateCache(ddlRec.Metadata.Table)
	return nil
}

// ── Stream Load (HTTP) ─────────────────────────────────────────────────

// writeViaStreamLoad groups records by table and sends each group via Doris
// Stream Load HTTP API. Doris's Unique Key model handles upsert automatically.
func (s *DorisSink) writeViaStreamLoad(ctx context.Context, records []core.Record) error {
	// Group by table to send one Stream Load request per table.
	tableGroups := make(map[string][]core.Record)
	for _, rec := range records {
		tableName, err := s.resolveTable(rec)
		if err != nil {
			return err
		}
		tableGroups[tableName] = append(tableGroups[tableName], rec)
	}

	for tableName, recs := range tableGroups {
		started := time.Now()
		if err := s.streamLoad(ctx, tableName, recs); err != nil {
			s.tableMetrics.record(tableName, 0, time.Since(started), true)
			return fmt.Errorf("stream load to %s: %w", tableName, err)
		}
		s.tableMetrics.record(tableName, len(recs), time.Since(started), false)
	}
	return nil
}

// streamLoad sends a batch of records to Doris via the Stream Load HTTP API.
// Uses a deterministic label per batch so Doris can deduplicate on retry.
// Retries transient failures (5xx, network errors) with exponential backoff.
func (s *DorisSink) streamLoad(ctx context.Context, tableName string, records []core.Record) error {
	backoff := time.Second
	const maxBackoff = 10 * time.Second
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		err := s.streamLoadOnce(ctx, tableName, records)
		if err == nil {
			return nil
		}
		lastErr = err
		// Only retry on transient errors.
		if !isRetryableStreamLoadErr(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
	return lastErr
}

func isRetryableStreamLoadErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Network/timeout errors are retryable.
	if strings.Contains(msg, "connection") || strings.Contains(msg, "timeout") || strings.Contains(msg, "EOF") || strings.Contains(msg, "refused") {
		return true
	}
	return core.IsRetryableError(err) || strings.Contains(msg, "HTTP 5") || strings.Contains(msg, "HTTP 429")
}

// streamLoadOnce performs a single Stream Load HTTP request without retry.
func (s *DorisSink) streamLoadOnce(ctx context.Context, tableName string, records []core.Record) error {
	var body []byte
	var contentType string

	if s.streamLoadFormat == "csv" {
		body = s.buildCSVBody(records, tableName)
		contentType = "text/csv"
	} else {
		body = s.buildJSONBody(records)
		contentType = "application/json"
	}

	streamURL := fmt.Sprintf("%s://%s:%d/api/%s/%s/_stream_load",
		s.streamLoadScheme, s.host, s.httpPort, url.PathEscape(s.database), url.PathEscape(tableName))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, streamURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build stream load request: %w", err)
	}

	// Doris Stream Load headers
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("format", s.streamLoadFormat)
	if s.streamLoadFormat == "json" {
		req.Header.Set("read_json_by_line", "true")
	}
	// Deterministic label: hash of (database, table, payload) so retries
	// produce the same label and Doris can deduplicate. Different batches
	// get different labels because the payload differs.
	label := s.streamLoadLabel(tableName, body)
	req.Header.Set("label", label)

	// For CSV, specify column order so Doris maps correctly.
	if s.streamLoadFormat == "csv" {
		cols := s.csvColumnOrder(records)
		if len(cols) > 0 {
			req.Header.Set("columns", strings.Join(cols, ","))
		}
		req.Header.Set("column_separator", ",")
	}

	// Auth
	if s.user != "" {
		req.SetBasicAuth(s.user, s.password)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stream load HTTP request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusOK {
		return classifyDorisStreamLoadError(resp.StatusCode, fmt.Sprintf("stream load failed (HTTP %d): %s", resp.StatusCode, string(respBody)))
	}

	// Parse Doris Stream Load response JSON to check success.
	var slResp struct {
		Status           string `json:"status"`
		StatusCode       int    `json:"StatusCode"`
		Message          string `json:"Message"`
		NumberTotalRows  int    `json:"NumberTotalRows"`
		NumberLoadedRows int    `json:"NumberLoadedRows"`
	}
	if err := json.Unmarshal(respBody, &slResp); err != nil {
		// If the response is not valid JSON, it's likely an error page.
		return core.ClassifiedError{Class: core.ErrorClassTransient, Err: fmt.Errorf("stream load: unparseable response (HTTP %d): %s", resp.StatusCode, string(respBody)[:min(len(respBody), 200)])}
	}
	if slResp.Status == "Fail" || slResp.StatusCode >= 400 {
		return classifyDorisStreamLoadError(slResp.StatusCode, fmt.Sprintf("stream load rejected: %s (loaded %d/%d)",
			slResp.Message, slResp.NumberLoadedRows, slResp.NumberTotalRows))
	}
	// Remember the label of the acknowledged sub-batch: Doris keeps it for
	// label_keep_max_second, so a replayed identical batch is deduplicated
	// server-side, and the label is the audit token of this commit boundary.
	s.recordAckedLabel(tableName, label)

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func classifyDorisStreamLoadError(statusCode int, message string) error {
	class := core.ErrorClassData
	lower := strings.ToLower(message)
	switch {
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		class = core.ErrorClassAuth
	case statusCode == http.StatusTooManyRequests || statusCode >= 500:
		class = core.ErrorClassTransient
	case strings.Contains(lower, "unknown column") || strings.Contains(lower, "schema") || (strings.Contains(lower, "table") && strings.Contains(lower, "not exist")):
		class = core.ErrorClassSchema
	case strings.Contains(lower, "data quality") || strings.Contains(lower, "too many filtered rows") || strings.Contains(lower, "invalid") || strings.Contains(lower, "out of range") || strings.Contains(lower, "data too long"):
		class = core.ErrorClassData
	default:
		if statusCode == 0 {
			class = core.ClassifyError(fmt.Errorf("%s", message))
		}
	}
	return core.ClassifiedError{Class: class, Err: fmt.Errorf("%s", message)}
}

// buildJSONBody converts records to Doris Stream Load JSON format.
// Each record becomes a JSON object on its own line (JSON line format).
func (s *DorisSink) buildJSONBody(records []core.Record) []byte {
	var buf bytes.Buffer
	for _, rec := range records {
		data, err := json.Marshal(rec.Data)
		if err != nil {
			data = []byte(`{}`)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// csvColumnOrder returns the sorted column names from the first record.
func (s *DorisSink) csvColumnOrder(records []core.Record) []string {
	if len(records) == 0 {
		return nil
	}
	cols := make([]string, 0, len(records[0].Data))
	for k := range records[0].Data {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

// buildCSVBody converts records to CSV format for Stream Load.
// Column order is derived from the first record and sorted alphabetically.
func (s *DorisSink) buildCSVBody(records []core.Record, tableName string) []byte {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	cols := s.csvColumnOrder(records)

	for _, rec := range records {
		row := make([]string, len(cols))
		for i, c := range cols {
			val := rec.Data[c]
			if val != nil {
				row[i] = fmt.Sprint(val)
			}
		}
		_ = w.Write(row)
	}
	w.Flush()
	return buf.Bytes()
}

// ── MySQL protocol INSERT (fallback) ────────────────────────────────────

func (s *DorisSink) writeViaInsert(ctx context.Context, records []core.Record) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	type groupKey struct {
		table string
		cols  string
	}
	type groupBuf struct {
		cols []string
		rows [][]any
	}
	groups := make(map[groupKey]*groupBuf)
	var order []groupKey

	for _, rec := range records {
		tableName, err := s.resolveTable(rec)
		if err != nil {
			return err
		}
		cols := make([]string, 0, len(rec.Data))
		for k := range rec.Data {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		key := groupKey{table: tableName, cols: strings.Join(cols, ",")}

		g, ok := groups[key]
		if !ok {
			g = &groupBuf{cols: cols}
			groups[key] = g
			order = append(order, key)
		}
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = rec.Data[c]
		}
		g.rows = append(g.rows, row)
	}

	for _, key := range order {
		g := groups[key]
		started := time.Now()
		if err := s.batchInsert(ctx, tx, key.table, g.cols, g.rows); err != nil {
			s.tableMetrics.record(key.table, 0, time.Since(started), true)
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		for _, key := range order {
			s.tableMetrics.record(key.table, 0, 0, true)
		}
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	for _, key := range order {
		s.tableMetrics.record(key.table, len(groups[key].rows), 0, false)
	}
	return nil
}

func (s *DorisSink) batchInsert(ctx context.Context, tx *sql.Tx, table string, cols []string, rows [][]any) error {
	for offset := 0; offset < len(rows); offset += s.insertChunkSize {
		end := offset + s.insertChunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[offset:end]
		if len(chunk) == 0 {
			continue
		}
		query := s.buildInsertStatement(table, cols, len(chunk))
		args := make([]any, 0, len(chunk)*len(cols))
		for _, row := range chunk {
			args = append(args, row...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("batch insert %s (rows=%d): %w", table, len(chunk), err)
		}
	}
	return nil
}

func (s *DorisSink) buildInsertStatement(table string, cols []string, rowCount int) string {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quoteIdentMySQL(c)
	}
	colList := strings.Join(quotedCols, ",")
	rowPlaceholder := "(" + strings.Repeat("?,", len(cols)-1) + "?)"

	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(quoteIdentMySQL(table))
	b.WriteString(" (")
	b.WriteString(colList)
	b.WriteString(") VALUES ")
	b.WriteString(strings.Repeat(rowPlaceholder+",", rowCount-1))
	b.WriteString(rowPlaceholder)
	return b.String()
}

// ── DELETE via MySQL protocol ───────────────────────────────────────────

func (s *DorisSink) batchDeleteRecords(ctx context.Context, records []core.Record, pkColumnsByTable map[string][]string) error {
	// Group by table
	tableGroups := make(map[string][]core.Record)
	for _, rec := range records {
		tableName, err := s.resolveTable(rec)
		if err != nil {
			return err
		}
		tableGroups[tableName] = append(tableGroups[tableName], rec)
	}

	for tableName, recs := range tableGroups {
		started := time.Now()
		pkCols := s.pkColumns
		if derived, ok := pkColumnsByTable[tableName]; ok {
			pkCols = derived
		}
		if len(pkCols) == 0 {
			pkCols = []string{"id"}
		}
		for offset := 0; offset < len(recs); offset += s.insertChunkSize {
			end := offset + s.insertChunkSize
			if end > len(recs) {
				end = len(recs)
			}
			chunk := recs[offset:end]
			if len(chunk) == 0 {
				continue
			}
			if len(pkCols) == 1 {
				query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
					quoteIdentMySQL(tableName), quoteIdentMySQL(pkCols[0]), strings.Repeat("?,", len(chunk)-1)+"?")
				args := make([]any, 0, len(chunk))
				for _, rec := range chunk {
					keys, err := ResolveDeleteKeys(rec, pkCols)
					if err != nil {
						return fmt.Errorf("batch delete %s: %w", tableName, err)
					}
					args = append(args, keys[pkCols[0]])
				}
				if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
					s.tableMetrics.record(tableName, 0, time.Since(started), true)
					return fmt.Errorf("batch delete %s (rows=%d): %w", tableName, len(chunk), err)
				}
				continue
			}

			quotedPk := make([]string, len(pkCols))
			for i, c := range pkCols {
				quotedPk[i] = quoteIdentMySQL(c) + "=?"
			}
			rowCond := "(" + strings.Join(quotedPk, " AND ") + ")"

			var b strings.Builder
			b.WriteString("DELETE FROM ")
			b.WriteString(quoteIdentMySQL(tableName))
			b.WriteString(" WHERE ")
			b.WriteString(strings.Repeat(rowCond+" OR ", len(chunk)-1))
			b.WriteString(rowCond)

			args := make([]any, 0, len(chunk)*len(pkCols))
			for _, rec := range chunk {
				keys, err := ResolveDeleteKeys(rec, pkCols)
				if err != nil {
					return fmt.Errorf("batch delete %s: %w", tableName, err)
				}
				for _, pk := range pkCols {
					args = append(args, keys[pk])
				}
			}
			if _, err := s.db.ExecContext(ctx, b.String(), args...); err != nil {
				s.tableMetrics.record(tableName, 0, time.Since(started), true)
				return fmt.Errorf("batch delete %s (rows=%d): %w", tableName, len(chunk), err)
			}
		}
		s.tableMetrics.record(tableName, len(recs), time.Since(started), false)
	}
	return nil
}

// ── Schema Management (SchemaManager) ───────────────────────────────────

// EnsureSchema implements core.SchemaManager.
func (s *DorisSink) EnsureSchema(ctx context.Context, tableName string, fields []string, fieldValues map[string]any) error {
	return s.ensureSchemaWithPK(ctx, tableName, fields, fieldValues, nil)
}

func (s *DorisSink) ensureSchemaWithPK(ctx context.Context, tableName string, fields []string, fieldValues map[string]any, pkColumns []string) error {
	return core.EnsureSchemaGeneric(ctx, s.schemaCache, tableName, fields, fieldValues,
		s.autoCreate, core.SchemaDriftMode(s.schemaDrift),
		s.tableExists,
		func(ctx context.Context, table string, columns []string, values map[string]any) error {
			return s.createTableFromFieldsWithPK(ctx, table, columns, values, pkColumns)
		},
		s.getExistingColumns, s.addColumn,
	)
}

func (s *DorisSink) ensureTablesAndColumns(ctx context.Context, records []core.Record, pkColumnsByTable map[string][]string) error {
	if !s.autoCreate && s.schemaDrift != "add_columns" && s.schemaDrift != "fail" && s.schemaDrift != dorisSchemaDriftSync {
		return nil
	}
	tableCols, tableValues, err := s.collectSchemaInputs(records)
	if err != nil {
		return err
	}
	for tableName, cols := range tableCols {
		if err := s.ensureSchemaWithPK(ctx, tableName, cols, tableValues[tableName], pkColumnsByTable[tableName]); err != nil {
			return err
		}
	}
	if s.schemaDrift == dorisSchemaDriftSync {
		return s.syncColumnTypes(ctx, records, tableValues)
	}
	return nil
}

// syncColumnTypes is the Doris counterpart of ClickHouse's schema_drift=sync: a
// source column that widened (INT -> BIGINT, VARCHAR(32) -> VARCHAR(128), ...)
// is ALTERed on the target instead of silently truncating or failing the load.
// Doris only supports widening, so a narrowing request is refused rather than
// risking data loss.
func (s *DorisSink) syncColumnTypes(ctx context.Context, records []core.Record, tableValues map[string]map[string]any) error {
	existingByTable := map[string]map[string]string{}
	for _, rec := range records {
		table, err := s.resolveTable(rec)
		if err != nil {
			return err
		}
		if table == "" {
			continue
		}
		existing, ok := existingByTable[table]
		if !ok {
			cols, err := s.getExistingColumnInfo(ctx, table)
			if err != nil {
				return fmt.Errorf("doris schema_drift=sync: read columns for %s.%s: %w", s.database, table, err)
			}
			existing = make(map[string]string, len(cols))
			for _, col := range cols {
				existing[normalizeIdentifier(col.Name)] = col.DataType
			}
			existingByTable[table] = existing
		}
		values := tableValues[table]
		names := make([]string, 0, len(rec.Data))
		for name := range rec.Data {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			current, ok := existing[normalizeIdentifier(name)]
			if !ok {
				// Missing columns are handled by add_columns above.
				continue
			}
			declared := ""
			if values != nil {
				if types, ok := values["__column_types__"].(map[string]string); ok {
					declared = types[name]
				}
			}
			desired := inferDorisTypeDeclared(name, rec.Data[name], declared)
			if dorisTypeCompatible(current, desired) {
				continue
			}
			if !dorisTypeWidens(current, desired) {
				return fmt.Errorf("doris schema_drift=sync refuses to narrow %s.%s.%s from %s to %s: Doris cannot shrink a column without losing data; change the source contract or keep schema_drift=add_columns",
					s.database, table, name, current, desired)
			}
			ddl := fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s",
				quoteIdentMySQL(s.database), quoteIdentMySQL(table), quoteIdentMySQL(name), desired)
			if _, err := s.db.ExecContext(ctx, ddl); err != nil {
				return fmt.Errorf("doris schema_drift=sync: modify %s.%s.%s to %s: %w", s.database, table, name, desired, err)
			}
			existing[normalizeIdentifier(name)] = desired
			s.schemaCache.InvalidateCache(table)
		}
	}
	return nil
}

// dorisSchemaDriftSync is the additive schema drift mode: add_columns plus
// ALTER ... MODIFY COLUMN for widened types.
const dorisSchemaDriftSync = "sync"

// dorisTypeCompatible reports whether an existing column type already holds
// every value the desired type can carry, i.e. no ALTER is needed.
func dorisTypeCompatible(existing, desired string) bool {
	e := normalizeDorisType(existing)
	d := normalizeDorisType(desired)
	if e == d {
		return true
	}
	switch {
	case dorisStringType(e) && dorisStringType(d):
		el, elOK := dorisTypeLength(e)
		dl, dlOK := dorisTypeLength(d)
		if elOK && dlOK {
			return el >= dl
		}
		return true
	case dorisDecimalType(e) && dorisDecimalType(d):
		return dorisDecimalCovers(e, d)
	case dorisDecimalType(e):
		// A DECIMAL cannot absorb an arbitrary-width integer or a float.
		return false
	case dorisDecimalType(d):
		// Widening an integer/float column to DECIMAL keeps every value.
		return dorisIntegerType(e) || dorisFloatType(e)
	case dorisIntegerType(e) && dorisIntegerType(d):
		return dorisIntegerRank(e) >= dorisIntegerRank(d)
	case dorisFloatType(e) && dorisIntegerType(d):
		return true
	case dorisFloatType(e) && dorisFloatType(d):
		return dorisFloatRank(e) >= dorisFloatRank(d)
	default:
		// INT -> FLOAT/DOUBLE and anything -> string handled above; the rest
		// (float -> integer, int -> float) lose precision or fractions.
		return false
	}
}

// dorisTypeWidens reports whether desired is an acceptable strict widening of
// existing, the only type change Doris can apply without losing data.
func dorisTypeWidens(existing, desired string) bool {
	e := normalizeDorisType(existing)
	d := normalizeDorisType(desired)
	if e == d || dorisTypeCompatible(e, d) {
		return true
	}
	switch {
	case dorisStringType(e) && dorisStringType(d):
		return true
	case dorisDecimalType(e) && dorisDecimalType(d):
		return true
	case dorisDecimalType(d):
		return dorisIntegerType(e) || dorisFloatType(e)
	case dorisIntegerType(e) && dorisIntegerType(d):
		return dorisIntegerRank(d) > dorisIntegerRank(e)
	case dorisIntegerType(e) && dorisFloatType(d):
		return true
	case dorisFloatType(e) && dorisFloatType(d):
		return dorisFloatRank(d) > dorisFloatRank(e)
	default:
		return false
	}
}

func normalizeDorisType(value string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(value), " ", ""))
}

func dorisStringType(t string) bool {
	for _, prefix := range []string{"VARCHAR", "CHAR", "STRING", "TEXT"} {
		if strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}

func dorisDecimalType(t string) bool { return strings.HasPrefix(t, "DECIMAL") }

func dorisFloatType(t string) bool {
	return strings.HasPrefix(t, "FLOAT") || strings.HasPrefix(t, "DOUBLE")
}

func dorisIntegerType(t string) bool {
	for _, prefix := range []string{"TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"} {
		if strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}

func dorisIntegerRank(t string) int {
	switch {
	case strings.HasPrefix(t, "TINYINT"):
		return 1
	case strings.HasPrefix(t, "SMALLINT"):
		return 2
	case strings.HasPrefix(t, "INT"):
		return 3
	case strings.HasPrefix(t, "BIGINT"):
		return 4
	case strings.HasPrefix(t, "LARGEINT"):
		return 5
	default:
		return 0
	}
}

func dorisFloatRank(t string) int {
	if strings.HasPrefix(t, "DOUBLE") {
		return 2
	}
	return 1
}

func dorisTypeLength(t string) (int, bool) {
	open := strings.Index(t, "(")
	closeIdx := strings.Index(t, ")")
	if open < 0 || closeIdx <= open {
		return 0, false
	}
	n, err := strconv.Atoi(t[open+1 : closeIdx])
	if err != nil {
		return 0, false
	}
	return n, true
}

// dorisDecimalCovers reports whether DECIMAL(existing) can hold every value of
// DECIMAL(desired): at least as many integer digits and at least the same scale.
func dorisDecimalCovers(existing, desired string) bool {
	ep, es, eok := dorisDecimalParts(existing)
	dp, ds, dok := dorisDecimalParts(desired)
	if !eok || !dok {
		return true
	}
	return (ep-es) >= (dp-ds) && es >= ds
}

func dorisDecimalParts(t string) (precision, scale int, ok bool) {
	open := strings.Index(t, "(")
	closeIdx := strings.Index(t, ")")
	if open < 0 || closeIdx <= open {
		return 0, 0, false
	}
	parts := strings.Split(t[open+1:closeIdx], ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	p, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	s, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return p, s, true
}

// dorisSchemaDriftSync is the additive schema drift mode: add_columns plus
// ALTER ... MODIFY COLUMN for widened types.

func (s *DorisSink) collectSchemaInputs(records []core.Record) (map[string][]string, map[string]map[string]any, error) {
	tableCols := make(map[string][]string)
	tableValues := make(map[string]map[string]any)
	tableDeclared := make(map[string]map[string]string)
	for _, rec := range records {
		tableName, err := s.resolveTable(rec)
		if err != nil {
			return nil, nil, err
		}
		if tableName == "" {
			continue
		}
		if tableValues[tableName] == nil {
			tableValues[tableName] = make(map[string]any)
		}
		if tableDeclared[tableName] == nil {
			tableDeclared[tableName] = make(map[string]string)
		}
		for k := range rec.Data {
			found := false
			for _, existing := range tableCols[tableName] {
				if existing == k {
					found = true
					break
				}
			}
			if !found {
				tableCols[tableName] = append(tableCols[tableName], k)
			}
			if _, ok := tableValues[tableName][k]; !ok && rec.Data[k] != nil {
				tableValues[tableName][k] = rec.Data[k]
			}
		}
		for column, declaredType := range rec.Metadata.ColumnTypes {
			if strings.TrimSpace(column) == "" || strings.TrimSpace(declaredType) == "" {
				continue
			}
			if _, exists := tableDeclared[tableName][column]; !exists {
				tableDeclared[tableName][column] = declaredType
			}
		}
	}
	for tableName, declared := range tableDeclared {
		if len(declared) > 0 {
			tableValues[tableName]["__column_types__"] = declared
		}
	}
	return tableCols, tableValues, nil
}

func (s *DorisSink) tableExists(ctx context.Context, table string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
		s.database, table,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *DorisSink) getExistingColumns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?`,
		s.database, table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := make(map[string]bool)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols[col] = true
	}
	return cols, nil
}

func (s *DorisSink) getExistingColumnInfo(ctx context.Context, table string) ([]core.ColumnInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT column_name, column_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`,
		s.database, table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []core.ColumnInfo
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return nil, err
		}
		cols = append(cols, core.ColumnInfo{Name: name, DataType: dataType, Nullable: strings.EqualFold(nullable, "YES")})
	}
	return cols, rows.Err()
}

func (s *DorisSink) validateUniqueKeyModel(ctx context.Context, table string) error {
	createStmt, err := s.showCreateTable(ctx, table)
	if err != nil {
		return fmt.Errorf("validate doris unique key model for %s.%s: %w", s.database, table, err)
	}
	return s.validateUniqueKeyStatement(createStmt, table)
}

// validateUniqueKeyStatement checks the UNIQUE KEY contract against an already
// fetched SHOW CREATE TABLE statement (so callers can share one query with the
// source_order contract check).
func (s *DorisSink) validateUniqueKeyStatement(createStmt, table string) error {
	uniqueKeys := parseDorisUniqueKeyColumns(createStmt)
	if len(uniqueKeys) == 0 {
		return fmt.Errorf("schema validation failed for doris %s.%s: batch_mode=upsert requires a Doris UNIQUE KEY table; existing table is not Unique Key or SHOW CREATE TABLE did not expose UNIQUE KEY", s.database, table)
	}
	if !sameIdentifierSet(uniqueKeys, s.pkColumns) {
		return fmt.Errorf("schema validation failed for doris %s.%s: pk_columns %v do not match Doris UNIQUE KEY %v; replay-safe upsert requires the configured business key to be the table unique key", s.database, table, s.pkColumns, uniqueKeys)
	}
	return nil
}

// validateSourceOrderTable enforces the Doris table contract required by
// version_mode=source_order on an existing table.
func (s *DorisSink) validateSourceOrderTable(ctx context.Context, table string) error {
	if !s.sourceOrderEnabled() {
		return nil
	}
	createStmt, err := s.showCreateTable(ctx, table)
	if err != nil {
		return fmt.Errorf("validate doris source_order contract for %s.%s: %w", s.database, table, err)
	}
	return s.validateSourceOrderStatement(createStmt, table)
}

// ensureSourceOrderTables validates the source_order contract for every table
// this batch routes to, caching successes per process. ValidateSchema covers
// the static-table case at startup; this covers table_template fanout and
// pipelines that were started before the table was created.
func (s *DorisSink) ensureSourceOrderTables(ctx context.Context, records []core.Record) error {
	if !s.sourceOrderEnabled() {
		return nil
	}
	tables := make(map[string]struct{}, 4)
	for _, rec := range records {
		table, err := s.resolveTable(rec)
		if err != nil {
			return err
		}
		if table == "" {
			continue
		}
		tables[table] = struct{}{}
	}
	for table := range tables {
		if _, ok := s.validatedSourceOrderTables.Load(table); ok {
			continue
		}
		if err := s.validateSourceOrderTable(ctx, table); err != nil {
			return err
		}
		s.validatedSourceOrderTables.Store(table, struct{}{})
	}
	return nil
}

// validateSourceOrderStatement is the pure (SHOW CREATE TABLE -> error) half of
// validateSourceOrderTable. Doris 2.1.11 cannot acquire this contract after the
// fact: ALTER TABLE ... SET ("function_column.sequence_col") fails with
// "Unknown table property" and ADD COLUMN ... DEFAULT is a syntax error, so a
// mismatching table must be replaced, never mutated in place.
func (s *DorisSink) validateSourceOrderStatement(createStmt, table string) error {
	if !s.sourceOrderEnabled() {
		return nil
	}
	sequenceColumn := s.effectiveSequenceColumn()
	sequenceNorm := normalizeIdentifier(sequenceColumn)
	target := fmt.Sprintf("doris %s.%s", s.database, table)

	uniqueKeys := parseDorisUniqueKeyColumns(createStmt)
	if len(uniqueKeys) == 0 {
		return fmt.Errorf("schema validation failed for %s: version_mode=source_order requires a Doris UNIQUE KEY table", target)
	}
	for _, key := range uniqueKeys {
		if normalizeIdentifier(key) == sequenceNorm {
			return fmt.Errorf("schema validation failed for %s: sequence_column %q is part of the UNIQUE KEY %v; the sequence column must be a non-key column so ordering never changes the row identity", target, sequenceColumn, uniqueKeys)
		}
	}

	properties := parseDorisTableProperties(createStmt)
	if !strings.EqualFold(properties["enable_unique_key_merge_on_write"], "true") {
		return fmt.Errorf("schema validation failed for %s: version_mode=source_order requires PROPERTIES(\"enable_unique_key_merge_on_write\"=\"true\"); without merge-on-write a DELETE tombstone does not participate in sequence comparison, so an older replay resurrects the deleted row. %s",
			target, dorisSourceOrderMigrationRemedy(target, sequenceColumn, uniqueKeys))
	}

	gotSequence := strings.TrimSpace(properties["function_column.sequence_col"])
	if gotSequence == "" {
		return fmt.Errorf("schema validation failed for %s: table does not declare PROPERTIES(\"function_column.sequence_col\"=%q), so Doris ignores source-order versions and silently keeps last-write-wins. %s",
			target, sequenceColumn, dorisSourceOrderMigrationRemedy(target, sequenceColumn, uniqueKeys))
	}
	if normalizeIdentifier(gotSequence) != sequenceNorm {
		return fmt.Errorf("schema validation failed for %s: table declares function_column.sequence_col=%q but the sink writes %q; either align sequence_column or point the table at the column the sink fills. %s",
			target, gotSequence, sequenceColumn, dorisSourceOrderMigrationRemedy(target, sequenceColumn, uniqueKeys))
	}

	declaredType, ok := parseDorisColumnType(createStmt, sequenceColumn)
	if !ok {
		return fmt.Errorf("schema validation failed for %s: sequence column %q is not declared in the table schema. %s",
			target, sequenceColumn, dorisSourceOrderMigrationRemedy(target, sequenceColumn, uniqueKeys))
	}
	if !dorisSequenceTypeCompatible(declaredType, s.effectiveSequenceType()) {
		return fmt.Errorf("schema validation failed for %s: sequence column %q is %s but sequence_type=%s requires %s; Doris rejects DECIMAL sequence columns and silently wraps BIGINT values above math.MaxInt64, so the declared type matters. %s",
			target, sequenceColumn, declaredType, s.effectiveSequenceType(),
			strings.ToUpper(s.dorisSequenceColumnDDLType()), dorisSourceOrderMigrationRemedy(target, sequenceColumn, uniqueKeys))
	}
	return nil
}

// dorisSourceOrderMigrationRemedy is the migration guidance attached to every
// source_order contract failure. It deliberately does not suggest ALTER TABLE
// (Doris 2.1.11 cannot set function_column.sequence_col on an existing table).
func dorisSourceOrderMigrationRemedy(target, sequenceColumn string, uniqueKeys []string) string {
	keys := strings.Join(uniqueKeys, ", ")
	if keys == "" {
		keys = "<pk columns>"
	}
	return fmt.Sprintf("Doris cannot add the sequence contract to an existing table (ALTER TABLE ... SET (\"function_column.sequence_col\") is rejected and ADD COLUMN ... DEFAULT is a syntax error). "+
		"Migrate by: (1) CREATE TABLE with UNIQUE KEY(%s) and PROPERTIES(\"enable_unique_key_merge_on_write\"=\"true\", \"function_column.sequence_col\"=\"%s\") where %q is LARGEINT; "+
		"(2) backfill the current state with monotonically increasing sequence values; (3) reconcile row counts/checksums against the old table; "+
		"(4) switch the pipeline to the new table and keep %s for rollback.",
		keys, sequenceColumn, sequenceColumn, target)
}

// parseDorisTableProperties extracts the quoted "name" = "value" pairs from a
// SHOW CREATE TABLE statement's PROPERTIES block.
func parseDorisTableProperties(createStmt string) map[string]string {
	props := make(map[string]string)
	for _, match := range dorisQuotedPairPattern.FindAllStringSubmatch(createStmt, -1) {
		props[strings.ToLower(strings.TrimSpace(match[1]))] = match[2]
	}
	return props
}

// parseDorisColumnType returns the declared type of a column from a SHOW CREATE
// TABLE statement (e.g. "_version LARGEINT NOT NULL" -> "LARGEINT").
func parseDorisColumnType(createStmt, column string) (string, bool) {
	want := normalizeIdentifier(column)
	for _, line := range strings.Split(createStmt, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
		if !strings.HasPrefix(line, "`") {
			continue
		}
		end := strings.Index(line[1:], "`")
		if end < 0 {
			continue
		}
		name := normalizeIdentifier(line[1 : 1+end])
		if name != want {
			continue
		}
		rest := strings.TrimSpace(line[2+end:])
		if rest == "" {
			return "", false
		}
		// Declared type is the first token, but keep a parenthesised precision
		// attached even if SHOW CREATE TABLE emitted "DECIMAL (20, 0)".
		fields := strings.Fields(rest)
		typ := fields[0]
		for i := 1; i < len(fields); i++ {
			next := fields[i]
			if strings.HasPrefix(next, "(") || strings.HasSuffix(typ, "(") || strings.HasSuffix(typ, ",") {
				typ += next
				continue
			}
			break
		}
		return strings.ToUpper(strings.TrimSpace(typ)), true
	}
	return "", false
}

// dorisSequenceTypeCompatible reports whether a declared column type can carry
// source-order values for the configured sequence_type.
func dorisSequenceTypeCompatible(declaredType, sequenceType string) bool {
	declared := strings.ToUpper(strings.TrimSpace(declaredType))
	if idx := strings.Index(declared, "("); idx >= 0 {
		declared = strings.TrimSpace(declared[:idx])
	}
	switch sequenceType {
	case dorisSequenceTypeBigInt:
		return declared == "BIGINT" || declared == "LARGEINT"
	default:
		// largeint (and any unknown fallback) needs the full uint64 domain.
		return declared == "LARGEINT"
	}
}

func (s *DorisSink) showCreateTable(ctx context.Context, table string) (string, error) {
	rows, err := s.db.QueryContext(ctx, "SHOW CREATE TABLE "+quoteIdentMySQL(table))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", sql.ErrNoRows
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([]sql.NullString, len(cols))
	scan := make([]any, len(cols))
	for i := range values {
		scan[i] = &values[i]
	}
	if err := rows.Scan(scan...); err != nil {
		return "", err
	}
	for i, col := range cols {
		if strings.EqualFold(col, "Create Table") && values[i].Valid {
			return values[i].String, nil
		}
	}
	if len(values) > 1 && values[1].Valid {
		return values[1].String, nil
	}
	return "", fmt.Errorf("SHOW CREATE TABLE returned no create statement")
}

func parseDorisUniqueKeyColumns(createStmt string) []string {
	upper := strings.ToUpper(createStmt)
	idx := strings.Index(upper, "UNIQUE KEY")
	if idx < 0 {
		return nil
	}
	rest := createStmt[idx+len("UNIQUE KEY"):]
	open := strings.Index(rest, "(")
	if open < 0 {
		return nil
	}
	rest = rest[open+1:]
	close := strings.Index(rest, ")")
	if close < 0 {
		return nil
	}
	rawCols := strings.Split(rest[:close], ",")
	cols := make([]string, 0, len(rawCols))
	for _, raw := range rawCols {
		col := normalizeIdentifier(raw)
		if col != "" {
			cols = append(cols, col)
		}
	}
	return cols
}

func normalizeIdentifier(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, "`\"[]")
	if dot := strings.LastIndex(v, "."); dot >= 0 {
		v = v[dot+1:]
		v = strings.Trim(v, "`\"[]")
	}
	return strings.ToLower(v)
}

func schemaHasColumn(schema core.SchemaInfo, name string) bool {
	for _, col := range schema.Columns {
		if strings.EqualFold(col.Name, name) {
			return true
		}
	}
	return false
}

func orderDorisColumns(columns, keyCols []string) []string {
	keySet := make(map[string]bool, len(keyCols))
	out := make([]string, 0, len(columns))
	for _, key := range keyCols {
		keyNorm := normalizeIdentifier(key)
		if keySet[keyNorm] {
			continue
		}
		keySet[keyNorm] = true
		out = append(out, key)
	}

	rest := make([]string, 0, len(columns)-len(out))
	for _, col := range columns {
		if !keySet[normalizeIdentifier(col)] {
			rest = append(rest, col)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

func validateDorisApplyDDL(ddl string) error {
	normalized := strings.ToUpper(strings.Join(strings.Fields(ddl), " "))
	if strings.HasPrefix(normalized, "ALTER TABLE ") && strings.Contains(normalized, " ADD COLUMN ") {
		for _, blocked := range []string{" DROP ", " DROP COLUMN ", " MODIFY ", " CHANGE ", " RENAME ", " TRUNCATE "} {
			if strings.Contains(normalized, blocked) {
				return fmt.Errorf("doris ddl_policy=apply only permits safe ALTER TABLE ADD COLUMN statements, got %q", ddl)
			}
		}
		return nil
	}
	return fmt.Errorf("doris ddl_policy=apply only permits safe ALTER TABLE ADD COLUMN statements, got %q", ddl)
}

// createTableFromFields creates a Doris table with UNIQUE KEY model.
// Doris requires ENGINE=OLAP and a key definition. We use configured or
// metadata-derived pk_columns, or id only when present, so replay-safe upsert
// never relies on an arbitrary column.
func (s *DorisSink) createTableFromFields(ctx context.Context, table string, columns []string, fieldValues map[string]any) error {
	return s.createTableFromFieldsWithPK(ctx, table, columns, fieldValues, nil)
}

func (s *DorisSink) createTableFromFieldsWithPK(ctx context.Context, table string, columns []string, fieldValues map[string]any, pkColumns []string) error {
	var (
		ddl string
		err error
	)
	if len(pkColumns) > 0 {
		ddl, err = s.buildCreateTableDDLWithPK(table, columns, fieldValues, pkColumns)
	} else {
		ddl, err = s.buildCreateTableDDL(table, columns, fieldValues)
	}
	if err != nil || ddl == "" {
		return err
	}
	_, err = s.db.ExecContext(ctx, ddl)
	return err
}

func (s *DorisSink) buildCreateTableDDL(table string, columns []string, fieldValues map[string]any) (string, error) {
	return s.buildCreateTableDDLWithPK(table, columns, fieldValues, s.pkColumns)
}

func (s *DorisSink) buildCreateTableDDLWithPK(table string, columns []string, fieldValues map[string]any, pkColumns []string) (string, error) {
	if len(columns) == 0 {
		return "", nil
	}
	sort.Strings(columns)

	// Determine key columns
	columnByNorm := make(map[string]string, len(columns))
	for _, c := range columns {
		columnByNorm[normalizeIdentifier(c)] = c
	}
	keyCols := make([]string, 0, len(pkColumns))
	if len(pkColumns) > 0 {
		seen := make(map[string]bool, len(pkColumns))
		for _, configured := range pkColumns {
			norm := normalizeIdentifier(configured)
			if seen[norm] {
				continue
			}
			actual, ok := columnByNorm[norm]
			if !ok {
				return "", fmt.Errorf("create doris table %s: pk_column %q is not present in source fields %v", table, configured, columns)
			}
			seen[norm] = true
			keyCols = append(keyCols, actual)
		}
	} else {
		// Default: use "id" if present. Do not silently pick the first
		// arbitrary column: replay-safe Doris upsert requires a stable key.
		if actual, ok := columnByNorm["id"]; ok {
			keyCols = []string{actual}
		}
		if len(keyCols) == 0 {
			return "", fmt.Errorf("create doris table %s: pk_columns is required when no id column is present; Doris production upsert requires an explicit UNIQUE KEY", table)
		}
	}
	columns = orderDorisColumns(columns, keyCols)
	keySet := make(map[string]bool)
	for _, c := range keyCols {
		keySet[normalizeIdentifier(c)] = true
	}

	// source_order adds the sequence column to the auto-created table. It is
	// not part of the source data, so it must be injected here even when the
	// caller never saw it in `columns`.
	sequenceColumn := ""
	if s.sourceOrderEnabled() {
		sequenceColumn = s.effectiveSequenceColumn()
		if keySet[normalizeIdentifier(sequenceColumn)] {
			return "", fmt.Errorf("create doris table %s: sequence_column %q collides with the UNIQUE KEY %v; the sequence column must not be part of the key", table, sequenceColumn, keyCols)
		}
		present := false
		for _, c := range columns {
			if normalizeIdentifier(c) == normalizeIdentifier(sequenceColumn) {
				present = true
				break
			}
		}
		if !present {
			columns = append(columns, sequenceColumn)
			columns = orderDorisColumns(columns, keyCols)
		}
	}

	var recordDeclared map[string]string
	if fieldValues != nil {
		if declared, ok := fieldValues["__column_types__"].(map[string]string); ok {
			recordDeclared = declared
		}
	}

	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(quoteIdentMySQL(table))
	b.WriteString(" (\n")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("  ")
		b.WriteString(quoteIdentMySQL(c))
		b.WriteString(" ")

		if sequenceColumn != "" && normalizeIdentifier(c) == normalizeIdentifier(sequenceColumn) {
			// Force the configured type: inferring it from a sample value would
			// pick BIGINT for a small first batch and then silently wrap larger
			// source-order values.
			b.WriteString(s.dorisSequenceColumnDDLType())
			b.WriteString(" NOT NULL")
		} else if keySet[normalizeIdentifier(c)] {
			b.WriteString(inferDorisKeyTypeDeclared(c, fieldValues[c], recordDeclared[c]))
			b.WriteString(" NOT NULL")
		} else {
			b.WriteString(inferDorisTypeDeclared(c, fieldValues[c], recordDeclared[c]))
		}
	}

	// UNIQUE KEY clause
	quotedKeys := make([]string, len(keyCols))
	for i, c := range keyCols {
		quotedKeys[i] = quoteIdentMySQL(c)
	}
	b.WriteString("\n) ENGINE=OLAP\n")
	b.WriteString("UNIQUE KEY(")
	b.WriteString(strings.Join(quotedKeys, ", "))
	b.WriteString(")\n")
	b.WriteString("DISTRIBUTED BY HASH(")
	b.WriteString(strings.Join(quotedKeys, ", "))
	b.WriteString(") BUCKETS 10\n")
	b.WriteString("PROPERTIES (\n")
	b.WriteString("  \"replication_allocation\" = \"tag.location.default: 1\",\n")
	b.WriteString("  \"light_schema_change\" = \"true\"")
	if sequenceColumn != "" {
		// merge-on-write is required for delete ordering: with it disabled the
		// DELETE tombstone does not participate in sequence comparison and an
		// older replay resurrects the deleted row (verified on Doris 2.1.11).
		b.WriteString(",\n  \"enable_unique_key_merge_on_write\" = \"true\",\n")
		b.WriteString(fmt.Sprintf("  \"function_column.sequence_col\" = %q", sequenceColumn))
	}
	b.WriteString("\n)")

	return b.String(), nil
}

// addColumn adds a column to an existing Doris table.
// Doris supports lightweight schema changes for ADD COLUMN.
func (s *DorisSink) addColumn(ctx context.Context, table, column string, fieldValues map[string]any) error {
	declaredType := ""
	if fieldValues != nil {
		if declared, ok := fieldValues["__column_types__"].(map[string]string); ok {
			declaredType = declared[column]
		}
	}
	ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		quoteIdentMySQL(table), quoteIdentMySQL(column), inferDorisTypeDeclared(column, fieldValues[column], declaredType))
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

// inferDorisType delegates to the unified typing engine so auto-created
// Doris tables get name-hinted + value-driven types (id→BIGINT, amount→
// DECIMAL, _at→DATETIME, …) consistent with the other relational sinks,
// instead of the old name-only local inference (P4-22, SK-1).
func inferDorisType(colName string, v any) string {
	return inferDorisTypeDeclared(colName, v, "")
}

func inferDorisTypeDeclared(colName string, value any, declared string) string {
	return typing.ResolveColumnDDL(typing.DialectDoris, colName, value, declared, "")
}

func inferDorisKeyType(colName string, v any) string {
	return inferDorisKeyTypeDeclared(colName, v, "")
}

func inferDorisKeyTypeDeclared(colName string, value any, declared string) string {
	t := inferDorisTypeDeclared(colName, value, declared)
	// Doris UNIQUE KEY columns should avoid non-comparable or oversized types.
	switch t {
	case "JSON", "STRING":
		return "VARCHAR(255)"
	}
	return t
}

// ── Helpers ─────────────────────────────────────────────────────────────

// resolveTable resolves the destination Doris table for a record. With
// table_template set (e.g. "ods_{table}"), {table}/{db} are substituted from
// record metadata; otherwise the static configured table (or record metadata
// table when static is empty) is used. A template that references {table}/
// {db} but the record carries no such metadata is a configuration error
// rather than silently emitting a malformed name (e.g. "ods_") which would
// mix unrelated tables into one destination or write to a wrong table.
func (s *DorisSink) resolveTable(rec core.Record) (string, error) {
	if s.tableTemplate == "" {
		if s.table != "" {
			return s.table, nil
		}
		return rec.Metadata.Table, nil
	}
	if strings.Contains(s.tableTemplate, "{db}") && rec.Metadata.Database == "" {
		return "", fmt.Errorf("doris sink: table_template %q references {db} but record has no database metadata", s.tableTemplate)
	}
	if strings.Contains(s.tableTemplate, "{table}") && rec.Metadata.Table == "" {
		return "", fmt.Errorf("doris sink: table_template %q references {table} but record has no table metadata", s.tableTemplate)
	}
	table := strings.ReplaceAll(s.tableTemplate, "{db}", rec.Metadata.Database)
	table = strings.ReplaceAll(table, "{table}", rec.Metadata.Table)
	if table == "" {
		return "", fmt.Errorf("doris sink: table_template %q resolved to empty", s.tableTemplate)
	}
	return table, nil
}

func (s *DorisSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

var _ core.SchemaValidator = (*DorisSink)(nil)

// ── source_order write path (IT-7 / T7.3) ─────────────────────────────

// prepareSourceOrderRecords injects core.SourceOrder().Version into the
// sequence column of every non-delete record. It is a no-op unless
// version_mode=source_order, so the legacy path stays byte-identical.
//
// Deletes are deliberately left alone: Doris's native DELETE (MySQL protocol)
// is already order-protected on a table with a sequence column — an older
// INSERT arriving after the delete does not resurrect the row (T7.1 part 4/5).
// That is why source_order does not need to carry a delete-sign in the Stream
// Load payload (which Doris 2.1.11 does not support anyway, T7.1 fact 3).
//
// An UPDATE that changes the primary key expands into a live row for the new
// key plus a native DELETE for the old key, so the stale key cannot linger.
func (s *DorisSink) prepareSourceOrderRecords(records []core.Record, pkColumnsByTable map[string][]string) ([]core.Record, error) {
	if !s.sourceOrderEnabled() || len(records) == 0 {
		return records, nil
	}
	column := s.effectiveSequenceColumn()
	bigint := s.effectiveSequenceType() == dorisSequenceTypeBigInt
	out := make([]core.Record, 0, len(records))
	for _, rec := range records {
		if rec.Operation == core.OpDelete {
			out = append(out, rec)
			continue
		}
		order := core.SourceOrder(rec)
		if !order.Available || !order.VersionAvailable {
			return nil, dorisSourceOrderError(rec, order)
		}
		if bigint && order.Version > math.MaxInt64 {
			return nil, fmt.Errorf("doris source_order sequence_type=bigint cannot carry source position %d for table %q: Doris BIGINT silently wraps values above %d to a negative number, which would look older than every earlier record; use sequence_type=largeint (default)", order.Version, rec.Metadata.Table, int64(math.MaxInt64))
		}
		prepared := cloneDorisRecord(rec)
		prepared.Data[column] = order.Version

		if prepared.Operation == core.OpUpdate {
			stale, changed, err := s.sourceOrderStaleKey(prepared, pkColumnsByTable)
			if err != nil {
				return nil, err
			}
			out = append(out, prepared)
			if changed {
				out = append(out, stale)
			}
			continue
		}
		out = append(out, prepared)
	}
	return out, nil
}

// sourceOrderStaleKey returns the delete record that removes the pre-update key
// when an UPDATE moved a primary-key column. The old key is taken from the
// before image merged under the after image so unchanged composite-key
// components stay available (Canal-style partial before images).
func (s *DorisSink) sourceOrderStaleKey(rec core.Record, pkColumnsByTable map[string][]string) (core.Record, bool, error) {
	if len(rec.Before) == 0 {
		return core.Record{}, false, nil
	}
	table, err := s.resolveTable(rec)
	if err != nil {
		return core.Record{}, false, err
	}
	pkCols := pkColumnsByTable[table]
	if len(pkCols) == 0 {
		pkCols = s.pkColumns
	}
	if len(pkCols) == 0 {
		// No stable key: nothing to compare, and guessing would delete a row.
		return core.Record{}, false, nil
	}

	oldData := make(map[string]any, len(rec.Data)+len(rec.Before))
	for key, value := range rec.Data {
		oldData[key] = value
	}
	for key, value := range rec.Before {
		oldData[key] = value
	}

	changed := false
	for _, column := range pkCols {
		before, beforeOK := rec.Before[column]
		if !beforeOK {
			continue
		}
		after, afterOK := rec.Data[column]
		if !afterOK {
			continue
		}
		if fmt.Sprint(before) != fmt.Sprint(after) {
			changed = true
			break
		}
	}
	if !changed {
		return core.Record{}, false, nil
	}

	stale := cloneDorisRecord(rec)
	stale.Operation = core.OpDelete
	stale.Data = oldData
	stale.Before = nil
	return stale, true, nil
}

// dorisSourceOrderError explains exactly which metadata fields are missing for
// the record's source type, mirroring the ClickHouse remediation text so
// operators see one contract instead of two.
func dorisSourceOrderError(rec core.Record, order core.SourceOrderResult) error {
	fields := "metadata.source_type and a connector-owned durable position"
	sourceType := strings.ToLower(strings.TrimSpace(rec.Metadata.SourceType))
	if sourceType == "" {
		sourceType = strings.ToLower(strings.TrimSpace(rec.Metadata.Source))
	}
	switch sourceType {
	case core.SourceTypeMySQLCDC:
		fields = "metadata.binlog_file and metadata.binlog_pos"
	case core.SourceTypeMySQLSnapshotCDC:
		if strings.EqualFold(rec.Metadata.SourcePhase, core.SourcePhaseSnapshot) || rec.Metadata.BinlogFile == "" {
			fields = "metadata.snapshot_handoff_file and metadata.snapshot_handoff_pos for snapshot rows"
		} else {
			fields = "metadata.binlog_file and metadata.binlog_pos for CDC rows"
		}
	case core.SourceTypePostgresCDC:
		fields = "metadata.lsn (PostgreSQL initial snapshot rows currently have no numeric source order)"
	case core.SourceTypeKafka:
		fields = "metadata.partition and metadata.offset"
	case core.SourceTypeMySQLBatch:
		fields = "metadata.cursor and metadata.cursor_kind=numeric"
	}
	return fmt.Errorf("doris version_mode=source_order cannot derive a sequence value for source_type=%q table=%q: reason=%s; required fields: %s; do not fall back to wall clock, and use version_mode=off only when last-write-wins is acceptable", sourceType, rec.Metadata.Table, order.Reason, fields)
}

// cloneDorisRecord deep-copies the record maps before mutation so a rejected or
// replayed batch never mutates the caller's records.
func cloneDorisRecord(rec core.Record) core.Record {
	clone := rec
	clone.Data = make(map[string]any, len(rec.Data)+1)
	for key, value := range rec.Data {
		clone.Data[key] = value
	}
	if rec.Before != nil {
		clone.Before = make(map[string]any, len(rec.Before))
		for key, value := range rec.Before {
			clone.Before[key] = value
		}
	}
	return clone
}

// ── Write acknowledgement / dedup contract (A4) ────────────────────────

// dorisCommitState is the last fully acknowledged Doris batch. The runner reads
// it through SinkCommitMetadata after Write returns and before saving the
// checkpoint, so the persisted boundary names exactly which Stream Load labels
// Doris has already accepted.
type dorisCommitState struct {
	batchSeq   uint64
	rowCount   int
	ackedAt    time.Time
	labels     map[string]string // table -> deterministic Stream Load label
	tables     map[string]int    // table -> rows
	protocol   string            // stream_load | insert | delete_only
	uniqueKeys bool
}

// streamLoadLabel is the deterministic Doris label for one table sub-batch: the
// same payload always yields the same label, so a retried/replayed identical
// batch is deduplicated by Doris instead of double-written.
func (s *DorisSink) streamLoadLabel(tableName string, body []byte) string {
	labelHash := sha256.Sum256(append([]byte(fmt.Sprintf("%s:%d/%s.%s|", s.host, s.httpPort, s.database, tableName)), body...))
	return fmt.Sprintf("etl_%x", labelHash[:16])
}

// beginDorisBatch resets the per-Write label accumulator.
func (s *DorisSink) beginDorisBatch() {
	s.ackMu.Lock()
	s.pendingLabels = make(map[string]string)
	s.ackMu.Unlock()
}

func (s *DorisSink) recordAckedLabel(tableName, label string) {
	s.ackMu.Lock()
	if s.pendingLabels == nil {
		s.pendingLabels = make(map[string]string)
	}
	s.pendingLabels[tableName] = label
	s.ackMu.Unlock()
}

// finalizeDorisBatch publishes the acknowledged boundary after every phase of
// the Write succeeded. A partial failure must call invalidateDorisBatch so no
// stale token is ever reported as a committed boundary.
func (s *DorisSink) finalizeDorisBatch(batchSeq uint64, records int, tableRows map[string]int, protocol string) {
	s.ackMu.Lock()
	labels := make(map[string]string, len(s.pendingLabels))
	for table, label := range s.pendingLabels {
		labels[table] = label
	}
	s.pendingLabels = nil
	s.ackMu.Unlock()
	if len(labels) == 0 {
		labels = nil
	}
	tables := make(map[string]int, len(tableRows))
	for table, n := range tableRows {
		tables[table] = n
	}
	if len(tables) == 0 {
		tables = nil
	}
	s.dorisCommit.Store(&dorisCommitState{
		batchSeq:   batchSeq,
		rowCount:   records,
		ackedAt:    time.Now().UTC(),
		labels:     labels,
		tables:     tables,
		protocol:   protocol,
		uniqueKeys: s.batchMode == "upsert" || len(s.pkColumns) > 0 || s.pkColumnsFromMetadata,
	})
}

// invalidateDorisBatch drops the pending labels after a failed Write.
func (s *DorisSink) invalidateDorisBatch() {
	s.ackMu.Lock()
	s.pendingLabels = nil
	s.ackMu.Unlock()
	s.dorisCommit.Store(nil)
}

// SinkCommitMetadata implements core.SinkCommitMetadataProvider. The runner
// persists this under sink_commit.native so the checkpoint records which Doris
// Stream Load labels acknowledged the batch: a crash between the sink write and
// the checkpoint save then replays a batch whose label Doris already accepted,
// and Doris drops the duplicate instead of writing it twice.
//
// A delete-only batch has no label (the native DELETE is not labelled), so it
// reports dedup_mode=unique_key: the Doris UNIQUE KEY model makes the replay of
// a delete idempotent.
func (s *DorisSink) SinkCommitMetadata(ctx context.Context) (map[string]any, error) {
	st := s.dorisCommit.Load()
	if st == nil {
		// No acknowledged batch since the last checkpoint: returning nil (not an
		// error) keeps an idle interval from blocking checkpoint progress. The
		// unique-key model, not a token, absorbs a replay in that window.
		return nil, nil
	}
	meta := map[string]any{
		"batch_seq":  st.batchSeq,
		"row_count":  st.rowCount,
		"acked_at":   st.ackedAt.Format(time.RFC3339Nano),
		"protocol":   st.protocol,
		"dedup_mode": "unique_key",
	}
	if len(st.labels) > 0 {
		meta["dedup_mode"] = "label"
		labels := make(map[string]any, len(st.labels))
		for table, label := range st.labels {
			labels[table] = label
		}
		meta["stream_load_labels"] = labels
		meta["label_retention"] = "Doris keeps a label for label_keep_max_second (default 3 days); replaying the same payload inside that window is deduplicated server-side"
	}
	if len(st.tables) > 0 {
		tables := make(map[string]any, len(st.tables))
		for table, n := range st.tables {
			tables[table] = n
		}
		meta["tables"] = tables
	}
	if st.uniqueKeys {
		meta["unique_key_model"] = true
	}
	return meta, nil
}

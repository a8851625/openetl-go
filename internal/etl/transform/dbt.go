package transform

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("dbt", func(config map[string]any) (core.Transform, error) {
		return NewDBTTransform(config)
	})
}

// DBTTransform bridges OpenETL-Go batches into a dbt project.
//
// Phase 1 supports postgres and duckdb adapters. dbt is an optional runtime
// capability: OpenETL-Go does not vendor dbt-core; the host must provide a
// working dbt CLI and adapter (for example `pip install dbt-core dbt-postgres`
// or `dbt-duckdb`).
//
// Flow for each batch:
//  1. Write upstream records into source_schema.source_table
//  2. Run `dbt run --select <model_name>`
//  3. Read target_schema.target_table rows as output records
//
// YAML config example:
//
//	transforms:
//	  - type: dbt
//	    config:
//	      project_dir: /etc/etl/dbt/my_project
//	      model_name: transformed_orders
//	      source_schema: etl_staging
//	      source_table: orders_raw
//	      target_schema: etl_output
//	      target_table: transformed_orders
//	      adapter: postgres
//	      dsn: postgres://user:pass@localhost:5432/etl?sslmode=disable
//	      threads: 4
//	      target: dev
//	      exec_timeout_sec: 600
type DBTTransform struct {
	name            string
	projectDir      string
	modelName       string
	sourceSchema    string
	sourceTable     string
	targetSchema    string
	targetTable     string
	materialization string
	threads         int
	target          string
	dbtBinary       string
	profilesDir     string
	adapter         string // postgres | duckdb
	dsn             string
	path            string // duckdb database file path
	schema          string // default schema for profiles
	execTimeout     time.Duration
	writeMode       string // insert | replace
	vars            map[string]string
	fullRefresh     bool
	keepProfiles    bool

	// Runtime state
	db            *sql.DB
	dbOwner       bool
	profilesOwned bool
	mu            sync.Mutex
	columnOrder   []string

	// Metrics
	batches      int64
	recordsIn    int64
	recordsOut   int64
	dbtRuns      int64
	dbtFailures  int64
	writeErrors  int64
	readErrors   int64
	lastDuration int64 // nanoseconds

	// Injected for unit tests
	commandRunner func(ctx context.Context, name string, args []string, env []string, dir string) (stdout, stderr string, exitCode int, err error)
	openDB        func(driver, dsn string) (*sql.DB, error)
}

// NewDBTTransform builds a dbt transform from YAML/JSON config.
func NewDBTTransform(config map[string]any) (*DBTTransform, error) {
	t := &DBTTransform{
		name:            "dbt",
		threads:         4,
		target:          "dev",
		dbtBinary:       "dbt",
		materialization: "table",
		adapter:         "postgres",
		execTimeout:     600 * time.Second,
		writeMode:       "replace",
		vars:            map[string]string{},
		commandRunner:   defaultCommandRunner,
		openDB:          defaultOpenDB,
	}

	if v, ok := configString(config, "name"); ok && v != "" {
		t.name = v
	}
	if v, ok := configString(config, "project_dir"); ok {
		t.projectDir = v
	}
	if v, ok := configString(config, "model_name"); ok {
		t.modelName = v
	}
	if v, ok := configString(config, "source_schema"); ok {
		t.sourceSchema = v
	}
	if v, ok := configString(config, "source_table"); ok {
		t.sourceTable = v
	}
	if v, ok := configString(config, "target_schema"); ok {
		t.targetSchema = v
	}
	if v, ok := configString(config, "target_table"); ok {
		t.targetTable = v
	}
	if v, ok := configString(config, "materialization"); ok && v != "" {
		t.materialization = strings.ToLower(v)
	}
	if v, ok := config["threads"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.threads = n
		}
	}
	if v, ok := configString(config, "target"); ok && v != "" {
		t.target = v
	}
	if v, ok := configString(config, "dbt_binary"); ok && v != "" {
		t.dbtBinary = v
	}
	if v, ok := configString(config, "profiles_dir"); ok {
		t.profilesDir = v
	}
	if v, ok := configString(config, "adapter"); ok && v != "" {
		t.adapter = strings.ToLower(v)
	}
	if v, ok := configString(config, "dsn"); ok {
		t.dsn = v
	}
	if v, ok := configString(config, "path"); ok {
		t.path = v
	}
	if v, ok := configString(config, "schema"); ok {
		t.schema = v
	}
	if v, ok := config["exec_timeout_sec"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.execTimeout = time.Duration(n) * time.Second
		}
	}
	if v, ok := configString(config, "write_mode"); ok && v != "" {
		t.writeMode = strings.ToLower(v)
	}
	if v, ok := config["full_refresh"].(bool); ok {
		t.fullRefresh = v
	}
	if v, ok := config["keep_profiles"].(bool); ok {
		t.keepProfiles = v
	}
	if rawVars, ok := config["vars"].(map[string]any); ok {
		for k, val := range rawVars {
			t.vars[k] = fmt.Sprintf("%v", val)
		}
	}

	// Infer adapter from DSN/path when adapter was not explicitly set.
	if _, explicit := configString(config, "adapter"); !explicit {
		if t.path != "" || strings.HasPrefix(t.dsn, "duckdb://") || strings.HasSuffix(strings.ToLower(t.dsn), ".duckdb") {
			t.adapter = "duckdb"
		}
	}

	if err := t.validate(); err != nil {
		return nil, err
	}

	// Defaults for schemas/tables when only one side is given.
	if t.targetSchema == "" {
		t.targetSchema = t.sourceSchema
	}
	if t.targetTable == "" {
		t.targetTable = t.modelName
	}
	if t.schema == "" {
		t.schema = t.sourceSchema
	}
	if t.schema == "" {
		t.schema = "public"
	}

	return t, nil
}

func (t *DBTTransform) validate() error {
	if strings.TrimSpace(t.projectDir) == "" {
		return fmt.Errorf("dbt: project_dir is required")
	}
	if strings.TrimSpace(t.modelName) == "" {
		return fmt.Errorf("dbt: model_name is required")
	}
	if strings.TrimSpace(t.sourceTable) == "" {
		return fmt.Errorf("dbt: source_table is required")
	}
	switch t.adapter {
	case "postgres", "postgresql":
		t.adapter = "postgres"
		if strings.TrimSpace(t.dsn) == "" {
			return fmt.Errorf("dbt: dsn is required for adapter=postgres")
		}
	case "duckdb":
		if strings.TrimSpace(t.path) == "" && strings.TrimSpace(t.dsn) == "" {
			return fmt.Errorf("dbt: path or dsn is required for adapter=duckdb")
		}
		if t.path == "" {
			t.path = strings.TrimPrefix(t.dsn, "duckdb://")
		}
	default:
		return fmt.Errorf("dbt: unsupported adapter %q (phase 1 supports postgres, duckdb)", t.adapter)
	}
	switch t.materialization {
	case "table", "view", "ephemeral", "incremental":
	default:
		return fmt.Errorf("dbt: unsupported materialization %q", t.materialization)
	}
	switch t.writeMode {
	case "insert", "replace":
	default:
		return fmt.Errorf("dbt: write_mode must be insert or replace, got %q", t.writeMode)
	}
	return nil
}

func (t *DBTTransform) Name() string { return t.name }

// Apply runs a single-record batch. Prefer ApplyBatch for production use.
func (t *DBTTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	out, err := t.ApplyBatch(ctx, []core.Record{rec})
	if err != nil {
		return rec, err
	}
	if len(out) == 0 {
		return core.Record{}, core.ErrRecordFiltered
	}
	return out[0], nil
}

// ApplyBatch implements core.BatchTransform.
func (t *DBTTransform) ApplyBatch(ctx context.Context, recs []core.Record) ([]core.Record, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	start := time.Now()
	atomic.AddInt64(&t.batches, 1)
	atomic.AddInt64(&t.recordsIn, int64(len(recs)))

	if err := t.ensureDB(ctx); err != nil {
		return nil, err
	}
	if err := t.ensureProfilesDir(); err != nil {
		return nil, err
	}

	if err := t.writeSourceRows(ctx, recs); err != nil {
		atomic.AddInt64(&t.writeErrors, 1)
		return nil, fmt.Errorf("dbt: write source table: %w", err)
	}

	if err := t.runDBT(ctx); err != nil {
		atomic.AddInt64(&t.dbtFailures, 1)
		return nil, err
	}
	atomic.AddInt64(&t.dbtRuns, 1)

	out, err := t.readTargetRows(ctx, recs)
	if err != nil {
		atomic.AddInt64(&t.readErrors, 1)
		return nil, fmt.Errorf("dbt: read target table: %w", err)
	}
	atomic.AddInt64(&t.recordsOut, int64(len(out)))
	atomic.StoreInt64(&t.lastDuration, time.Since(start).Nanoseconds())
	return out, nil
}

// TransformMetrics implements core.TransformMetricsProvider.
func (t *DBTTransform) TransformMetrics() core.TransformMetrics {
	return core.TransformMetrics{
		Node:      t.name,
		Transform: "dbt",
		Counters: map[string]int64{
			"batches":       atomic.LoadInt64(&t.batches),
			"records_in":    atomic.LoadInt64(&t.recordsIn),
			"records_out":   atomic.LoadInt64(&t.recordsOut),
			"dbt_runs":      atomic.LoadInt64(&t.dbtRuns),
			"dbt_failures":  atomic.LoadInt64(&t.dbtFailures),
			"write_errors":  atomic.LoadInt64(&t.writeErrors),
			"read_errors":   atomic.LoadInt64(&t.readErrors),
			"last_duration": atomic.LoadInt64(&t.lastDuration),
		},
	}
}

// Close releases the database connection and temporary profiles directory.
func (t *DBTTransform) Close() error {
	var first error
	if t.dbOwner && t.db != nil {
		if err := t.db.Close(); err != nil && first == nil {
			first = err
		}
		t.db = nil
	}
	if t.profilesOwned && t.profilesDir != "" && !t.keepProfiles {
		_ = os.RemoveAll(t.profilesDir)
	}
	return first
}

// ── DB helpers ──────────────────────────────────────────────────────

func (t *DBTTransform) ensureDB(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.db != nil {
		return nil
	}
	driver, dsn, err := t.driverAndDSN()
	if err != nil {
		return err
	}
	db, err := t.openDB(driver, dsn)
	if err != nil {
		return fmt.Errorf("dbt: open db: %w", err)
	}
	// Soft ping — duckdb may not be linked; unit tests inject openDB.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return fmt.Errorf("dbt: ping db: %w", err)
	}
	t.db = db
	t.dbOwner = true
	return nil
}

func (t *DBTTransform) driverAndDSN() (driver, dsn string, err error) {
	switch t.adapter {
	case "postgres":
		return "pgx", t.dsn, nil
	case "duckdb":
		// modernc/go-duckdb is not a core dependency. Callers may inject
		// openDB for duckdb, or register a driver named "duckdb" themselves.
		path := t.path
		if path == "" {
			path = strings.TrimPrefix(t.dsn, "duckdb://")
		}
		return "duckdb", path, nil
	default:
		return "", "", fmt.Errorf("dbt: unsupported adapter %q", t.adapter)
	}
}

func defaultOpenDB(driver, dsn string) (*sql.DB, error) {
	return sql.Open(driver, dsn)
}

// ── Source write ────────────────────────────────────────────────────

func (t *DBTTransform) writeSourceRows(ctx context.Context, recs []core.Record) error {
	cols := t.collectColumns(recs)
	if len(cols) == 0 {
		return fmt.Errorf("no columns found in batch records")
	}
	t.mu.Lock()
	t.columnOrder = cols
	t.mu.Unlock()

	fqtn := t.qualifiedName(t.sourceSchema, t.sourceTable)
	if err := t.ensureTable(ctx, fqtn, cols, recs); err != nil {
		return err
	}

	if t.writeMode == "replace" {
		if _, err := t.db.ExecContext(ctx, "DELETE FROM "+fqtn); err != nil {
			return fmt.Errorf("truncate source table: %w", err)
		}
	}

	placeholders, argsBuilder := t.insertPlaceholders(len(cols))
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		fqtn, t.quoteColumns(cols), placeholders)

	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, rec := range recs {
		args := argsBuilder(rec, cols)
		if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
			return fmt.Errorf("insert into %s: %w", fqtn, err)
		}
	}
	return tx.Commit()
}

func (t *DBTTransform) collectColumns(recs []core.Record) []string {
	seen := map[string]struct{}{}
	var cols []string
	for _, rec := range recs {
		for k := range rec.Data {
			if _, ok := seen[k]; ok {
				continue
			}
			// Skip internal metadata fields that start with underscore and
			// are typically not useful as dbt source columns.
			if strings.HasPrefix(k, "_") {
				continue
			}
			seen[k] = struct{}{}
			cols = append(cols, k)
		}
	}
	sort.Strings(cols)
	return cols
}

func (t *DBTTransform) ensureTable(ctx context.Context, fqtn string, cols []string, recs []core.Record) error {
	// Best-effort CREATE TABLE IF NOT EXISTS. Types are inferred loosely so
	// dbt models remain the source of truth for final schemas.
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(fqtn)
	b.WriteString(" (")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(t.quoteIdent(c))
		b.WriteString(" ")
		b.WriteString(t.inferSQLType(c, recs))
	}
	b.WriteString(")")
	if _, err := t.db.ExecContext(ctx, b.String()); err != nil {
		// Table may already exist with a different shape; continue and let
		// INSERT report a clearer error if columns truly mismatch.
		if !isAlreadyExists(err) {
			return fmt.Errorf("create source table: %w", err)
		}
	}
	return nil
}

func (t *DBTTransform) inferSQLType(col string, recs []core.Record) string {
	for _, rec := range recs {
		v, ok := rec.Data[col]
		if !ok || v == nil {
			continue
		}
		switch v.(type) {
		case bool:
			return "BOOLEAN"
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return "BIGINT"
		case float32, float64:
			return "DOUBLE PRECISION"
		case time.Time:
			return "TIMESTAMP"
		case []byte:
			return "BYTEA"
		default:
			return "TEXT"
		}
	}
	return "TEXT"
}

func (t *DBTTransform) insertPlaceholders(n int) (string, func(core.Record, []string) []any) {
	ph := make([]string, n)
	for i := range ph {
		if t.adapter == "postgres" {
			ph[i] = fmt.Sprintf("$%d", i+1)
		} else {
			ph[i] = "?"
		}
	}
	return "(" + strings.Join(ph, ", ") + ")", func(rec core.Record, cols []string) []any {
		args := make([]any, len(cols))
		for i, c := range cols {
			args[i] = rec.Data[c]
		}
		return args
	}
}

// ── dbt subprocess ──────────────────────────────────────────────────

// BuildDBTArgs constructs the dbt CLI argument list. Exported for unit tests.
func (t *DBTTransform) BuildDBTArgs() []string {
	args := []string{
		"run",
		"--project-dir", t.projectDir,
		"--profiles-dir", t.profilesDir,
		"--target", t.target,
		"--select", t.modelName,
		"--threads", fmt.Sprintf("%d", t.threads),
	}
	if t.fullRefresh {
		args = append(args, "--full-refresh")
	}
	if len(t.vars) > 0 {
		// Stable order for tests/repro.
		keys := make([]string, 0, len(t.vars))
		for k := range t.vars {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s: %s", k, t.vars[k]))
		}
		args = append(args, "--vars", "{"+strings.Join(parts, ", ")+"}")
	}
	return args
}

func (t *DBTTransform) runDBT(ctx context.Context) error {
	if t.profilesDir == "" {
		return fmt.Errorf("dbt: profiles_dir not configured")
	}
	runCtx, cancel := context.WithTimeout(ctx, t.execTimeout)
	defer cancel()

	args := t.BuildDBTArgs()
	stdout, stderr, exitCode, err := t.commandRunner(runCtx, t.dbtBinary, args, nil, t.projectDir)
	if runCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("dbt: run timed out after %s (model=%s)", t.execTimeout, t.modelName)
	}
	if err != nil {
		return fmt.Errorf("dbt: exec %s: %w\nstdout:\n%s\nstderr:\n%s",
			t.dbtBinary, err, truncateLog(stdout), truncateLog(stderr))
	}
	if exitCode != 0 {
		logHint := t.dbtLogHint()
		return fmt.Errorf("dbt: run failed (exit=%d, model=%s)%s\nstdout:\n%s\nstderr:\n%s",
			exitCode, t.modelName, logHint, truncateLog(stdout), truncateLog(stderr))
	}
	return nil
}

func (t *DBTTransform) dbtLogHint() string {
	logPath := filepath.Join(t.projectDir, "logs", "dbt.log")
	if _, err := os.Stat(logPath); err == nil {
		return fmt.Sprintf("; see %s", logPath)
	}
	return ""
}

func defaultCommandRunner(ctx context.Context, name string, args []string, env []string, dir string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			exitCode = ee.ExitCode()
			err = nil
		} else {
			return stdout.String(), stderr.String(), -1, err
		}
	}
	return stdout.String(), stderr.String(), exitCode, err
}

// ── Target read ─────────────────────────────────────────────────────

func (t *DBTTransform) readTargetRows(ctx context.Context, input []core.Record) ([]core.Record, error) {
	fqtn := t.qualifiedName(t.targetSchema, t.targetTable)
	rows, err := t.db.QueryContext(ctx, "SELECT * FROM "+fqtn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var out []core.Record
	idx := 0
	for rows.Next() {
		raw := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		data := make(map[string]any, len(colNames))
		for i, name := range colNames {
			data[name] = normalizeSQLValue(raw[i])
		}
		meta := core.Metadata{
			Source:    "dbt",
			Table:     t.targetTable,
			Database:  t.targetSchema,
			Timestamp: time.Now().UTC(),
		}
		if idx < len(input) {
			// Preserve lineage from the corresponding input when sizes match.
			meta.Source = input[idx].Metadata.Source
			if meta.Source == "" {
				meta.Source = "dbt"
			}
			if input[idx].Metadata.Key != "" {
				meta.Key = input[idx].Metadata.Key
			}
			if !input[idx].Metadata.Timestamp.IsZero() {
				meta.Timestamp = input[idx].Metadata.Timestamp
			}
		}
		out = append(out, core.Record{
			Operation: core.OpInsert,
			Data:      data,
			Metadata:  meta,
		})
		idx++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ── profiles.yml injection ──────────────────────────────────────────

func (t *DBTTransform) ensureProfilesDir() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.profilesDir != "" {
		// If caller supplied a profiles dir, ensure profiles.yml exists or write one.
		if _, err := os.Stat(filepath.Join(t.profilesDir, "profiles.yml")); err == nil {
			return nil
		}
		return t.writeProfilesYML(t.profilesDir)
	}
	dir, err := os.MkdirTemp("", "openetl-dbt-profiles-*")
	if err != nil {
		return fmt.Errorf("dbt: create profiles dir: %w", err)
	}
	t.profilesDir = dir
	t.profilesOwned = true
	return t.writeProfilesYML(dir)
}

// BuildProfilesYML renders a minimal profiles.yml for the configured adapter.
// Exported for unit tests.
func (t *DBTTransform) BuildProfilesYML() (string, error) {
	profileName := "openetl"
	// Prefer the project profile name if dbt_project.yml is present.
	if name, err := readDBTProjectProfileName(t.projectDir); err == nil && name != "" {
		profileName = name
	}

	var body strings.Builder
	body.WriteString(profileName)
	body.WriteString(":\n")
	body.WriteString("  target: ")
	body.WriteString(t.target)
	body.WriteString("\n")
	body.WriteString("  outputs:\n")
	body.WriteString("    ")
	body.WriteString(t.target)
	body.WriteString(":\n")

	switch t.adapter {
	case "postgres":
		host, port, user, password, dbname, sslmode, err := parsePostgresDSN(t.dsn)
		if err != nil {
			return "", err
		}
		schema := t.schema
		if schema == "" {
			schema = "public"
		}
		fmt.Fprintf(&body, "      type: postgres\n")
		fmt.Fprintf(&body, "      host: %q\n", host)
		fmt.Fprintf(&body, "      user: %q\n", user)
		fmt.Fprintf(&body, "      password: %q\n", password)
		fmt.Fprintf(&body, "      port: %d\n", port)
		fmt.Fprintf(&body, "      dbname: %q\n", dbname)
		fmt.Fprintf(&body, "      schema: %q\n", schema)
		fmt.Fprintf(&body, "      threads: %d\n", t.threads)
		if sslmode != "" {
			fmt.Fprintf(&body, "      sslmode: %q\n", sslmode)
		}
	case "duckdb":
		path := t.path
		if path == "" {
			path = strings.TrimPrefix(t.dsn, "duckdb://")
		}
		schema := t.schema
		if schema == "" {
			schema = "main"
		}
		fmt.Fprintf(&body, "      type: duckdb\n")
		fmt.Fprintf(&body, "      path: %q\n", path)
		fmt.Fprintf(&body, "      schema: %q\n", schema)
		fmt.Fprintf(&body, "      threads: %d\n", t.threads)
	default:
		return "", fmt.Errorf("dbt: cannot build profiles for adapter %q", t.adapter)
	}
	return body.String(), nil
}

func (t *DBTTransform) writeProfilesYML(dir string) error {
	content, err := t.BuildProfilesYML()
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "profiles.yml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("dbt: write profiles.yml: %w", err)
	}
	return nil
}

func readDBTProjectProfileName(projectDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(projectDir, "dbt_project.yml"))
	if err != nil {
		return "", err
	}
	// Minimal YAML scrape — avoid pulling a second YAML parser just for this.
	for _, line := range strings.Split(string(data), "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "profile:") {
			val := strings.TrimSpace(strings.TrimPrefix(trim, "profile:"))
			val = strings.Trim(val, `"'`)
			return val, nil
		}
	}
	return "", nil
}

// parsePostgresDSN extracts connection fields from a libpq/pgx URL or
// keyword/value DSN. Only the fields needed for profiles.yml are returned.
func parsePostgresDSN(dsn string) (host string, port int, user, password, dbname, sslmode string, err error) {
	port = 5432
	sslmode = "prefer"
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		// Manual parse to avoid importing net/url edge cases around password.
		u := dsn
		u = strings.TrimPrefix(u, "postgres://")
		u = strings.TrimPrefix(u, "postgresql://")
		// user:pass@host:port/db?opts
		var rest string
		if at := strings.Index(u, "@"); at >= 0 {
			cred := u[:at]
			rest = u[at+1:]
			if colon := strings.Index(cred, ":"); colon >= 0 {
				user = cred[:colon]
				password = cred[colon+1:]
			} else {
				user = cred
			}
		} else {
			rest = u
		}
		// strip query
		if q := strings.Index(rest, "?"); q >= 0 {
			query := rest[q+1:]
			rest = rest[:q]
			for _, part := range strings.Split(query, "&") {
				kv := strings.SplitN(part, "=", 2)
				if len(kv) == 2 && kv[0] == "sslmode" {
					sslmode = kv[1]
				}
			}
		}
		// host:port/db
		if slash := strings.Index(rest, "/"); slash >= 0 {
			dbname = rest[slash+1:]
			rest = rest[:slash]
		}
		if colon := strings.LastIndex(rest, ":"); colon >= 0 {
			host = rest[:colon]
			fmt.Sscanf(rest[colon+1:], "%d", &port)
		} else {
			host = rest
		}
		if host == "" {
			host = "localhost"
		}
		return host, port, user, password, dbname, sslmode, nil
	}

	// keyword=value form
	for _, part := range strings.Fields(dsn) {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "host":
			host = kv[1]
		case "port":
			fmt.Sscanf(kv[1], "%d", &port)
		case "user":
			user = kv[1]
		case "password":
			password = kv[1]
		case "dbname", "database":
			dbname = kv[1]
		case "sslmode":
			sslmode = kv[1]
		}
	}
	if host == "" {
		host = "localhost"
	}
	return host, port, user, password, dbname, sslmode, nil
}

// ── SQL dialect helpers ─────────────────────────────────────────────

func (t *DBTTransform) quoteIdent(name string) string {
	switch t.adapter {
	case "postgres":
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	default:
		// duckdb accepts double quotes for identifiers
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

func (t *DBTTransform) quoteColumns(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = t.quoteIdent(c)
	}
	return strings.Join(quoted, ", ")
}

func (t *DBTTransform) qualifiedName(schema, table string) string {
	if schema == "" {
		return t.quoteIdent(table)
	}
	return t.quoteIdent(schema) + "." + t.quoteIdent(table)
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate")
}

func truncateLog(s string) string {
	const max = 4000
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n... (truncated)"
}

func configString(config map[string]any, key string) (string, bool) {
	v, ok := config[key]
	if !ok || v == nil {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	default:
		return fmt.Sprintf("%v", s), true
	}
}

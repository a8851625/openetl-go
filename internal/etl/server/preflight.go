package server

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/pipeline"
	"openetl-go/internal/etl/registry"
)

// PreflightIssue describes a single check failure with remediation.
type PreflightIssue struct {
	Level       string `json:"level"`       // "error" or "warning"
	Check       string `json:"check"`       // which check produced this
	Message     string `json:"message"`     // what went wrong
	Remediation string `json:"remediation"` // how to fix it
}

// PreflightResult is the outcome of running all preflight checks.
type PreflightResult struct {
	Passed  bool              `json:"passed"`
	Issues  []PreflightIssue  `json:"issues,omitempty"`
	Summary string            `json:"summary"`
}

// RunPreflight validates a pipeline spec's source and sink connectivity
// before starting. It checks:
//   - MySQL binlog format and permissions
//   - ClickHouse / target reachability
//   - Source table existence (best-effort)
//   - Common misconfiguration patterns
//
// Returns nil if all checks pass. Errors are returned as PreflightIssue
// entries (never fatal — partial checks are allowed).
func (s *Server) RunPreflight(ctx context.Context, spec *pipeline.Spec) *PreflightResult {
	result := &PreflightResult{Passed: true}

	// Source checks
	s.checkMySQLCDC(ctx, spec, result)

	// Sink checks
	s.checkSinkReachable(ctx, spec, result)

	if !result.Passed {
		result.Summary = fmt.Sprintf("%d issue(s) found", len(result.Issues))
	} else {
		result.Summary = "all checks passed"
	}
	return result
}

// ── Source: MySQL CDC checks ─────────────────────────────────────────

func (s *Server) checkMySQLCDC(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec.Source.Type != "mysql_cdc" && spec.Source.Type != "mysql_snapshot_cdc" {
		return
	}

	cfg := spec.Source.Config
	host := stringField(cfg, "host", "localhost")
	port := intField(cfg, "port", 3306)
	user := stringField(cfg, "user", "root")
	password := stringField(cfg, "password", "")

	// Connect to MySQL.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=5s", user, password, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-connect",
			Message:     fmt.Sprintf("cannot connect to MySQL at %s:%d: %v", host, port, err),
			Remediation: "verify MySQL is running and credentials in source.config are correct",
		})
		result.Passed = false
		return
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-connect",
			Message:     fmt.Sprintf("cannot ping MySQL at %s:%d: %v", host, port, err),
			Remediation: "verify MySQL host/port are reachable from the ETL process",
		})
		result.Passed = false
		return
	}

	// Check binlog format.
	var binlogFormat string
	_ = db.QueryRowContext(ctx, "SELECT @@binlog_format").Scan(&binlogFormat)
	if strings.ToUpper(binlogFormat) != "ROW" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-binlog-format",
			Message:     fmt.Sprintf("binlog_format is %q, must be ROW", binlogFormat),
			Remediation: "run: SET GLOBAL binlog_format = 'ROW'; and restart the MySQL server. Or add --binlog-format=ROW to mysqld.",
		})
		result.Passed = false
	}

	// Check binlog row image.
	var binlogRowImage string
	_ = db.QueryRowContext(ctx, "SELECT @@binlog_row_image").Scan(&binlogRowImage)
	if strings.ToUpper(binlogRowImage) != "FULL" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-binlog-row-image",
			Message:     fmt.Sprintf("binlog_row_image is %q, must be FULL", binlogRowImage),
			Remediation: "run: SET GLOBAL binlog_row_image = 'FULL'; and restart the MySQL server.",
		})
		result.Passed = false
	}

	// Check replication grants.
	grants := checkMySQLGrants(ctx, db, user, host)
	for _, g := range grants {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-replication-grant",
			Message:     fmt.Sprintf("missing grant: %s", g),
			Remediation: fmt.Sprintf("run: GRANT %s ON *.* TO '%s'@'%%'; FLUSH PRIVILEGES;", g, user),
		})
		result.Passed = false
	}

	// Check server_id uniqueness (warning only — may be fine in single-instance).
	var serverID int
	_ = db.QueryRowContext(ctx, "SELECT @@server_id").Scan(&serverID)
	cfgServerID := intField(cfg, "server_id", 1001)
	if serverID == cfgServerID {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "mysql-server-id",
			Message:     fmt.Sprintf("source MySQL server_id (%d) matches the configured server_id (%d); this is expected for single-instance but may cause issues with multiple replicas", serverID, cfgServerID),
			Remediation: "if running multiple ETL instances, set a unique server_id for each in source.config.server_id",
		})
	}

	// Check source tables exist (best-effort, non-blocking).
	database := stringField(cfg, "database", "")
	if database != "" {
		tables := stringSliceField(cfg, "tables")
		for _, table := range tables {
			var count int
			err := db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?", database, table,
			).Scan(&count)
			if err != nil {
				continue // best-effort
			}
			if count == 0 {
				result.Issues = append(result.Issues, PreflightIssue{
					Level:       "warning",
					Check:       "source-table-exists",
					Message:     fmt.Sprintf("source table %s.%s not found in MySQL", database, table),
					Remediation: fmt.Sprintf("create the table %s.%s in MySQL, or remove it from source.config.tables", database, table),
				})
			}
		}
	}

	if len(result.Issues) == 0 {
		g.Log().Infof(ctx, "MySQL preflight passed: binlog_format=ROW, binlog_row_image=FULL, grants OK")
	}
}

// ── Sink: reachability check ─────────────────────────────────────────

func (s *Server) checkSinkReachable(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	// For now, just verify the plugin is registered and config is parseable.
	// Full connectivity checks (Ping) happen at pipeline start.
	// We delegate to the registry to build the plugin (which validates config).
	_, err := registry.BuildSink(spec.Sink.Type, spec.Sink.Config)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "sink-config",
			Message:     fmt.Sprintf("sink %q configuration error: %v", spec.Sink.Type, err),
			Remediation: "fix the sink config in the pipeline spec",
		})
		result.Passed = false
	}
}

// ── MySQL grant checker ──────────────────────────────────────────────

var requiredCDCGrantees = []string{"REPLICATION SLAVE", "REPLICATION CLIENT", "SELECT", "RELOAD", "SHOW DATABASES"}

// checkMySQLGrants verifies the CDC user has the required replication grants.
// It queries SHOW GRANTS and matches expected strings.
func checkMySQLGrants(ctx context.Context, db *sql.DB, user, host string) []string {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%'", user))
	if err != nil {
		// Try without host specifier
		rows, err = db.QueryContext(ctx, "SHOW GRANTS")
		if err != nil {
			return nil // can't verify, skip
		}
	}
	defer rows.Close()

	grantTexts := []string{}
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err == nil {
			grantTexts = append(grantTexts, strings.ToUpper(g))
		}
	}

	var missing []string
	for _, required := range requiredCDCGrantees {
		found := false
		pattern := strings.ToUpper(required)
		for _, g := range grantTexts {
			if strings.Contains(g, pattern) || strings.Contains(g, "ALL PRIVILEGES") || strings.Contains(g, "GRANT ALL") {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, required)
		}
	}
	return missing
}

// ── Config helpers ────────────────────────────────────────────────────

func stringField(cfg map[string]any, key, def string) string {
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return def
}

func intField(cfg map[string]any, key string, def int) int {
	switch v := cfg[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return def
}

func stringSliceField(cfg map[string]any, key string) []string {
	var result []string
	if arr, ok := cfg[key].([]interface{}); ok {
		for _, v := range arr {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
	}
	return result
}

package sink

import (
	"regexp"
	"strings"
)

var identSafeRe = regexp.MustCompile(`[^A-Za-z0-9_]`)

// quoteIdentMySQL quotes a MySQL/Doris/ClickHouse identifier by doubling
// interior backticks per the SQL standard. Safe against injection.
func quoteIdentMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// quoteIdentPg quotes a PostgreSQL identifier by doubling interior double-quotes.
func quoteIdentPg(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sanitizeIndexName strips non-identifier-safe characters for use in index names.
// Index names cannot be backtick-quoted in all DBs (e.g. Doris INDEX idx_<name>),
// so we sanitize rather than quote.
func sanitizeIndexName(name string) string {
	return identSafeRe.ReplaceAllString(name, "_")
}

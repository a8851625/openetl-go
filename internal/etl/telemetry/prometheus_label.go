package telemetry

import (
	"strings"
	"unicode/utf8"
)

// EscapePrometheusLabel escapes a label value per Prometheus text exposition
// format: backslash, double-quote, and newline must be escaped. Invalid UTF-8
// is replaced so a hostile pipeline name cannot break the exposition scrape.
func EscapePrometheusLabel(v string) string {
	if v == "" {
		return ""
	}
	if !utf8.ValidString(v) {
		v = strings.ToValidUTF8(v, "\uFFFD")
	}
	var b strings.Builder
	b.Grow(len(v) + 8)
	for i := 0; i < len(v); i++ {
		switch c := v[i]; c {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			// Treat CR as escaped newline boundary so multi-line names stay one sample.
			b.WriteString(`\n`)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

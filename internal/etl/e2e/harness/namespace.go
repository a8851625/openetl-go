package harness

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"testing"
)

// nsInvalid matches every rune that is not allowed in a derived namespace.
// Namespaces feed MySQL database names, ClickHouse database names, pipeline
// names and file paths, so they are restricted to [a-z0-9_].
var nsInvalid = regexp.MustCompile(`[^a-z0-9]+`)

// nsMaxLen caps the sanitized part before the disambiguation suffix. MySQL
// database names are limited to 64 characters; keeping the namespace short
// leaves room for the e2e_ prefix and role suffix.
const nsMaxLen = 40

// Namespace derives a stable, sanitized identifier from the test name.
// Subtests, spaces and punctuation collapse to underscores; long names are
// truncated and disambiguated with a short FNV-1a hash of the full name so
// distinct tests never collide.
func Namespace(t *testing.T) string {
	t.Helper()
	return NamespaceForName(t.Name())
}

// NamespaceForName is the string-in-pure-logic core of Namespace.
func NamespaceForName(name string) string {
	s := nsInvalid.ReplaceAllString(strings.ToLower(name), "_")
	s = strings.Trim(s, "_")
	if s == "" {
		s = "e2e"
	}
	if len(s) > nsMaxLen {
		h := fnv.New32a()
		_, _ = h.Write([]byte(name))
		s = s[:nsMaxLen] + fmt.Sprintf("_%08x", h.Sum32())
	}
	return s
}

// DBName builds a MySQL/ClickHouse database identifier scoped to the test
// namespace. role is a short discriminator such as "src", "tgt" or "ch".
func DBName(ns, role string) string {
	return fmt.Sprintf("e2e_%s_%s", ns, role)
}

// ObjectPrefix builds a generic identifier prefix (topics, tables, indexes)
// scoped to the test namespace.
func ObjectPrefix(ns string) string {
	return "e2e_" + ns
}

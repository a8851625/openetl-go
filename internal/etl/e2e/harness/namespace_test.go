package harness

import (
	"strings"
	"testing"
)

func TestNamespaceForNameSanitizes(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"TestPathMySQLCDCMySQLUpsert", "testpathmysqlcdcmysqlupsert"},
		{"TestSomething/sub_case:value", "testsomething_sub_case_value"},
		{"Test中文 Namespace", "test_namespace"},
		{"Test trailing underscores __", "test_trailing_underscores"},
		{"Test!!", "test"},
	}
	for _, c := range cases {
		if got := NamespaceForName(c.in); got != c.want {
			t.Errorf("NamespaceForName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if got := NamespaceForName("!!!"); got != "e2e" {
		t.Errorf("NamespaceForName(\"!!!\") = %q, want e2e", got)
	}
}

func TestNamespaceForNameTruncatesAndDisambiguates(t *testing.T) {
	long1 := "Test" + strings.Repeat("LongSegmentA", 10)
	long2 := "Test" + strings.Repeat("LongSegmentB", 10)

	got1 := NamespaceForName(long1)
	got2 := NamespaceForName(long2)

	if len(got1) > 49 { // 40 truncated + "_" + 8 hex
		t.Fatalf("namespace too long: %d (%q)", len(got1), got1)
	}
	if !strings.HasPrefix(got1, "test") {
		t.Fatalf("unexpected prefix: %q", got1)
	}
	if got1 == got2 {
		t.Fatalf("distinct long names collapsed to the same namespace: %q", got1)
	}
	if NamespaceForName(long1) != got1 {
		t.Fatalf("namespace derivation is not deterministic for %q", long1)
	}
}

func TestNamespaceUsesTestName(t *testing.T) {
	if got := Namespace(t); got != "testnamespaceusestestname" {
		t.Errorf("Namespace(t) = %q", got)
	}
	t.Run("with/sub case", func(t *testing.T) {
		if got := Namespace(t); got != "testnamespaceusestestname_with_sub_case" {
			t.Errorf("Namespace(sub) = %q", got)
		}
	})
}

func TestDBNameAndObjectPrefix(t *testing.T) {
	if got := DBName("ns1", "src"); got != "e2e_ns1_src" {
		t.Errorf("DBName = %q", got)
	}
	if got := ObjectPrefix("ns1"); got != "e2e_ns1" {
		t.Errorf("ObjectPrefix = %q", got)
	}
}

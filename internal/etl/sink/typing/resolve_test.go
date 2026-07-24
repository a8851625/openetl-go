package typing

import "testing"

func TestResolveColumnDDLPriority(t *testing.T) {
	// override wins over everything
	got := ResolveColumnDDL(DialectMySQL, "deleted", 0, "int", "TINYINT(1)")
	if got != "TINYINT(1)" {
		t.Fatalf("override: got %q", got)
	}
	// declared source type over sample/name hint
	got = ResolveColumnDDL(DialectMySQL, "deleted", 0, "tinyint(1)", "")
	if got != "TINYINT(1)" {
		t.Fatalf("declared tinyint(1): got %q", got)
	}
	got = ResolveColumnDDL(DialectMySQL, "deleted", 0, "int", "")
	if got != "INT" {
		t.Fatalf("declared int: got %q want INT", got)
	}
	// sample inference when no declared/override
	got = ResolveColumnDDL(DialectMySQL, "deleted", 0, "", "")
	if got != "TINYINT(1)" {
		t.Fatalf("sample flag: got %q", got)
	}
	// MySQL COLUMN_TYPE passthrough
	got = ResolveColumnDDL(DialectMySQL, "phone", "x", "varchar(32)", "")
	if got != "varchar(32)" {
		t.Fatalf("varchar passthrough: got %q", got)
	}
}

func TestMapSourceTypeDebeziumPrimitives(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"int16", "SMALLINT"},
		{"int32", "INT"},
		{"int64", "BIGINT"},
		{"boolean", "TINYINT(1)"},
		{"io.debezium.time.Timestamp", "DATETIME(3)"},
		{"io.debezium.time.Date", "DATE"},
		{"string", "TEXT"},
		{"", ""},
		{"not-a-real-type-xyz", ""},
	}
	for _, tc := range cases {
		got := MapSourceType(DialectMySQL, tc.in)
		if got != tc.want {
			t.Errorf("MapSourceType(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

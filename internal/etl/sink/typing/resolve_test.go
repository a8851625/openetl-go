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

func TestMapSourceTypeYear(t *testing.T) {
	cases := []struct {
		dialect Dialect
		in      string
		want    string
	}{
		{DialectClickHouse, "year", "UInt16"},
		{DialectClickHouse, "year(4)", "UInt16"},
		{DialectMySQL, "year", "SMALLINT"},
		{DialectPostgreSQL, "year", "SMALLINT"},
		{DialectDoris, "year", "SMALLINT"},
	}
	for _, tc := range cases {
		got := MapSourceType(tc.dialect, tc.in)
		if got != tc.want {
			t.Errorf("MapSourceType(%v, %q) = %q, want %q", tc.dialect, tc.in, got, tc.want)
		}
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

// TestDecimalSourcePrecisionPassthrough verifies MapSourceType preserves the
// source decimal precision/scale for every dialect instead of collapsing to
// the hard-coded (18,2) default (precision-loss residual fix, 2026-08-21).
func TestDecimalSourcePrecisionPassthrough(t *testing.T) {
	cases := []struct {
		raw     string
		dialect Dialect
		want    string
	}{
		{"decimal(5,2)", DialectClickHouse, "Decimal(5, 2)"},
		{"decimal(10,2)", DialectMySQL, "decimal(10,2)"}, // MySQL dialect passthrough preserves raw (pre-existing)
		{"DECIMAL(8,3)", DialectPostgreSQL, "NUMERIC(8,3)"},
		{"decimal(5,2)", DialectDoris, "DECIMAL(5,2)"},
		{"decimal(10)", DialectClickHouse, "Decimal(10, 0)"}, // scale defaults to 0
		{"numeric(12,4)", DialectClickHouse, "Decimal(12, 4)"},
		{"decimal unsigned", DialectClickHouse, "Decimal(18, 2)"}, // no (p,s) suffix -> default
		{"decimal(0,0)", DialectClickHouse, "Decimal(18, 2)"},     // p<1 -> clamped to default
		{"decimal(70,2)", DialectMySQL, "decimal(70,2)"},          // MySQL passthrough: server validates at DDL time
		{"decimal(70,2)", DialectClickHouse, "Decimal(18, 2)"},    // CH clamp via p>65 gate
		{"decimal(2,5)", DialectClickHouse, "Decimal(18, 2)"},     // scale>precision -> default
	}
	for _, c := range cases {
		if got := MapSourceType(c.dialect, c.raw); got != c.want {
			t.Errorf("MapSourceType(%v, %q) = %q, want %q", c.dialect, c.raw, got, c.want)
		}
	}
}

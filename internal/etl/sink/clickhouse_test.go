package sink

import (
	"sync"
	"testing"

	"github.com/shopspring/decimal"
	"time"
)

func TestNextVersionMonotonic(t *testing.T) {
	s := &ClickHouseSink{}
	var last int64
	for i := 0; i < 100_000; i++ {
		v := s.nextVersion()
		if v <= last {
			t.Fatalf("version regression at iteration %d: %d <= %d", i, v, last)
		}
		last = v
	}
}

func TestNextVersionConcurrent(t *testing.T) {
	s := &ClickHouseSink{}
	var wg sync.WaitGroup
	results := make(chan int64, 10000)

	// Launch 10 goroutines, each generating 1000 versions.
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				results <- s.nextVersion()
			}
		}()
	}
	wg.Wait()
	close(results)

	// Verify all values are unique (strictly monotonic across all goroutines).
	seen := make(map[int64]bool)
	for v := range results {
		if seen[v] {
			t.Fatalf("duplicate version: %d", v)
		}
		seen[v] = true
	}
	if len(seen) != 10000 {
		t.Fatalf("expected 10000 unique versions, got %d", len(seen))
	}
}

func TestClickHouseSchemaDriftBoolCompatibility(t *testing.T) {
	enabled, err := NewClickHouseSink(map[string]any{"schema_drift": true})
	if err != nil {
		t.Fatalf("NewClickHouseSink(true): %v", err)
	}
	if enabled.schemaDrift != "add_columns" {
		t.Fatalf("schemaDrift = %q, want add_columns", enabled.schemaDrift)
	}

	disabled, err := NewClickHouseSink(map[string]any{"schema_drift": false})
	if err != nil {
		t.Fatalf("NewClickHouseSink(false): %v", err)
	}
	if disabled.schemaDrift != "ignore" {
		t.Fatalf("schemaDrift = %q, want ignore", disabled.schemaDrift)
	}
}

func TestClickHouseAsyncInsertWaitDefaultsToTrue(t *testing.T) {
	sink, err := NewClickHouseSink(map[string]any{})
	if err != nil {
		t.Fatalf("NewClickHouseSink(defaults): %v", err)
	}
	if !sink.asyncInsertWait {
		t.Fatal("asyncInsertWait = false, want schema default true")
	}

	disabled, err := NewClickHouseSink(map[string]any{"async_insert_wait": false})
	if err != nil {
		t.Fatalf("NewClickHouseSink(async_insert_wait=false): %v", err)
	}
	if disabled.asyncInsertWait {
		t.Fatal("asyncInsertWait = true, want explicit false override")
	}
}

func TestConvertClickHouseHTTPValueFormatsTemporalTypes(t *testing.T) {
	ts := time.Date(2026, 6, 29, 14, 13, 15, 123456789, time.UTC)

	tests := []struct {
		name string
		typ  string
		want any
	}{
		{name: "datetime64", typ: "DateTime64(3)", want: "2026-06-29 14:13:15.123"},
		{name: "nullable_datetime", typ: "Nullable(DateTime)", want: "2026-06-29 14:13:15"},
		{name: "date", typ: "Date", want: "2026-06-29"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertClickHouseHTTPValue(ts, tt.typ)
			if got != tt.want {
				t.Fatalf("convertClickHouseHTTPValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConvertClickHouseValueEmptyStringToNumeric(t *testing.T) {
	tests := []struct {
		name string
		typ  string
		val  any
		want any
	}{
		{name: "int64_empty", typ: "Int64", val: "", want: int64(0)},
		{name: "int64_blank", typ: "Int64", val: "   ", want: int64(0)},
		{name: "uint64_empty", typ: "UInt64", val: "", want: int64(0)},
		{name: "nullable_int32_empty", typ: "Nullable(Int32)", val: "", want: int64(0)},
		{name: "lowcard_int_empty", typ: "LowCardinality(Int16)", val: "", want: int64(0)},
		{name: "float64_empty", typ: "Float64", val: "", want: float64(0)},
		{name: "decimal_empty", typ: "Decimal(18,2)", val: "", want: decimal.NewFromInt(0)},
		// Non-empty garbage must NOT be coerced silently: it stays a string so
		// AppendRow fails loudly and the row lands in the DLQ.
		{name: "int64_garbage", typ: "Int64", val: "abc", want: "abc"},
		// Existing happy paths must not regress.
		{name: "int64_parse", typ: "Int64", val: "123", want: int64(123)},
		{name: "int64_float_string", typ: "Int64", val: "12.5", want: int64(12)},
		{name: "decimal_parse", typ: "Decimal(18,2)", val: "12.34", want: decimal.RequireFromString("12.34")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertClickHouseValue(tt.val, tt.typ)
			// decimal.Decimal is a big.Int-backed struct: compare via Equal.
			if dwant, ok := tt.want.(decimal.Decimal); ok {
				dgot, dok := got.(decimal.Decimal)
				if !dok || !dgot.Equal(dwant) {
					t.Fatalf("convertClickHouseValue(%v, %s) = %v (%T), want %v", tt.val, tt.typ, got, got, tt.want)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("convertClickHouseValue(%v, %s) = %v (%T), want %v (%T)", tt.val, tt.typ, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestConvertClickHouseValueEmptyStringToDateTime(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	tests := []struct {
		name string
		typ  string
		val  any
		want any
	}{
		// Empty/blank -> epoch for non-nullable DateTime (avoids parse failure
		// aborting the whole batch; mirrors numeric empty->0).
		{name: "datetime_empty", typ: "DateTime", val: "", want: epoch},
		{name: "datetime64_blank", typ: "DateTime64(3)", val: "   ", want: epoch},
		// Nullable columns get NULL instead of epoch.
		{name: "nullable_datetime_empty", typ: "Nullable(DateTime64(3))", val: "", want: nil},
		// Parseable timestamp strings still parse normally.
		{name: "datetime_parse", typ: "DateTime64(3)", val: "2026-06-01 08:00:00", want: time.Date(2026, 6, 1, 8, 0, 0, 0, time.Local)},
		{name: "datetime_rfc3339", typ: "DateTime64(3)", val: "2026-05-31T21:23:39+08:00", want: time.Date(2026, 5, 31, 21, 23, 39, 0, time.FixedZone("CST", 8*3600))},
		// Non-empty unparseable strings stay as-is so AppendRow fails loudly
		// and the row lands in the DLQ (e.g. work_time="[1,2,3]").
		{name: "datetime_junk", typ: "DateTime64(3)", val: "[1,2,3]", want: "[1,2,3]"},
		{name: "datetime_text", typ: "DateTime", val: "hello", want: "hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertClickHouseValue(tt.val, tt.typ)
			if twant, ok := tt.want.(time.Time); ok {
				tgot, tok := got.(time.Time)
				if !tok || !tgot.Equal(twant) {
					t.Fatalf("convertClickHouseValue(%v, %s) = %v, want %v", tt.val, tt.typ, got, tt.want)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("convertClickHouseValue(%v, %s) = %v (%T), want %v (%T)", tt.val, tt.typ, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestInferClickHouseTypeDeclaredPriority(t *testing.T) {
	tests := []struct {
		name     string
		col      string
		val      any
		declared string
		want     string
	}{
		// Source metainfo (MySQL COLUMN_TYPE) wins: request_id stayed varchar.
		{name: "varchar_declared", col: "request_id", val: "33b6338a2c78d0c7", declared: "varchar(255)", want: "String"},
		{name: "bigint_declared", col: "request_id", val: "33b6338a2c78d0c7", declared: "bigint unsigned", want: "Int64"},
		{name: "int_declared", col: "user_id", val: "7", declared: "int", want: "Int32"},
		{name: "decimal_declared", col: "amount", val: "12.50", declared: "decimal(10,2)", want: "Decimal(18, 2)"},
		{name: "datetime_declared", col: "created_at", val: "2026-05-31 21:23:39", declared: "datetime", want: "DateTime64(3)"},
		{name: "enum_declared", col: "status", val: "1", declared: "enum('0','1')", want: "String"},
		{name: "json_declared", col: "custom_field", val: "[]", declared: "json", want: "String"},
		// No metainfo: falls back to value+name inference (hex text stays String).
		{name: "no_declared_hex", col: "request_id", val: "33b6338a2c78d0c7", declared: "", want: "String"},
		{name: "no_declared_numeric", col: "request_id", val: "123456", declared: "", want: "Int64"},
		// Unknown declared types also fall back instead of producing garbage DDL.
		{name: "unknown_declared", col: "weird", val: "x", declared: "hstore", want: "String"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inferClickHouseType(tt.col, tt.val, tt.declared)
			if got != tt.want {
				t.Fatalf("inferClickHouseType(%s, %v, %q) = %q, want %q", tt.col, tt.val, tt.declared, got, tt.want)
			}
		})
	}
}

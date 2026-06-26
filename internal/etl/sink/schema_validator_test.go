package sink

import (
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestValidateSchemaCompatibilityAcceptsCompatibleColumns(t *testing.T) {
	source := core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "bigint"},
		{Name: "amount", DataType: "decimal(12,2)"},
		{Name: "created_at", DataType: "datetime"},
		{Name: "payload", DataType: "json"},
	}}
	target := []core.ColumnInfo{
		{Name: "id", DataType: "Int64"},
		{Name: "amount", DataType: "Decimal(12, 2)"},
		{Name: "created_at", DataType: "DateTime64(3)"},
		{Name: "payload", DataType: "String"},
	}
	if err := validateSchemaCompatibility(source, target, schemaValidationOptions{targetName: "clickhouse orders"}); err != nil {
		t.Fatalf("validateSchemaCompatibility() = %v, want nil", err)
	}
}

func TestValidateSchemaCompatibilityReportsMissingColumns(t *testing.T) {
	source := core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "bigint"},
		{Name: "email", DataType: "varchar(255)"},
	}}
	target := []core.ColumnInfo{{Name: "id", DataType: "bigint"}}
	err := validateSchemaCompatibility(source, target, schemaValidationOptions{targetName: "mysql users"})
	if err == nil {
		t.Fatal("validateSchemaCompatibility() = nil, want missing column error")
	}
	if !strings.Contains(err.Error(), "missing target columns [email]") {
		t.Fatalf("error = %v, want missing email column", err)
	}
}

func TestValidateSchemaCompatibilityAllowsConfiguredMissingColumns(t *testing.T) {
	source := core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "id", DataType: "bigint"},
		{Name: "email", DataType: "varchar(255)"},
	}}
	target := []core.ColumnInfo{{Name: "id", DataType: "bigint"}}
	if err := validateSchemaCompatibility(source, target, schemaValidationOptions{allowMissing: true}); err != nil {
		t.Fatalf("validateSchemaCompatibility() = %v, want nil when missing columns are allowed", err)
	}
}

func TestValidateSchemaCompatibilityReportsIncompatibleTypes(t *testing.T) {
	source := core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "status", DataType: "varchar(32)"},
	}}
	target := []core.ColumnInfo{{Name: "status", DataType: "Int64"}}
	err := validateSchemaCompatibility(source, target, schemaValidationOptions{targetName: "clickhouse orders"})
	if err == nil {
		t.Fatal("validateSchemaCompatibility() = nil, want incompatible type error")
	}
	if !strings.Contains(err.Error(), "status source=varchar(32) target=Int64") {
		t.Fatalf("error = %v, want status type mismatch", err)
	}
}

func TestSchemaTypeFamilyHandlesCommonDatabaseTypes(t *testing.T) {
	cases := map[string]string{
		"character varying":                "string",
		"timestamp without time zone":      "time",
		"Nullable(LowCardinality(String))": "string",
		"UInt64":                           "uint",
		"bytea":                            "bytes",
	}
	for typ, want := range cases {
		if got := schemaTypeFamily(typ); got != want {
			t.Fatalf("schemaTypeFamily(%q) = %q, want %q", typ, got, want)
		}
	}
}

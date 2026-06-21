package ddl

import (
	"strings"
	"testing"
)

func TestTranslateAddColumn_MySQLToClickHouse(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		wantSQL string
	}{
		{
			"int_to_int32",
			"ALTER TABLE users ADD COLUMN age INT",
			"ALTER TABLE users ADD COLUMN age Int32",
		},
		{
			"varchar_to_string",
			"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			"ALTER TABLE users ADD COLUMN email String",
		},
		{
			"bigint_to_int64",
			"ALTER TABLE orders ADD COLUMN amount BIGINT",
			"ALTER TABLE orders ADD COLUMN amount Int64",
		},
		{
			"datetime_to_datetime64",
			"ALTER TABLE events ADD COLUMN created_at DATETIME",
			"ALTER TABLE events ADD COLUMN created_at DateTime64(3)",
		},
		{
			"decimal_preserves_params",
			"ALTER TABLE products ADD COLUMN price DECIMAL(10,2)",
			"ALTER TABLE products ADD COLUMN price Decimal(10,2)",
		},
		{
			"text_to_string",
			"ALTER TABLE posts ADD COLUMN body TEXT",
			"ALTER TABLE posts ADD COLUMN body String",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TranslateDDL(tt.stmt, DialectMySQL, DialectClickHouse)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Statement != tt.wantSQL {
				t.Errorf("got  %q\nwant %q", result.Statement, tt.wantSQL)
			}
		})
	}
}

func TestTranslateAddColumn_MySQLToPostgreSQL(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		wantSQL string
	}{
		{
			"int_to_integer",
			"ALTER TABLE users ADD COLUMN age INT",
			"ALTER TABLE users ADD COLUMN age INTEGER",
		},
		{
			"varchar_kept",
			"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		},
		{
			"text_to_text",
			"ALTER TABLE posts ADD COLUMN body TEXT",
			"ALTER TABLE posts ADD COLUMN body TEXT",
		},
		{
			"datetime_to_timestamp",
			"ALTER TABLE events ADD COLUMN created_at DATETIME",
			"ALTER TABLE events ADD COLUMN created_at TIMESTAMP(3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TranslateDDL(tt.stmt, DialectMySQL, DialectClickHouse)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// Use PostgreSQL as target
			result, err = TranslateDDL(tt.stmt, DialectMySQL, DialectPostgreSQL)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Statement != tt.wantSQL {
				t.Errorf("got  %q\nwant %q", result.Statement, tt.wantSQL)
			}
		})
	}
}

func TestTranslateDropColumn(t *testing.T) {
	result, err := TranslateDDL("ALTER TABLE users DROP COLUMN email", DialectMySQL, DialectClickHouse)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "ALTER TABLE users DROP COLUMN email"
	if result.Statement != want {
		t.Errorf("got %q, want %q", result.Statement, want)
	}
}

func TestTranslateModifyColumn_MySQLToPostgreSQL(t *testing.T) {
	result, err := TranslateDDL("ALTER TABLE users MODIFY COLUMN age BIGINT", DialectMySQL, DialectPostgreSQL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "ALTER TABLE users ALTER COLUMN age TYPE BIGINT"
	if result.Statement != want {
		t.Errorf("got %q, want %q", result.Statement, want)
	}
}

func TestTranslateModifyColumn_MySQLToClickHouse(t *testing.T) {
	result, err := TranslateDDL("ALTER TABLE users MODIFY COLUMN age BIGINT", DialectMySQL, DialectClickHouse)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.Statement, "MODIFY COLUMN") {
		t.Errorf("expected MODIFY COLUMN in %q", result.Statement)
	}
	if len(result.Warnings) == 0 {
		t.Error("expected warning about ClickHouse MODIFY COLUMN limitations")
	}
}

func TestTranslateDDL_SameDialect(t *testing.T) {
	stmt := "ALTER TABLE users ADD COLUMN name VARCHAR(100)"
	result, err := TranslateDDL(stmt, DialectMySQL, DialectMySQL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Statement != stmt {
		t.Errorf("same dialect should pass through unchanged, got %q", result.Statement)
	}
}

func TestTranslateDDL_DropTable(t *testing.T) {
	result, err := TranslateDDL("DROP TABLE users", DialectMySQL, DialectClickHouse)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Statement != "DROP TABLE users" {
		t.Errorf("got %q", result.Statement)
	}
}

func TestTranslateDDL_Unsupported(t *testing.T) {
	_, err := TranslateDDL("GRANT SELECT ON users TO 'user'", DialectMySQL, DialectClickHouse)
	if err == nil {
		t.Error("expected error for unsupported DDL type")
	}
}

func TestTranslateMySQLTypeToClickHouse(t *testing.T) {
	tests := []struct {
		mysqlType string
		wantCH    string
	}{
		{"TINYINT", "Int8"},
		{"SMALLINT", "Int16"},
		{"INT", "Int32"},
		{"INTEGER", "Int32"},
		{"MEDIUMINT", "Int32"},
		{"BIGINT", "Int64"},
		{"FLOAT", "Float32"},
		{"DOUBLE", "Float64"},
		{"REAL", "Float64"},
		{"DECIMAL(18,2)", "Decimal(18,2)"},
		{"VARCHAR(255)", "String"},
		{"CHAR(10)", "String"},
		{"TEXT", "String"},
		{"BLOB", "String"},
		{"DATE", "Date"},
		{"DATETIME", "DateTime64(3)"},
		{"TIMESTAMP", "DateTime64(3)"},
		{"JSON", "String"},
		{"UNKNOWN_TYPE", "String"},
	}

	for _, tt := range tests {
		got := mysqlTypeToClickHouse(tt.mysqlType)
		if got != tt.wantCH {
			t.Errorf("mysqlTypeToClickHouse(%q) = %q, want %q", tt.mysqlType, got, tt.wantCH)
		}
	}
}

func TestParseAddColumn(t *testing.T) {
	tests := []struct {
		stmt        string
		wantTable   string
		wantCol     string
		wantType    string
	}{
		{"ALTER TABLE users ADD COLUMN age INT", "users", "age", "INT"},
		{"ALTER TABLE users ADD COLUMN `email` VARCHAR(255) NOT NULL", "users", "email", "VARCHAR(255)"},
		{"ALTER TABLE orders ADD amount DECIMAL(10,2) DEFAULT 0", "orders", "amount", "DECIMAL(10,2)"},
		{"ALTER TABLE `mydb`.`users` ADD COLUMN name TEXT", "users", "name", "TEXT"},
	}

	for _, tt := range tests {
		table, col, typ, err := parseAddColumn(tt.stmt)
		if err != nil {
			t.Errorf("parseAddColumn(%q): %v", tt.stmt, err)
			continue
		}
		if table != tt.wantTable {
			t.Errorf("table = %q, want %q", table, tt.wantTable)
		}
		if col != tt.wantCol {
			t.Errorf("col = %q, want %q", col, tt.wantCol)
		}
		if typ != tt.wantType {
			t.Errorf("type = %q, want %q", typ, tt.wantType)
		}
	}
}

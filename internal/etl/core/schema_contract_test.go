package core

import "testing"

func TestComputeSchemaFingerprintStable(t *testing.T) {
	a := []ColumnContract{{Name: "id", Type: "INT"}, {Name: "name", Type: "VARCHAR(64)"}, {Name: "amt", Type: "DECIMAL(12,2)", Nullable: true}}
	b := []ColumnContract{{Name: "amt", Type: "decimal(12,2)", Nullable: true}, {Name: "name", Type: "varchar(64)"}, {Name: "id", Type: "int"}}
	fa, fb := ComputeSchemaFingerprint(a), ComputeSchemaFingerprint(b)
	if fa != fb {
		t.Fatalf("fingerprint must be order- and case-insensitive: %s != %s", fa, fb)
	}
	// A nullable change is a fingerprint change.
	c := []ColumnContract{{Name: "id", Type: "INT"}, {Name: "name", Type: "VARCHAR(64)"}, {Name: "amt", Type: "DECIMAL(12,2)"}}
	if ComputeSchemaFingerprint(c) == fa {
		t.Fatal("nullability must participate in the fingerprint")
	}
}

func TestDiffSchemaContractMatrix(t *testing.T) {
	base := []ColumnContract{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR(64)"},
		{Name: "amount", Type: "DECIMAL(12,2)"},
	}
	contract := NormalizeSchemaContract("p", 1, base, "mysql_batch", "db", "t")

	// Identical live schema -> match.
	if d := DiffSchemaContract(contract, base); d.Severity() != SchemaContractMatch {
		t.Fatalf("identical schema must match, got %#v", d)
	}

	// Additive live column -> allowed.
	added := append(append([]ColumnContract{}, base...), ColumnContract{Name: "loyalty", Type: "VARCHAR(32)"})
	d := DiffSchemaContract(contract, added)
	if d.Severity() != SchemaContractAdditive || len(d.Added) != 1 || d.Added[0] != "loyalty" {
		t.Fatalf("additive drift misdetected: %#v", d)
	}

	// Removed column -> destructive.
	reduced := []ColumnContract{{Name: "id", Type: "INT"}, {Name: "name", Type: "VARCHAR(64)"}}
	d = DiffSchemaContract(contract, reduced)
	if d.Severity() != SchemaContractDestructive || len(d.Removed) != 1 || d.Removed[0] != "amount" {
		t.Fatalf("removed column must be destructive: %#v", d)
	}

	// Safe widening int -> bigint -> additive (no diff).
	widened := []ColumnContract{{Name: "id", Type: "BIGINT"}, {Name: "name", Type: "VARCHAR(64)"}, {Name: "amount", Type: "DECIMAL(12,2)"}}
	d = DiffSchemaContract(contract, widened)
	if d.Severity() == SchemaContractDestructive {
		t.Fatalf("int->bigint widening must be allowed: %#v", d)
	}

	// Narrowing bigint -> int (contract is bigint, live int) -> destructive.
	contractWide := NormalizeSchemaContract("p", 2, []ColumnContract{{Name: "id", Type: "BIGINT"}}, "", "", "")
	d = DiffSchemaContract(contractWide, []ColumnContract{{Name: "id", Type: "INT"}})
	if d.Severity() != SchemaContractDestructive || len(d.TypeChanged) != 1 || d.TypeChanged[0].Column != "id" {
		t.Fatalf("bigint->int narrowing must be destructive: %#v", d)
	}

	// Kind change string -> int -> destructive.
	contractStr := NormalizeSchemaContract("p", 3, []ColumnContract{{Name: "v", Type: "VARCHAR(10)"}}, "", "", "")
	d = DiffSchemaContract(contractStr, []ColumnContract{{Name: "v", Type: "INT"}})
	if d.Severity() != SchemaContractDestructive {
		t.Fatalf("string->int kind change must be destructive: %#v", d)
	}

	// Nullability loss -> destructive.
	contractNull := NormalizeSchemaContract("p", 4, []ColumnContract{{Name: "v", Type: "INT", Nullable: true}}, "", "", "")
	d = DiffSchemaContract(contractNull, []ColumnContract{{Name: "v", Type: "INT"}})
	if d.Severity() != SchemaContractDestructive {
		t.Fatalf("nullable -> not null must be destructive: %#v", d)
	}

	// Text family widening varchar -> text -> allowed.
	contractVC := NormalizeSchemaContract("p", 5, []ColumnContract{{Name: "v", Type: "VARCHAR(10)"}}, "", "", "")
	d = DiffSchemaContract(contractVC, []ColumnContract{{Name: "v", Type: "TEXT"}})
	if d.Severity() == SchemaContractDestructive {
		t.Fatalf("varchar->text widening must be allowed: %#v", d)
	}
}

package source

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestMySQLInfoSchemaTarget(t *testing.T) {
	schema, table := mysqlInfoSchemaTarget("app", "orders")
	if schema != "app" || table != "orders" {
		t.Fatalf("mysqlInfoSchemaTarget(app, orders) = %s, %s", schema, table)
	}
	schema, table = mysqlInfoSchemaTarget("app", "archive.orders")
	if schema != "archive" || table != "orders" {
		t.Fatalf("mysqlInfoSchemaTarget(app, archive.orders) = %s, %s", schema, table)
	}
}

func TestSortMySQLColumnsBySelection(t *testing.T) {
	cols := []core.ColumnInfo{
		{Name: "amount"},
		{Name: "id"},
		{Name: "created_at"},
	}
	sortMySQLColumnsBySelection(cols, map[string]int{
		"id":         0,
		"created_at": 1,
		"amount":     2,
	})
	got := []string{cols[0].Name, cols[1].Name, cols[2].Name}
	want := []string{"id", "created_at", "amount"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sorted columns = %v, want %v", got, want)
		}
	}
}

func TestSingleDescribableMySQLTable(t *testing.T) {
	if table, ok := singleDescribableMySQLTable([]string{"orders"}); !ok || table != "orders" {
		t.Fatalf("singleDescribableMySQLTable(orders) = %q, %v; want orders, true", table, ok)
	}
	for _, tables := range [][]string{{}, {"*"}, {"orders", "items"}} {
		if table, ok := singleDescribableMySQLTable(tables); ok || table != "" {
			t.Fatalf("singleDescribableMySQLTable(%v) = %q, %v; want empty, false", tables, table, ok)
		}
	}
}

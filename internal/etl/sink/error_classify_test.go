package sink

import (
	"errors"
	"fmt"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassifyMySQLSQLErrors(t *testing.T) {
	cases := []struct {
		num  uint16
		want core.ErrorClass
	}{
		{1062, core.ErrorClassData},      // duplicate entry
		{1452, core.ErrorClassData},      // FK constraint
		{1054, core.ErrorClassSchema},    // unknown column
		{1146, core.ErrorClassSchema},    // table doesn't exist
		{1045, core.ErrorClassAuth},      // access denied
		{1213, core.ErrorClassTransient}, // deadlock
		{2013, core.ErrorClassTransient}, // lost connection
	}
	for _, c := range cases {
		wrapped := classifySQLError(&mysql.MySQLError{Number: c.num})
		ce, ok := wrapped.(core.ClassifiedError)
		if !ok {
			t.Errorf("mysql %d: not classified (%T)", c.num, wrapped)
			continue
		}
		if ce.Class != c.want {
			t.Errorf("mysql %d: class=%v want=%v", c.num, ce.Class, c.want)
		}
	}
}

func TestClassifyPgSQLErrors(t *testing.T) {
	cases := []struct {
		code string
		want core.ErrorClass
	}{
		{"23505", core.ErrorClassData},
		{"23503", core.ErrorClassData},
		{"42703", core.ErrorClassSchema},
		{"42P01", core.ErrorClassSchema},
		{"28P01", core.ErrorClassAuth},
		{"40001", core.ErrorClassTransient},
		{"40P01", core.ErrorClassTransient},
	}
	for _, c := range cases {
		err := &pgconn.PgError{Code: c.code}
		wrapped := classifySQLError(err)
		ce, ok := wrapped.(core.ClassifiedError)
		if !ok {
			t.Errorf("pg %s: not classified (%T)", c.code, wrapped)
			continue
		}
		if ce.Class != c.want {
			t.Errorf("pg %s: class=%v want=%v", c.code, ce.Class, c.want)
		}
	}
}

func TestClassifySQLPassthrough(t *testing.T) {
	plain := errors.New("some random driver error")
	if got := classifySQLError(plain); !errors.Is(got, plain) {
		t.Fatalf("unknown error must pass through, got %v", got)
	}
	if got := classifySQLError(nil); got != nil {
		t.Fatalf("nil must stay nil")
	}
	// wrapping preserves errors.As on the original
	orig := &mysql.MySQLError{Number: 1062}
	wrapped := classifySQLError(fmt.Errorf("ctx: %w", orig))
	var ce core.ClassifiedError
	if !errors.As(wrapped, &ce) || ce.Class != core.ErrorClassData {
		t.Fatalf("wrapped dup not classified: %v", wrapped)
	}
	var target *mysql.MySQLError
	if !errors.As(wrapped, &target) {
		t.Fatal("classification must not hide the original error type")
	}
}

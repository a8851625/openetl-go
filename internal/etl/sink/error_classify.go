package sink

import (
	"errors"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// classifySQLError wraps driver-level SQL errors with a best-effort
// core.ClassifiedError so DLQ entries carry a precise error_class instead of
// relying on global string heuristics (GAP-5). Constraint/duplicate failures
// are Data (not replayable as-is), missing table/column are Schema, auth
// codes are Auth, and lock/deadlock/server-gone are Transient. Unknown errors
// pass through unchanged — the global core.ClassifyError string matcher
// remains the fallback.
func classifySQLError(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1062, 1586, 1169, 1451, 1452, 1557: // duplicate / FK constraint
			return core.ClassifiedError{Class: core.ErrorClassData, Err: err}
		case 1054, 1146, 1050, 1051, 1091, 1060: // unknown column/table/db etc.
			return core.ClassifiedError{Class: core.ErrorClassSchema, Err: err}
		case 1044, 1045, 1142, 1143: // access denied
			return core.ClassifiedError{Class: core.ErrorClassAuth, Err: err}
		case 1205, 1213, 2006, 2013: // lock wait / deadlock / server gone
			return core.ClassifiedError{Class: core.ErrorClassTransient, Err: err}
		}
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505", "23503", "23514": // unique/FK/check violation
			return core.ClassifiedError{Class: core.ErrorClassData, Err: err}
		case "42703", "42P01", "42710", "42704": // undefined column/table/dup/type
			return core.ClassifiedError{Class: core.ErrorClassSchema, Err: err}
		case "28P01", "42501": // auth / permission
			return core.ClassifiedError{Class: core.ErrorClassAuth, Err: err}
		case "40001", "40P01", "55P03": // serialization/deadlock/lock
			return core.ClassifiedError{Class: core.ErrorClassTransient, Err: err}
		}
	}
	return err
}

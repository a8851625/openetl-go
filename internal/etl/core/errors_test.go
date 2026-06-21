package core

import (
	"errors"
	"testing"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		class ErrorClass
	}{
		{name: "transient", err: errors.New("dial tcp: connection refused"), class: ErrorClassTransient},
		{name: "auth", err: errors.New("access denied for user"), class: ErrorClassAuth},
		{name: "schema", err: errors.New("unknown column amount"), class: ErrorClassSchema},
		{name: "missing table", err: errors.New("Error 1146 (42S02): Table 'target.orders' doesn't exist"), class: ErrorClassSchema},
		{name: "config", err: errors.New("host is required"), class: ErrorClassConfig},
		{name: "data", err: errors.New("duplicate key constraint failed"), class: ErrorClassData},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyError(tt.err); got != tt.class {
				t.Fatalf("ClassifyError() = %s, want %s", got, tt.class)
			}
		})
	}
}

func TestIsRetryableError(t *testing.T) {
	if !IsRetryableError(errors.New("i/o timeout")) {
		t.Fatal("transient timeout should be retryable")
	}
	if IsRetryableError(errors.New("access denied")) {
		t.Fatal("auth error should not be retryable")
	}
	if IsRetryableError(errors.New("unknown column name")) {
		t.Fatal("schema error should not be retryable")
	}
}

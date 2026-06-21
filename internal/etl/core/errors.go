package core

import (
	"errors"
	"net"
	"strings"
)

type ErrorClass string

const (
	ErrorClassUnknown     ErrorClass = "unknown"
	ErrorClassTransient   ErrorClass = "transient"
	ErrorClassData        ErrorClass = "data"
	ErrorClassSchema      ErrorClass = "schema"
	ErrorClassAuth        ErrorClass = "auth"
	ErrorClassConfig      ErrorClass = "config"
	ErrorClassProgramming ErrorClass = "programming"
)

type ClassifiedError struct {
	Class ErrorClass
	Err   error
}

func (e ClassifiedError) Error() string { return e.Err.Error() }
func (e ClassifiedError) Unwrap() error { return e.Err }

func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ErrorClassUnknown
	}
	var classified ClassifiedError
	if errors.As(err, &classified) {
		return classified.Class
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return ErrorClassTransient
	}
	msg := strings.ToLower(err.Error())
	switch {
	case containsAny(msg, "connection refused", "connection reset", "i/o timeout", "timeout", "temporarily", "too many connections", "deadlock", "try again", "broken pipe"):
		return ErrorClassTransient
	case containsAny(msg, "access denied", "permission denied", "unauthorized", "forbidden", "authentication", "invalid credentials"):
		return ErrorClassAuth
	case containsAny(msg, "unknown column", "missing column", "no such column", "doesn't exist", "does not exist", "unknown table", "schema", "type mismatch", "cannot convert", "unsupported conversion"):
		return ErrorClassSchema
	case containsAny(msg, "invalid config", "is required", "unsupported", "unknown source", "unknown sink", "unknown transform"):
		return ErrorClassConfig
	case containsAny(msg, "duplicate", "constraint", "invalid input", "parse", "decode", "malformed", "truncated"):
		return ErrorClassData
	case containsAny(msg, "panic", "nil pointer", "index out of range"):
		return ErrorClassProgramming
	default:
		return ErrorClassUnknown
	}
}

func IsRetryableError(err error) bool {
	switch ClassifyError(err) {
	case ErrorClassTransient, ErrorClassUnknown:
		return true
	default:
		return false
	}
}

func containsAny(input string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(input, needle) {
			return true
		}
	}
	return false
}

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

// PartialBatchError is an optional sink error contract for batch writes that
// partially succeeded. FailedRecordIndices returns zero-based indexes into the
// batch passed to Sink.Write. Records not listed are treated as accepted by the
// sink, so the runner can route only failed records to DLQ without re-writing
// successful records.
type PartialBatchError interface {
	error
	FailedRecordIndices() []int
	ErrorForRecord(index int) error
}

// TransformRecordFailure describes one record-level transform failure inside a
// batch transform that still produced usable output records for other inputs.
type TransformRecordFailure struct {
	Record Record
	Err    error
}

// PartialTransformError lets BatchTransform implementations report record-level
// failures without poisoning the whole batch. The runner routes FailedRecords to
// DLQ and continues writing any output records returned by ApplyBatch.
type PartialTransformError interface {
	error
	FailedRecords() []TransformRecordFailure
}

type partialTransformError struct {
	message  string
	failures []TransformRecordFailure
}

func (e partialTransformError) Error() string {
	if e.message != "" {
		return e.message
	}
	return "partial transform failed"
}

func (e partialTransformError) FailedRecords() []TransformRecordFailure {
	return append([]TransformRecordFailure(nil), e.failures...)
}

// NewPartialTransformError creates a PartialTransformError, or nil when there
// are no failures.
func NewPartialTransformError(message string, failures []TransformRecordFailure) error {
	if len(failures) == 0 {
		return nil
	}
	return partialTransformError{message: message, failures: append([]TransformRecordFailure(nil), failures...)}
}

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
	case containsAny(msg, "connection refused", "connection reset", "i/o timeout", "timeout", "temporarily", "too many connections", "deadlock", "try again", "broken pipe", "no scannode backend available", "not alive", "in black list"):
		return ErrorClassTransient
	case containsAny(msg, "access denied", "permission denied", "unauthorized", "forbidden", "authentication", "invalid credentials"):
		return ErrorClassAuth
	case containsAny(msg, "unknown column", "missing column", "no such column", "doesn't exist", "does not exist", "unknown table", "schema", "type mismatch", "cannot convert", "unsupported conversion"):
		return ErrorClassSchema
	case containsAny(msg, "invalid config", "is required", "unsupported", "unknown source", "unknown sink", "unknown transform"):
		return ErrorClassConfig
	case containsAny(msg, "duplicate", "constraint", "invalid input", "parse", "decode", "malformed", "truncated", "out of range", "data too long", "incorrect datetime value", "incorrect date value", "incorrect integer value"):
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

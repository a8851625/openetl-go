package server

import (
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// dlqConfigWarnings makes the DLQ configuration surface honest.
//
// The runtime always wires the control-plane SQL DLQ writer, so `dlq.enable`
// cannot switch DLQ off and `dlq.sink` is never used to write. Both fields are
// still accepted for backward compatibility, but silently ignoring them while
// preflight and AI review advise on their values is a documented/intended vs
// implemented gap. These warnings tell the operator what actually happens.
func dlqConfigWarnings(spec *pipeline.Spec) []string {
	if spec == nil || spec.DLQ == nil {
		return nil
	}
	var warnings []string
	if !spec.DLQ.Enable {
		warnings = append(warnings, "dlq.enable=false is accepted for backward compatibility but has no runtime effect: "+
			"failed records are always persisted to the control-plane DLQ so they are never dropped silently. Remove the field to avoid confusion.")
	}
	if spec.DLQ.Sink.Type != "" || len(spec.DLQ.Sink.Config) > 0 || spec.DLQ.Sink.Connection != "" || spec.DLQ.Sink.ConnectionRef != "" {
		warnings = append(warnings, fmt.Sprintf("dlq.sink=%q is deprecated and ignored: the DLQ is persisted in the configured SQL storage backend, not in a separate sink. Remove the field.", spec.DLQ.Sink.Type))
	}
	return warnings
}

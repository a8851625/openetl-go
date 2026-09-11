package pipeline

import (
	"fmt"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// MetadataIdentityCompatibilityIssue is a configuration-time incompatibility
// between a sink that consumes Metadata.Key and a source path that cannot
// produce an authoritative, complete key contract.
type MetadataIdentityCompatibilityIssue struct {
	Field       string
	Message     string
	Remediation string
}

// MetadataIdentityRequired reports whether the sink explicitly opts into the
// per-record metadata primary-key contract. Static pk_columns modes continue
// to validate key fields in the sink and are outside this gate.
func MetadataIdentityRequired(spec *Spec) bool {
	if spec == nil || spec.Sink.Config == nil {
		return false
	}
	enabled, ok := boolConfig(spec.Sink.Config, "pk_columns_from_metadata")
	return ok && enabled
}

// CheckMetadataIdentityCompatibility performs an offline composition check.
// Runtime RecordIdentity validation remains mandatory because preflight can
// prove connector capability, not the completeness of each individual event.
func CheckMetadataIdentityCompatibility(spec *Spec) *MetadataIdentityCompatibilityIssue {
	if !MetadataIdentityRequired(spec) || spec == nil {
		return nil
	}
	sourceType := strings.ToLower(strings.TrimSpace(spec.Source.Type))
	switch sourceType {
	case "mysql_cdc", "mysql_snapshot_cdc", "postgres_cdc", "mysql_batch":
		return nil
	case "kafka":
		format := "json"
		if configured, ok := stringConfig(spec.Source.Config, "format"); ok && strings.TrimSpace(configured) != "" {
			format = strings.ToLower(strings.TrimSpace(configured))
		}
		if format == "canal_json" || format == "envelope" {
			return nil
		}
		if format == "json" && hasTransformType(spec, "debezium_cdc") {
			return nil
		}
		return &MetadataIdentityCompatibilityIssue{
			Field:       "source.config.format",
			Message:     fmt.Sprintf("kafka source format %q does not provide the declared complete Metadata.Key required by sink.config.pk_columns_from_metadata", format),
			Remediation: "use format=canal_json with pkNames, format=envelope with primary_key_columns and a JSON-object Kafka key, or disable pk_columns_from_metadata and configure static sink.config.pk_columns",
		}
	default:
		return &MetadataIdentityCompatibilityIssue{
			Field:       "source.type",
			Message:     fmt.Sprintf("source %q has no registered complete record-identity contract required by sink.config.pk_columns_from_metadata", spec.Source.Type),
			Remediation: "use mysql_cdc, mysql_snapshot_cdc, postgres_cdc, mysql_batch, or a Kafka canal_json/envelope path that carries an authoritative primary-key declaration; otherwise configure a static sink key",
		}
	}
}

func hasTransformType(spec *Spec, target string) bool {
	for _, transform := range spec.Transforms {
		if strings.EqualFold(strings.TrimSpace(transform.Type), target) {
			return true
		}
	}
	return false
}

func incompleteRecordIdentityError(result core.RecordIdentityResult) error {
	message := fmt.Sprintf("record identity is incomplete: reason=%s", result.Reason)
	if len(result.MissingColumns) > 0 {
		message += fmt.Sprintf(" missing_columns=%s", strings.Join(result.MissingColumns, ","))
	}
	return core.ClassifiedError{Class: core.ErrorClassData, Err: fmt.Errorf("%s", message)}
}

func sourceRecordRejectionError(rejection *core.RecordRejection) error {
	if rejection == nil {
		return core.ClassifiedError{Class: core.ErrorClassData, Err: fmt.Errorf("source record rejected")}
	}
	class := rejection.Class
	if class == "" {
		class = core.ErrorClassData
	}
	message := rejection.Message
	if message == "" {
		message = "source record rejected"
	}
	if rejection.Code != "" {
		message = fmt.Sprintf("%s: %s", rejection.Code, message)
	}
	return core.ClassifiedError{Class: class, Err: fmt.Errorf("%s", message)}
}

func dlqIdentityContext(spec *Spec, record core.Record, failure error) core.DLQIdentityContext {
	targetDatabase := ""
	targetTable := record.Metadata.Table
	if spec != nil {
		if configured, ok := stringConfig(spec.Sink.Config, "database"); ok {
			targetDatabase = configured
		}
		if configured, ok := stringConfig(spec.Sink.Config, "table"); ok && strings.TrimSpace(configured) != "" {
			targetTable = configured
		}
	}
	failureText := ""
	if failure != nil {
		failureText = failure.Error()
	}
	return core.NewDLQIdentityContext(record, failureText, targetDatabase, targetTable)
}

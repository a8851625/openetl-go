package server

import "github.com/a8851625/openetl-go/internal/etl/pipeline"

type checkpointResetContract struct {
	Source           string `json:"source"`
	Effect           string `json:"effect"`
	ExternalBoundary string `json:"external_boundary"`
	OperatorAction   string `json:"operator_action"`
}

func checkpointResetSemantics(spec *pipeline.Spec) checkpointResetContract {
	sourceType := "unknown"
	if spec != nil && spec.Source.Type != "" {
		sourceType = spec.Source.Type
	}
	switch sourceType {
	case "kafka":
		return checkpointResetContract{
			Source:           sourceType,
			Effect:           "Deletes OpenETL's stored offsets. This does not reset the broker consumer-group offsets.",
			ExternalBoundary: "The next start is still constrained by the Kafka consumer group's committed offsets and topic retention.",
			OperatorAction:   "Reset the consumer group separately when replay from an earlier broker offset is required, then set an explicit replay_from checkpoint if needed.",
		}
	case "mysql_cdc":
		return checkpointResetContract{
			Source:           sourceType,
			Effect:           "Deletes the saved binlog file/position; the next start discovers the current MySQL master position.",
			ExternalBoundary: "Reset is not replay from the oldest binlog and can skip changes between the prior checkpoint and the new current master position.",
			OperatorAction:   "Use checkpoint/set with a retained binlog file/position for controlled replay; verify sink duplicate absorption first.",
		}
	case "mysql_snapshot_cdc":
		return checkpointResetContract{
			Source:           sourceType,
			Effect:           "Deletes snapshot and CDC handoff state; the next start begins a new snapshot phase.",
			ExternalBoundary: "The full snapshot is read again before CDC handoff and can re-deliver every source row.",
			OperatorAction:   "Confirm target upsert/version semantics and capacity before reset; use business-key reconciliation after the new snapshot.",
		}
	case "postgres_cdc":
		return checkpointResetContract{
			Source:           sourceType,
			Effect:           "Deletes OpenETL's saved LSN; the replication slot remains the external source of available progress.",
			ExternalBoundary: "WAL older than the slot's retained/confirmed position may no longer be available, so reset does not guarantee replay from the beginning.",
			OperatorAction:   "Inspect the replication slot and WAL retention before reset; recreate or reposition the slot only through a separately reviewed database operation.",
		}
	default:
		return checkpointResetContract{
			Source:           sourceType,
			Effect:           "Deletes the OpenETL checkpoint for this pipeline.",
			ExternalBoundary: "The next start follows the source connector's first-start behaviour and may re-deliver or skip data depending on that source.",
			OperatorAction:   "Review the connector runbook and confirm the sink can absorb replay before reset.",
		}
	}
}

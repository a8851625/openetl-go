package server

import (
	"encoding/json"
	"net/http"
	"sort"
)

// PathContract is the machine-readable production path contract (PR-2.1).
// Each production path is certified as a full source → transforms → sink + write
// mode + business key + storage/runtime combination, not a lone connector maturity.
type PathContract struct {
	PathID            string   `json:"path_id"`
	Source            string   `json:"source"`
	SourceMode        string   `json:"source_mode"`
	Transforms        []string `json:"transforms"`
	Sink              string   `json:"sink"`
	WriteMode         string   `json:"write_mode"`
	BusinessKey       []string `json:"business_key"`
	VersionColumn     string   `json:"version_column,omitempty"`
	Storage           []string `json:"storage"`
	Runtime           string   `json:"runtime"`
	Delivery          string   `json:"delivery"`
	ReplayAbsorption  string   `json:"replay_absorption"`
	Evidence          []string `json:"evidence"`
	UnitGates         []string `json:"unit_gates,omitempty"`
	LastCertified     string   `json:"last_certified,omitempty"`
	Maturity          string   `json:"maturity"`
	RPO               string   `json:"rpo"`
	RTO               string   `json:"rto"`
	Residuals         []string `json:"residuals"`
	RequiredCases     []string `json:"required_cases"`
	DescriptorRefs    []string `json:"descriptor_refs"`
	ForcedPrimary     bool     `json:"forced_primary"`
}

const pathContractDelivery = "at_least_once"

var defaultPathRPO = "last durable checkpoint; in-flight uncheckpointed batch may replay (never silently skip)"
var defaultPathRTO = "process restart + source/sink reconnect + one checkpoint_interval_sec recovery window (standalone)"

// productionPathContracts returns the certified / candidate path contracts.
// Forced primary paths are the PR-2.2 acceptance set.
func productionPathContracts() []PathContract {
	contracts := []PathContract{
		{
			PathID:           "mysql_cdc__mysql_upsert",
			Source:           "mysql_cdc",
			SourceMode:       "cdc",
			Transforms:       []string{"identity"},
			Sink:             "mysql",
			WriteMode:        "upsert",
			BusinessKey:      []string{"id"},
			Storage:          []string{"sqlite", "mysql", "postgres"},
			Runtime:          "standalone",
			Delivery:         pathContractDelivery,
			ReplayAbsorption: "MySQL batch_mode=upsert overwrites the same primary key on replay",
			Evidence: []string{
				"hack/e2e-path-mysql-cdc-mysql.sh",
				"hack/e2e-cdc-mysql.sh",
				"hack/e2e-cdc-crash-recovery.sh",
			},
			UnitGates: []string{
				"internal/etl/checkpoint/*_test.go",
				"internal/etl/pipeline/runner_test.go",
				"internal/etl/server/dlq_test.go",
			},
			Maturity:      "production",
			RPO:           defaultPathRPO,
			RTO:           defaultPathRTO,
			Residuals:     []string{"source binlog and sink are not a distributed transaction", "not exactly-once"},
			RequiredCases: pathRequiredCases(),
			DescriptorRefs: []string{
				"source:mysql_cdc",
				"sink:mysql",
			},
			ForcedPrimary: true,
		},
		{
			PathID:           "mysql_snap_cdc__ch_rmt",
			Source:           "mysql_snapshot_cdc",
			SourceMode:       "snapshot+cdc",
			Transforms:       []string{"identity"},
			Sink:             "clickhouse",
			WriteMode:        "replacing_merge_tree",
			BusinessKey:      []string{"id"},
			VersionColumn:    "_version",
			Storage:          []string{"sqlite", "mysql", "postgres"},
			Runtime:          "standalone",
			Delivery:         pathContractDelivery,
			ReplayAbsorption: "ClickHouse ReplacingMergeTree collapses duplicates by ORDER BY + version; queries needing exact current state should use FINAL",
			Evidence: []string{
				"hack/e2e-snapshot-cdc-clickhouse.sh",
				"hack/e2e-snapshot-cdc-crash.sh",
			},
			UnitGates: []string{
				"internal/etl/checkpoint/*_test.go",
				"internal/etl/pipeline/runner_test.go",
				"internal/etl/server/dlq_test.go",
			},
			Maturity:      "production",
			RPO:           defaultPathRPO,
			RTO:           defaultPathRTO,
			Residuals:     []string{"source binlog and sink are not a distributed transaction", "raw duplicates may exist before merge/FINAL"},
			RequiredCases: pathRequiredCases(),
			DescriptorRefs: []string{
				"source:mysql_snapshot_cdc",
				"sink:clickhouse",
			},
			ForcedPrimary: true,
		},
		{
			PathID:           "debezium_kafka__mysql",
			Source:           "kafka",
			SourceMode:       "debezium_cdc",
			Transforms:       []string{"debezium_cdc"},
			Sink:             "mysql",
			WriteMode:        "upsert",
			BusinessKey:      []string{"metadata.key / pk_columns_from_metadata"},
			Storage:          []string{"sqlite", "mysql", "postgres"},
			Runtime:          "standalone",
			Delivery:         pathContractDelivery,
			ReplayAbsorption: "MySQL upsert with stable keys from Debezium message key",
			Evidence:         []string{"hack/e2e-debezium-mysql.sh"},
			Maturity:         "production_with_review",
			RPO:              defaultPathRPO,
			RTO:              defaultPathRTO,
			Residuals:        []string{"Debezium connector lifecycle remains external"},
			RequiredCases:    []string{"happy", "app_restart", "broker_rebalance"},
			DescriptorRefs:   []string{"source:kafka", "sink:mysql", "transform:debezium_cdc"},
		},
		{
			PathID:           "kafka__file_unsafe",
			Source:           "kafka",
			SourceMode:       "streaming",
			Transforms:       nil,
			Sink:             "file_sink",
			WriteMode:        "content_addressed_append",
			BusinessKey:      []string{"content-addressed object/file key"},
			Storage:          []string{"sqlite", "mysql", "postgres"},
			Runtime:          "standalone",
			Delivery:         pathContractDelivery,
			ReplayAbsorption: "content-addressed file key keeps object count stable for identical batches; require allow_unsafe: true",
			Evidence:         []string{"hack/e2e-kafka.sh"},
			Maturity:         "production_with_review",
			RPO:              defaultPathRPO,
			RTO:              defaultPathRTO,
			Residuals:        []string{"blocked by default without allow_unsafe", "changed batch boundaries may produce different objects"},
			RequiredCases:    []string{"happy", "crash_restart", "broker_restart"},
			DescriptorRefs:   []string{"source:kafka", "sink:file_sink"},
		},
		{
			PathID:           "mysql_snap_cdc__doris_uk",
			Source:           "mysql_snapshot_cdc",
			SourceMode:       "snapshot+cdc",
			Transforms:       []string{"identity"},
			Sink:             "doris",
			WriteMode:        "upsert",
			BusinessKey:      []string{"id"},
			Storage:          []string{"sqlite", "mysql", "postgres"},
			Runtime:          "standalone",
			Delivery:         pathContractDelivery,
			ReplayAbsorption: "Doris Unique Key / upsert with stable PK",
			Evidence:         []string{"hack/e2e-doris.sh"},
			Maturity:         "production_with_review",
			RPO:              defaultPathRPO,
			RTO:              defaultPathRTO,
			Residuals:        []string{"mixed write/delete batches remain constrained"},
			RequiredCases:    pathRequiredCases(),
			DescriptorRefs:   []string{"source:mysql_snapshot_cdc", "sink:doris"},
		},
		{
			PathID:           "file_batch__s3_content_key",
			Source:           "file",
			SourceMode:       "batch",
			Transforms:       nil,
			Sink:             "s3",
			WriteMode:        "content_addressed_append",
			BusinessKey:      []string{"content-addressed object key"},
			Storage:          []string{"sqlite", "mysql", "postgres"},
			Runtime:          "standalone",
			Delivery:         pathContractDelivery,
			ReplayAbsorption: "deterministic content-addressed object key overwrites identical batch",
			Evidence:         []string{"hack/e2e-s3-minio.sh"},
			Maturity:         "production_with_review",
			RPO:              defaultPathRPO,
			RTO:              defaultPathRTO,
			Residuals:        []string{"first-class manifests are not implemented"},
			RequiredCases:    []string{"happy", "checkpoint_reset", "sink_outage_dlq_replay"},
			DescriptorRefs:   []string{"source:file", "sink:s3"},
		},
	}
	sort.Slice(contracts, func(i, j int) bool {
		if contracts[i].ForcedPrimary != contracts[j].ForcedPrimary {
			return contracts[i].ForcedPrimary
		}
		return contracts[i].PathID < contracts[j].PathID
	})
	return contracts
}

func pathRequiredCases() []string {
	return []string{
		"happy",
		"crash_restart",
		"checkpoint_reset",
		"sink_outage_dlq_replay",
		"schema_drift",
	}
}

func pathContractByID(id string) (PathContract, bool) {
	for _, c := range productionPathContracts() {
		if c.PathID == id {
			return c, true
		}
	}
	return PathContract{}, false
}

func forcedPrimaryPathContracts() []PathContract {
	var out []PathContract
	for _, c := range productionPathContracts() {
		if c.ForcedPrimary {
			out = append(out, c)
		}
	}
	return out
}

func (s *Server) handlePathContracts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	contracts := productionPathContracts()
	primary := forcedPrimaryPathContracts()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"version":          "v1",
		"delivery_default": pathContractDelivery,
		"doc":              "docs/path-contract.md",
		"reliability_doc":  "docs/reliability-certification.md",
		"idempotency_doc":  "docs/etl-idempotency.md",
		"rpo":              defaultPathRPO,
		"rto":              defaultPathRTO,
		"forced_primary":   primary,
		"contracts":        contracts,
	})
}

package source

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func checkpointJSON(raw string) *core.Checkpoint {
	return &core.Checkpoint{Position: json.RawMessage(raw)}
}

func TestSourceCheckpointValidatorsFailClosedOnSemanticCorruption(t *testing.T) {
	gtidSource := &MySQLCDCSource{enableGTID: true}
	pgSnapshot := &PostgresCDCSource{enableSnapshot: true}
	pgCDC := &PostgresCDCSource{}
	snapshot := &MySQLSnapshotCDCSource{}
	cases := []struct {
		name   string
		source core.CheckpointValidator
		pos    string
		wantOK bool
		wantIn string
	}{
		{"kafka valid", &KafkaSource{topic: "orders"}, `{"topic":"orders","offsets":{"0":0}}`, true, ""},
		{"kafka legacy sentinel", &KafkaSource{topic: "orders"}, `{"offsets":{"0":-1}}`, true, ""},
		{"kafka empty offsets", &KafkaSource{topic: "orders"}, `{}`, false, "offsets"},
		{"kafka negative partition", &KafkaSource{topic: "orders"}, `{"offsets":{"-1":4}}`, false, "partition"},
		{"kafka negative offset", &KafkaSource{topic: "orders"}, `{"offsets":{"0":-2}}`, false, "offset"},
		{"kafka topic mismatch", &KafkaSource{topic: "orders"}, `{"topic":"payments","offsets":{"0":4}}`, false, "does not match"},
		{"http page zero", &HTTPSource{}, `{"page":0}`, true, ""},
		{"http missing page", &HTTPSource{}, `{}`, false, "page"},
		{"http negative page", &HTTPSource{}, `{"page":-1}`, false, ">= 0"},
		{"rest offset valid", &RestSource{pagination: "offset"}, `{"offset":0,"page_count":0}`, true, ""},
		{"rest offset missing count", &RestSource{pagination: "offset"}, `{"offset":10}`, false, "page_count"},
		{"rest cursor valid terminal", &RestSource{pagination: "cursor"}, `{"page_count":2}`, true, ""},
		{"rest cursor wrong type", &RestSource{pagination: "cursor"}, `{"page_count":2,"cursor":3}`, false, "cursor"},
		{"rest token negative count", &RestSource{pagination: "page_token"}, `{"page_count":-1}`, false, "page_count"},
		{"redis zero", &RedisSource{}, `0`, true, ""},
		{"redis JSON integer", &RedisSource{}, `42`, true, ""},
		{"redis negative", &RedisSource{}, `-1`, false, ">= 0"},
		{"redis object", &RedisSource{}, `{}`, false, "decimal"},
		{"mysql batch zero", &MySQLBatchSource{}, `{"last_id":0}`, true, ""},
		{"mysql batch missing", &MySQLBatchSource{}, `{}`, false, "last_id"},
		{"mysql batch negative", &MySQLBatchSource{}, `{"last_id":-1}`, false, ">= 0"},
		{"mysql cdc file", &MySQLCDCSource{}, `{"file":"mysql-bin.000001","pos":4}`, true, ""},
		{"mysql cdc gtid", gtidSource, `{"gtid":"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}`, true, ""},
		{"mysql cdc empty", &MySQLCDCSource{}, `{}`, false, "file/position or a GTID"},
		{"mysql cdc zero file", &MySQLCDCSource{}, `{"file":"mysql-bin.000001","pos":0}`, false, "positive"},
		{"mysql cdc position overflow", &MySQLCDCSource{}, `{"file":"mysql-bin.000001","pos":4294967296}`, false, "exceeds uint32"},
		{"mysql cdc bad gtid", gtidSource, `{"gtid":"not-a-gtid"}`, false, "GTID"},
		{"postgres cdc", pgCDC, `{"phase":"cdc","lsn":"0/16B3748"}`, true, ""},
		{"postgres snapshot empty lsn", pgSnapshot, `{"phase":"snapshot"}`, true, ""},
		{"postgres bad lsn", pgCDC, `{"phase":"cdc","lsn":"nope"}`, false, "LSN"},
		{"postgres bad phase", pgCDC, `{"phase":"warmup","lsn":"0/20"}`, false, "phase"},
		{"file zero", &FileSource{}, `{"offset":0}`, true, ""},
		{"file missing offset", &FileSource{}, `{}`, false, "offset"},
		{"file negative byte offset", &FileSource{}, `{"offset":1,"byte_offset":-1}`, false, "byte_offset"},
		{"snapshot valid", snapshot, `{"phase":"snapshot","file":"binlog.000001","pos":4,"last_id":0}`, true, ""},
		{"snapshot cdc valid", snapshot, `{"phase":"cdc","file":"binlog.000001","pos":4,"last_ids":{"orders":9},"last_strs":{"users":"u-1"}}`, true, ""},
		{"snapshot missing handoff", snapshot, `{"phase":"snapshot","last_id":2}`, false, "handoff"},
		{"snapshot negative cursor", snapshot, `{"phase":"snapshot","file":"binlog.000001","pos":4,"last_ids":{"orders":-1}}`, false, "last_ids"},
		{"demo valid", &DemoSource{}, `0`, true, ""},
		{"demo malformed decimal", &DemoSource{}, `12junk`, false, "decimal"},
		{"feishu rejects unsupported resume", &FeishuSheetSource{}, `{"offset":1}`, false, "does not implement"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.source.ValidateCheckpoint(context.Background(), checkpointJSON(tc.pos))
			if tc.wantOK {
				if err != nil {
					t.Fatalf("ValidateCheckpoint() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateCheckpoint() = nil, want error containing %q", tc.wantIn)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.wantIn)) {
				t.Fatalf("error = %v, want substring %q", err, tc.wantIn)
			}
			var actionable *core.CheckpointValidationError
			if !errors.As(err, &actionable) {
				t.Fatalf("error %T does not expose CheckpointValidationError", err)
			}
			if actionable.ErrorCode() == "" || actionable.ErrorRemediation() == "" {
				t.Fatalf("actionable error missing code/remediation: %#v", actionable)
			}
		})
	}
}

func TestSourceCheckpointValidatorsAcceptNilFirstStart(t *testing.T) {
	validators := []core.CheckpointValidator{
		&KafkaSource{}, &HTTPSource{}, &RestSource{}, &RedisSource{},
		&MySQLBatchSource{}, &MySQLCDCSource{}, &PostgresCDCSource{},
		&FileSource{}, &MySQLSnapshotCDCSource{}, &DemoSource{}, &FeishuSheetSource{},
	}
	for _, validator := range validators {
		if err := validator.ValidateCheckpoint(context.Background(), nil); err != nil {
			t.Errorf("%T rejected nil first-start checkpoint: %v", validator, err)
		}
	}
}

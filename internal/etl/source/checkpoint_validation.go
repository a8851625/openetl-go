package source

// Source checkpoint validation lives in one file so every built-in source
// applies the same recovery rule: a persisted checkpoint is either a known,
// source-specific position or startup fails.  A syntactically valid JSON
// object is not enough because the source Open methods historically decoded
// missing fields as zero values and then started from the current position.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jackc/pglogrepl"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

const checkpointRemediation = "Keep the pipeline stopped, verify or repair the persisted source position, then retry; reset only after confirming the sink can absorb replay."

func invalidSourceCheckpoint(source, detail string) error {
	return core.NewCheckpointValidationError("checkpoint."+source+".invalid", detail, checkpointRemediation)
}

func checkpointObject(cp *core.Checkpoint, source string) (map[string]json.RawMessage, error) {
	if cp == nil {
		return nil, nil
	}
	raw := bytes.TrimSpace(cp.Position)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, invalidSourceCheckpoint(source, "checkpoint source position is empty")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		if err == nil {
			err = fmt.Errorf("expected a JSON object")
		}
		return nil, invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint source position must be a JSON object: %v", err))
	}
	return obj, nil
}

func rawField(obj map[string]json.RawMessage, key string) (json.RawMessage, bool) {
	raw, ok := obj[key]
	if !ok {
		return nil, false
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false
	}
	return trimmed, true
}

func requiredInt64(obj map[string]json.RawMessage, key, source string) (int64, error) {
	raw, ok := rawField(obj, key)
	if !ok {
		return 0, invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint is missing %q", key))
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint field %q must be an integer: %v", key, err))
	}
	return value, nil
}

func optionalInt64(obj map[string]json.RawMessage, key, source string) (value int64, present bool, err error) {
	raw, exists := obj[key]
	if !exists {
		return 0, false, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, true, invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint field %q must not be null", key))
	}
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return 0, true, invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint field %q must be an integer: %v", key, err))
	}
	return value, true, nil
}

func optionalString(obj map[string]json.RawMessage, key, source string) (value string, present bool, err error) {
	raw, exists := obj[key]
	if !exists {
		return "", false, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", true, invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint field %q must not be null", key))
	}
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", true, invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint field %q must be a string: %v", key, err))
	}
	return value, true, nil
}

func validateNonNegative(value int64, field, source string) error {
	if value < 0 {
		return invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint field %q must be >= 0, got %d", field, value))
	}
	return nil
}

func validateLocalIntRange(value int64, field, source string) error {
	if value > int64(^uint(0)>>1) {
		return invalidSourceCheckpoint(source, fmt.Sprintf("checkpoint field %q exceeds local integer range", field))
	}
	return nil
}

func validateBinlogFile(file, source string) error {
	file = strings.TrimSpace(file)
	if file == "" {
		return invalidSourceCheckpoint(source, "checkpoint binlog file is empty")
	}
	for _, r := range file {
		if unicode.IsSpace(r) || unicode.IsControl(r) || r == '/' || r == '\\' {
			return invalidSourceCheckpoint(source, "checkpoint binlog file contains an unsafe path/control character")
		}
	}
	return nil
}

// ValidateCheckpoint rejects missing/negative Kafka cursor fields while
// preserving Sarama's -1 committed-offset sentinel (used to replay partition
// offset zero).  A topic is optional for legacy positions, but when present it
// must match the configured source topic.
func (s *KafkaSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "kafka")
	if err != nil {
		return err
	}
	topic, _, err := optionalString(obj, "topic", "kafka")
	if err != nil {
		return err
	}
	if topic != "" && s.topic != "" && topic != s.topic {
		return invalidSourceCheckpoint("kafka", fmt.Sprintf("checkpoint topic %q does not match source topic %q", topic, s.topic))
	}
	rawOffsets, ok := rawField(obj, "offsets")
	if !ok {
		return invalidSourceCheckpoint("kafka", "checkpoint is missing non-empty offsets")
	}
	var offsets map[string]json.RawMessage
	if err := json.Unmarshal(rawOffsets, &offsets); err != nil || offsets == nil || len(offsets) == 0 {
		if err == nil {
			err = fmt.Errorf("offsets must be a non-empty JSON object")
		}
		return invalidSourceCheckpoint("kafka", fmt.Sprintf("invalid offsets: %v", err))
	}
	for partitionText, rawOffset := range offsets {
		partition, err := strconv.ParseInt(strings.TrimSpace(partitionText), 10, 32)
		if err != nil || partition < 0 {
			return invalidSourceCheckpoint("kafka", fmt.Sprintf("partition %q must be a non-negative integer", partitionText))
		}
		var offset int64
		if err := json.Unmarshal(bytes.TrimSpace(rawOffset), &offset); err != nil {
			return invalidSourceCheckpoint("kafka", fmt.Sprintf("offset for partition %d must be an integer: %v", partition, err))
		}
		if offset < -1 {
			return invalidSourceCheckpoint("kafka", fmt.Sprintf("offset for partition %d must be >= -1, got %d", partition, offset))
		}
	}
	return nil
}

func (s *HTTPSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "http")
	if err != nil {
		return err
	}
	page, err := requiredInt64(obj, "page", "http")
	if err != nil {
		return err
	}
	if err := validateNonNegative(page, "page", "http"); err != nil {
		return err
	}
	return validateLocalIntRange(page, "page", "http")
}

func (s *RestSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "rest_source")
	if err != nil {
		return err
	}
	pageCount, err := requiredInt64(obj, "page_count", "rest_source")
	if err != nil {
		return err
	}
	if err := validateNonNegative(pageCount, "page_count", "rest_source"); err != nil {
		return err
	}
	if err := validateLocalIntRange(pageCount, "page_count", "rest_source"); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(s.pagination)) {
	case "", "offset":
		offset, err := requiredInt64(obj, "offset", "rest_source")
		if err != nil {
			return err
		}
		if err := validateNonNegative(offset, "offset", "rest_source"); err != nil {
			return err
		}
		if err := validateLocalIntRange(offset, "offset", "rest_source"); err != nil {
			return err
		}
	case "cursor":
		if raw, ok := obj["cursor"]; ok && (len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))) {
			return invalidSourceCheckpoint("rest_source", "checkpoint cursor must be a string when present")
		} else if ok {
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return invalidSourceCheckpoint("rest_source", fmt.Sprintf("checkpoint cursor must be a string: %v", err))
			}
		}
	case "page_token":
		if raw, ok := obj["page_token"]; ok && (len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null"))) {
			return invalidSourceCheckpoint("rest_source", "checkpoint page_token must be a string when present")
		} else if ok {
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return invalidSourceCheckpoint("rest_source", fmt.Sprintf("checkpoint page_token must be a string: %v", err))
			}
		}
	default:
		return invalidSourceCheckpoint("rest_source", fmt.Sprintf("unsupported pagination mode %q", s.pagination))
	}
	return nil
}

func (s *RedisSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	value, err := parseRedisCheckpointPosition(cp.Position)
	if err != nil {
		return invalidSourceCheckpoint("redis", err.Error())
	}
	if value < 0 {
		return invalidSourceCheckpoint("redis", fmt.Sprintf("checkpoint key offset must be >= 0, got %d", value))
	}
	return validateLocalIntRange(value, "key offset", "redis")
}

func parseRedisCheckpointPosition(raw json.RawMessage) (int64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0, fmt.Errorf("checkpoint key offset is empty")
	}
	// Redis historically stored a plain decimal string in the position column;
	// accept that representation first, then a JSON integer for hand-authored
	// or migrated checkpoints.
	if value, err := strconv.ParseInt(string(trimmed), 10, 64); err == nil {
		return value, nil
	}
	var value int64
	if err := json.Unmarshal(trimmed, &value); err == nil {
		return value, nil
	}
	return 0, fmt.Errorf("checkpoint key offset must be a non-negative decimal integer")
}

func (s *MySQLBatchSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "mysql_batch")
	if err != nil {
		return err
	}
	lastID, err := requiredInt64(obj, "last_id", "mysql_batch")
	if err != nil {
		return err
	}
	return validateNonNegative(lastID, "last_id", "mysql_batch")
}

func (s *MySQLCDCSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "mysql_cdc")
	if err != nil {
		return err
	}
	file, filePresent, err := optionalString(obj, "file", "mysql_cdc")
	if err != nil {
		return err
	}
	if !filePresent {
		file, filePresent, err = optionalString(obj, "binlog_file", "mysql_cdc")
		if err != nil {
			return err
		}
	}
	pos, posPresent, err := optionalInt64(obj, "pos", "mysql_cdc")
	if err != nil {
		return err
	}
	if !posPresent {
		pos, posPresent, err = optionalInt64(obj, "binlog_pos", "mysql_cdc")
		if err != nil {
			return err
		}
	}
	gtid, gtidPresent, err := optionalString(obj, "gtid", "mysql_cdc")
	if err != nil {
		return err
	}
	gtid = strings.TrimSpace(gtid)
	if gtidPresent && gtid != "" {
		if _, err := mysql.ParseGTIDSet("mysql", gtid); err != nil {
			return invalidSourceCheckpoint("mysql_cdc", fmt.Sprintf("checkpoint GTID is invalid: %v", err))
		}
	}
	if filePresent {
		if err := validateBinlogFile(file, "mysql_cdc"); err != nil {
			return err
		}
		if !posPresent || pos <= 0 {
			if gtid == "" {
				return invalidSourceCheckpoint("mysql_cdc", "checkpoint binlog file requires a positive position")
			}
			if !s.enableGTID {
				return invalidSourceCheckpoint("mysql_cdc", "checkpoint contains GTID-only progress but source enable_gtid is false")
			}
		}
	} else if gtid == "" {
		return invalidSourceCheckpoint("mysql_cdc", "checkpoint must contain a binlog file/position or a GTID")
	} else if !s.enableGTID {
		return invalidSourceCheckpoint("mysql_cdc", "checkpoint contains GTID-only progress but source enable_gtid is false")
	}
	if posPresent && pos < 0 {
		return invalidSourceCheckpoint("mysql_cdc", fmt.Sprintf("checkpoint binlog position must be >= 0, got %d", pos))
	}
	if posPresent && pos > math.MaxUint32 {
		return invalidSourceCheckpoint("mysql_cdc", fmt.Sprintf("checkpoint binlog position %d exceeds uint32", pos))
	}
	return nil
}

// decodeMySQLCDCCheckpointPosition also understands the pre-v2
// binlog_file/binlog_pos field names.  Validation and source resume must use
// the same decoded value; accepting a legacy position and then silently
// ignoring it in Open would recreate the original data-loss boundary.
func decodeMySQLCDCCheckpointPosition(raw json.RawMessage) (mysqlCDCPosition, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(raw), &obj); err != nil || obj == nil {
		if err == nil {
			err = fmt.Errorf("expected a JSON object")
		}
		return mysqlCDCPosition{}, err
	}
	var pos mysqlCDCPosition
	if value, ok := rawField(obj, "file"); ok {
		if err := json.Unmarshal(value, &pos.File); err != nil {
			return mysqlCDCPosition{}, err
		}
	} else if value, ok := rawField(obj, "binlog_file"); ok {
		if err := json.Unmarshal(value, &pos.File); err != nil {
			return mysqlCDCPosition{}, err
		}
	}
	if value, ok := rawField(obj, "pos"); ok {
		var n int64
		if err := json.Unmarshal(value, &n); err != nil {
			return mysqlCDCPosition{}, err
		}
		pos.Pos = uint32(n)
	} else if value, ok := rawField(obj, "binlog_pos"); ok {
		var n int64
		if err := json.Unmarshal(value, &n); err != nil {
			return mysqlCDCPosition{}, err
		}
		pos.Pos = uint32(n)
	}
	if value, ok := rawField(obj, "gtid"); ok {
		if err := json.Unmarshal(value, &pos.GTID); err != nil {
			return mysqlCDCPosition{}, err
		}
	}
	return pos, nil
}

func (s *PostgresCDCSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "postgres_cdc")
	if err != nil {
		return err
	}
	lsn, lsnPresent, err := optionalString(obj, "lsn", "postgres_cdc")
	if err != nil {
		return err
	}
	phase, phasePresent, err := optionalString(obj, "phase", "postgres_cdc")
	if err != nil {
		return err
	}
	phase = strings.ToLower(strings.TrimSpace(phase))
	if !phasePresent || phase == "" {
		phase = "cdc"
	}
	if phase != "snapshot" && phase != "cdc" {
		return invalidSourceCheckpoint("postgres_cdc", fmt.Sprintf("checkpoint phase %q must be snapshot or cdc", phase))
	}
	if phase == "snapshot" && !s.enableSnapshot {
		return invalidSourceCheckpoint("postgres_cdc", "checkpoint is in snapshot phase but source enable_snapshot is false")
	}
	lsn = strings.TrimSpace(lsn)
	if lsnPresent && lsn != "" {
		if _, err := pglogrepl.ParseLSN(lsn); err != nil {
			return invalidSourceCheckpoint("postgres_cdc", fmt.Sprintf("checkpoint LSN %q is invalid: %v", lsn, err))
		}
	}
	if phase == "cdc" && (!lsnPresent || lsn == "") {
		return invalidSourceCheckpoint("postgres_cdc", "CDC checkpoint is missing an LSN")
	}
	return nil
}

func (s *FileSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "file")
	if err != nil {
		return err
	}
	offset, err := requiredInt64(obj, "offset", "file")
	if err != nil {
		return err
	}
	if err := validateNonNegative(offset, "offset", "file"); err != nil {
		return err
	}
	if byteOffset, present, err := optionalInt64(obj, "byte_offset", "file"); err != nil {
		return err
	} else if present {
		if err := validateNonNegative(byteOffset, "byte_offset", "file"); err != nil {
			return err
		}
	}
	return nil
}

func (s *MySQLSnapshotCDCSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	obj, err := checkpointObject(cp, "mysql_snapshot_cdc")
	if err != nil {
		return err
	}
	phase, present, err := optionalString(obj, "phase", "mysql_snapshot_cdc")
	if err != nil {
		return err
	}
	if !present || strings.TrimSpace(phase) == "" {
		phase = "snapshot"
	}
	phase = strings.ToLower(strings.TrimSpace(phase))
	if phase != "snapshot" && phase != "cdc" {
		return invalidSourceCheckpoint("mysql_snapshot_cdc", fmt.Sprintf("checkpoint phase %q must be snapshot or cdc", phase))
	}

	file, filePresent, err := optionalString(obj, "file", "mysql_snapshot_cdc")
	if err != nil {
		return err
	}
	if !filePresent {
		file, filePresent, err = optionalString(obj, "binlog_file", "mysql_snapshot_cdc")
		if err != nil {
			return err
		}
	}
	pos, posPresent, err := optionalInt64(obj, "pos", "mysql_snapshot_cdc")
	if err != nil {
		return err
	}
	if !posPresent {
		pos, posPresent, err = optionalInt64(obj, "binlog_pos", "mysql_snapshot_cdc")
		if err != nil {
			return err
		}
	}
	if !filePresent || strings.TrimSpace(file) == "" || !posPresent || pos <= 0 {
		return invalidSourceCheckpoint("mysql_snapshot_cdc", fmt.Sprintf("%s checkpoint is missing a positive CDC handoff file/position", phase))
	}
	if err := validateBinlogFile(file, "mysql_snapshot_cdc"); err != nil {
		return err
	}
	if pos > math.MaxUint32 {
		return invalidSourceCheckpoint("mysql_snapshot_cdc", fmt.Sprintf("checkpoint binlog position %d exceeds uint32", pos))
	}

	if lastID, present, err := optionalInt64(obj, "last_id", "mysql_snapshot_cdc"); err != nil {
		return err
	} else if present {
		if err := validateNonNegative(lastID, "last_id", "mysql_snapshot_cdc"); err != nil {
			return err
		}
	}
	if raw, present := obj["last_ids"]; present {
		var lastIDs map[string]json.RawMessage
		if err := json.Unmarshal(raw, &lastIDs); err != nil || lastIDs == nil {
			if err == nil {
				err = fmt.Errorf("last_ids must be a JSON object")
			}
			return invalidSourceCheckpoint("mysql_snapshot_cdc", fmt.Sprintf("invalid last_ids: %v", err))
		}
		for table, rawID := range lastIDs {
			if strings.TrimSpace(table) == "" {
				return invalidSourceCheckpoint("mysql_snapshot_cdc", "last_ids contains an empty table name")
			}
			var id int64
			if err := json.Unmarshal(rawID, &id); err != nil {
				return invalidSourceCheckpoint("mysql_snapshot_cdc", fmt.Sprintf("last_ids[%q] must be an integer: %v", table, err))
			}
			if err := validateNonNegative(id, "last_ids["+table+"]", "mysql_snapshot_cdc"); err != nil {
				return err
			}
		}
	}
	if raw, present := obj["last_strs"]; present {
		var lastStrs map[string]json.RawMessage
		if err := json.Unmarshal(raw, &lastStrs); err != nil || lastStrs == nil {
			if err == nil {
				err = fmt.Errorf("last_strs must be a JSON object")
			}
			return invalidSourceCheckpoint("mysql_snapshot_cdc", fmt.Sprintf("invalid last_strs: %v", err))
		}
		for table, rawCursor := range lastStrs {
			if strings.TrimSpace(table) == "" {
				return invalidSourceCheckpoint("mysql_snapshot_cdc", "last_strs contains an empty table name")
			}
			var cursor string
			if err := json.Unmarshal(rawCursor, &cursor); err != nil {
				return invalidSourceCheckpoint("mysql_snapshot_cdc", fmt.Sprintf("last_strs[%q] must be a string: %v", table, err))
			}
		}
	}
	return nil
}

// Demo is intentionally small but is used by smoke tests and examples.  It
// still must not turn a malformed persisted decimal into a fresh counter.
func (s *DemoSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	value, err := parseRedisCheckpointPosition(cp.Position)
	if err != nil {
		return invalidSourceCheckpoint("demo", err.Error())
	}
	if value < 0 {
		return invalidSourceCheckpoint("demo", fmt.Sprintf("checkpoint counter must be >= 0, got %d", value))
	}
	return nil
}

// Feishu Sheet currently has no durable row cursor.  Rejecting a non-empty
// checkpoint is safer than silently restarting the full sheet and making the
// control plane report a misleading resume boundary.
func (s *FeishuSheetSource) ValidateCheckpoint(_ context.Context, cp *core.Checkpoint) error {
	if cp == nil {
		return nil
	}
	return invalidSourceCheckpoint("feishu_sheet", "source does not implement a durable row checkpoint")
}

// decodeSnapshotCDCCheckpointPosition mirrors the validator's legacy aliases
// and keeps the canonical source Open path from dropping a valid old cursor.
func decodeSnapshotCDCCheckpointPosition(raw json.RawMessage) (snapshotCDCPosition, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(raw), &obj); err != nil || obj == nil {
		if err == nil {
			err = fmt.Errorf("expected a JSON object")
		}
		return snapshotCDCPosition{}, err
	}
	var pos snapshotCDCPosition
	if value, ok := rawField(obj, "phase"); ok {
		if err := json.Unmarshal(value, &pos.Phase); err != nil {
			return snapshotCDCPosition{}, err
		}
	}
	if value, ok := rawField(obj, "last_id"); ok {
		if err := json.Unmarshal(value, &pos.LastID); err != nil {
			return snapshotCDCPosition{}, err
		}
	}
	if value, ok := rawField(obj, "last_ids"); ok {
		if err := json.Unmarshal(value, &pos.LastIDs); err != nil {
			return snapshotCDCPosition{}, err
		}
	}
	if value, ok := rawField(obj, "last_strs"); ok {
		if err := json.Unmarshal(value, &pos.LastStrs); err != nil {
			return snapshotCDCPosition{}, err
		}
	}
	if value, ok := rawField(obj, "file"); ok {
		if err := json.Unmarshal(value, &pos.File); err != nil {
			return snapshotCDCPosition{}, err
		}
	} else if value, ok := rawField(obj, "binlog_file"); ok {
		if err := json.Unmarshal(value, &pos.File); err != nil {
			return snapshotCDCPosition{}, err
		}
	}
	if value, ok := rawField(obj, "pos"); ok {
		var n int64
		if err := json.Unmarshal(value, &n); err != nil {
			return snapshotCDCPosition{}, err
		}
		pos.Pos = uint32(n)
	} else if value, ok := rawField(obj, "binlog_pos"); ok {
		var n int64
		if err := json.Unmarshal(value, &n); err != nil {
			return snapshotCDCPosition{}, err
		}
		pos.Pos = uint32(n)
	}
	return pos, nil
}

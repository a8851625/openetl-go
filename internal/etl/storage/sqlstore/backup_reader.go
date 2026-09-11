package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// ReadBackupPage deliberately bypasses dispatch/API list views. Each query is
// bounded, ordered by its unique key, and includes orphaned/terminal history.
// Bad stored JSON is an export failure, never a silently omitted row.
func (s *Store) ReadBackupPage(ctx context.Context, table string, afterKey *string, limit int) ([]storage.BackupRow, error) {
	if limit < 1 || limit > 5000 {
		return nil, fmt.Errorf("backup page limit must be between 1 and 5000")
	}
	var columns, key string
	numericKey := false
	switch table {
	case "pipelines":
		columns, key = "id,name,spec_yaml,status,desired_state,observed_state,generation,COALESCE(restore_error,''),created_at,updated_at", "id"
	case "pipeline_versions":
		columns, key, numericKey = "id,pipeline,version,spec_yaml,created_at", "id", true
	case "checkpoints":
		columns, key = "job_name,source,position,generation,timestamp,updated_at", "job_name"
	case "dead_letters":
		columns, key, numericKey = "id,job_name,record_json,error,error_class,COALESCE(identity_context_json,'{}'),attempt,COALESCE(record_hash,''),COALESCE(pipeline_version,0),COALESCE(dag_node,''),created_at", "id", true
	case "audit_logs":
		columns, key, numericKey = "id,action,method,path,target,remote,created_at", "id", true
	case "run_history":
		columns, key, numericKey = "id,job_name,status,started_at,finished_at,duration_ms,records_read,records_written,records_failed,records_dlq", "id", true
	case "workers":
		columns, key = "id,host,port,slots,status,labels,last_heartbeat,registered_at", "id"
	case "task_assignments":
		columns, key, numericKey = "id,task_id,pipeline,shard_index,shard_total,worker_id,status,assigned_at,started_at,finished_at,required_labels,generation,attempt,lease_expires_at,last_error", "id", true
	case "plugins":
		columns, key = "name,kind,wasm_path,version,COALESCE(abi,''),COALESCE(min_runtime_version,''),COALESCE(manifest_json,''),manifest_validated,enabled,installed_at", "name"
	case "connections":
		columns, key = "name,kind,type,config_json,COALESCE(last_status,''),COALESCE(last_error,''),last_tested_at,created_at,updated_at", "name"
	case "settings":
		key = s.dialect.SettingKeyColumn()
		columns = key + ",value"
	default:
		return nil, fmt.Errorf("unsupported backup table %q", table)
	}
	if err := s.injectFailure("backup.export." + table); err != nil {
		return nil, err
	}
	query := "SELECT " + columns + " FROM " + table
	var args []any
	if afterKey != nil {
		var cursor any = *afterKey
		if numericKey {
			n, err := strconv.ParseInt(*afterKey, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid numeric backup cursor: %w", err)
			}
			cursor = n
		}
		query += " WHERE " + key + " > ?"
		args = append(args, cursor)
	}
	query += " ORDER BY " + key + " ASC LIMIT ?"
	args = append(args, limit)
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	page := make([]storage.BackupRow, 0, limit)
	for rows.Next() {
		row, err := scanBackupRow(table, rows)
		if err != nil {
			return nil, fmt.Errorf("read backup %s: %w", table, err)
		}
		page = append(page, row)
	}
	return page, rows.Err()
}

func scanBackupRow(table string, rows *sql.Rows) (storage.BackupRow, error) {
	var value any
	var key string
	var err error
	switch table {
	case "pipelines":
		r := new(storage.PipelineRow)
		err = rows.Scan(&r.ID, &r.Name, &r.SpecYAML, &r.Status, &r.DesiredState, &r.ObservedState, &r.Generation, &r.RestoreError, &r.CreatedAt, &r.UpdatedAt)
		value, key = r, r.ID
	case "pipeline_versions":
		r := new(storage.PipelineVersion)
		err = rows.Scan(&r.ID, &r.Pipeline, &r.Version, &r.SpecYAML, &r.CreatedAt)
		value, key = r, strconv.FormatInt(r.ID, 10)
	case "checkpoints":
		r := new(storage.CheckpointRecord)
		var position string
		err = rows.Scan(&r.JobName, &r.Source, &position, &r.Generation, &r.Timestamp, &r.UpdatedAt)
		r.Position = json.RawMessage(position)
		value, key = r, r.JobName
	case "dead_letters":
		r := new(storage.DLQRecord)
		var record, identity string
		var message, class sql.NullString
		err = rows.Scan(&r.ID, &r.JobName, &record, &message, &class, &identity, &r.Attempt, &r.RecordHash, &r.PipelineVersion, &r.DAGNode, &r.CreatedAt)
		r.Error, r.ErrorClass = message.String, class.String
		if err == nil {
			err = decodeBackupJSON(record, &r.Record)
		}
		if err == nil {
			err = decodeBackupJSON(identity, &r.IdentityContext)
		}
		value, key = r, strconv.FormatInt(r.ID, 10)
	case "audit_logs":
		r := new(storage.AuditEntry)
		err = rows.Scan(&r.ID, &r.Action, &r.Method, &r.Path, &r.Target, &r.Remote, &r.CreatedAt)
		value, key = r, strconv.FormatInt(r.ID, 10)
	case "run_history":
		r := new(storage.RunRecord)
		err = rows.Scan(&r.ID, &r.JobName, &r.Status, &r.StartedAt, &r.FinishedAt, &r.DurationMs, &r.RecordsRead, &r.RecordsWritten, &r.RecordsFailed, &r.RecordsDLQ)
		value, key = r, strconv.FormatInt(r.ID, 10)
	case "workers":
		r := new(storage.WorkerInfo)
		var labels string
		err = rows.Scan(&r.ID, &r.Host, &r.Port, &r.Slots, &r.Status, &labels, &r.LastHeartbeat, &r.RegisteredAt)
		if err == nil && labels != "" {
			err = decodeBackupJSON(labels, &r.Labels)
		}
		value, key = r, r.ID
	case "task_assignments":
		r := new(storage.TaskAssignment)
		var worker sql.NullString
		var labels string
		var generation, attempt sql.NullInt64
		err = rows.Scan(&r.ID, &r.TaskID, &r.Pipeline, &r.ShardIndex, &r.ShardTotal, &worker, &r.Status, &r.AssignedAt, &r.StartedAt, &r.FinishedAt, &labels, &generation, &attempt, &r.LeaseExpiresAt, &r.LastError)
		r.WorkerID, r.Generation, r.Attempt = worker.String, generation.Int64, int(attempt.Int64)
		if err == nil && labels != "" {
			err = decodeBackupJSON(labels, &r.RequiredLabels)
		}
		value, key = r, strconv.FormatInt(r.ID, 10)
	case "plugins":
		r := new(storage.PluginEntry)
		var valid, enabled any
		err = rows.Scan(&r.Name, &r.Kind, &r.WASMPath, &r.Version, &r.ABI, &r.MinRuntimeVersion, &r.ManifestJSON, &valid, &enabled, &r.InstalledAt)
		r.ManifestValidated, r.Enabled = dbBool(valid), dbBool(enabled)
		value, key = r, r.Name
	case "connections":
		r := new(storage.ConnectionEntry)
		var config string
		err = rows.Scan(&r.Name, &r.Kind, &r.Type, &config, &r.LastStatus, &r.LastError, &r.LastTestedAt, &r.CreatedAt, &r.UpdatedAt)
		if err == nil && config != "" {
			err = decodeBackupJSON(config, &r.Config)
		}
		value, key = r, r.Name
	case "settings":
		r := new(storage.BackupSetting)
		err = rows.Scan(&r.Key, &r.Value)
		value, key = r, r.Key
	default:
		return storage.BackupRow{}, fmt.Errorf("unsupported backup table %q", table)
	}
	return storage.BackupRow{Key: key, Value: value}, err
}

func decodeBackupJSON(encoded string, value any) error {
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.UseNumber() // Preserve integer business keys above 2^53.
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("stored JSON contains trailing data")
	}
	return nil
}

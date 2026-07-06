package sqlstore

import (
	"strconv"
	"strings"
)

type SQLiteDialect struct{}

func (SQLiteDialect) Bind(query string) string { return query }
func (SQLiteDialect) Now() string              { return "CURRENT_TIMESTAMP" }
func (SQLiteDialect) PipelineUpsert() string {
	return `INSERT INTO pipelines (id, name, spec_yaml, status, updated_at)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(id) DO UPDATE SET name=excluded.name, spec_yaml=excluded.spec_yaml, status=excluded.status, updated_at=CURRENT_TIMESTAMP`
}
func (SQLiteDialect) CheckpointUpsert() string {
	return `INSERT INTO checkpoints (job_name, source, position, timestamp, updated_at)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(job_name) DO UPDATE SET source=excluded.source, position=excluded.position, timestamp=excluded.timestamp, updated_at=CURRENT_TIMESTAMP`
}
func (SQLiteDialect) WorkerUpsert() string {
	return `INSERT INTO workers (id, host, port, slots, status, labels, last_heartbeat, registered_at)
		 VALUES (?, ?, ?, ?, 'online', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		 ON CONFLICT(id) DO UPDATE SET host=excluded.host, port=excluded.port, slots=excluded.slots, status='online', labels=excluded.labels, last_heartbeat=CURRENT_TIMESTAMP`
}
func (SQLiteDialect) PluginUpsert() string {
	return `INSERT INTO plugins (name, kind, wasm_path, version, abi, min_runtime_version, manifest_json, manifest_validated, enabled, installed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(name) DO UPDATE SET kind=excluded.kind, wasm_path=excluded.wasm_path, version=excluded.version, abi=excluded.abi, min_runtime_version=excluded.min_runtime_version, manifest_json=excluded.manifest_json, manifest_validated=excluded.manifest_validated, enabled=excluded.enabled`
}
func (SQLiteDialect) ConnectionUpsert() string {
	return `INSERT INTO connections (name, kind, type, config_json, last_status, last_error, last_tested_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(name) DO UPDATE SET kind=excluded.kind, type=excluded.type, config_json=excluded.config_json, updated_at=CURRENT_TIMESTAMP`
}
func (SQLiteDialect) SettingUpsert() string {
	return `INSERT INTO settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP`
}
func (SQLiteDialect) SettingKeyColumn() string { return "key" }
func (SQLiteDialect) BoolValue(v bool) any {
	if v {
		return 1
	}
	return 0
}
func (SQLiteDialect) RunHistoryInsertReturningID() bool { return false }

type MySQLDialect struct{ SQLiteDialect }

func (MySQLDialect) PipelineUpsert() string {
	return `INSERT INTO pipelines (id, name, spec_yaml, status, updated_at)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE name=VALUES(name), spec_yaml=VALUES(spec_yaml), status=VALUES(status), updated_at=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) CheckpointUpsert() string {
	return `INSERT INTO checkpoints (job_name, source, position, timestamp, updated_at)
		 VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE source=VALUES(source), position=VALUES(position), timestamp=VALUES(timestamp), updated_at=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) WorkerUpsert() string {
	return `INSERT INTO workers (id, host, port, slots, status, labels, last_heartbeat, registered_at)
		 VALUES (?, ?, ?, ?, 'online', ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE host=VALUES(host), port=VALUES(port), slots=VALUES(slots), status='online', labels=VALUES(labels), last_heartbeat=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) PluginUpsert() string {
	return `INSERT INTO plugins (name, kind, wasm_path, version, abi, min_runtime_version, manifest_json, manifest_validated, enabled, installed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE kind=VALUES(kind), wasm_path=VALUES(wasm_path), version=VALUES(version), abi=VALUES(abi), min_runtime_version=VALUES(min_runtime_version), manifest_json=VALUES(manifest_json), manifest_validated=VALUES(manifest_validated), enabled=VALUES(enabled)`
}
func (MySQLDialect) ConnectionUpsert() string {
	return `INSERT INTO connections (name, kind, type, config_json, last_status, last_error, last_tested_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE kind=VALUES(kind), type=VALUES(type), config_json=VALUES(config_json), updated_at=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) SettingUpsert() string {
	return `INSERT INTO settings (` + "`key`" + `, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) SettingKeyColumn() string { return "`key`" }

type PostgresDialect struct{ SQLiteDialect }

func (PostgresDialect) Bind(query string) string {
	var out strings.Builder
	n := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(n))
			n++
			continue
		}
		out.WriteByte(query[i])
	}
	return out.String()
}
func (PostgresDialect) RunHistoryInsertReturningID() bool { return true }
func (PostgresDialect) BoolValue(v bool) any              { return v }
func (PostgresDialect) PipelineUpsert() string            { return SQLiteDialect{}.PipelineUpsert() }
func (PostgresDialect) CheckpointUpsert() string          { return SQLiteDialect{}.CheckpointUpsert() }
func (PostgresDialect) WorkerUpsert() string              { return SQLiteDialect{}.WorkerUpsert() }
func (PostgresDialect) PluginUpsert() string              { return SQLiteDialect{}.PluginUpsert() }
func (PostgresDialect) ConnectionUpsert() string          { return SQLiteDialect{}.ConnectionUpsert() }
func (PostgresDialect) SettingUpsert() string             { return SQLiteDialect{}.SettingUpsert() }

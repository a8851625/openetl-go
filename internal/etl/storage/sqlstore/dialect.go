package sqlstore

import (
	"strconv"
	"strings"
)

type SQLiteDialect struct{}

func (SQLiteDialect) Bind(query string) string { return query }
func (SQLiteDialect) Now() string              { return "CURRENT_TIMESTAMP" }
func (SQLiteDialect) PipelineUpsert() string {
	return `INSERT INTO pipelines (id, name, spec_yaml, status, desired_state, observed_state, generation, restore_error, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(id) DO UPDATE SET name=excluded.name, spec_yaml=excluded.spec_yaml, status=excluded.status, restore_error=excluded.restore_error, updated_at=CURRENT_TIMESTAMP`
}
func (SQLiteDialect) CheckpointUpsert() string {
	return `INSERT INTO checkpoints (job_name, source, position, generation, timestamp, updated_at)
		 VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(job_name) DO UPDATE SET source=excluded.source, position=excluded.position, generation=excluded.generation, timestamp=excluded.timestamp, updated_at=CURRENT_TIMESTAMP`
}
func (SQLiteDialect) FencedCheckpointUpsert() string {
	return `INSERT INTO checkpoints (job_name, source, position, generation, timestamp, updated_at)
		 SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
		 WHERE EXISTS (SELECT 1 FROM pipelines WHERE id=? AND generation=?)
		 ON CONFLICT(job_name) DO UPDATE SET source=excluded.source, position=excluded.position, generation=excluded.generation, timestamp=excluded.timestamp, updated_at=CURRENT_TIMESTAMP`
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

// SchemaContractUpsert (CH-C2): upsert one (pipeline, version) contract row.
func (SQLiteDialect) SchemaContractUpsert() string {
	return `INSERT INTO pipeline_schema_contracts (pipeline, version, fingerprint, columns_json, source_type, database, table_name, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		 ON CONFLICT(pipeline, version) DO UPDATE SET
		   fingerprint = excluded.fingerprint,
		   columns_json = excluded.columns_json,
		   source_type = excluded.source_type,
		   database = excluded.database,
		   table_name = excluded.table_name,
		   created_at = CURRENT_TIMESTAMP`
}

// DeadLetterUpsert deduplicates on (job_name, record_hash): a repeated capture
// of the same failed record refreshes the existing row and bumps attempt.
func (SQLiteDialect) DeadLetterUpsert() string {
	return `INSERT INTO dead_letters (job_name, record_json, error, error_class, identity_context_json, attempt, record_hash, pipeline_version, dag_node, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(job_name, record_hash) DO UPDATE SET
		   record_json = excluded.record_json,
		   error = excluded.error,
		   error_class = excluded.error_class,
		   identity_context_json = excluded.identity_context_json,
		   attempt = dead_letters.attempt + 1,
		   pipeline_version = excluded.pipeline_version,
		   dag_node = excluded.dag_node,
		   created_at = excluded.created_at`
}

// InsertIgnore renders an insert that silently skips rows conflicting with an
// existing primary key or unique index. Restore uses it for dead_letters: a
// backup taken before the (job_name, record_hash) unique key may contain
// duplicates that the new invariant cannot represent, and a restore must not
// fail because of them.
func (SQLiteDialect) InsertIgnore(table, columns, placeholders string) string {
	return "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ") ON CONFLICT DO NOTHING"
}

func (SQLiteDialect) BoolValue(v bool) any {
	if v {
		return 1
	}
	return 0
}
func (SQLiteDialect) RunHistoryInsertReturningID() bool { return false }
func (SQLiteDialect) SupportsDeleteLimit() bool         { return true }

type MySQLDialect struct{ SQLiteDialect }

func (MySQLDialect) PipelineUpsert() string {
	return `INSERT INTO pipelines (id, name, spec_yaml, status, desired_state, observed_state, generation, restore_error, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE name=VALUES(name), spec_yaml=VALUES(spec_yaml), status=VALUES(status), restore_error=VALUES(restore_error), updated_at=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) CheckpointUpsert() string {
	return `INSERT INTO checkpoints (job_name, source, position, generation, timestamp, updated_at)
		 VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE source=VALUES(source), position=VALUES(position), generation=VALUES(generation), timestamp=VALUES(timestamp), updated_at=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) FencedCheckpointUpsert() string {
	return `INSERT INTO checkpoints (job_name, source, position, generation, timestamp, updated_at)
		 SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3) FROM pipelines WHERE id=? AND generation=?
		 ON DUPLICATE KEY UPDATE source=VALUES(source), position=VALUES(position), generation=VALUES(generation), timestamp=VALUES(timestamp), updated_at=CURRENT_TIMESTAMP(3)`
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

// SchemaContractUpsert mirrors the SQLite statement with MySQL upsert syntax.
func (MySQLDialect) SchemaContractUpsert() string {
	return `INSERT INTO pipeline_schema_contracts (pipeline, version, fingerprint, columns_json, source_type, database, table_name, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE fingerprint=VALUES(fingerprint), columns_json=VALUES(columns_json), source_type=VALUES(source_type), database=VALUES(database), table_name=VALUES(table_name), created_at=CURRENT_TIMESTAMP(3)`
}

func (MySQLDialect) SettingUpsert() string {
	return `INSERT INTO settings (` + "`key`" + `, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP(3))
		 ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=CURRENT_TIMESTAMP(3)`
}
func (MySQLDialect) SettingKeyColumn() string { return "`key`" }

// InsertIgnore mirrors the SQLite statement with MySQL syntax. ON DUPLICATE KEY
// UPDATE is used instead of INSERT IGNORE because INSERT IGNORE downgrades every
// error (including truncation and NOT NULL violations) to a warning; restore
// must only tolerate duplicate-key conflicts.
func (MySQLDialect) InsertIgnore(table, columns, placeholders string) string {
	return "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ") ON DUPLICATE KEY UPDATE id=id"
}

// DeadLetterUpsert mirrors the SQLite statement with MySQL upsert syntax.
func (MySQLDialect) DeadLetterUpsert() string {
	return `INSERT INTO dead_letters (job_name, record_json, error, error_class, identity_context_json, attempt, record_hash, pipeline_version, dag_node, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE
		   record_json = VALUES(record_json),
		   error = VALUES(error),
		   error_class = VALUES(error_class),
		   identity_context_json = VALUES(identity_context_json),
		   attempt = attempt + 1,
		   pipeline_version = VALUES(pipeline_version),
		   dag_node = VALUES(dag_node),
		   created_at = VALUES(created_at)`
}

type PostgresDialect struct{ SQLiteDialect }

// SchemaContractUpsert reuses the SQLite (ON CONFLICT) statement verbatim —
// PostgreSQL supports the same syntax.

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
func (PostgresDialect) SupportsDeleteLimit() bool         { return false }
func (PostgresDialect) PipelineUpsert() string            { return SQLiteDialect{}.PipelineUpsert() }
func (PostgresDialect) CheckpointUpsert() string          { return SQLiteDialect{}.CheckpointUpsert() }
func (PostgresDialect) FencedCheckpointUpsert() string {
	return SQLiteDialect{}.FencedCheckpointUpsert()
}
func (PostgresDialect) WorkerUpsert() string     { return SQLiteDialect{}.WorkerUpsert() }
func (PostgresDialect) PluginUpsert() string     { return SQLiteDialect{}.PluginUpsert() }
func (PostgresDialect) ConnectionUpsert() string { return SQLiteDialect{}.ConnectionUpsert() }
func (PostgresDialect) SettingUpsert() string    { return SQLiteDialect{}.SettingUpsert() }

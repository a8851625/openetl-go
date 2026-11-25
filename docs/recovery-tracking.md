# Durable recovery records

SQL-backed DLQ capture records one server UUID for an explicit runtime scope:
pipeline UUID, endpoint, DAG node when known, and saved connection when known.
An open scope reuses its UUID across records and retries; a later capture after
resolution or abandonment starts a new UUID. Error text and timestamps do not
merge scopes. This is a recovery record, not an inferred root-cause incident.

Linear source, sink and identity failures identify the corresponding endpoint.
A single-transform pipeline can identify its saved connection; a failure in a
multi-transform chain leaves that connection unknown. DAG capture identifies
the actual failed node. Shards retain their pipeline UUID and only namespace
their checkpoints. Runtime spec version is currently unknown (`0`), so the UI
does not present a version as authoritative.

Use `GET /api/v2/recovery-events?pipeline_id=<uuid>&state=open&limit=50&offset=0`
and `GET /api/v2/recovery-events/<uuid>` to inspect persisted facts and operations.
The detail remains available after restart, DLQ removal or pipeline deletion.
The list includes `unassociated_operations` for tracked rows whose capture
metadata could not be saved. Old DLQ rows retain unknown association; startup
does not backfill identities from similar errors or close unfinished operations.

| State | Recorded meaning |
| --- | --- |
| `open` | Associated DLQ rows remain. |
| `replaying` | A replay operation began; its outcome is not fully recorded. This does not prove a process is still running. |
| `resolved` | All counted associated rows were replayed and their removal was recorded. Check pipeline health and business reconciliation separately. |
| `abandoned` | At least one counted row was deliberately discarded, and no associated DLQ rows remain. Mixed replay/discard is never resolved. |
| `unknown` | Tracking or counts cannot establish a complete closing result. |

Operations retain `started`, `acked`, `completed`, `failed`, `not_attempted`,
`preview_passed`, `preview_blocked` or `abandoned`. A `started` operation with no
result is unknown, including after a crash. A dry run only evaluates the frozen
identity gate: it records the preview but writes no sink data and changes no DLQ
delivery state. It does not certify transform output or sink availability.

DLQ records include `recovery_event_id`, `recovery_operation_id`,
`recovery_context`, `recovery_tracking_error` and `recovery_association_known`.
Use the decimal string `row_id` for exact row navigation in JavaScript. Numeric
`id` remains for compatibility; operation history also exposes `dlq_row_key`.

The list, replay and purge endpoints accept exact `id`, `recovery_event_id`,
`node_id` and `connection_name` query constraints. They are combined, never
treated as alternatives. Empty, invalid or repeated identity constraints are
rejected before any mutation. Purge ignores listing pagination and deletes the
whole matching scope; retention remains explicitly batch bounded.

The existing UI routes carry `pipeline_id`, `recovery_id`, `row_id`, `node_id`,
`connection_name` and `state`. IDs and filters are validated against a fixed
allowlist. A DLQ identity link requires its pipeline UUID. Missing or mismatched
objects are explicit and do not select another pipeline or recovery record.
Sensitive/unsupported parameters are removed from the URL and never echoed.

## Failure and delivery boundaries

Capture bookkeeping has a one-second transaction budget. If it fails, the
runtime still attempts the durable DLQ write and exposes
`capture_metadata_unavailable`. Only successful DLQ durability permits the
source checkpoint to advance; a DLQ write failure still blocks it. No UUID from
a rolled-back capture is exposed as committed.

Replay persists the `sink_acked` DLQ checkpoint before removal. Completion
history, counters and DLQ deletion share one SQL transaction. If result storage
fails, the API returns `503 / recovery_metadata_unavailable` and retains the
row. Retry after restart finalizes an acknowledged row without another sink
write. A failure before the acknowledgement checkpoint remains the documented
at-least-once duplicate boundary. Recovery bookkeeping never advances source
checkpoints or provides cross-sink exactly-once delivery.

Manual deletion, retention expiry, capacity eviction and atomic pipeline
deletion retain abandonment outcomes. A persistence error rolls back deletion;
it cannot be hidden as a successful purge. Metadata-fallback rows retain
unassociated outcomes rather than invented event associations.

## Migration, backup and rollback

SQLite, MySQL and PostgreSQL migrations 26–32 add nullable DLQ association
fields and recovery tables/indexes. Legacy rows remain readable and unassociated.
MySQL recovery times preserve milliseconds; PostgreSQL stores timezone-aware
timestamps. Existing IDs, counters and timestamps survive v3 streaming backup
and failure-atomic restore, including results for deleted pipelines.

Portable backup now writes format v3 and still reads v1/v2. v2 and v3 retain the
WASM-artifact requirement. Recovery metadata is included in portable JSON and
legacy JSONL exports, object counts and reconciliation. Perform restore offline.
Keep the new tables/columns when reverting application code; old binaries may
omit new metadata from exports, so use a v3-capable backup before rollback.

Recovery history is retained independently of the DLQ TTL/capacity policy.
Protect the API and backup as operational production data. The raw-DB
`storage.ApplyRetention` helper refuses a nonzero DLQ policy before mutation;
use `Storage.DeleteDeadLettersByFilter` with `DeletionReason=retention`.

This technical contract does not replace the UI-E external user-task research.

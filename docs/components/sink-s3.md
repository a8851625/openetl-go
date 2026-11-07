# sink/s3

## Purpose
Write batches to S3-compatible object storage such as MinIO.

## Config Fields
- `endpoint`, `bucket`: required target endpoint and bucket.
- `region`, `access_key`, `secret_key`: connection fields; keys are secrets.
- `format`, `prefix`, `output_dir`, retry fields: output controls.

## Record Shape
Writes record batches as JSON/JSONL/CSV/Parquet objects.

## Checkpoint, DLQ, Idempotency
Replay protection has two layers:

1. deterministic content-addressed object keys, so replaying an identical batch overwrites the same object instead of adding a duplicate;
2. optional two-step commit (`manifest: true`): the data object is written first, then a manifest object lists it (key, sha256, bytes, rows, batch sequence). The manifest name is derived from its content, so a retried batch rewrites the same commit marker.

With manifests enabled the manifest key is also recorded in the checkpoint as `sink_commit.native.manifest_key`, so the persisted boundary and the readable boundary are the same object. Manifests are written as `<prefix><manifest_prefix><date>/<hash>.manifest.json`, so the `.manifest.json` suffix keeps them clear of `*.json` data objects. Readers list the manifest prefix and treat the union of referenced keys as committed data; an object no manifest references is a crash-window leftover and safe to delete.

**Cross-day replay**: a batch is identified by its *content* (`batch_id` = payload digests + row counts + format, deliberately excluding the date-partitioned object key). Replaying an already-committed batch on a later day still writes that day's data object, but publishes **no second manifest**, so the duplicate stays uncommitted — invisible to readers and safe to delete. The same `batch_id` is what a compaction manifest carries in `source_batch_ids`, so a compacted batch is still recognised after its objects were folded.

**Small-file compaction**: `POST /api/v2/pipelines/{id}/output/compact` merges the committed objects of one day into a single object and publishes a compaction manifest with `kind: compaction` and `replaces: [keys]`. `CommittedObjectKeys` resolves the manifest history chronologically, so the retired keys leave the committed view while the files stay on disk (reversible, and a re-run is idempotent). Compaction is what makes the layout usable for Hive/Spark readers after a long CDC run.

Boundary (documented, not claimed away): a manifest is still not a lakehouse transaction — it does not rewrite Hive/Delta/Iceberg metadata, does not delete the replaced objects, and a compaction pass interrupted midway is simply re-run rather than resumed.

## Fits
File/object landing and replay-tolerant exports.

## Does Not Fit
Strict exactly-once lakehouse commits (table-format transactions, compaction, snapshot expiry).

## Example
```yaml
sink:
  type: s3
  config:
    endpoint: http://minio:9000
    bucket: openetl
    prefix: orders/
    format: jsonl
    manifest: true
```

## Evidence
Covered by `hack/e2e-s3-minio.sh` and S3 sink tests. The e2e covers deterministic checkpoint reset replay, MinIO outage -> transient DLQ, and replay after target recovery. The manifest commit contract (data object + manifest object, content-addressed idempotency, crash-window orphan detection, checkpoint `manifest_key`) is covered by `internal/etl/sink/manifest_test.go` and by `hack/e2e-data-integrity.sh`. Preflight blocks missing `endpoint`/`bucket` and opens the S3-compatible target to check bucket reachability; use `file_sink` for local file output.

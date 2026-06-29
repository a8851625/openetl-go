# sink/s3

## Purpose
Write batches to S3-compatible object storage such as MinIO.

## Config Fields
- `bucket`: required target bucket.
- `endpoint`, `region`, `access_key`, `secret_key`: connection fields; keys are secrets.
- `format`, `prefix`, `output_dir`, retry fields: output controls.

## Record Shape
Writes record batches as JSON/JSONL/CSV/Parquet objects.

## Checkpoint, DLQ, Idempotency
Current replay protection uses deterministic content-addressed object keys for identical batches. First-class manifest exactly-once is not implemented.

## Fits
File/object landing and replay-tolerant exports.

## Does Not Fit
Strict exactly-once lakehouse commits.

## Example
```yaml
sink:
  type: s3
  config:
    endpoint: http://minio:9000
    bucket: openetl
    prefix: orders/
    format: jsonl
```

## Evidence
Covered by `hack/e2e-s3-minio.sh` and S3 sink tests.

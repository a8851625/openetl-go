# source/file

## Purpose
Read local CSV or JSON/JSONL files for simple batch imports and quickstarts.

## Config Fields
- `path`: required file path.
- `format`: `csv` or `json`.
- `delimiter`, `has_header`: CSV parsing controls.

## Record Shape
Each row or JSON object becomes one record with parsed fields in `data`.

## Checkpoint, DLQ, Idempotency
Checkpoint tracks file read progress. Re-running file pipelines can duplicate append sinks unless output keys or sink upserts absorb replay.

## Fits
Small file imports, local demos, file -> S3/file landing flows.

## Does Not Fit
Large distributed file ingestion with exactly-once manifests.

## Example
```yaml
source:
  type: file
  config:
    path: /app/data/orders.jsonl
    format: json
```

## Evidence
Covered by `hack/e2e.sh` and `internal/etl/source/file_test.go`.

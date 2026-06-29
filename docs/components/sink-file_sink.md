# sink/file_sink

## Purpose
Write records to local files for demos, exports, and simple landing jobs.

## Config Fields
- `output_dir` or `path`: output location.
- `format`: `json`, `jsonl`, `csv`, or `parquet`.
- `prefix`: output filename prefix.

## Record Shape
Serializes record `data` or records as configured by the sink format.

## Checkpoint, DLQ, Idempotency
Local file output is append/object oriented. Use deterministic output keys where available; do not assume generic exactly-once file output.

## Fits
Quickstarts, local smoke tests, file landing.

## Does Not Fit
CDC append sinks without duplicate absorption.

## Example
```yaml
sink:
  type: file_sink
  config:
    output_dir: /app/data/output
    format: jsonl
```

## Evidence
Covered by `hack/e2e.sh` and file sink tests.

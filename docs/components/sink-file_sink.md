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

## Commit Manifests
`manifest: true` enables a two-step commit: the data file is written (atomic temp+rename) and then a manifest file under `manifest_prefix` (default `_manifests/`) lists it with its sha256, bytes, row count and batch sequence. The manifest key is recorded in the checkpoint, and `sink.ReadLocalManifests` / `sink.CommittedObjectKeys` resolve the committed view without reimplementing the layout. Without it, a crash between the file write and the checkpoint save leaves a file no reader can classify.

Two guarantees come with the manifest: a batch is identified by content (`batch_id`), so replaying an already-committed batch — including on a later day, whose path differs — never publishes a second commit entry; and compaction (`POST /api/v2/pipelines/{id}/output/compact`) can fold a day of small objects into one, retiring the old keys through `replaces` without deleting them.

## Example
```yaml
sink:
  type: file_sink
  config:
    output_dir: /app/data/output
    format: jsonl
    manifest: true
```

## Evidence
Covered by `hack/e2e.sh`, `internal/etl/sink/manifest_test.go` (commit, idempotency, crash-window orphan, prefix override, checkpoint metadata), `internal/etl/sink/manifest_compact_test.go` (compaction, dry run, cross-day replay skip) and `hack/e2e-data-integrity.sh`.

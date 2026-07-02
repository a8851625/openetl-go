# source/feishu_sheet

## Purpose
Pull rows from a Feishu/Lark spreadsheet using the tenant_access_token client-credentials OAuth2 flow. A batch-style pull source: each Open() fetches the configured range once.

## Config Fields
- `app_id`, `app_secret`: Feishu app credentials (required).
- `spreadsheet_token`: target spreadsheet token from the sheet URL (required).
- `sheet_range`: A1-style range like `Sheet1!A1:Z1000` (or use `sheet_id`).
- `sheet_id`: alternative explicit sheet id.
- `base_url`: API base, default `https://open.feishu.cn` (override for Lark).
- `poll_interval_sec`: reserved; use `schedule.type: periodic` for refresh.

## Record Shape
First row is treated as a header (when all cells are strings); subsequent rows become records keyed by the header. Otherwise keys are `col_0`, `col_1`, etc.

## Checkpoint, DLQ, Idempotency
Batch source; no checkpoint offset is persisted (each run re-reads the whole range). For idempotent writes pair with `batch_mode: upsert` + `pk_columns` or `pre_write`.

## Token Refresh
Token is fetched once and cached until 60s before expiry; subsequent Open() calls reuse it. Fetch failure surfaces as an Open() error (no silent degradation).

## Fits
- Periodic ETL pulls from a shared Feishu sheet.
- Small lookups / dimension refresh from a sheet.

## Does Not Fit
- Real-time streaming (use a proper CDC source).
- Large multi-sheet joins (materialize to a DB first).

## Example
```yaml
source:
  type: feishu_sheet
  config:
    app_id: "${FEISHU_APP_ID}"
    app_secret: "${FEISHU_APP_SECRET}"
    spreadsheet_token: "sheetABC123"
    sheet_range: "data!A1:E1000"
schedule:
  type: periodic
  interval_sec: 300
```

## Maturity
beta — token refresh, sheet fetch, and header inference are unit-tested via httptest mocks; real Feishu environment e2e and 429/rate-limit handling still need production evidence before promoting to production.

## Evidence
Covered by `internal/etl/source/feishu_sheet_test.go` (config validation, token fetch + cache, sheet fetch with header detection, token fetch failure).

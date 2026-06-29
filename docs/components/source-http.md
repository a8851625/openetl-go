# source/http

## Purpose
Poll HTTP APIs and convert JSON responses into records.

## Config Fields
- `url`: required endpoint.
- `method`, `headers`, `body`: request shape.
- `pagination`, `page_param`, `size_param`, `page_size`, `max_pages`, `result_key`: pagination and extraction.
- `auth_type`, `auth_token`, `auth_user`, `auth_pass`: auth fields; token/password are secrets.

## Record Shape
Each JSON object from the response array becomes one record.

## Checkpoint, DLQ, Idempotency
Checkpoint tracks pages. Periodic reruns should use idempotent sinks or deterministic output keys.

## Fits
Small API imports and scheduled HTTP landing jobs.

## Does Not Fit
Long-lived streaming APIs or arbitrary webhook servers.

## Example
```yaml
source:
  type: http
  config:
    url: https://api.example.com/orders
    pagination: page
    page_param: page
    size_param: size
```

## Evidence
Covered by `hack/e2e-http-source.sh` and `internal/etl/source/http_test.go`.

# Feishu Sheet Source Plugin Sample

Official third-party **source plugin** sample for Plugin ABI v1.

This proves the full extension path:

```text
web/plugin-sdk TypeScript
  -> offline esbuild bundle + extism-js compile
  -> POST /api/v2/plugins/install (wasm + manifest)
  -> pipeline type: plugin_feishu-sheet-source
  -> project / type_convert
  -> file_sink (or mysql / clickhouse)
```

It does **not** replace the built-in `feishu_sheet` beta source. Use the built-in connector for production Feishu pulls today. Use this sample to validate plugin install, manifest validation, registry naming, and source entrypoint contracts.

## Maturity

`beta` / `dev-only` until:

- a real Feishu environment run exists
- token failure / empty sheet / header errors have runtime evidence
- DLQ / restart boundaries are documented for the chosen sink

Do not mark this plugin production-certified without those gates.

## Config

| Field | Required | Secret | Notes |
| --- | --- | --- | --- |
| `app_id` | yes | no | Feishu/Lark app id |
| `app_secret` | yes | yes | Feishu/Lark app secret |
| `spreadsheet_token` | yes | no | From sheet URL |
| `sheet_range` | one of range/id | no | e.g. `Sheet1!A1:Z1000` |
| `sheet_id` | one of range/id | no | Alternative to range |
| `base_url` | no | no | Default `https://open.feishu.cn` |
| `fixture_rows` | no | no | Mock header+rows for cert/tests |
| `sheet_rows` | no | no | Host-injected live rows |

Host ABI v1 does not expose arbitrary HTTP from WASM. Live sheet fetch is expected via host-injected `sheet_rows` or the built-in source. Fixture mode covers install/registry and record emission.

## Compile (offline)

```sh
cd web/plugin-sdk
mkdir -p dist/examples
esbuild examples/feishu-sheet-source/feishu-sheet-source.ts \
  --bundle --platform=neutral --format=cjs --target=es2020 \
  --outfile=dist/examples/feishu-sheet-source.js
extism-js dist/examples/feishu-sheet-source.js \
  -i plugin-source.d.ts \
  -o dist/examples/feishu-sheet-source.wasm
```

Use the pinned compiler image from `hack/wasm-compiler.Dockerfile` when these
tools are not installed locally. The compiler does not support an
`extism-js compile` subcommand.

Server-side `/api/v2/plugins/compile` is transform-only. Source plugins must be compiled offline.

## Install

```sh
curl -F wasm=@web/plugin-sdk/dist/examples/feishu-sheet-source.wasm \
  -F name=feishu-sheet-source \
  -F kind=source \
  -F version=0.1.0 \
  -F manifest=@web/plugin-sdk/examples/feishu-sheet-source/manifest.json \
  http://localhost:8000/api/v2/plugins/install
```

After install, registry exposes `plugin_feishu-sheet-source`.

## Example pipeline

See `pipeline.example.yaml`:

```yaml
name: feishu-plugin-to-file
source:
  type: plugin_feishu-sheet-source
  config:
    app_id: ${FEISHU_APP_ID}
    app_secret: ${FEISHU_APP_SECRET}
    spreadsheet_token: ${FEISHU_SHEET_TOKEN}
    sheet_range: "Sheet1!A1:D100"
transforms:
  - type: project
    config:
      fields: [id, name, amount]
  - type: type_convert
    config:
      conversions:
        amount: float
sink:
  type: file_sink
  config:
    output_dir: /tmp/feishu-plugin-out
    format: jsonl
```

## Fixture coverage

`fixture.test.ts` documents expected behaviors:

- token/config missing -> error
- empty sheet -> EOF / empty output
- bad header -> error
- happy path rows -> one record per data row

Certification kit checks for this sample live in:

```sh
go test ./internal/etl/server -run 'TestFeishuSheetSourcePluginSampleCertification|TestPluginABIV1CertificationDocs' -count=1
```

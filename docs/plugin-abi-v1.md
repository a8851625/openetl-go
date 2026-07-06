# Plugin ABI v1

Plugin ABI v1 is the stable contract for third-party WASM plugins installed through `/api/v2/plugins/install`.

The plugin system is production-ready as an extension boundary, but an individual plugin is not production-certified until it has its own manifest, tests, docs, and runtime evidence.

## Manifest

Install requests may include a multipart `manifest` field containing JSON:

```json
{
  "name": "vip-order-enricher",
  "kind": "transform",
  "version": "1.0.0",
  "abi": "openetl.plugin.abi/v1",
  "min_runtime_version": "openetl-runtime/v1",
  "entrypoints": ["transform"],
  "capabilities": ["dimension_enrichment"],
  "config": [
    { "name": "vip_threshold", "type": "float", "required": false, "default": 10000 },
    { "name": "api_token", "type": "string", "secret": true }
  ]
}
```

The server validates explicit manifests before writing the WASM file. Legacy uploads without `manifest` are still accepted for compatibility, but they are reported as `manifest_validated=false` and should not be treated as certified plugins.

## Compatibility Matrix

| Runtime support | Accepted ABI | Minimum runtime | Notes |
| --- | --- | --- | --- |
| OpenETL-Go Plugin ABI v1 | `openetl.plugin.abi/v1` | `openetl-runtime/v1` | Current stable contract |
| Future ABI values | rejected | rejected | Upload a plugin compiled for a supported ABI |

Required entrypoints:

| Kind | Entrypoint |
| --- | --- |
| `transform` | `transform` |
| `source` | `read` |
| `sink` | `write` |

Config field types are `string`, `int`, `bool`, `float`, `string_array`, and `map`.

## Runtime Contract

- Transform input is one JSON-encoded `core.Record`.
- Transform output may be empty, `null`, or `false` to drop the input.
- Transform output may be one record/data object or an array of record/data objects.
- Source plugins emit one JSON record per `read` call and return empty output at EOF.
- Sink plugins receive a JSON array of records in `write`.
- Plugin config is read through the host config bridge and is also captured in the manifest schema for UI/preflight/certification.

## Build And Install

Recommended production flow:

```sh
extism-js compile src/transform.ts -o dist/transform.wasm
curl -F wasm=@dist/transform.wasm \
  -F name=vip-order-enricher \
  -F kind=transform \
  -F version=1.0.0 \
  -F manifest=<manifest.json \
  http://localhost:8000/api/v2/plugins/install
```

`/api/v2/plugins/compile` is limited to transform plugins. It uses a pre-installed `extism-js` compiler when available. Runtime `npx` fallback is disabled by default and should only be enabled in trusted development environments.

Source and sink plugins must be compiled offline and installed through `/api/v2/plugins/install`.

## Deprecation Policy

- ABI v1 fields documented here are additive-stable.
- New optional manifest fields may be added without changing the ABI string.
- Removing or changing required entrypoints, transform output semantics, or supported field types requires a new ABI string.
- Unsupported ABI or `min_runtime_version` values are rejected at install time.

## Certification Gate

A plugin can be considered production-certified only when it has:

- explicit validated ABI v1 manifest
- typed config schema with secret markers
- component docs with operational limits and examples
- unit tests for manifest and transform/source/sink behavior
- repeatable e2e or certification evidence for failure, restart, DLQ/replay, and idempotency boundaries that apply to the plugin kind

# Replay Matrix Transform

Real TypeScript-to-WASM certification fixture for Plugin ABI v1. It is intentionally `dev-only` and validates the runtime boundary rather than providing a production business transform.

The v1 plugin supports:

- `mode=drop`: zero outputs
- `mode=single`: one output
- `mode=split`: two outputs
- `mode=fail`: thrown WASM error, routed to DLQ

The v2 plugin upgrades the same installed name and accepts the previous failing record so DLQ replay can be verified. Both manifests include a typed `label` and secret `api_token`; the output proves configuration is available without emitting the secret value.

Compilation is offline from the OpenETL-Go request path:

```sh
podman build -t openetl-extism-js:1.6.0 -f hack/wasm-compiler.Dockerfile .
podman run --rm -v "$PWD:/workspace" -w /workspace/web/plugin-sdk \
  openetl-extism-js:1.6.0 sh -c '
    esbuild examples/replay-matrix-transform/replay-matrix-v1.ts \
      --bundle --platform=neutral --format=cjs --target=es2020 \
      --outfile=/tmp/replay-matrix-v1.js
    extism-js /tmp/replay-matrix-v1.js \
      -i examples/replay-matrix-transform/plugin.d.ts \
      -o /tmp/replay-matrix-v1.wasm
  '
```

The compiler image pins esbuild 0.25.6, Extism JS PDK v1.6.0, and Binaryen 130, and verifies both architecture-specific official release checksums. Full install, execution, DLQ/replay, upgrade, and restart coverage is in `hack/e2e-wasm-plugin.sh`.

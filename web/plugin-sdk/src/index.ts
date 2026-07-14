/**
 * @etl/sdk - Plugin SDK for ETL Platform
 *
 * Develop custom ETL plugins in TypeScript that compile to WebAssembly (WASM)
 * via the Extism JS PDK. The resulting .wasm file can be uploaded to the ETL
 * Platform UI or installed via the API.
 *
 * Quick start:
 *   npm install @etl/sdk
 *   esbuild src/transform.ts --bundle --platform=neutral --format=cjs --target=es2020 --outfile=dist/transform.js
 *   extism-js dist/transform.js -i plugin-transform.d.ts -o dist/transform.wasm
 *
 * Two modes:
 *   1. EXTISM MODE (for WASM compilation):
 *      Use `createExtismTransformPlugin()` which bridges to the Extism JS PDK
 *      and provides access to host functions (logging, config, KV store).
 *   2. TEST MODE (for unit testing):
 *      Use `createTransformPlugin()` which returns a plain function that accepts
 *      and returns Uint8Array, suitable for testing without WASM.
 */

declare const Host: {
  inputString(): string;
  outputString(value: string): void;
};
declare const Config: { get(key: string): string | null };
declare const Var: {
  getString(key: string): string | null;
  set(key: string, value: string): void;
};

// ── Core Types ────────────────────────────────────────────────────────

export const OPENETL_PLUGIN_ABI = 'openetl.plugin.abi/v1';
export const OPENETL_MIN_RUNTIME_VERSION = 'openetl-runtime/v1';

export type PluginKind = 'transform' | 'source' | 'sink';
export type ManifestFieldType = 'string' | 'int' | 'bool' | 'float' | 'string_array' | 'map';

export interface ManifestField {
  name: string;
  type: ManifestFieldType;
  required?: boolean;
  default?: any;
  description?: string;
  secret?: boolean;
  enum?: string[];
}

export interface PluginManifest {
  name: string;
  kind: PluginKind;
  version: string;
  abi: typeof OPENETL_PLUGIN_ABI;
  min_runtime_version: typeof OPENETL_MIN_RUNTIME_VERSION;
  entrypoints: string[];
  capabilities?: string[];
  config?: ManifestField[];
}

export interface Record {
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  data: { [key: string]: any };
  before?: { [key: string]: any };
  metadata: {
    source: string;
    table?: string;
    timestamp: string;
    binlog_file?: string;
    binlog_pos?: number;
    gtid?: string;
  };
}

export interface Context {
  log(message: string): void;
  config: { [key: string]: any };
  state: {
    get(key: string): string | null;
    set(key: string, value: string): void;
  };
  metrics: {
    increment(name: string, value?: number): void;
    gauge(name: string, value: number): void;
  };
}

// ── Plugin Interfaces ─────────────────────────────────────────────────

export interface TransformPlugin {
  name: string;
  version: string;
  apply(record: Record, ctx: Context): Record | Record[] | { [key: string]: any } | Array<{ [key: string]: any }> | null | false | undefined;
}

export interface SourcePlugin {
  name: string;
  version: string;
  open(ctx: Context): void;
  read(ctx: Context): Record | null;
  close(ctx: Context): void;
}

export interface SinkPlugin {
  name: string;
  version: string;
  open(ctx: Context): void;
  write(records: Record[], ctx: Context): void;
  close(ctx: Context): void;
}

export function definePluginManifest(input: Omit<PluginManifest, 'abi' | 'min_runtime_version' | 'entrypoints'> & { entrypoints?: string[] }): PluginManifest {
  const entrypointByKind: { [K in PluginKind]: string } = {
    transform: 'transform',
    source: 'read',
    sink: 'write',
  };
  return {
    ...input,
    abi: OPENETL_PLUGIN_ABI,
    min_runtime_version: OPENETL_MIN_RUNTIME_VERSION,
    entrypoints: input.entrypoints ?? [entrypointByKind[input.kind]],
  };
}

// ── Extism PDK Bridge ─────────────────────────────────────────────────
//
// When compiled with extism-js (via Javy/QuickJS), these functions use
// the Extism JS PDK to communicate with the Go host via stdin/stdout.
// The host function bridge provides access to logging, config, KV store,
// and metrics via the 6 host functions exposed by the Go server.

function _decodeConfig(raw: string | null): any {
  if (raw === null || raw === '') return undefined;
  try { return JSON.parse(raw); } catch { return raw; }
}

function _bridgeContext(): Context {
  const config = new Proxy({} as { [key: string]: any }, {
    get: (_target, key) => typeof key === 'string' ? _decodeConfig(Config.get(key)) : undefined,
  });
  return {
    log: (msg: string) => console.log(msg),
    config,
    state: {
      get: (key: string) => Var.getString(key),
      set: (key: string, value: string) => Var.set(key, value),
    },
    metrics: {
      increment: (_name: string, _value?: number) => {},
      gauge: (_name: string, _value: number) => {},
    },
  };
}

/**
 * Create a transform entry point for extism WASM compilation.
 * The returned function is exported as the plugin's main handler.
 *
 * In extism mode, the function reads input from `Host.inputString()`,
 * processes it through the plugin's `apply()`, and writes output via
 * `Host.outputString()`.
 *
 * @example
 * ```typescript
 * import { createExtismTransformPlugin } from '@etl/sdk';
 *
 * const plugin = createExtismTransformPlugin({
 *   name: 'add-timestamp',
 *   version: '1.0.0',
 *   apply(record, ctx) {
 *     ctx.log(`Processing record from ${record.metadata.source}`);
 *     record.data['processed_at'] = new Date().toISOString();
 *     return record;
 *   }
 * });
 *
 * export const transform = plugin;  // extism calls this
 * ```
 */
export function createExtismTransformPlugin(plugin: TransformPlugin): () => void {
  return () => {
    const inputStr = Host.inputString();
    const record = JSON.parse(inputStr) as Record;
    const ctx = _bridgeContext();
    const result = plugin.apply(record, ctx);
    if (result === null || result === undefined || result === false) {
      Host.outputString('');
      return;
    }
    Host.outputString(JSON.stringify(result));
  };
}

/**
 * Create a source plugin entry point for extism WASM compilation.
 * The exported function runs one read cycle per call.
 *
 * @example
 * ```typescript
 * import { createExtismSourcePlugin } from '@etl/sdk';
 *
 * const plugin = createExtismSourcePlugin({
 *   name: 'custom-api',
 *   version: '1.0.0',
 *   open(ctx) { },
 *   read(ctx) { return null; },
 *   close(ctx) { },
 * });
 *
 * export const read = plugin;
 * ```
 */
export function createExtismSourcePlugin(plugin: SourcePlugin): () => void {
  return () => {
    const ctx = _bridgeContext();
    const record = plugin.read(ctx);
    if (record === null) {
      Host.outputString('');
      return;
    }
    Host.outputString(JSON.stringify(record));
  };
}

/**
 * Create a sink plugin entry point for extism WASM compilation.
 * The exported function reads a JSON array of Records and writes them.
 *
 * @example
 * ```typescript
 * import { createExtismSinkPlugin } from '@etl/sdk';
 *
 * const plugin = createExtismSinkPlugin({
 *   name: 'custom-http',
 *   version: '1.0.0',
 *   open(ctx) { },
 *   write(records, ctx) {
 *     for (const rec of records) {
 *       ctx.log(`Writing ${rec.operation} record`);
 *     }
 *   },
 *   close(ctx) { },
 * });
 *
 * export const write = plugin;
 * ```
 */
export function createExtismSinkPlugin(plugin: SinkPlugin): () => void {
  return () => {
    const inputStr = Host.inputString();
    const records = JSON.parse(inputStr) as Record[];
    const ctx = _bridgeContext();
    plugin.write(records, ctx);
  };
}

// ── Test Mode Helpers ─────────────────────────────────────────────────
//
// These helpers create plain functions (not extism exports) that accept
// and return Uint8Array. Use them for unit testing your plugin logic
// without needing the extism WASM runtime.

/**
 * Creates a transform function suitable for test mode or direct WASM I/O.
 * - Input: JSON-encoded Record as Uint8Array
 * - Output: empty, one JSON-encoded Record/data object, or an array of
 *   Record/data objects as Uint8Array
 */
export function createTransformPlugin(plugin: TransformPlugin): (input: Uint8Array) => Uint8Array {
  const state = new Map<string, string>();
  return (input: Uint8Array): Uint8Array => {
    const jsonStr = new TextDecoder().decode(input);
    const record = JSON.parse(jsonStr) as Record;
    const ctx: Context = {
      log: (_msg: string) => {},
      config: {},
      state: {
        get: (key: string) => state.get(key) ?? null,
        set: (key: string, value: string) => { state.set(key, value); },
      },
      metrics: {
        increment: (_name: string, _value?: number) => {},
        gauge: (_name: string, _value: number) => {},
      },
    };
    const result = plugin.apply(record, ctx);
    if (result === null || result === undefined || result === false) {
      return new Uint8Array(0);
    }
    const output = JSON.stringify(result);
    return new TextEncoder().encode(output);
  };
}

// ── Utility Functions ─────────────────────────────────────────────────

export function now(): string {
  return new Date().toISOString();
}

export function parseJSON<T>(input: string): T {
  return JSON.parse(input) as T;
}

export function toJSON(obj: any): string {
  return JSON.stringify(obj);
}

// ── Re-exports ────────────────────────────────────────────────────────

export { Record as ETLRecord, Context as ETLContext };

/**
 * @etl/sdk - Plugin SDK for ETL Platform
 *
 * Develop custom ETL plugins in TypeScript that compile to WebAssembly (WASM)
 * via the Extism JS PDK. The resulting .wasm file can be uploaded to the ETL
 * Platform UI or installed via the API.
 *
 * Quick start:
 *   npm install @etl/sdk
 *   npx extism-js compile src/transform.ts -o dist/transform.wasm
 *
 * Two modes:
 *   1. EXTISM MODE (for WASM compilation):
 *      Use `createExtismTransformPlugin()` which bridges to the Extism JS PDK
 *      and provides access to host functions (logging, config, KV store).
 *   2. TEST MODE (for unit testing):
 *      Use `createTransformPlugin()` which returns a plain function that accepts
 *      and returns Uint8Array, suitable for testing without WASM.
 */

// ── Core Types ────────────────────────────────────────────────────────

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
  metrics: {
    increment(name: string, value?: number): void;
    gauge(name: string, value: number): void;
  };
}

// ── Plugin Interfaces ─────────────────────────────────────────────────

export interface TransformPlugin {
  name: string;
  version: string;
  apply(record: Record, ctx: Context): Record | null;
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

// ── Extism PDK Bridge ─────────────────────────────────────────────────
//
// When compiled with extism-js (via Javy/QuickJS), these functions use
// the Extism JS PDK to communicate with the Go host via stdin/stdout.
// The host function bridge provides access to logging, config, KV store,
// and metrics via the 6 host functions exposed by the Go server.

let _hasExtismPdk = false;
try {
  if (typeof require !== 'undefined') {
    require('@extism/js-pdk');
    _hasExtismPdk = true;
  }
} catch {
  _hasExtismPdk = false;
}

function _bridgeContext(config: Record<string, any>): Context {
  if (_hasExtismPdk) {
    // In extism mode, use Host for logging/config.
    const { Host } = require('@extism/js-pdk');
    return {
      log: (msg: string) => Host.log(msg),
      config: config,
      metrics: {
        increment: (_name: string, _value?: number) => {},
        gauge: (_name: string, _value: number) => {},
      },
    };
  }
  return {
    log: (_msg: string) => {},
    config: config,
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
    const { Host } = require('@extism/js-pdk');
    const inputStr = Host.inputString();
    const record = JSON.parse(inputStr) as Record;
    const config = JSON.parse(Host.config() || '{}');
    const ctx = _bridgeContext(config);
    const result = plugin.apply(record, ctx);
    if (result === null) {
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
 *   open(ctx) { /* init */ },
 *   read(ctx) { return null; /* return a Record or null */ },
 *   close(ctx) { /* cleanup */ },
 * });
 *
 * export const read = plugin;
 * ```
 */
export function createExtismSourcePlugin(plugin: SourcePlugin): () => void {
  return () => {
    const { Host } = require('@extism/js-pdk');
    const config = JSON.parse(Host.config() || '{}');
    const ctx = _bridgeContext(config);
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
 *   open(ctx) { /* init */ },
 *   write(records, ctx) {
 *     for (const rec of records) {
 *       ctx.log(`Writing ${rec.operation} record`);
 *     }
 *   },
 *   close(ctx) { /* cleanup */ },
 * });
 *
 * export const write = plugin;
 * ```
 */
export function createExtismSinkPlugin(plugin: SinkPlugin): () => void {
  return () => {
    const { Host } = require('@extism/js-pdk');
    const inputStr = Host.inputString();
    const records = JSON.parse(inputStr) as Record[];
    const config = JSON.parse(Host.config() || '{}');
    const ctx = _bridgeContext(config);
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
 * - Output: JSON-encoded Record as Uint8Array (empty = filtered)
 */
export function createTransformPlugin(plugin: TransformPlugin): (input: Uint8Array) => Uint8Array {
  return (input: Uint8Array): Uint8Array => {
    const jsonStr = new TextDecoder().decode(input);
    const record = JSON.parse(jsonStr) as Record;
    const ctx: Context = {
      log: (_msg: string) => {},
      config: {},
      metrics: {
        increment: (_name: string, _value?: number) => {},
        gauge: (_name: string, _value: number) => {},
      },
    };
    const result = plugin.apply(record, ctx);
    if (result === null) {
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

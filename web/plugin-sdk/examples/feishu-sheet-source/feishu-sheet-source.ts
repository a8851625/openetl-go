/**
 * Feishu / Lark Sheet Source Plugin (ABI v1 sample)
 * ─────────────────────────────────────────────────
 * Third-party source plugin sample that mirrors the built-in feishu_sheet
 * connector path while proving the full plugin install surface:
 *
 *   offline esbuild bundle + extism-js compile
 *     -> POST /api/v2/plugins/install (manifest + wasm)
 *     -> type: plugin_feishu-sheet-source in a normal pipeline
 *
 * This does NOT replace the built-in feishu_sheet source. Maturity is beta /
 * dev-only until a real Feishu environment and fault-injection evidence exist.
 *
 * Host ABI note: WASM plugins only receive config + host log/kv bridges.
 * This sample therefore supports two modes:
 *   1. fixture mode (default for unit/cert tests): config.fixture_rows
 *   2. live mode: when compiled against a host that injects pre-fetched
 *      sheet rows via config.sheet_rows (operator/sidecar pattern)
 *
 * Live Feishu flow (outside WASM host ABI v1 HTTP surface):
 *   POST {base_url}/open-apis/auth/v3/tenant_access_token/internal
 *   -> tenant_access_token
 *   -> GET spreadsheet values (value_range.values)
 * Prefer the built-in feishu_sheet source for production HTTP pulls.
 *
 * Compile: see the current esbuild + extism-js command in README.md.
 *
 * Install:
 *   curl -F wasm=@feishu-sheet-source.wasm \
 *     -F name=feishu-sheet-source \
 *     -F kind=source \
 *     -F version=0.1.0 \
 *     -F manifest=@manifest.json \
 *     http://localhost:8000/api/v2/plugins/install
 */

import {
  createExtismSourcePlugin,
  definePluginManifest,
  type Context,
  type Record,
} from '../../src/index';

type FeishuConfig = {
  app_id?: string;
  app_secret?: string;
  spreadsheet_token?: string;
  sheet_range?: string;
  sheet_id?: string;
  base_url?: string;
  /** Preloaded rows for fixture / host-injected live mode. First row is header. */
  fixture_rows?: any[][];
  sheet_rows?: any[][];
  /** Internal cursor persisted via host KV between read() calls. */
  _cursor?: number;
};

function requiredConfig(cfg: FeishuConfig): string | null {
  if (!cfg.app_id) return 'app_id is required';
  if (!cfg.app_secret) return 'app_secret is required';
  if (!cfg.spreadsheet_token) return 'spreadsheet_token is required';
  if (!cfg.sheet_range && !cfg.sheet_id) return 'sheet_range or sheet_id is required';
  return null;
}

function loadRows(cfg: FeishuConfig): any[][] {
  if (Array.isArray(cfg.sheet_rows) && cfg.sheet_rows.length > 0) {
    return cfg.sheet_rows;
  }
  if (Array.isArray(cfg.fixture_rows) && cfg.fixture_rows.length > 0) {
    return cfg.fixture_rows;
  }
  return [];
}

function headerFrom(rows: any[][]): string[] {
  if (!rows.length) {
    throw new Error('feishu sheet is empty');
  }
  const header = rows[0];
  if (!Array.isArray(header) || header.length === 0) {
    throw new Error('feishu sheet header row is missing');
  }
  if (!header.every((h) => typeof h === 'string' && h.trim() !== '')) {
    throw new Error('feishu sheet header must be non-empty strings');
  }
  return header.map((h) => String(h));
}

function rowToRecord(header: string[], row: any[], idx: number): Record {
  const data: { [key: string]: any } = {};
  for (let i = 0; i < header.length; i++) {
    data[header[i]] = row[i] ?? null;
  }
  return {
    operation: 'INSERT',
    data,
    metadata: {
      source: 'plugin_feishu-sheet-source',
      table: 'feishu_sheet',
      timestamp: new Date().toISOString(),
      binlog_pos: idx,
    },
  };
}

const plugin = createExtismSourcePlugin({
  name: 'feishu-sheet-source',
  version: '0.1.0',
  open(_ctx: Context) {},
  read(ctx: Context) {
    const cfg = ctx.config as FeishuConfig;
    const missing = requiredConfig(cfg);
    if (missing) {
      throw new Error(missing);
    }

    const rows = loadRows(cfg);
    if (rows.length === 0) {
      // Empty output signals EOF to the host source plugin reader.
      // Live HTTP fetch is intentionally out of WASM host ABI v1; inject
      // sheet_rows from a sidecar or use the built-in feishu_sheet source.
      ctx.log('feishu-sheet-source: no fixture_rows/sheet_rows; returning EOF');
      return null;
    }

    const header = headerFrom(rows);
    const cursorKey = 'feishu_sheet_cursor';
    let cursor = Number(ctx.state.get(cursorKey) ?? cfg._cursor ?? 0);
    if (!Number.isFinite(cursor) || cursor < 0) cursor = 0;

    // Skip header on first call.
    const dataIndex = cursor === 0 ? 1 : cursor;
    if (dataIndex >= rows.length) {
      return null;
    }

    const rec = rowToRecord(header, rows[dataIndex] || [], dataIndex);
    ctx.state.set(cursorKey, String(dataIndex + 1));
    cfg._cursor = dataIndex + 1;
    return rec;
  },
  close(_ctx: Context) {},
});

export const manifest = definePluginManifest({
  name: 'feishu-sheet-source',
  kind: 'source',
  version: '0.1.0',
  capabilities: ['batch', 'oauth2_client_credentials', 'plugin_sample'],
  config: [
    { name: 'app_id', type: 'string', required: true, description: 'Feishu/Lark app_id', secret: false },
    { name: 'app_secret', type: 'string', required: true, description: 'Feishu/Lark app_secret', secret: true },
    { name: 'spreadsheet_token', type: 'string', required: true, description: 'Spreadsheet token from sheet URL' },
    { name: 'sheet_range', type: 'string', required: false, description: "A1 range, e.g. 'Sheet1!A1:Z1000'" },
    { name: 'sheet_id', type: 'string', required: false, description: 'Alternative sheet id' },
    { name: 'base_url', type: 'string', required: false, default: 'https://open.feishu.cn', description: 'API base URL' },
    { name: 'fixture_rows', type: 'map', required: false, description: 'Test fixture rows (header first) for dry install paths' },
    { name: 'sheet_rows', type: 'map', required: false, description: 'Host-injected value_range.values for live runs' },
  ],
});

export const read = plugin;

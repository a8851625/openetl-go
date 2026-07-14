import {
  createExtismTransformPlugin,
  type Context,
  type Record as ETLRecord,
} from '../../src/index';

function output(record: ETLRecord, id: string, version: string, ctx: Context): ETLRecord {
  return {
    ...record,
    data: {
      ...record.data,
      id,
      plugin_label: String(ctx.config.label ?? 'missing'),
      plugin_version: version,
      secret_was_configured: Boolean(ctx.config.api_token),
    },
  };
}

const plugin = createExtismTransformPlugin({
  name: 'replay-matrix-transform',
  version: '1.0.0',
  apply(record: ETLRecord, ctx: Context) {
    const mode = String(record.data.mode ?? 'single');
    if (mode === 'drop') return null;
    if (mode === 'fail') throw new Error('injected wasm transform failure');
    if (mode === 'split') {
      return [
        output(record, `${record.data.id}-a`, '1.0.0', ctx),
        output(record, `${record.data.id}-b`, '1.0.0', ctx),
      ];
    }
    return output(record, String(record.data.id), '1.0.0', ctx);
  },
});

export const transform = plugin;

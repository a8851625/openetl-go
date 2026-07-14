import {
  createExtismTransformPlugin,
  type Context,
  type Record as ETLRecord,
} from '../../src/index';

function output(record: ETLRecord, id: string, ctx: Context): ETLRecord {
  return {
    ...record,
    data: {
      ...record.data,
      id,
      plugin_label: String(ctx.config.label ?? 'missing'),
      plugin_version: '1.1.0',
      secret_was_configured: Boolean(ctx.config.api_token),
      recovered: record.data.mode === 'fail',
    },
  };
}

const plugin = createExtismTransformPlugin({
  name: 'replay-matrix-transform',
  version: '1.1.0',
  apply(record: ETLRecord, ctx: Context) {
    const mode = String(record.data.mode ?? 'single');
    if (mode === 'drop') return null;
    if (mode === 'split') {
      return [
        output(record, `${record.data.id}-a`, ctx),
        output(record, `${record.data.id}-b`, ctx),
      ];
    }
    return output(record, String(record.data.id), ctx);
  },
});

export const transform = plugin;

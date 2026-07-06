/**
 * VIP Order Enricher Plugin
 * ──────────────────────────────────
 * A real-world TypeScript transform plugin that:
 * 1. Adds a processing timestamp
 * 2. Classifies orders into tiers (VIP/standard) by amount
 * 3. Masks sensitive fields (credit_card)
 * 4. Computes a risk score based on amount + customer tier
 *
 * Compile: extism-js compile examples/vip-order-enricher.ts -o vip-order-enricher.wasm
 * Install:  Upload the .wasm file via UI or:
 *           curl -F wasm=@vip-order-enricher.wasm -F name=vip-order-enricher -F kind=transform \
 *             http://localhost:8000/api/v2/plugins/install
 *
 * Use in YAML:
 *   transforms:
 *     - type: plugin_vip-order-enricher
 *       config:
 *         vip_threshold: 10000
 *         mask_fields: ["credit_card", "ssn"]
 */

import { createExtismTransformPlugin, definePluginManifest, type Record, type Context } from '@etl/sdk';

interface VIPConfig {
  vip_threshold?: number;
  mask_fields?: string[];
  risk_weights?: { high: number; medium: number; low: number };
}

const plugin = createExtismTransformPlugin({
  name: 'vip-order-enricher',
  version: '1.0.0',

  apply(record: Record, ctx: Context) {
    const cfg = ctx.config as VIPConfig;
    const threshold = cfg.vip_threshold ?? 10000;
    const data = record.data as Record<string, any>;

    // 1. Add processing metadata
    data.processed_at = new Date().toISOString();
    data.plugin_version = '1.0.0';

    // 2. Classify order tier
    const amount = Number(data.amount) || 0;
    if (amount >= threshold) {
      data.order_tier = 'vip';
    } else if (amount >= threshold * 0.5) {
      data.order_tier = 'premium';
    } else {
      data.order_tier = 'standard';
    }

    // 3. Mask sensitive fields
    const maskFields = cfg.mask_fields ?? ['credit_card', 'ssn', 'password'];
    for (const field of maskFields) {
      if (data[field]) {
        const val = String(data[field]);
        if (val.length > 4) {
          data[field] = '*'.repeat(val.length - 4) + val.slice(-4);
        } else {
          data[field] = '****';
        }
      }
    }

    // 4. Compute risk score (0-100)
    let risk = 0;
    const weights = cfg.risk_weights ?? { high: 40, medium: 20, low: 10 };

    if (data.order_tier === 'vip') risk += weights.high;
    else if (data.order_tier === 'premium') risk += weights.medium;
    else risk += weights.low;

    if (data.status === 'pending') risk += 30;
    if (data.status === 'test') risk += 50;

    // High-amount orders are riskier
    if (amount > 50000) risk += 20;
    else if (amount > 10000) risk += 10;

    data.risk_score = Math.min(100, risk);

    // 5. Log high-risk orders
    if (risk >= 70) {
      ctx.log(`⚠️ HIGH RISK order: ${data.order_id} (score=${risk})`);
    }

    return record;
  },
});

export const manifest = definePluginManifest({
  name: 'vip-order-enricher',
  kind: 'transform',
  version: '1.0.0',
  capabilities: ['dimension_enrichment', 'masking', 'risk_scoring'],
  config: [
    { name: 'vip_threshold', type: 'float', required: false, default: 10000 },
    { name: 'mask_fields', type: 'string_array', required: false, default: ['credit_card', 'ssn', 'password'] },
    { name: 'risk_weights', type: 'map', required: false },
  ],
});

export const transform = plugin;

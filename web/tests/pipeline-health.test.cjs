const test = require('node:test');
const assert = require('node:assert/strict');
const { loadSource } = require('./load-typescript.cjs');
const health = loadSource('src/lib/pipeline-health.ts');

function pipeline(overrides = {}) {
  return {
    id: 'pipe-a', name: 'orders', status: 'running',
    dlq_backlog: 0, dlq_backlog_known: true,
    stats: { records_read: 10, records_written: 10, records_failed: 0, records_dlq: 0 },
    ...overrides,
  };
}

function metric(overrides = {}) {
  return {
    id: 'pipe-a', name: 'orders', status: 'running', health: 'healthy',
    checkpoint_age_seconds: 0, cdc_lag_ms: 0,
    dlq_file_count: 0, circuit_breaker_state: 0,
    ...overrides,
  };
}

test('an open write circuit is actionable and recovers after it closes', () => {
  const p = pipeline();
  const open = metric({ circuit_breaker_state: 1, health: 'degraded' });
  assert.equal(health.derivePipelineHealth(p, open), 'degraded');
  const issues = health.deriveIssues([p], [open]);
  assert.ok(issues.some(issue => issue.id.endsWith(':breaker')));
  assert.equal(health.derivePipelineHealth(p, metric()), 'healthy');
  assert.equal(health.deriveIssues([p], [metric()]).length, 0);
});

test('failed restoration appears in issues and the failed health count', () => {
  const p = pipeline({ status: 'restore_failed' });
  assert.equal(health.derivePipelineHealth(p), 'failed');
  const issues = health.deriveIssues([p], []);
  assert.equal(issues.length, 1);
  assert.equal(issues[0].titleKey, 'issue.restoreFailed');
  assert.equal(health.countHealth([p]).failed, 1);
});

test('an unreadable DLQ backlog remains unknown and actionable', () => {
  const p = pipeline({ dlq_backlog_known: false });
  const m = metric({ dlq_file_count: -1, health: 'unknown' });
  assert.equal(health.derivePipelineHealth(p, m), 'unknown');
  const issues = health.deriveIssues([p], [m]);
  assert.equal(issues.length, 1);
  assert.equal(issues[0].action, 'dlq');
  assert.equal(health.issueSummary(issues[0], key =>
    key === 'issue.dlqUnknownSummary' ? '无法读取当前死信数量，请检查元数据存储。' : key),
  '无法读取当前死信数量，请检查元数据存储。');
});

test('confirmed degradation remains visible when another signal is unknown', () => {
  const p = pipeline({ dlq_backlog_known: false });
  const m = metric({ dlq_file_count: -1, circuit_breaker_state: 1, health: 'degraded' });
  assert.equal(health.derivePipelineHealth(p, m), 'degraded');
});

test('server health applies configured thresholds in both directions', () => {
  assert.equal(health.derivePipelineHealth(pipeline(), metric({
    health: 'healthy', checkpoint_age_seconds: 999, cdc_lag_ms: 90_000,
  })), 'healthy');
  assert.equal(health.derivePipelineHealth(pipeline(), metric({
    health: 'degraded', checkpoint_age_seconds: 45,
  })), 'degraded');
});

test('legacy metrics retain default thresholds and cumulative errors stay diagnostic', () => {
  const p = pipeline({ stats: { records_failed: 5, records_dlq: 5, last_error: 'old failure' } });
  assert.equal(health.derivePipelineHealth(p, metric({ health: undefined })), 'healthy');
  assert.equal(health.derivePipelineHealth(p, metric({
    health: undefined, checkpoint_age_seconds: 301,
  })), 'degraded');
});

test('issue classification uses the same threshold snapshot as server health', () => {
  const p = pipeline();
  const thresholds = { checkpoint_stale_seconds: 1000, cdc_lag_ms: 120_000 };
  const open = metric({
    health: 'degraded', circuit_breaker_state: 1, checkpoint_age_seconds: 999,
    cdc_lag_ms: 90_000, health_thresholds: thresholds,
  });
  const issues = health.deriveIssues([p], [open]);
  assert.deepEqual(issues.map(issue => issue.id), ['pipe-a:breaker']);
  const stale = metric({
    health: 'degraded', checkpoint_age_seconds: 45,
    health_thresholds: { checkpoint_stale_seconds: 30, cdc_lag_ms: 120_000 },
  });
  assert.ok(health.deriveIssues([p], [stale]).some(issue => issue.id.endsWith(':cp')));
});

test('missing or stale metrics cannot confirm healthy delivery', () => {
  assert.equal(health.derivePipelineHealth(pipeline()), 'unknown');
  assert.equal(health.derivePipelineHealth(pipeline(), metric({ status: 'paused' })), 'unknown');
});

test('stable IDs keep metrics separate for pipelines with the same display name', () => {
  const p = pipeline();
  const other = pipeline({ id: 'pipe-b' });
  const metrics = [
    metric({ id: 'pipe-b', health: 'healthy' }),
    metric({ health: 'degraded' }),
  ];
  assert.equal(health.findMetric(p, metrics).id, 'pipe-a');
  assert.equal(health.findMetric(p, [metrics[0]]), undefined);
  const counts = health.countHealth([p, other], metrics);
  assert.equal(counts.healthy, 1);
  assert.equal(counts.degraded, 1);
  assert.equal(health.findMetric(p, [metric({ id: undefined })]).name, 'orders');
});

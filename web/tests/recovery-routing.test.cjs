const test = require('node:test');
const assert = require('node:assert/strict');
const { loadSource } = require('./load-typescript.cjs');
const routing = loadSource('src/lib/routing.ts');
const recovery = loadSource('src/lib/recovery.ts');
const pipelineId = 'ebd3b045-0a15-4d71-b46e-e9e55dd7b0c9';
const recoveryId = 'fb60f523-4a64-49ed-817e-d931d46e3fdd';

test('task identity and filter context survives route serialization on existing pages', () => {
  const context = { pipelineId, recoveryId, rowId: '9007199254740995', nodeId: 'write', connectionName: 'warehouse' };
  for (const page of ['issues', 'dlq', 'connections']) {
    const route = { page, context };
    assert.deepEqual(routing.parseHash(routing.routeToHash(route)), route);
  }
  const detail = { page: 'pipeline-detail', id: pipelineId, tab: 'issues', context };
  assert.deepEqual(routing.parseHash(routing.routeToHash(detail)), detail);
});

test('invalid, duplicate and sensitive identity parameters yield explicit safe state', () => {
  for (const hash of [
    '#/dlq?pipeline_id=display-name', '#/issues?recovery_id=missing',
    '#/dlq?row_id=-1', '#/dlq?row_id=9223372036854775808',
    '#/connections?connection_name=bad%2Fname',
    `#/dlq?pipeline_id=${pipelineId}&pipeline_id=${pipelineId}`,
    `#/issues?recovery_id=${recoveryId}&api_token=never-echo-this`,
    '#/dlq?password=secret', '#/dlq?dsn=secret', '#/dlq?error_contains=secret',
    '#/pipelines/%E0%A4%A/overview',
    `#/dlq?recovery_id=${recoveryId}`, '#/dlq?row_id=23',
    `#/dlq?pipeline_id=${pipelineId}?password=secret`,
  ]) {
    const route = routing.parseHash(hash);
    assert.equal(route.page, 'not-found');
    assert.equal(route.invalidContext, true);
    assert.ok(!JSON.stringify(route).includes('secret'));
    assert.ok(!JSON.stringify(route).includes('never-echo-this'));
  }
});

test('a task cannot silently select a different pipeline in a detail route', () => {
  assert.equal(routing.parseHash(`#/pipelines/${pipelineId}/issues?pipeline_id=${recoveryId}`).invalidContext, true);
});

test('legacy navigation stays usable without inventing recovery identity', () => {
  assert.deepEqual(routing.parseHash('#/dlq'), { page: 'dlq' });
  assert.deepEqual(routing.parseHash('#/pipelines/legacy-id/overview'), { page: 'pipeline-detail', id: 'legacy-id', tab: 'overview' });
  assert.deepEqual(routing.parseHash('#/pipelines/new?step=source'), { page: 'pipeline-new', step: 'source' });
});

test('row identity uses the exact server string and refuses rounded legacy numbers', () => {
  assert.equal(recovery.dlqRowId({ id: 9007199254740996, row_id: '9007199254740995' }), '9007199254740995');
  assert.equal(recovery.dlqRowId({ id: 9007199254740996 }), undefined);
});

test('known association comes from the stored record and mismatches are visible', () => {
  const event = { id: recoveryId, pipeline_id: pipelineId, node_id: 'sink-a', connection_name: 'warehouse', connection_known: true };
  assert.equal(recovery.contextMatchesRecovery({ pipelineId, recoveryId, nodeId: 'sink-b' }, event), false);
  assert.equal(recovery.contextMatchesRecovery(recovery.recoveryContext(event), event), true);
  assert.equal(recovery.recoveryContext({ ...event, connection_known: false }).connectionName, undefined);
});

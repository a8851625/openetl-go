import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateTopology, wouldCreateCycle, issueNodeIds } from './dag-topology.ts';
import type { TopoNode, TopoEdge } from './dag-topology.ts';

const n = (id: string, kind: string): TopoNode => ({ id, kind });
const e = (source: string, target: string): TopoEdge => ({ source, target });

const linear: TopoNode[] = [n('source-1', 'source'), n('transform-1', 'transform'), n('sink-1', 'sink')];
const linearEdges: TopoEdge[] = [e('source-1', 'transform-1'), e('transform-1', 'sink-1')];

test('a connected source -> transform -> sink passes', () => {
  const r = validateTopology(linear, linearEdges);
  assert.equal(r.ok, true, JSON.stringify(r.issues));
  assert.deepEqual(r.issues, []);
});

test('source and sink with no edges fails as unreachable sink', () => {
  const r = validateTopology([n('source-1', 'source'), n('sink-1', 'sink')], []);
  assert.equal(r.ok, false);
  const codes = r.issues.map((i) => i.code);
  assert.ok(codes.includes('unreachable_sink'), codes.join(','));
  const sinkIssue = r.issues.find((i) => i.code === 'unreachable_sink')!;
  assert.deepEqual(sinkIssue.nodeIds, ['sink-1']);
});

test('missing source is reported', () => {
  const r = validateTopology([n('sink-1', 'sink')], []);
  assert.ok(r.issues.some((i) => i.code === 'missing_source'));
});

test('missing sink is reported', () => {
  const r = validateTopology([n('source-1', 'source')], []);
  assert.ok(r.issues.some((i) => i.code === 'missing_sink'));
});

test('a cycle is detected and its nodes are returned', () => {
  const nodes = [n('source-1', 'source'), n('transform-1', 'transform'), n('transform-2', 'transform'), n('sink-1', 'sink')];
  const edges = [
    e('source-1', 'transform-1'),
    e('transform-1', 'transform-2'),
    e('transform-2', 'transform-1'),
    e('transform-2', 'sink-1'),
  ];
  const r = validateTopology(nodes, edges);
  assert.equal(r.ok, false);
  assert.ok(r.issues.some((i) => i.code === 'cycle'));
  assert.deepEqual(r.cycleNodeIds.sort(), ['transform-1', 'transform-2']);
});

test('self loop is reported', () => {
  const r = validateTopology([n('source-1', 'source'), n('sink-1', 'sink')], [e('source-1', 'source-1')]);
  assert.ok(r.issues.some((i) => i.code === 'self_loop'));
});

test('duplicate edge is reported once', () => {
  const nodes = [n('source-1', 'source'), n('sink-1', 'sink')];
  const edges = [e('source-1', 'sink-1'), e('source-1', 'sink-1')];
  const r = validateTopology(nodes, edges);
  const dupes = r.issues.filter((i) => i.code === 'duplicate_edge');
  assert.equal(dupes.length, 1);
});

test('a disconnected transform fragment is an orphan', () => {
  const nodes = [...linear, n('transform-9', 'transform')];
  const r = validateTopology(nodes, linearEdges);
  assert.equal(r.ok, false);
  const orphan = r.issues.find((i) => i.code === 'orphan_node');
  assert.ok(orphan, JSON.stringify(r.issues));
  assert.deepEqual(orphan!.nodeIds, ['transform-9']);
});

test('a transform with input but no output is an orphan (dead end)', () => {
  const nodes = [n('source-1', 'source'), n('transform-1', 'transform'), n('sink-1', 'sink')];
  const edges = [e('source-1', 'transform-1'), e('source-1', 'sink-1')];
  const r = validateTopology(nodes, edges);
  assert.ok(r.issues.some((i) => i.code === 'orphan_node' && i.nodeIds.includes('transform-1')));
});

test('fan-out to two sinks is valid', () => {
  const nodes = [n('source-1', 'source'), n('fanout-1', 'fanout'), n('sink-1', 'sink'), n('sink-2', 'sink')];
  const edges = [
    e('source-1', 'fanout-1'),
    e('fanout-1', 'sink-1'),
    e('fanout-1', 'sink-2'),
  ];
  const r = validateTopology(nodes, edges);
  assert.equal(r.ok, true, JSON.stringify(r.issues));
});

test('multi-source into one sink is valid', () => {
  const nodes = [n('source-1', 'source'), n('source-2', 'source'), n('sink-1', 'sink')];
  const edges = [e('source-1', 'sink-1'), e('source-2', 'sink-1')];
  const r = validateTopology(nodes, edges);
  assert.equal(r.ok, true, JSON.stringify(r.issues));
});

test('wouldCreateCycle rejects closing a loop', () => {
  const nodes = [n('a', 'transform'), n('b', 'transform')];
  const edges = [e('a', 'b')];
  assert.equal(wouldCreateCycle(nodes, edges, 'b', 'a'), true);
});

test('wouldCreateCycle allows a forward edge', () => {
  const nodes = [n('a', 'transform'), n('b', 'transform'), n('c', 'transform')];
  const edges = [e('a', 'b')];
  assert.equal(wouldCreateCycle(nodes, edges, 'b', 'c'), false);
});

test('wouldCreateCycle rejects self connection', () => {
  const nodes = [n('a', 'transform')];
  assert.equal(wouldCreateCycle(nodes, [], 'a', 'a'), true);
});

test('issueNodeIds collects every flagged node', () => {
  const r = validateTopology([n('source-1', 'source'), n('sink-1', 'sink'), n('transform-9', 'transform')], []);
  const ids = issueNodeIds(r);
  assert.ok(ids.has('sink-1'));
  assert.ok(ids.has('transform-9'));
});

test('edges referencing unknown nodes are ignored, not crashed on', () => {
  const nodes = [n('source-1', 'source'), n('sink-1', 'sink')];
  const edges = [e('source-1', 'ghost'), e('ghost', 'sink-1')];
  const r = validateTopology(nodes, edges);
  // sink-1 has no real inbound path, so it is unreachable; the ghost edges must
  // not make it look connected.
  assert.ok(r.issues.some((i) => i.code === 'unreachable_sink'));
});

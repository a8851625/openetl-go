/**
 * DAG topology validation for the pipeline designer.
 *
 * The designer used to serialize nodes/edges verbatim and leave all semantic
 * checking to the server preflight, which only knows about single-node
 * configuration (path exists, schema readable) and has no notion of which sink
 * is reachable. That let a canvas with a source and a sink and zero edges pass
 * validation, because the existence guard only asks "is there a node of kind
 * sink anywhere".
 *
 * These helpers are pure so they can be unit tested without React Flow.
 */

export type TopoNode = {
  id: string;
  kind: string;
};

export type TopoEdge = {
  id?: string;
  source: string;
  target: string;
};

export type TopologyIssueCode =
  | 'missing_source'
  | 'missing_sink'
  | 'orphan_node'
  | 'unreachable_sink'
  | 'cycle'
  | 'duplicate_edge'
  | 'self_loop';

export type TopologyIssue = {
  code: TopologyIssueCode;
  /** Node ids the operator should look at; the canvas highlights these. */
  nodeIds: string[];
  /** English fallback message; the UI localizes via the code. */
  message: string;
};

export type TopologyResult = {
  ok: boolean;
  issues: TopologyIssue[];
  /** Node ids participating in a cycle (for highlighting). */
  cycleNodeIds: string[];
};

const SOURCE_KIND = 'source';
const SINK_KIND = 'sink';

/** Adjacency in edge direction (source -> target). */
function buildAdjacency(nodes: TopoNode[], edges: TopoEdge[]): Map<string, string[]> {
  const known = new Set(nodes.map((n) => n.id));
  const adj = new Map<string, string[]>();
  for (const n of nodes) adj.set(n.id, []);
  for (const e of edges) {
    if (!known.has(e.source) || !known.has(e.target)) continue;
    adj.get(e.source)!.push(e.target);
  }
  return adj;
}

/**
 * findCycleNodes returns the nodes that actually sit on a cycle.
 *
 * A plain Kahn peel leaves every node downstream of a cycle with a non-zero
 * in-degree too, which reported innocent sinks as "in a cycle". Tarjan's SCC
 * is used instead: a node is on a cycle only if it belongs to a strongly
 * connected component of size > 1, or has a self loop.
 */
function findCycleNodes(nodes: TopoNode[], edges: TopoEdge[]): string[] {
  const adjacency = buildAdjacency(nodes, edges);
  const ids = nodes.map((n) => n.id);

  let index = 0;
  const indices = new Map<string, number>();
  const low = new Map<string, number>();
  const onStack = new Set<string>();
  const stack: string[] = [];
  const result = new Set<string>();

  const strongConnect = (v: string) => {
    indices.set(v, index);
    low.set(v, index);
    index += 1;
    stack.push(v);
    onStack.add(v);

    for (const w of adjacency.get(v) || []) {
      if (!indices.has(w)) {
        strongConnect(w);
        low.set(v, Math.min(low.get(v)!, low.get(w)!));
      } else if (onStack.has(w)) {
        low.set(v, Math.min(low.get(v)!, indices.get(w)!));
      }
    }

    if (low.get(v) === indices.get(v)) {
      const component: string[] = [];
      let w: string;
      do {
        w = stack.pop()!;
        onStack.delete(w);
        component.push(w);
      } while (w !== v);
      const selfLoop = component.length === 1 && edges.some((e) => e.source === v && e.target === v);
      if (component.length > 1 || selfLoop) {
        for (const id of component) result.add(id);
      }
    }
  };

  for (const id of ids) {
    if (!indices.has(id)) strongConnect(id);
  }
  return nodes.filter((n) => result.has(n.id)).map((n) => n.id);
}

/** Every node reachable from any source (following edge direction). */
function reachableFromSources(nodes: TopoNode[], adjacency: Map<string, string[]>): Set<string> {
  const seen = new Set<string>();
  const stack = nodes.filter((n) => n.kind === SOURCE_KIND).map((n) => n.id);
  while (stack.length > 0) {
    const id = stack.pop()!;
    if (seen.has(id)) continue;
    seen.add(id);
    for (const next of adjacency.get(id) || []) {
      if (!seen.has(next)) stack.push(next);
    }
  }
  return seen;
}

/**
 * validateTopology enforces the minimum contract for a runnable pipeline:
 *
 *   - at least one source and at least one sink exist;
 *   - every sink is reachable from some source (otherwise the pipeline would
 *     start and silently deliver nothing);
 *   - every non-sink node leads somewhere (no dead-end fragments);
 *   - the graph is acyclic (the runtime walks it forward only);
 *   - no duplicate or self edges.
 */
export function validateTopology(nodes: TopoNode[], edges: TopoEdge[]): TopologyResult {
  const issues: TopologyIssue[] = [];
  const ids = new Set(nodes.map((n) => n.id));

  const sources = nodes.filter((n) => n.kind === SOURCE_KIND);
  const sinks = nodes.filter((n) => n.kind === SINK_KIND);

  if (sources.length === 0) {
    issues.push({
      code: 'missing_source',
      nodeIds: [],
      message: 'Add at least one source node.',
    });
  }
  if (sinks.length === 0) {
    issues.push({
      code: 'missing_sink',
      nodeIds: [],
      message: 'Add at least one sink node.',
    });
  }

  // Self loops and duplicates are cheap to detect and always a mistake.
  const seenPairs = new Set<string>();
  for (const e of edges) {
    if (!ids.has(e.source) || !ids.has(e.target)) continue;
    if (e.source === e.target) {
      issues.push({
        code: 'self_loop',
        nodeIds: [e.source],
        message: `Node ${e.source} is connected to itself.`,
      });
      continue;
    }
    const key = `${e.source}->${e.target}`;
    if (seenPairs.has(key)) {
      issues.push({
        code: 'duplicate_edge',
        nodeIds: [e.source, e.target],
        message: `Duplicate connection ${e.source} → ${e.target}.`,
      });
    }
    seenPairs.add(key);
  }

  const cycleNodeIds = findCycleNodes(nodes, edges);
  if (cycleNodeIds.length > 0) {
    issues.push({
      code: 'cycle',
      nodeIds: cycleNodeIds,
      message: `Cycle detected involving: ${cycleNodeIds.join(', ')}.`,
    });
  }

  const adjacency = buildAdjacency(nodes, edges);
  const reachable = reachableFromSources(nodes, adjacency);

  if (sources.length > 0 && sinks.length > 0) {
    for (const sink of sinks) {
      if (!reachable.has(sink.id)) {
        issues.push({
          code: 'unreachable_sink',
          nodeIds: [sink.id],
          message: `Sink ${sink.id} is not connected from any source.`,
        });
      }
    }
  }

  // A node that nothing reaches and that reaches nothing is a dropped fragment.
  // Sinks are covered by unreachable_sink above, so skip them here to avoid
  // reporting the same node twice.
  for (const n of nodes) {
    if (n.kind === SINK_KIND || n.kind === SOURCE_KIND) continue;
    const hasIn = edges.some((e) => e.target === n.id);
    const hasOut = edges.some((e) => e.source === n.id);
    if (!hasIn || !hasOut) {
      issues.push({
        code: 'orphan_node',
        nodeIds: [n.id],
        message: `Node ${n.id} is not fully connected (needs both an input and an output).`,
      });
    }
  }

  return { ok: issues.length === 0, issues, cycleNodeIds };
}

/**
 * wouldCreateCycle reports whether adding source -> target would close a loop.
 * Used by the canvas to reject a connection at drag time instead of letting
 * the operator build a graph that only fails later at validation.
 */
export function wouldCreateCycle(nodes: TopoNode[], edges: TopoEdge[], source: string, target: string): boolean {
  if (source === target) return true;
  const adjacency = buildAdjacency(nodes, edges);
  // Walk forward from target; if we can get back to source, the new edge closes
  // a loop.
  const seen = new Set<string>();
  const stack = [target];
  while (stack.length > 0) {
    const id = stack.pop()!;
    if (id === source) return true;
    if (seen.has(id)) continue;
    seen.add(id);
    for (const next of adjacency.get(id) || []) {
      if (!seen.has(next)) stack.push(next);
    }
  }
  return false;
}

/** Convenience: node ids flagged by any issue, for canvas highlighting. */
export function issueNodeIds(result: TopologyResult): Set<string> {
  const out = new Set<string>();
  for (const issue of result.issues) for (const id of issue.nodeIds) out.add(id);
  return out;
}

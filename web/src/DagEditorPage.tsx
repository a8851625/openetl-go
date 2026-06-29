import React, { useState, useCallback, useMemo, useEffect } from 'react';
import {
  ReactFlow, Controls, Background, MiniMap, Handle, Position,
  addEdge, useNodesState, useEdgesState,
  type Node, type Edge, type Connection, type NodeTypes, NodeProps,
  BackgroundVariant, MarkerType,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import YAML from 'yaml';
import cronstrue from 'cronstrue';
import type { TFunc, Lang } from './types';
import { ConfigForm, type PluginSchemaField } from './configFields';

// ── Types ─────────────────────────────────────────────────────────────

type PluginSchemaResp = {
  sources: Record<string, PluginSchemaField[]>;
  sinks: Record<string, PluginSchemaField[]>;
  transforms: Record<string, PluginSchemaField[]>;
};

type PluginListResp = { sources: string[]; sinks: string[]; transforms: string[] };

type ConnectorDescriptor = {
  kind: string;
  type: string;
  supported_schedules?: string[];
  default_schedule?: string;
};

type ConnectionEntry = {
  name: string;
  kind: 'source' | 'sink' | 'transform';
  type: string;
  last_status?: string;
  last_error?: string;
  updated_at?: string;
};

type ConnectionContext = {
  recommendations?: { field: string; value: unknown; reason: string }[];
  introspection?: {
    ok?: boolean;
    status?: string;
    error?: string;
    tables?: { name: string; schema?: string; columns?: { name: string; data_type?: string }[] }[];
    topics?: { name: string; partitions?: { id: number }[] }[];
    schema?: { name: string; data_type?: string }[];
    sample?: Record<string, unknown>[];
    warnings?: string[];
  };
};

function normalizeConnectionEntry(raw: any): ConnectionEntry | null {
  if (!raw || typeof raw !== 'object') return null;
  const name = String(raw.name || raw.Name || '').trim();
  const kind = String(raw.kind || raw.Kind || '').trim();
  const type = String(raw.type || raw.Type || '').trim();
  if (!name || !type || (kind !== 'source' && kind !== 'sink' && kind !== 'transform')) return null;
  return {
    name,
    kind,
    type,
    last_status: raw.last_status || raw.LastStatus,
    last_error: raw.last_error || raw.LastError,
    updated_at: raw.updated_at || raw.UpdatedAt,
  };
}

type DAGNodeData = {
  kind: string;
  plugin: string;
  connection?: string;
  config: Record<string, unknown>;
  label: string;
};

type ScheduleConfig = {
  type: string;
  cron?: string;
  interval_sec?: number;
  depends_on?: string[];
};

type ValidateResult = {
  valid?: boolean;
  warnings?: string[];
  errors?: string[];
  preflight?: {
    passed?: boolean;
    summary?: string;
    issues?: { level: string; check: string; message: string; remediation?: string }[];
    field_issues?: { level: string; field: string; check: string; message: string; remediation?: string }[];
    ddl_preview?: { dialect: string; table: string; statements?: string[]; warnings?: string[] };
  };
};

type AIGenerationResp = {
  yaml?: string;
  context_pack_version?: string;
  validation?: ValidateResult;
  review?: {
    missing_fields?: { kind: string; type: string; field: string; secret?: boolean; message: string }[];
    risk_flags?: { code: string; level: string; message: string; remediation?: string }[];
    requires_confirmation?: { code: string; message: string }[];
    recommended_actions?: string[];
  };
};

// ── Visual Constants ──────────────────────────────────────────────────

const KIND_STYLES: Record<string, { color: string; bg: string; border: string; icon: string }> = {
  source:       { color: '#0ea5e9', bg: '#ecfeff', border: '#67e8f9', icon: '⬛' },
  transform:    { color: '#8b5cf6', bg: '#f5f3ff', border: '#c4b5fd', icon: '◆' },
  sink:         { color: '#10b981', bg: '#ecfdf5', border: '#6ee7b7', icon: '▼' },
  fanout:       { color: '#f59e0b', bg: '#fffbeb', border: '#fcd34d', icon: 'Ⓕ' },
  router:       { color: '#ef4444', bg: '#fef2f2', border: '#fca5a5', icon: 'Ⓡ' },
  tap:          { color: '#06b6d4', bg: '#ecfeff', border: '#67e8f9', icon: 'Ⓣ' },
  rate_limiter: { color: '#84cc16', bg: '#f7fee7', border: '#bef264', icon: 'ⓛ' },
  enricher:     { color: '#ec4899', bg: '#fdf2f8', border: '#f9a8d4', icon: 'Ⓔ' },
  lookup:       { color: '#a855f7', bg: '#faf5ff', border: '#d8b4fe', icon: 'Ⓛ' },
};

const ADVANCED_NODE_KINDS = ['fanout', 'router', 'tap', 'rate_limiter', 'enricher', 'lookup'];
const ADVANCED_TRANSFORM_PLUGINS = new Set(ADVANCED_NODE_KINDS);

// Toolbar palette grouped by category
const NODE_PALETTE = (t: (key: string) => string): { category: string; catLabel: string; catColor: string; nodes: { kind: string; label: string; defaultPlugin: string }[] }[] => [
  {
    category: 'io', catLabel: t('category.io'), catColor: '#0ea5e9',
    nodes: [
      { kind: 'source', label: t('node.source'), defaultPlugin: 'file' },
      { kind: 'sink', label: t('node.sink'), defaultPlugin: 'file_sink' },
    ],
  },
  {
    category: 'process', catLabel: t('category.process'), catColor: '#8b5cf6',
    nodes: [
      { kind: 'transform', label: t('node.transform'), defaultPlugin: 'identity' },
    ],
  },
  {
    category: 'flow', catLabel: t('category.flowControl'), catColor: '#f59e0b',
    nodes: [
      { kind: 'fanout', label: t('node.fanout'), defaultPlugin: 'fanout' },
      { kind: 'router', label: t('node.router'), defaultPlugin: 'router' },
    ],
  },
  {
    category: 'observe', catLabel: t('category.observe'), catColor: '#06b6d4',
    nodes: [
      { kind: 'tap', label: t('node.tap'), defaultPlugin: 'tap' },
      { kind: 'rate_limiter', label: t('node.rateLimiter'), defaultPlugin: 'rate_limiter' },
    ],
  },
  {
    category: 'enrich', catLabel: t('category.enrich'), catColor: '#ec4899',
    nodes: [
      { kind: 'enricher', label: t('node.enricher'), defaultPlugin: 'enricher' },
      { kind: 'lookup', label: t('node.lookup'), defaultPlugin: 'lookup' },
    ],
  },
];

let nodeCounter = 0;
function nextNodeId(kind: string) {
  nodeCounter++;
  return `${kind}-${nodeCounter}`;
}

function schedulePolicyForSources(sourcePlugins: string[], descriptors: ConnectorDescriptor[]) {
  if (sourcePlugins.length === 0) return { supported: ALL_SCHEDULE_TYPES, defaultType: 'streaming' };
  const sourceDescriptors = sourcePlugins
    .map((plugin) => descriptors.find((d) => d.kind === 'source' && d.type === plugin))
    .filter(Boolean) as ConnectorDescriptor[];
  if (sourceDescriptors.length !== sourcePlugins.length) {
    return { supported: ALL_SCHEDULE_TYPES, defaultType: 'streaming' };
  }
  const supported = sourceDescriptors
    .map((d) => d.supported_schedules || ALL_SCHEDULE_TYPES)
    .reduce((acc, current) => acc.filter((value) => current.includes(value)), ALL_SCHEDULE_TYPES);
  const firstDefault = sourceDescriptors[0]?.default_schedule || supported[0] || 'streaming';
  return {
    supported: supported.length > 0 ? supported : [],
    defaultType: supported.includes(firstDefault) ? firstDefault : supported[0] || firstDefault,
  };
}

// ── Custom Node Component with Handles ────────────────────────────────

function PipelineNode({ id, data, selected }: NodeProps) {
  const d = data as DAGNodeData;
  const style = KIND_STYLES[d.kind] || KIND_STYLES.transform;
  return (
    <div
      style={{
        background: style.bg,
        borderColor: selected ? style.color : style.border,
        borderWidth: selected ? 2.5 : 2,
        borderStyle: 'solid',
        borderRadius: 10,
        padding: '10px 16px',
        minWidth: 150,
        position: 'relative',
      }}
    >
      {/* Input handle (top) — except for sources */}
      {d.kind !== 'source' && (
        <Handle
          type="target"
          position={Position.Top}
          style={{ background: style.color, width: 10, height: 10, border: 'none' }}
          id="in"
        />
      )}
      {/* Node label */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{ fontSize: 10, color: style.color }}>{style.icon}</span>
        <span style={{ fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.5, color: '#64748b' }}>{d.kind}</span>
      </div>
      <div style={{ fontSize: 13, fontWeight: 600, color: style.color, marginTop: 2 }}>{d.label || d.plugin}</div>
      <div style={{ fontSize: 11, color: '#475569', marginTop: 1 }}>{d.plugin}</div>
      {/* Output handle (bottom) — except for sinks */}
      {d.kind !== 'sink' && (
        <Handle
          type="source"
          position={Position.Bottom}
          style={{ background: style.color, width: 10, height: 10, border: 'none' }}
          id="out"
        />
      )}
    </div>
  );
}

const nodeTypes: NodeTypes = { pipelineNode: PipelineNode as any };

// ── Schedule Config Form ──────────────────────────────────────────────

const ALL_SCHEDULE_TYPES = ['streaming', 'cron', 'periodic', 'once', 'dependency'];

function ScheduleForm({
  schedule,
  onChange,
  t,
  supportedTypes = ALL_SCHEDULE_TYPES,
}: {
  schedule: ScheduleConfig;
  onChange: (s: ScheduleConfig) => void;
  t: TFunc;
  supportedTypes?: string[];
}) {
  const [cronDesc, setCronDesc] = useState('');
  const types = [
    { value: 'streaming', label: t('sched.streaming') },
    { value: 'cron', label: t('sched.cron') },
    { value: 'periodic', label: t('sched.periodic') },
    { value: 'once', label: t('sched.once') },
    { value: 'dependency', label: t('sched.dependency') },
  ].filter((tp) => supportedTypes.includes(tp.value));

  const updateType = (type: string) => {
    onChange({ ...schedule, type });
  };

  const parseCron = (cron: string) => {
    onChange({ ...schedule, cron });
    if (!cron) { setCronDesc(''); return; }
    try { setCronDesc(cronstrue.toString(cron)); } catch { setCronDesc('⚠ Invalid expression'); }
  };

  return (
    <div className="space-y-3">
      <div>
        <label className="mb-1 block text-xs font-medium text-slate-500">{t('sched.triggerType')}</label>
        <div className="flex flex-wrap gap-1">
          {types.map((tp) => (
            <button
              key={tp.value}
              className={`btn btn-sm ${schedule.type === tp.value ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => updateType(tp.value)}
            >
              {tp.label}
            </button>
          ))}
        </div>
      </div>
      {schedule.type === 'cron' && (
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-500">{t('common.cron')}</label>
          <input className="input w-full font-mono text-sm" value={schedule.cron || ''} onChange={(e) => parseCron(e.target.value)} placeholder="*/5 * * * *" />
          {cronDesc && <div className={`mt-1 text-xs ${cronDesc.startsWith('⚠') ? 'text-rose-500' : 'text-emerald-600'}`}>{cronDesc}</div>}
          <div className="mt-1 flex flex-wrap gap-1 text-xs">
            <code className="cursor-pointer rounded bg-slate-100 px-1.5 py-0.5 text-slate-500" onClick={() => parseCron('*/5 * * * *')}>*/5 * * * *</code>
            <code className="cursor-pointer rounded bg-slate-100 px-1.5 py-0.5 text-slate-500" onClick={() => parseCron('0 */6 * * *')}>0 */6 * * *</code>
            <code className="cursor-pointer rounded bg-slate-100 px-1.5 py-0.5 text-slate-500" onClick={() => parseCron('0 2 * * *')}>0 2 * * *</code>
          </div>
        </div>
      )}
      {schedule.type === 'periodic' && (
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-500">{t('common.interval')}</label>
          <input type="number" className="input w-full text-sm" value={schedule.interval_sec || 60} onChange={(e) => onChange({ ...schedule, interval_sec: parseInt(e.target.value) || 60 })} />
        </div>
      )}
      {schedule.type === 'dependency' && (
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-500">{t('sched.dependsOn')}</label>
          <input className="input w-full text-sm" value={(schedule.depends_on || []).join(', ')} onChange={(e) => onChange({ ...schedule, depends_on: e.target.value.split(',').map((s) => s.trim()).filter(Boolean) })} />
        </div>
      )}
      {schedule.type === 'streaming' && (
        <div className="rounded-lg bg-cyan-50 px-3 py-2 text-xs text-cyan-700">
          {t('sched.streaming')} — {t('dag.streamingDesc')}
        </div>
      )}
      {schedule.type === 'once' && (
        <div className="rounded-lg bg-emerald-50 px-3 py-2 text-xs text-emerald-700">
          {t('sched.once')} — {t('dag.onceDesc')}
        </div>
      )}
    </div>
  );
}

// ── Main Designer Component ───────────────────────────────────────────

export function DagEditorPage({ t, lang, plugins, schema, onAction, editTarget }: {
  t: TFunc;
  lang: Lang;
  plugins: any;
  schema: any;
  onAction: any;
  editTarget?: string;
}) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<DAGNodeData>>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [pipelineName, setPipelineName] = useState('my-pipeline');
  const [tags, setTags] = useState('');
  const [workerLabels, setWorkerLabels] = useState('');
  const [schedule, setSchedule] = useState<ScheduleConfig>({ type: 'streaming' });
  const [yamlOutput, setYamlOutput] = useState('');
  const [validateResult, setValidateResult] = useState<ValidateResult | null>(null);
  const [validateError, setValidateError] = useState('');
  // Drawer: 'schedule' | 'hooks' | 'advanced' | 'ai' | 'yaml' | null
  const [drawerTab, setDrawerTab] = useState<string | null>(null);
  const [aiPrompt, setAiPrompt] = useState('');
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState('');
  const [aiResult, setAiResult] = useState<AIGenerationResp | null>(null);
  const [parallelism, setParallelism] = useState(1);
  const [shardStrategy, setShardStrategy] = useState('round_robin');
  const [shardKey, setShardKey] = useState('');
  const [batchSize, setBatchSize] = useState(1000);
  const [flushIntervalMs, setFlushIntervalMs] = useState(1000);
  const [checkpointIntervalSec, setCheckpointIntervalSec] = useState(30);
  const [backpressureBuffer, setBackpressureBuffer] = useState(100);
  const [hooks, setHooks] = useState<Record<string, { type: string; code: string; name: string; enabled: boolean }>>({});
  const [showNodeProps, setShowNodeProps] = useState(false);
  const [testResult, setTestResult] = useState<string>('');
  const [connections, setConnections] = useState<ConnectionEntry[]>([]);
  const [descriptors, setDescriptors] = useState<ConnectorDescriptor[]>([]);
  const [selectedConnectionContext, setSelectedConnectionContext] = useState<ConnectionContext | null>(null);

  const testNodeConnection = async () => {
    if (!selectedNode) {
      setTestResult('⚠ ' + t('dag.testSelectNode'));
      return;
    }
    setTestResult('⏳ ' + t('dag.testing'));
    try {
      const kind = selKind === 'source' ? 'source' : selKind === 'sink' ? 'sink' : 'transform';
      if (selectedNode.data.connection) {
        const res = await apiPost(`/api/v2/connections/${encodeURIComponent(selectedNode.data.connection)}/test`, { open: true });
        if (res.ok) {
          setTestResult(`✅ ${selectedNode.data.connection} connection OK`);
        } else {
          setTestResult(`❌ ${res.stage || 'error'}: ${res.error}`);
        }
        return;
      }
      const res = await apiPost('/api/v2/connections/test', {
        kind,
        type: selPlugin,
        config: selectedNode.data.config,
        open: true,
      });
      if (res.ok) {
        const sampleInfo = res.sample ? ` (${res.count} sample records)` : '';
        setTestResult(`✅ ${kind}/${selPlugin} connection OK${sampleInfo}`);
      } else {
        setTestResult(`❌ ${res.stage || 'error'}: ${res.error}`);
      }
    } catch (e) {
      setTestResult(`❌ ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  useEffect(() => {
    apiGet<{ connections?: ConnectionEntry[] }>('/api/v2/connections')
      .then((res) => setConnections((res.connections || []).map(normalizeConnectionEntry).filter((conn): conn is ConnectionEntry => conn !== null)))
      .catch(() => setConnections([]));
    apiGet<{ descriptors?: ConnectorDescriptor[] }>('/api/v2/connectors/descriptors')
      .then((res) => setDescriptors(res.descriptors || []))
      .catch(() => setDescriptors([]));
  }, []);

  const loadSpecIntoCanvas = useCallback((spec: any) => {
    if (!spec || typeof spec !== 'object') return;
    setPipelineName(spec.name || 'my-pipeline');
    setTags(Array.isArray(spec.tags) ? spec.tags.join(', ') : '');
    if (spec.schedule?.type) setSchedule(spec.schedule);
    if (spec.execution) {
      setBatchSize(Number(spec.execution.batch_size) || batchSize);
      setFlushIntervalMs(Number(spec.execution.flush_interval_ms) || flushIntervalMs);
      setCheckpointIntervalSec(Number(spec.execution.checkpoint_interval_sec) || checkpointIntervalSec);
      setBackpressureBuffer(Number(spec.execution.backpressure_buffer) || backpressureBuffer);
    } else {
      setBatchSize(Number(spec.batch_size) || batchSize);
      setFlushIntervalMs(Number(spec.flush_interval_ms) || flushIntervalMs);
      setCheckpointIntervalSec(Number(spec.checkpoint_interval_sec) || checkpointIntervalSec);
      setBackpressureBuffer(Number(spec.backpressure_buffer) || backpressureBuffer);
    }

    if (spec.dag?.nodes) {
      const nextNodes: Node<DAGNodeData>[] = (spec.dag.nodes || []).map((n: any, i: number) => ({
        id: n.id || `${n.kind || 'node'}-${i + 1}`,
        type: 'pipelineNode',
        position: { x: Number(n.x) || 220 + i * 40, y: Number(n.y) || 80 + i * 100 },
        data: {
          kind: n.kind || 'transform',
          plugin: n.plugin || n.kind || 'identity',
          connection: n.connection || n.connection_ref || '',
          config: n.config || {},
          label: n.id || `${n.kind || 'node'}-${i + 1}`,
        },
      }));
      const nextEdges: Edge[] = (spec.dag.edges || []).map((e: any, i: number) => ({
        id: e.id || `e-${i}`,
        source: e.from || e.source,
        target: e.to || e.target,
        animated: true,
        markerEnd: { type: MarkerType.ArrowClosed },
      })).filter((e: Edge) => e.source && e.target);
      setNodes(nextNodes);
      setEdges(nextEdges);
      setSelectedNodeId(nextNodes[0]?.id || null);
      setValidateResult(null);
      setValidateError('');
      return;
    }

    const nextNodes: Node<DAGNodeData>[] = [];
    const nextEdges: Edge[] = [];
    if (spec.source?.type) {
      nextNodes.push({ id: 'source-1', type: 'pipelineNode', position: { x: 250, y: 50 }, data: { kind: 'source', plugin: spec.source.type, connection: spec.source.connection || spec.source.connection_ref || '', config: spec.source.config || {}, label: 'source-1' } });
    }
    const tfms = spec.transforms || [];
    tfms.forEach((tf: any, i: number) => {
      nextNodes.push({ id: `transform-${i + 1}`, type: 'pipelineNode', position: { x: 250, y: 150 + i * 100 }, data: { kind: 'transform', plugin: tf.type, connection: tf.connection || tf.connection_ref || '', config: tf.config || {}, label: `transform-${i + 1}` } });
    });
    if (spec.sink?.type) {
      const lastY = 150 + tfms.length * 100;
      nextNodes.push({ id: 'sink-1', type: 'pipelineNode', position: { x: 250, y: lastY }, data: { kind: 'sink', plugin: spec.sink.type, connection: spec.sink.connection || spec.sink.connection_ref || '', config: spec.sink.config || {}, label: 'sink-1' } });
    }
    for (let i = 0; i < nextNodes.length - 1; i++) {
      nextEdges.push({ id: `e-${i}`, source: nextNodes[i].id, target: nextNodes[i + 1].id, animated: true, markerEnd: { type: MarkerType.ArrowClosed } });
    }
    setNodes(nextNodes);
    setEdges(nextEdges);
    setSelectedNodeId(nextNodes[0]?.id || null);
    setValidateResult(null);
    setValidateError('');
  }, [backpressureBuffer, batchSize, checkpointIntervalSec, flushIntervalMs, setEdges, setNodes]);

  // Load pipeline when editTarget changes
  useEffect(() => {
    if (!editTarget) return;
    apiGet<{ spec: any }>(`/api/v2/pipelines/${encodeURIComponent(editTarget)}/spec`).then((res) => {
      if (res.spec) loadSpecIntoCanvas(res.spec);
    }).catch(() => {});
  }, [editTarget, loadSpecIntoCanvas]);

  const onConnect = useCallback((params: Connection) => {
    setEdges((eds) => addEdge({
      ...params,
      animated: true,
      markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18 },
      style: { stroke: '#94a3b8', strokeWidth: 2 },
    }, eds));
  }, [setEdges]);

  const addNode = (kind: string, defaultPlugin: string) => {
    const id = nextNodeId(kind);
    const offset = nodes.length * 30;
    // Lane Y positions by category for visual grouping
    const laneY: Record<string, number> = {
      source: 50, sink: 500,
      transform: 200, fanout: 200, router: 200, tap: 200,
      rate_limiter: 350, enricher: 350, lookup: 350,
    };
    const pos = { x: 200 + offset, y: laneY[kind] || 250 };
    const newNode: Node<DAGNodeData> = {
      id,
      type: 'pipelineNode',
      position: pos,
      data: { kind, plugin: defaultPlugin, config: {}, label: id },
    };
    setNodes((nds) => [...nds, newNode]);
    setSelectedNodeId(id);
  };

  const deleteSelected = useCallback(() => {
    if (!selectedNodeId) return;
    setNodes((nds) => nds.filter((n) => n.id !== selectedNodeId));
    setEdges((eds) => eds.filter((e) => e.source !== selectedNodeId && e.target !== selectedNodeId));
    setSelectedNodeId(null);
  }, [selectedNodeId, setNodes, setEdges]);

  // Keyboard delete
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.key === 'Delete' || e.key === 'Backspace') && selectedNodeId) {
        const target = e.target as HTMLElement;
        if (target.tagName !== 'INPUT' && target.tagName !== 'TEXTAREA' && target.tagName !== 'SELECT') {
          deleteSelected();
        }
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [selectedNodeId, deleteSelected]);

  const updateNodeConfig = (config: Record<string, unknown>) => {
    if (!selectedNodeId) return;
    setNodes((nds) => nds.map((n) => n.id === selectedNodeId ? { ...n, data: { ...n.data, config } } : n));
  };

  const updateNodePlugin = (plugin: string) => {
    if (!selectedNodeId) return;
    setNodes((nds) => nds.map((n) => n.id === selectedNodeId ? { ...n, data: { ...n.data, plugin, connection: '', config: {} } } : n));
  };

  const updateNodeLabel = (label: string) => {
    if (!selectedNodeId) return;
    setNodes((nds) => nds.map((n) => n.id === selectedNodeId ? { ...n, data: { ...n.data, label } } : n));
  };

  const updateNodeConnection = (connectionName: string) => {
    if (!selectedNodeId) return;
    const conn = connections.find((c) => c.name === connectionName);
    setNodes((nds) => nds.map((n) => {
      if (n.id !== selectedNodeId) return n;
      return {
        ...n,
        data: {
          ...n.data,
          connection: connectionName || '',
          plugin: conn?.type || n.data.plugin,
        },
      };
    }));
  };

  // ── Export & Create ───────────────────────────────────────────────

  const buildSpec = () => {
    const sourceNode = nodes.find((n) => n.data.kind === 'source');
    const sinkNode = nodes.find((n) => n.data.kind === 'sink');
    const transforms = nodes.filter((n) => n.data.kind === 'transform');
    const tagList = tags.split(',').map((s) => s.trim()).filter(Boolean);
    const matchLabels: Record<string, string> = {};
    workerLabels.split(',').forEach((pair) => {
      const [k, v] = pair.trim().split('=');
      if (k && v) matchLabels[k.trim()] = v.trim();
    });

    // Detect non-linear nodes that require DAG format
    const hasComplexTopology = nodes.some((n) => ADVANCED_NODE_KINDS.includes(n.data.kind));
    const hasMultipleSources = nodes.filter((n) => n.data.kind === 'source').length > 1;
    const hasMultipleSinks = nodes.filter((n) => n.data.kind === 'sink').length > 1;

    if (hasComplexTopology || hasMultipleSources || hasMultipleSinks) {
      // ── DAG format ───────────────────────────────────
      return {
        name: pipelineName,
        dag: {
          nodes: nodes.map((n) => ({
            id: n.id,
            kind: n.data.kind,
            plugin: n.data.plugin || n.data.kind,
            connection: n.data.connection || undefined,
            config: n.data.config || {},
            x: n.position.x,
            y: n.position.y,
          })),
          edges: edges.map((e) => ({ id: e.id, from: e.source, to: e.target })),
        },
        schedule: schedule.type !== 'streaming' ? schedule : undefined,
        tags: tagList.length > 0 ? tagList : undefined,
        worker_selector: Object.keys(matchLabels).length > 0 ? { match_labels: matchLabels } : undefined,
        execution: {
          batch_size: batchSize,
          flush_interval_ms: flushIntervalMs,
          checkpoint_interval_sec: checkpointIntervalSec,
          backpressure_buffer: backpressureBuffer,
        },
        retry: { max_attempts: 3, initial_interval_ms: 1000, max_interval_ms: 30000 },
        hooks: buildHooksSpec(),
      };
    }

    // ── Linear format (backward compatible) ────────────
    // Note: do NOT silently fall back to 'file'/'file_sink' when sourceNode/
    // sinkNode are missing — that would let an empty canvas create a phantom
    // pipeline. Leave source/sink undefined so validateAndCreate() rejects it.
    return {
      name: pipelineName,
      source: sourceNode ? { type: sourceNode.data.plugin || 'file', connection: sourceNode.data.connection || undefined, config: sourceNode.data.config || {} } : undefined,
      transforms: transforms.map((n) => ({ type: n.data.plugin, connection: n.data.connection || undefined, config: n.data.config })),
      sink: sinkNode ? { type: sinkNode.data.plugin || 'file_sink', connection: sinkNode.data.connection || undefined, config: sinkNode.data.config || {} } : undefined,
      schedule: schedule.type !== 'streaming' ? schedule : undefined,
      tags: tagList.length > 0 ? tagList : undefined,
      worker_selector: Object.keys(matchLabels).length > 0 ? { match_labels: matchLabels } : undefined,
      parallelism: parallelism > 1 ? { count: parallelism, shard_strategy: shardStrategy, shard_key: shardKey || undefined } : undefined,
      batch_size: batchSize,
      flush_interval_ms: flushIntervalMs,
      checkpoint_interval_sec: checkpointIntervalSec,
      backpressure_buffer: backpressureBuffer,
      hooks: buildHooksSpec(),
    };
  };

  const exportYaml = () => {
    const spec = { ...buildSpec(), name: pipelineName.trim() || pipelineName };
    const yamlStr = YAML.stringify(spec);
    setYamlOutput(yamlStr);
    return spec;
  };

  const syncYamlToCanvas = () => {
    try {
      const parsed = YAML.parse(yamlOutput);
      loadSpecIntoCanvas(parsed);
      setValidateError('');
    } catch (e) {
      setValidateError(`YAML parse error: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const validateCurrentSpec = async (spec = { ...buildSpec(), name: pipelineName.trim() || pipelineName }) => {
    setValidateError('');
    setValidateResult(null);
    try {
      const res = await apiPost('/api/v2/specs/validate', { spec });
      setValidateResult(res as ValidateResult);
      if ((res as ValidateResult).valid === false) {
        throw new Error(((res as ValidateResult).errors || (res as ValidateResult).warnings || ['validation failed']).join('\n'));
      }
      return res as ValidateResult;
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setValidateError(message);
      throw e;
    }
  };

  // Build hooks spec from state, filtering disabled hooks
  const buildHooksSpec = (): any | undefined => {
    const result: any = {};
    const hookKeys: [string, string][] = [
      ['on_init', 'OnInit'],
      ['on_pre_batch', 'OnPreBatch'],
      ['on_post_batch', 'OnPostBatch'],
      ['on_error', 'OnError'],
      ['on_checkpoint', 'OnCheckpoint'],
      ['on_shutdown', 'OnShutdown'],
    ];
    for (const [yamlKey, stateKey] of hookKeys) {
      const h = hooks[stateKey];
      if (h && h.enabled && (h.code || h.name)) {
        result[yamlKey] = { type: h.type, code: h.code, name: h.name };
      }
    }
    return Object.keys(result).length > 0 ? result : undefined;
  };

  // Update a single hook's state
  const updateHook = (key: string, patch: Partial<{ type: string; code: string; name: string; enabled: boolean }>) => {
    setHooks((prev) => ({
      ...prev,
      [key]: { type: 'lua', code: '', name: '', enabled: true, ...prev[key], ...patch },
    }));
  };

  const aiGenerate = async () => {
    if (!aiPrompt.trim()) return;
    setAiLoading(true);
    setAiError('');
    setAiResult(null);
    try {
      const res = await apiPost('/api/v2/ai/generate', { prompt: aiPrompt }) as AIGenerationResp;
      const yamlStr = res.yaml || '';
      setYamlOutput(yamlStr);
      setAiResult(res);
    } catch (e) {
      setAiError(e instanceof Error ? e.message : String(e));
    } finally {
      setAiLoading(false);
    }
  };

  const applyAiGeneratedSpec = () => {
    if (!aiResult?.yaml) return;
    try {
      const parsed = YAML.parse(aiResult.yaml);
      loadSpecIntoCanvas(parsed);
      setYamlOutput(aiResult.yaml);
      setValidateResult(aiResult.validation || null);
      setValidateError('');
      setDrawerTab(null);
    } catch (e) {
      setAiError(`YAML parse error: ${e instanceof Error ? e.message : String(e)}`);
    }
  };

  const validateAndCreate = () => {
    // Guard: pipeline name must be non-empty (otherwise the backend creates
    // a pipeline with name="" which corrupts subsequent pipeline listing and
    // renders the Pipelines page blank).
    const trimmedName = pipelineName.trim();
    if (!trimmedName) {
      onAction(t('dag.validate'), () => Promise.reject(new Error(t('dag.errNameRequired'))));
      return;
    }
    setPipelineName(trimmedName);

    const spec = buildSpec();
    const hasSource = spec.source?.type || spec.dag?.nodes?.some((n: any) => n.kind === 'source');
    const hasSink = spec.sink?.type || spec.dag?.nodes?.some((n: any) => n.kind === 'sink');
    if (!hasSource || !hasSink) {
      onAction(t('dag.validate'), () => Promise.reject(new Error(t('dag.errEmptyDag'))));
      return;
    }
    if (editTarget) {
      // Update mode: PUT + checkpoint warning
      const doUpdate = () => apiPost('/api/v2/pipelines', { id: editTarget, spec, reset_checkpoint: false }, 'PUT');
      onAction(`${t('dag.updatePipeline')}: ${pipelineName}`, doUpdate);
    } else {
      // Create mode: POST
      onAction(`${t('dag.createPipeline')}: ${pipelineName}`, () =>
        validateCurrentSpec(spec).then(() =>
          apiPost('/api/v2/pipelines', { spec }, 'POST')
        )
      );
    }
  };

  const resetCheckpointAndUpdate = () => {
    if (!editTarget) return;
    const spec = buildSpec();
    onAction(`${t('dag.updatePipeline')}: ${pipelineName}`, () =>
      apiPost('/api/v2/pipelines', { id: editTarget, spec, reset_checkpoint: true }, 'PUT')
    );
  };

  // ── Schema for selected node ──────────────────────────────────────

  const selectedNode = nodes.find((n) => n.id === selectedNodeId);
  const selKind = selectedNode?.data.kind;
  const selPlugin = selectedNode?.data.plugin;
  const pluginList: string[] = selKind === 'source' ? (plugins?.data?.sources || [])
    : selKind === 'sink' ? (plugins?.data?.sinks || [])
    : ADVANCED_NODE_KINDS.includes(selKind || '') ? [selKind || '']
    : (plugins?.data?.transforms || []).filter((name: string) => !ADVANCED_TRANSFORM_PLUGINS.has(name));
  const schemaFields: PluginSchemaField[] = useMemo(() => {
    if (!schema?.data || !selKind || !selPlugin) return [];
    const kindKey = selKind === 'source' ? 'sources' : selKind === 'sink' ? 'sinks' : 'transforms';
    return (schema.data[kindKey]?.[selPlugin] || []) as PluginSchemaField[];
  }, [schema, selKind, selPlugin]);

  const nodeConnectionKind = selKind === 'source' ? 'source' : selKind === 'sink' ? 'sink' : 'transform';
  const nodeSupportsConnection = ['source', 'sink', 'transform', 'enricher', 'lookup'].includes(selKind || '');
  const matchingConnections = connections
    .filter((c) => c.kind === nodeConnectionKind)
    .sort((a, b) => Number(b.type === selPlugin) - Number(a.type === selPlugin) || a.name.localeCompare(b.name));
  const selectedConnection = connections.find((conn) => conn.name === selectedNode?.data.connection);
  const selectedConnectionName = selectedNode?.data.connection || '';
  const sourcePlugins = useMemo(() => nodes.filter((n) => n.data.kind === 'source').map((n) => n.data.plugin), [nodes]);
  const schedulePolicy = useMemo(() => schedulePolicyForSources(sourcePlugins, descriptors), [sourcePlugins, descriptors]);

  useEffect(() => {
    if (!selectedConnectionName) {
      setSelectedConnectionContext(null);
      return;
    }
    let cancelled = false;
    apiGet<ConnectionContext>(`/api/v2/connections/${encodeURIComponent(selectedConnectionName)}/context`)
      .then((res) => { if (!cancelled) setSelectedConnectionContext(res); })
      .catch((e) => { if (!cancelled) setSelectedConnectionContext({ introspection: { ok: false, error: e instanceof Error ? e.message : String(e) } }); });
    return () => { cancelled = true; };
  }, [selectedConnectionName]);

  useEffect(() => {
    if (schedulePolicy.supported.length === 0) return;
    if (schedulePolicy.supported.includes(schedule.type)) return;
    setSchedule({ type: schedulePolicy.defaultType });
  }, [schedule.type, schedulePolicy.defaultType, schedulePolicy.supported]);

  // Toggle drawer: clicking same tab again closes it
  const toggleDrawer = (tab: string) => setDrawerTab((prev) => (prev === tab ? null : tab));

  return (
    <div className="flex h-[calc(100vh-120px)] flex-col gap-2">
      {/* ── Compact Toolbar ─────────────────────────────────────────── */}
      <div className="card card-body flex flex-wrap items-center gap-2 py-2">
        <div className="flex items-center gap-2">
          <input className="input w-48 text-sm" value={pipelineName} onChange={(e) => setPipelineName(e.target.value)} placeholder={t('design.name')} />
          {editTarget && <span className="badge badge-amber text-xs">✏️ {t('dag.editing').replace('{name}', editTarget)}</span>}
        </div>
        <div className="h-5 w-px bg-slate-200" />
        {/* Node palette — icon-only compact */}
        <div className="flex items-center gap-0.5">
          {NODE_PALETTE(t).map((cat) => cat.nodes.map((nd) => {
            const st = KIND_STYLES[nd.kind] || KIND_STYLES.transform;
            return (
              <button key={nd.kind} className="btn btn-secondary btn-sm flex items-center gap-1 px-2" title={`${cat.catLabel}: ${nd.label}`} onClick={() => addNode(nd.kind, nd.defaultPlugin)}>
                <span style={{ color: st.color }}>{st.icon}</span>
                <span className="text-xs">{nd.label}</span>
              </button>
            );
          }))}
        </div>
        <button className="btn btn-danger btn-sm px-2" onClick={deleteSelected} disabled={!selectedNodeId} title={t('dag.deleteNode')}>🗑</button>
        {/* Drawer tabs */}
        <div className="flex items-center gap-0.5">
          <button className={`btn btn-sm px-2 ${drawerTab === 'schedule' ? 'btn-primary' : 'btn-secondary'}`} onClick={() => toggleDrawer('schedule')} title={t('nav.schedules')}>📅</button>
          <button className={`btn btn-sm px-2 ${drawerTab === 'hooks' ? 'btn-primary' : 'btn-secondary'}`} onClick={() => toggleDrawer('hooks')} title={t('drawer.hooks')}>🪝</button>
          <button className={`btn btn-sm px-2 ${drawerTab === 'advanced' ? 'btn-primary' : 'btn-secondary'}`} onClick={() => toggleDrawer('advanced')} title={t('drawer.advanced')}>⚙️</button>
          <button className={`btn btn-sm px-2 ${drawerTab === 'ai' ? 'btn-primary' : 'btn-secondary'}`} onClick={() => toggleDrawer('ai')} title={t('drawer.ai')}>🤖</button>
          <button className="btn btn-secondary btn-sm px-2" onClick={() => { exportYaml(); setDrawerTab('yaml'); }} title={t('dag.exportYaml')}>📄</button>
          <button className="btn btn-secondary btn-sm px-2" onClick={() => validateCurrentSpec().catch(() => {})} title="Validate + preflight" data-testid="dag-validate-preflight">✓</button>
          <button className="btn btn-secondary btn-sm px-2" onClick={testNodeConnection} title={t('dag.testConnection')} disabled={!selectedNode}>🔌</button>
        </div>
        {testResult && (
          <span className={`text-xs ${testResult.startsWith('✅') ? 'text-emerald-600' : testResult.startsWith('⏳') ? 'text-amber-600' : 'text-rose-600'}`}>{testResult}</span>
        )}
        <div className="ml-auto flex gap-2">
          {editTarget ? (
            <>
              <button className="btn btn-primary btn-sm" onClick={validateAndCreate}>✏️ {t('dag.updatePipeline')}</button>
              <button className="btn btn-warning btn-sm" onClick={resetCheckpointAndUpdate}>↻ {t('dag.updateResetCheckpoint')}</button>
            </>
          ) : (
            <button className="btn btn-primary btn-sm" onClick={validateAndCreate}>✓ {t('dag.createPipeline')}</button>
          )}
        </div>
      </div>

      {(validateResult || validateError) && (
        <div data-testid="dag-validate-result" className={`rounded-lg border px-3 py-2 text-xs ${validateError || validateResult?.valid === false ? 'border-rose-200 bg-rose-50 text-rose-800' : 'border-emerald-200 bg-emerald-50 text-emerald-800'}`}>
          <div className="font-semibold">{validateError || validateResult?.valid === false ? 'Validation failed' : 'Validation passed'} · {validateResult?.preflight?.summary || 'spec checked'}</div>
          {validateError && <div className="mt-1 whitespace-pre-wrap">{validateError}</div>}
          {(validateResult?.warnings || validateResult?.errors || []).map((msg, i) => <div key={i} className="mt-1">{msg}</div>)}
          {(validateResult?.preflight?.issues || []).map((issue, i) => (
            <div key={`issue-${i}`} className="mt-2 rounded border border-white/70 bg-white/70 p-2">
              <div className="font-semibold">{issue.level} · {issue.check}</div>
              <div>{issue.message}</div>
              {issue.remediation && <div className="mt-1 text-slate-600">Fix: {issue.remediation}</div>}
            </div>
          ))}
          {(validateResult?.preflight?.field_issues || []).map((issue, i) => (
            <div key={`field-${i}`} className="mt-2 rounded border border-white/70 bg-white/70 p-2">
              <div className="font-semibold">{issue.field} · {issue.check}</div>
              <div>{issue.message}</div>
              {issue.remediation && <div className="mt-1 text-slate-600">Fix: {issue.remediation}</div>}
            </div>
          ))}
        </div>
      )}

      {/* ── Main Area: Canvas + Drawer ──────────────────────────────── */}
      <div className="flex min-h-0 flex-1 gap-2">
        {/* DAG Canvas — primary focus, fills available space */}
        <div className={`card relative overflow-hidden ${drawerTab ? 'flex-1' : 'flex-1'}`}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeClick={(_, node) => { setSelectedNodeId(node.id); setShowNodeProps(true); }}
            onPaneClick={() => { setSelectedNodeId(null); setShowNodeProps(false); }}
            nodeTypes={nodeTypes}
            fitView
            defaultEdgeOptions={{
              animated: true,
              markerEnd: { type: MarkerType.ArrowClosed },
              style: { stroke: '#94a3b8', strokeWidth: 2 },
            }}
          >
            <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#cbd5e1" />
            <Controls showInteractive={false} position="bottom-left" />
            <MiniMap
              nodeColor={(n) => KIND_STYLES[(n.data as DAGNodeData)?.kind]?.color || '#94a3b8'}
              nodeStrokeWidth={2}
              maskColor="rgba(0,0,0,0.05)"
              position="bottom-right"
            />
          </ReactFlow>

          {/* ── Node Properties Floating Overlay ──────────────────── */}
          {showNodeProps && selectedNode && (
            <div className="absolute right-3 top-3 z-20 w-72 max-h-[calc(100%-24px)] overflow-y-auto rounded-xl border border-slate-200 bg-white shadow-lg">
              <div className="flex items-center justify-between border-b border-slate-100 px-3 py-2">
                <div className="flex items-center gap-2">
                  <span className={`badge ${selKind === 'source' ? 'badge-cyan' : selKind === 'sink' ? 'badge-emerald' : 'badge-violet'}`}>{selKind}</span>
                  <span className="text-sm font-semibold text-slate-700">{selectedNode.id}</span>
                </div>
                <button className="text-xs text-slate-400 hover:text-slate-600" onClick={() => setShowNodeProps(false)}>✕</button>
              </div>
              <div className="space-y-3 p-3">
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-500">{t('dag.nodeId')}</label>
                  <input className="input w-full text-sm" value={selectedNode.data.label || ''} onChange={(e) => updateNodeLabel(e.target.value)} />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-medium text-slate-500">{t('dag.plugin')}</label>
                  {ADVANCED_NODE_KINDS.includes(selKind || '') ? (
                    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-medium text-slate-700">{selPlugin}</div>
                  ) : (
                    <select className="input w-full text-sm" value={selPlugin} onChange={(e) => updateNodePlugin(e.target.value)}>
                      {pluginList.length > 0 ? pluginList.map((p) => <option key={p} value={p}>{p}</option>) : <option value={selPlugin}>{selPlugin}</option>}
                    </select>
                  )}
                </div>
                {nodeSupportsConnection && (
                  <div>
                    <div className="mb-1 flex items-center justify-between">
                      <label className="block text-xs font-medium text-slate-500">{t('conn.useSaved')}</label>
                      {selectedNode.data.connection && (
                        <button className="text-xs font-medium text-indigo-600 hover:text-indigo-700" onClick={() => updateNodeConnection('')}>
                          {t('conn.useInline')}
                        </button>
                      )}
                    </div>
                    {selectedConnection ? (
                      <div className="rounded-lg border border-indigo-200 bg-indigo-50 p-2.5">
                        <div className="flex items-center justify-between gap-2">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-semibold text-indigo-800">{selectedConnection.name}</div>
                            <div className="text-xs text-indigo-600">{selectedConnection.kind} / {selectedConnection.type}</div>
                          </div>
                          <span className={`badge ${selectedConnection.last_status === 'ok' ? 'badge-emerald' : selectedConnection.last_status === 'error' ? 'badge-rose' : 'badge-slate'}`}>
                            {selectedConnection.last_status || 'unknown'}
                          </span>
                        </div>
                        {selectedConnection.last_error && <div className="mt-1 text-xs text-rose-600">{selectedConnection.last_error}</div>}
                      </div>
                    ) : (
                      <button className="w-full rounded-lg border border-dashed border-slate-300 bg-white px-3 py-2 text-left text-sm text-slate-500 hover:border-indigo-300 hover:bg-indigo-50/40" onClick={() => updateNodeConnection('')}>
                        {t('conn.inlineConfig')}
                      </button>
                    )}
                    {matchingConnections.length > 0 ? (
                      <div className="mt-2 max-h-36 space-y-1 overflow-y-auto">
                        {matchingConnections.slice(0, 8).map((conn) => (
                          <button
                            key={conn.name}
                            className={`w-full rounded-lg border px-2.5 py-2 text-left transition ${conn.name === selectedNode.data.connection ? 'border-indigo-400 bg-indigo-50' : conn.type === selPlugin ? 'border-slate-200 bg-white hover:border-indigo-300' : 'border-slate-200 bg-slate-50 hover:border-slate-300'}`}
                            onClick={() => updateNodeConnection(conn.name)}
                          >
                            <div className="flex items-center justify-between gap-2">
                              <span className="truncate text-xs font-semibold text-slate-700">{conn.name}</span>
                              <span className={`badge ${conn.type === selPlugin ? 'badge-indigo' : 'badge-slate'}`}>{conn.type}</span>
                            </div>
                          </button>
                        ))}
                      </div>
                    ) : (
                      <div className="mt-2 rounded-lg bg-slate-50 px-3 py-2 text-xs text-slate-400">{t('conn.noMatchingSaved')}</div>
                    )}
                    {selectedConnectionContext && (
                      <div className={`mt-2 rounded-lg border p-2.5 text-xs ${selectedConnectionContext.introspection?.ok === false ? 'border-rose-200 bg-rose-50 text-rose-700' : 'border-cyan-200 bg-cyan-50 text-slate-600'}`} data-testid="dag-connection-context">
                        <div className="mb-1 flex items-center justify-between">
                          <span className="font-semibold">Context</span>
                          <span className={`badge ${selectedConnectionContext.introspection?.ok === false ? 'badge-rose' : 'badge-blue'}`}>{selectedConnectionContext.introspection?.status || 'ready'}</span>
                        </div>
                        {selectedConnectionContext.introspection?.error && <div className="mb-1 text-rose-700">{selectedConnectionContext.introspection.error}</div>}
                        {selectedConnectionContext.recommendations?.length ? (
                          <div className="mb-1 flex flex-wrap gap-1">
                            {selectedConnectionContext.recommendations.slice(0, 3).map((rec) => <span key={rec.field} className="badge badge-blue text-[10px]">{rec.field}: {String(rec.value || 'review')}</span>)}
                          </div>
                        ) : null}
                        {selectedConnectionContext.introspection?.tables?.length ? (
                          <div className="truncate">Tables: {selectedConnectionContext.introspection.tables.slice(0, 4).map((table) => table.schema ? `${table.schema}.${table.name}` : table.name).join(', ')}</div>
                        ) : null}
                        {selectedConnectionContext.introspection?.topics?.length ? (
                          <div className="truncate">Topics: {selectedConnectionContext.introspection.topics.slice(0, 4).map((topic) => `${topic.name}${topic.partitions?.length ? `(${topic.partitions.length})` : ''}`).join(', ')}</div>
                        ) : null}
                        {(selectedConnectionContext.introspection?.schema || selectedConnectionContext.introspection?.tables?.find((table) => table.columns?.length)?.columns || []).slice(0, 6).map((col) => (
                          <span key={col.name} className="mr-1 mt-1 inline-block rounded bg-white/80 px-1.5 py-0.5 font-mono text-[10px]">{col.name}{col.data_type ? ` ${col.data_type}` : ''}</span>
                        ))}
                      </div>
                    )}
                  </div>
                )}
                <div>
                  <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-400">{t('dag.config')}</label>
                  <ConfigForm fields={schemaFields} config={selectedNode.data.config} onChange={updateNodeConfig} t={t} />
                </div>
              </div>
            </div>
          )}

          {/* ── Empty State Hint ─────────────────────────────────── */}
          {nodes.length === 0 && !selectedNodeId && (
            <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
              <div className="pointer-events-auto rounded-xl border border-dashed border-slate-300 bg-white/80 px-8 py-6 text-center backdrop-blur-sm">
                <div className="mb-2 text-2xl">🎨</div>
                <div className="text-sm font-medium text-slate-600">{t('dag.emptyHint')}</div>
                <div className="mt-1 text-xs text-slate-400">{t('dag.emptyHint2')}</div>
              </div>
            </div>
          )}
        </div>

        {/* ── Right Drawer ───────────────────────────────────────── */}
        {drawerTab && (
          <div className="card w-80 flex-shrink-0 overflow-hidden">
            <div className="flex items-center justify-between border-b border-slate-100 px-4 py-2">
              <h3 className="text-sm font-semibold">
                {drawerTab === 'schedule' && `📅 ${t('nav.schedules')}`}
                {drawerTab === 'hooks' && `🪝 ${t('drawer.hooks')}`}
                {drawerTab === 'advanced' && `⚙️ ${t('drawer.advanced')}`}
                {drawerTab === 'ai' && `🤖 ${t('drawer.ai')}`}
                {drawerTab === 'yaml' && `📄 ${t('design.yamlSpec')}`}
              </h3>
              <button className="text-xs text-slate-400 hover:text-slate-600" onClick={() => setDrawerTab(null)}>✕</button>
            </div>
            <div className="max-h-[calc(100%-44px)] overflow-y-auto p-4">
              {/* Schedule */}
              {drawerTab === 'schedule' && <ScheduleForm schedule={schedule} onChange={setSchedule} t={t} supportedTypes={schedulePolicy.supported} />}

              {/* Hooks */}
              {drawerTab === 'hooks' && (
                <>
                  <p className="mb-3 text-xs text-slate-500">
                    {t('dag.hooksDesc')}
                  </p>
                  <div className="space-y-2">
                    {([
                      { key: 'OnInit', label: t('hook.onInit'), desc: t('dag.hookOnInit') },
                      { key: 'OnPreBatch', label: t('hook.preBatch'), desc: t('dag.hookOnPreBatch') },
                      { key: 'OnPostBatch', label: t('hook.postBatch'), desc: t('dag.hookOnPostBatch') },
                      { key: 'OnError', label: t('hook.onError'), desc: t('dag.hookOnError') },
                      { key: 'OnCheckpoint', label: t('hook.onCheckpoint'), desc: t('dag.hookOnCheckpoint') },
                      { key: 'OnShutdown', label: t('hook.onShutdown'), desc: t('dag.hookOnShutdown') },
                    ] as const).map((hk) => {
                      const h = hooks[hk.key];
                      const enabled = h?.enabled ?? false;
                      return (
                        <div key={hk.key} className={`rounded-lg border p-2.5 ${enabled ? 'border-indigo-200 bg-indigo-50/30' : 'border-slate-200'}`}>
                          <div className="flex items-center justify-between">
                            <div className="flex-1 min-w-0">
                              <span className="text-xs font-semibold">{hk.label}</span>
                              <span className="ml-1.5 text-[10px] text-slate-400">{hk.desc}</span>
                            </div>
                            <label className="flex cursor-pointer items-center gap-1 text-[10px]">
                              <input type="checkbox" checked={enabled} onChange={(e) => updateHook(hk.key, { enabled: e.target.checked })} className="h-3 w-3 rounded border-slate-300 text-indigo-600" />
                              {enabled ? t('ui.on') : t('ui.off')}
                            </label>
                          </div>
                          {enabled && (
                            <div className="mt-2 space-y-1.5">
                              <select className="input w-full py-0.5 text-xs" value={h?.type || 'lua'} onChange={(e) => updateHook(hk.key, { type: e.target.value })}>
                                <option value="lua">Lua (inline)</option>
                                <option value="webhook">Webhook (HTTP)</option>
                              </select>
                              {h?.type === 'lua' ? (
                                <textarea className="input w-full font-mono text-xs" rows={2} placeholder="log('hook fired')" value={h?.code || ''} onChange={(e) => updateHook(hk.key, { code: e.target.value })} />
                              ) : (
                                <input className="input w-full text-xs" placeholder="https://alert-svc/notify" value={h?.name || ''} onChange={(e) => updateHook(hk.key, { name: e.target.value })} />
                              )}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </>
              )}

              {/* Advanced Settings */}
              {drawerTab === 'advanced' && (
                <div className="space-y-4">
                  {/* Parallelism */}
                  <div>
                    <label className="mb-1 block text-xs font-medium text-slate-500">⚡ Parallelism</label>
                    <div className="flex gap-1">
                      <input type="number" className="input w-16 text-sm" min={1} max={64} value={parallelism} onChange={(e) => setParallelism(Math.max(1, parseInt(e.target.value) || 1))} />
                      <select className="input flex-1 text-sm" value={shardStrategy} onChange={(e) => setShardStrategy(e.target.value)}>
                        <option value="round_robin">round_robin</option>
                        <option value="partition">partition (Kafka)</option>
                        <option value="id_range">id_range (MySQL)</option>
                        <option value="table">table (CDC)</option>
                      </select>
                    </div>
                    {parallelism > 1 && (
                      <input className="input mt-1 w-full text-sm" value={shardKey} onChange={(e) => setShardKey(e.target.value)} placeholder="shard key field (optional)" />
                    )}
                    <p className="mt-1 text-xs text-slate-400">{t('dag.parallelInstances').replace('{n}', String(parallelism))}</p>
                  </div>
                  <hr className="border-slate-100" />
                  {/* Batch & Flow Control */}
                  <div className="space-y-2.5">
                    <div>
                      <label className="mb-1 block text-xs font-medium text-slate-500">📦 Batch Size</label>
                      <input type="number" className="input w-full text-sm" min={1} max={100000} value={batchSize} onChange={(e) => setBatchSize(Math.max(1, parseInt(e.target.value) || 1000))} />
                    </div>
                    <div>
                      <label className="mb-1 block text-xs font-medium text-slate-500">⏱ Flush Interval (ms)</label>
                      <input type="number" className="input w-full text-sm" min={100} max={60000} value={flushIntervalMs} onChange={(e) => setFlushIntervalMs(Math.max(100, parseInt(e.target.value) || 1000))} />
                    </div>
                    <div>
                      <label className="mb-1 block text-xs font-medium text-slate-500">💾 Checkpoint Interval (s)</label>
                      <input type="number" className="input w-full text-sm" min={1} max={3600} value={checkpointIntervalSec} onChange={(e) => setCheckpointIntervalSec(Math.max(1, parseInt(e.target.value) || 30))} />
                    </div>
                    <div>
                      <label className="mb-1 block text-xs font-medium text-slate-500">🔄 Backpressure Buffer</label>
                      <input type="number" className="input w-full text-sm" min={1} max={10000} value={backpressureBuffer} onChange={(e) => setBackpressureBuffer(Math.max(1, parseInt(e.target.value) || 100))} />
                    </div>
                  </div>
                  <hr className="border-slate-100" />
                  {/* Tags & Worker Selector */}
                  <div>
                    <label className="mb-1 block text-xs font-medium text-slate-500">🏷 Tags</label>
                    <input className="input w-full text-sm" value={tags} onChange={(e) => setTags(e.target.value)} placeholder="production, critical" />
                  </div>
                  <div>
                    <label className="mb-1 block text-xs font-medium text-slate-500">🎯 Worker Selector</label>
                    <input className="input w-full text-sm" value={workerLabels} onChange={(e) => setWorkerLabels(e.target.value)} placeholder="zone=us-east, gpu=true" />
                  </div>
                </div>
              )}

              {/* AI Generator */}
              {drawerTab === 'ai' && (
                <div className="space-y-3">
                  <textarea
                    className="h-24 w-full rounded-lg border border-slate-300 p-2 text-sm"
                    placeholder={t('dag.aiPlaceholder')}
                    value={aiPrompt}
                    onChange={(e) => setAiPrompt(e.target.value)}
                  />
                  {aiError && <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">{aiError}</div>}
                  <button className="btn btn-primary btn-sm w-full" onClick={aiGenerate} disabled={aiLoading}>
                    {aiLoading ? '⏳ ' + t('dag.generating') : '✨ ' + t('dag.generatePipeline')}
                  </button>
                  {aiResult && (
                    <div className="space-y-2 rounded-lg border border-indigo-100 bg-indigo-50 p-3 text-xs" data-testid="dag-ai-review">
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-semibold text-indigo-900">AI draft review</span>
                        <span className={`badge ${aiResult.validation?.valid === false ? 'badge-rose' : 'badge-blue'}`}>
                          {aiResult.validation?.valid === false ? 'needs fixes' : 'validated'}
                        </span>
                      </div>
                      {aiResult.context_pack_version && <div className="text-slate-500">Context: {aiResult.context_pack_version}</div>}
                      {(aiResult.validation?.errors || []).map((msg, i) => <div key={`ai-err-${i}`} className="rounded border border-rose-200 bg-white/80 p-2 text-rose-700">{msg}</div>)}
                      {(aiResult.validation?.warnings || []).slice(0, 4).map((msg, i) => <div key={`ai-warn-${i}`} className="rounded border border-amber-200 bg-white/80 p-2 text-amber-800">{msg}</div>)}
                      {(aiResult.review?.missing_fields || []).length > 0 && (
                        <div className="rounded border border-rose-200 bg-white/80 p-2">
                          <div className="mb-1 font-semibold text-rose-700">Missing fields</div>
                          {aiResult.review?.missing_fields?.map((item, i) => <div key={i}>{item.kind}/{item.type}.{item.field}{item.secret ? ' (secret)' : ''}: {item.message}</div>)}
                        </div>
                      )}
                      {(aiResult.review?.risk_flags || []).length > 0 && (
                        <div className="rounded border border-amber-200 bg-white/80 p-2">
                          <div className="mb-1 font-semibold text-amber-800">Risks</div>
                          {aiResult.review?.risk_flags?.map((risk, i) => (
                            <div key={i} className={risk.level === 'error' ? 'text-rose-700' : 'text-amber-800'}>
                              {risk.level} · {risk.code}: {risk.message}
                              {risk.remediation ? <span className="block text-slate-500">Fix: {risk.remediation}</span> : null}
                            </div>
                          ))}
                        </div>
                      )}
                      {(aiResult.review?.requires_confirmation || []).length > 0 && (
                        <div className="rounded border border-slate-200 bg-white/80 p-2">
                          <div className="mb-1 font-semibold text-slate-700">Requires confirmation</div>
                          {aiResult.review?.requires_confirmation?.slice(0, 6).map((item, i) => <div key={i}>{item.message}</div>)}
                        </div>
                      )}
                      <div className="grid gap-2">
                        <div>
                          <div className="mb-1 font-semibold text-slate-600">Current YAML</div>
                          <pre className="max-h-28 overflow-auto rounded bg-white p-2 font-mono text-[11px] text-slate-500">{YAML.stringify(buildSpec())}</pre>
                        </div>
                        <div>
                          <div className="mb-1 font-semibold text-slate-600">AI YAML</div>
                          <pre className="max-h-40 overflow-auto rounded bg-white p-2 font-mono text-[11px] text-slate-700">{aiResult.yaml}</pre>
                        </div>
                      </div>
                      <button data-testid="dag-ai-apply" className="btn btn-secondary btn-sm w-full" onClick={applyAiGeneratedSpec}>
                        Apply reviewed draft
                      </button>
                    </div>
                  )}
                  <p className="text-xs text-slate-400">{t('dag.aiDesc')}</p>
                </div>
              )}

              {/* YAML Output */}
              {drawerTab === 'yaml' && (
                <div className="space-y-2">
                  <div className="grid grid-cols-2 gap-2">
                    <button className="btn btn-ghost btn-sm w-full" onClick={() => navigator.clipboard.writeText(yamlOutput)}>📋 {t('design.copy')}</button>
                    <button data-testid="dag-sync-yaml" className="btn btn-secondary btn-sm w-full" onClick={syncYamlToCanvas}>Sync YAML to canvas</button>
                  </div>
                  <textarea data-testid="dag-yaml" className="h-96 w-full rounded-lg border border-slate-200 bg-slate-50 p-2 font-mono text-xs" value={yamlOutput} onChange={(e) => setYamlOutput(e.target.value)} />
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Helper ────────────────────────────────────────────────────────────

async function apiPost(path: string, body: unknown, method: string = 'POST') {
  const token = window.localStorage.getItem('etl_api_token') || '';
  const res = await fetch(path, {
    method,
    headers: { 'Content-Type': 'application/json', ...(token ? { 'X-API-Token': token } : {}) },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function apiGet<T>(path: string): Promise<T> {
  const token = window.localStorage.getItem('etl_api_token') || '';
  const res = await fetch(path, {
    headers: { ...(token ? { 'X-API-Token': token } : {}) },
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

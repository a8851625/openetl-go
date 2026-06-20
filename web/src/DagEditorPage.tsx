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

// ── Types ─────────────────────────────────────────────────────────────

type PluginSchemaField = {
  name: string;
  type: 'string' | 'int' | 'float' | 'bool' | 'string_array' | 'map';
  required?: boolean;
  default?: any;
  description?: string;
  secret?: boolean;
  example?: string;
  enum?: string[];
};

type PluginSchemaResp = {
  sources: Record<string, PluginSchemaField[]>;
  sinks: Record<string, PluginSchemaField[]>;
  transforms: Record<string, PluginSchemaField[]>;
};

type PluginListResp = { sources: string[]; sinks: string[]; transforms: string[] };

type DAGNodeData = {
  kind: string;
  plugin: string;
  config: Record<string, unknown>;
  label: string;
};

type ScheduleConfig = {
  type: string;
  cron?: string;
  interval_sec?: number;
  depends_on?: string[];
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

// Toolbar palette grouped by category
const NODE_PALETTE = (t: (key: string) => string): { category: string; catLabel: string; catColor: string; nodes: { kind: string; label: string; defaultPlugin: string }[] }[] => [
  {
    category: 'io', catLabel: 'I/O', catColor: '#0ea5e9',
    nodes: [
      { kind: 'source', label: t('node.source'), defaultPlugin: 'file' },
      { kind: 'sink', label: t('node.sink'), defaultPlugin: 'file_sink' },
    ],
  },
  {
    category: 'process', catLabel: 'Process', catColor: '#8b5cf6',
    nodes: [
      { kind: 'transform', label: t('node.transform'), defaultPlugin: 'identity' },
    ],
  },
  {
    category: 'flow', catLabel: 'Flow Control', catColor: '#f59e0b',
    nodes: [
      { kind: 'fanout', label: t('node.fanout'), defaultPlugin: 'fanout' },
      { kind: 'router', label: t('node.router'), defaultPlugin: 'router' },
    ],
  },
  {
    category: 'observe', catLabel: 'Observe', catColor: '#06b6d4',
    nodes: [
      { kind: 'tap', label: t('node.tap'), defaultPlugin: 'tap' },
      { kind: 'rate_limiter', label: t('node.rateLimiter'), defaultPlugin: 'rate_limiter' },
    ],
  },
  {
    category: 'enrich', catLabel: 'Enrich', catColor: '#ec4899',
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

// ── Schema-driven Config Form ─────────────────────────────────────────

function ConfigForm({
  fields,
  config,
  onChange,
}: {
  fields: PluginSchemaField[];
  config: Record<string, unknown>;
  onChange: (cfg: Record<string, unknown>) => void;
}) {
  if (!fields || fields.length === 0) {
    return <div className="text-xs text-slate-400">This plugin has no configurable fields.</div>;
  }

  const update = (name: string, value: unknown) => {
    onChange({ ...config, [name]: value });
  };

  return (
    <div className="space-y-2.5">
      {fields.map((f) => {
        const val = config[f.name] ?? f.default ?? '';
        const label = (
          <label className="flex items-center gap-1 text-xs font-medium text-slate-600">
            {f.name}
            {f.required && <span className="text-rose-500">*</span>}
            {f.secret && <span className="text-xs text-amber-500" title="Secret">🔒</span>}
          </label>
        );
        let input: React.ReactNode;
        if (f.enum && f.enum.length > 0) {
          input = (
            <select className="input w-full text-sm" value={String(val)} onChange={(e) => update(f.name, e.target.value)}>
              {f.enum.map((opt) => <option key={opt} value={opt}>{opt}</option>)}
            </select>
          );
        } else if (f.type === 'bool') {
          input = (
            <label className="flex items-center gap-2">
              <input type="checkbox" checked={!!val} onChange={(e) => update(f.name, e.target.checked)} className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500" />
              <span className="text-xs text-slate-500">{val ? 'Enabled' : 'Disabled'}</span>
            </label>
          );
        } else if (f.type === 'int' || f.type === 'float') {
          input = <input type="number" step={f.type === 'float' ? '0.01' : '1'} className="input w-full text-sm" value={val} onChange={(e) => update(f.name, f.type === 'int' ? parseInt(e.target.value) || 0 : parseFloat(e.target.value) || 0)} />;
        } else if (f.type === 'string_array') {
          input = (
            <input
              className="input w-full text-sm"
              value={Array.isArray(val) ? val.join(', ') : String(val)}
              onChange={(e) => update(f.name, e.target.value.split(',').map((s) => s.trim()).filter(Boolean))}
              placeholder="comma, separated, values"
            />
          );
        } else {
          input = <input type={f.secret ? 'password' : 'text'} className="input w-full text-sm" value={String(val)} onChange={(e) => update(f.name, e.target.value)} placeholder={f.example || f.description || ''} />;
        }
        return (
          <div key={f.name}>
            {label}
            <div className="mt-1">{input}</div>
            {f.description && <div className="mt-0.5 text-xs text-slate-400">{f.description}</div>}
          </div>
        );
      })}
    </div>
  );
}

// ── Schedule Config Form ──────────────────────────────────────────────

function ScheduleForm({ schedule, onChange, t }: { schedule: ScheduleConfig; onChange: (s: ScheduleConfig) => void; t: TFunc }) {
  const [cronDesc, setCronDesc] = useState('');
  const types = [
    { value: 'streaming', label: t('sched.streaming') },
    { value: 'cron', label: t('sched.cron') },
    { value: 'periodic', label: t('sched.periodic') },
    { value: 'once', label: t('sched.once') },
  ];

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
  // Drawer: 'schedule' | 'hooks' | 'advanced' | 'ai' | 'yaml' | null
  const [drawerTab, setDrawerTab] = useState<string | null>(null);
  const [aiPrompt, setAiPrompt] = useState('');
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState('');
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

  const testNodeConnection = async () => {
    if (!selectedNode) {
      setTestResult('⚠ ' + t('dag.testSelectNode'));
      return;
    }
    setTestResult('⏳ ' + t('dag.testing'));
    try {
      const kind = selKind === 'source' ? 'source' : selKind === 'sink' ? 'sink' : 'transform';
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

  // Load pipeline when editTarget changes
  useEffect(() => {
    if (!editTarget) return;
    apiGet<{ spec: any }>(`/api/v2/pipelines/${editTarget}/spec`).then((res) => {
      if (res.spec) {
        const spec = res.spec;
        setPipelineName(spec.name);
        const newNodes: Node<DAGNodeData>[] = [];
        const newEdges: Edge[] = [];
        newNodes.push({ id: 'source-1', type: 'pipelineNode', position: { x: 250, y: 50 }, data: { kind: 'source', plugin: spec.source.type, config: spec.source.config || {}, label: 'source-1' } });
        const tfms = spec.transforms || [];
        tfms.forEach((tf: any, i: number) => {
          newNodes.push({ id: `transform-${i + 1}`, type: 'pipelineNode', position: { x: 250, y: 150 + i * 100 }, data: { kind: 'transform', plugin: tf.type, config: tf.config || {}, label: `transform-${i + 1}` } });
        });
        const lastY = 150 + tfms.length * 100;
        newNodes.push({ id: 'sink-1', type: 'pipelineNode', position: { x: 250, y: lastY }, data: { kind: 'sink', plugin: spec.sink?.type || 'file_sink', config: spec.sink?.config || {}, label: 'sink-1' } });
        for (let i = 0; i < newNodes.length - 1; i++) {
          newEdges.push({ id: `e-${i}`, source: newNodes[i].id, target: newNodes[i + 1].id, animated: true, markerEnd: { type: MarkerType.ArrowClosed } });
        }
        setNodes(newNodes);
        setEdges(newEdges);
      }
    }).catch(() => {});
  }, [editTarget]);

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
    setNodes((nds) => nds.map((n) => n.id === selectedNodeId ? { ...n, data: { ...n.data, plugin, config: {} } } : n));
  };

  const updateNodeLabel = (label: string) => {
    if (!selectedNodeId) return;
    setNodes((nds) => nds.map((n) => n.id === selectedNodeId ? { ...n, data: { ...n.data, label } } : n));
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
    const hasComplexTopology = nodes.some((n) =>
      ['fanout', 'router', 'tap', 'rate_limiter', 'enricher', 'lookup'].includes(n.data.kind)
    );
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
    return {
      name: pipelineName,
      source: { type: sourceNode?.data.plugin || 'file', config: sourceNode?.data.config || {} },
      transforms: transforms.map((n) => ({ type: n.data.plugin, config: n.data.config })),
      sink: { type: sinkNode?.data.plugin || 'file_sink', config: sinkNode?.data.config || {} },
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
    const spec = buildSpec();
    const yamlStr = YAML.stringify(spec);
    setYamlOutput(yamlStr);
    return spec;
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
    try {
      const res = await apiPost('/api/v2/ai/generate', { prompt: aiPrompt });
      const yamlStr = (res as any).yaml || '';
      setYamlOutput(yamlStr);
      // Try to parse and load into canvas
      try {
        const parsed = YAML.parse(yamlStr);
        if (parsed?.source?.type) {
          setPipelineName(parsed.name || 'ai-generated');
          const newNodes: Node<DAGNodeData>[] = [];
          newNodes.push({ id: 'source-1', type: 'pipelineNode', position: { x: 250, y: 50 }, data: { kind: 'source', plugin: parsed.source.type, config: parsed.source.config || {}, label: 'source-1' } });
          const tfms = parsed.transforms || [];
          tfms.forEach((tf: any, i: number) => {
            newNodes.push({ id: `transform-${i + 1}`, type: 'pipelineNode', position: { x: 250, y: 150 + i * 100 }, data: { kind: 'transform', plugin: tf.type, config: tf.config || {}, label: `transform-${i + 1}` } });
          });
          const lastY = 150 + tfms.length * 100;
          newNodes.push({ id: 'sink-1', type: 'pipelineNode', position: { x: 250, y: lastY }, data: { kind: 'sink', plugin: parsed.sink?.type || 'file_sink', config: parsed.sink?.config || {}, label: 'sink-1' } });
          // Auto-connect
          const newEdges: Edge[] = [];
          for (let i = 0; i < newNodes.length - 1; i++) {
            newEdges.push({ id: `e-${i}`, source: newNodes[i].id, target: newNodes[i + 1].id, animated: true, markerEnd: { type: MarkerType.ArrowClosed } });
          }
          setNodes(newNodes);
          setEdges(newEdges);
        }
      } catch { /* yaml parse error, just show YAML */ }
      setDrawerTab(null);
    } catch (e) {
      setAiError(e instanceof Error ? e.message : String(e));
    } finally {
      setAiLoading(false);
    }
  };

  const validateAndCreate = () => {
    const spec = buildSpec();
    const hasSource = spec.source?.type || spec.dag?.nodes?.some((n: any) => n.kind === 'source');
    const hasSink = spec.sink?.type || spec.dag?.nodes?.some((n: any) => n.kind === 'sink');
    if (!hasSource || !hasSink) {
      onAction(t('dag.validate'), () => Promise.reject(new Error('DAG needs at least one source and one sink')));
      return;
    }
    if (editTarget) {
      // Update mode: PUT + checkpoint warning
      const doUpdate = () => apiPost('/api/v2/pipelines', { spec, reset_checkpoint: false }, 'PUT');
      onAction(`${t('dag.updatePipeline')}: ${pipelineName}`, doUpdate);
    } else {
      // Create mode: POST
      onAction(`${t('dag.createPipeline')}: ${pipelineName}`, () =>
        apiPost('/api/v2/specs/validate', { spec }).then(() =>
          apiPost('/api/v2/pipelines', { spec }, 'POST')
        )
      );
    }
  };

  const resetCheckpointAndUpdate = () => {
    if (!editTarget) return;
    const spec = buildSpec();
    onAction(`${t('dag.updatePipeline')}: ${pipelineName}`, () =>
      apiPost('/api/v2/pipelines', { spec, reset_checkpoint: true }, 'PUT')
    );
  };

  // ── Schema for selected node ──────────────────────────────────────

  const selectedNode = nodes.find((n) => n.id === selectedNodeId);
  const selKind = selectedNode?.data.kind;
  const selPlugin = selectedNode?.data.plugin;
  // Advanced node kinds (fanout/router/tap/etc.) are registered as transforms.
  const ADVANCED_KINDS = ['fanout', 'router', 'tap', 'rate_limiter', 'enricher', 'lookup'];
  const pluginList: string[] = selKind === 'source' ? (plugins?.data?.sources || [])
    : selKind === 'sink' ? (plugins?.data?.sinks || [])
    : ADVANCED_KINDS.includes(selKind || '') ? [selKind || '']  // advanced kinds have exactly one plugin (themselves)
    : (plugins?.data?.transforms || []);
  const schemaFields: PluginSchemaField[] = useMemo(() => {
    if (!schema?.data || !selKind || !selPlugin) return [];
    const kindKey = selKind === 'source' ? 'sources' : selKind === 'sink' ? 'sinks' : 'transforms';
    return (schema.data[kindKey]?.[selPlugin] || []) as PluginSchemaField[];
  }, [schema, selKind, selPlugin]);

  // Toggle drawer: clicking same tab again closes it
  const toggleDrawer = (tab: string) => setDrawerTab((prev) => (prev === tab ? null : tab));

  return (
    <div className="flex h-[calc(100vh-120px)] flex-col gap-2">
      {/* ── Compact Toolbar ─────────────────────────────────────────── */}
      <div className="card card-body flex flex-wrap items-center gap-2 py-2">
        <div className="flex items-center gap-2">
          <input className="input w-48 text-sm" value={pipelineName} onChange={(e) => setPipelineName(e.target.value)} placeholder={t('design.name')} />
          {editTarget && <span className="badge badge-amber text-xs">✏️ {t('dag.editing')}</span>}
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
                  <select className="input w-full text-sm" value={selPlugin} onChange={(e) => updateNodePlugin(e.target.value)}>
                    {pluginList.length > 0 ? pluginList.map((p) => <option key={p} value={p}>{p}</option>) : <option value={selPlugin}>{selPlugin}</option>}
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-semibold uppercase tracking-wide text-slate-400">{t('dag.config')}</label>
                  <ConfigForm fields={schemaFields} config={selectedNode.data.config} onChange={updateNodeConfig} />
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
              {drawerTab === 'schedule' && <ScheduleForm schedule={schedule} onChange={setSchedule} t={t} />}

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
                              {enabled ? 'ON' : 'OFF'}
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
                  <p className="text-xs text-slate-400">{t('dag.aiDesc')}</p>
                </div>
              )}

              {/* YAML Output */}
              {drawerTab === 'yaml' && (
                <div className="space-y-2">
                  <button className="btn btn-ghost btn-sm w-full" onClick={() => navigator.clipboard.writeText(yamlOutput)}>📋 {t('design.copy')}</button>
                  <textarea className="h-96 w-full rounded-lg border border-slate-200 bg-slate-50 p-2 font-mono text-xs" value={yamlOutput} readOnly />
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

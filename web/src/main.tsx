import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import YAML from 'yaml';
import './styles.css';
import { getLang, setLang, translate, type Lang } from './i18n';
import { DagEditorPage } from './DagEditorPage';
import { WorkersPage } from './WorkersPage';
import { MyPluginsPage } from './MyPluginsPage';

// ════════════════════════════════════════════════
// Types
// ════════════════════════════════════════════════
type PipelineStats = {
  records_read: number; records_written: number; records_failed: number; records_dlq: number;
  last_error?: string; last_checkpoint?: string; started_at?: string; uptime?: string;
  bytes_read?: number; bytes_written?: number; dlq_replay_count?: number; dlq_delete_count?: number;
};
type MetricsPipeline = PipelineStats & {
  name: string; status: string;
  checkpoint_age_seconds: number; source_read_latency_ms: number; sink_write_latency_ms: number;
  last_batch_size: number; avg_batch_size: number; batch_count: number; cdc_lag_ms: number;
  dlq_file_count: number;
};
type Pipeline = { name: string; status: string; stats: PipelineStats; parallelism?: number; shard_strategy?: string; shard_count?: number; shards?: { index: number; status: string; stats: PipelineStats }[]; tags?: string[] };
type ShardInfo = { index: number; status: string; stats: PipelineStats; logs?: PipelineLogEntry[]; logs_last_seq?: number };
type PluginResponse = { sources: string[]; sinks: string[]; transforms: string[]; metadata?: Record<string, Record<string, PluginInfo>> };
type PluginInfo = { required?: string[]; capabilities?: string[]; maturity?: string };
type Checkpoint = { id: string; job_name: string; source: string; position: unknown; timestamp: string };
type DLQItem = { job_name: string; record: { operation: string; data: Record<string, unknown> }; error: string; timestamp: string; error_class?: string };
type AuditEvent = { timestamp?: string; action?: string; target?: string; method?: string; path?: string };
type PipelineLogEntry = { timestamp: string; message: string; level: string; seq: number };

// New types for version management, DAG preview, and runtime preview
type PipelineVersion = { id: number; pipeline: string; version: number; spec_yaml: string; created_at: string };
type DAGNode = { id: string; kind: string; plugin: string; config?: Record<string, unknown>; x?: number; y?: number };
type DAGEdge = { id?: string; from: string; to: string; condition?: { field: string; operator: string; value: unknown } };
type DAGResponse = {
  dag: { nodes: DAGNode[]; edges: DAGEdge[] };
  node_configs: { id: string; kind: string; plugin: string; config?: Record<string, unknown> }[];
  schedule?: { type: string; cron?: string };
  execution?: Record<string, unknown>;
  retry?: Record<string, unknown>;
};
type PreviewResponse = {
  stages: Record<string, PipelineLogEntry[]>;
  shard_logs: { shard: number; entries: PipelineLogEntry[] }[];
  total_logs: number;
};

// ════════════════════════════════════════════════
// API helpers
// ════════════════════════════════════════════════
function getToken() { return window.localStorage.getItem('etl_api_token') || ''; }

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  headers.set('Content-Type', headers.get('Content-Type') || 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) throw new Error((await res.text()) || `${res.status} ${res.statusText}`);
  return res.json();
}

function useApi<T>(path: string, refreshKey: number) {
  const [state, setState] = useState<{ data?: T; error?: string; loading: boolean }>({ loading: true });
  const firstRender = useRef(true);
  useEffect(() => {
    let cancelled = false;
    // Only show loading spinner on the very first fetch; on subsequent
    // refreshes, keep existing data visible to avoid UI flicker.
    if (firstRender.current) {
      setState((p) => ({ ...p, loading: true }));
    }
    api<T>(path).then((d) => {
      if (!cancelled) {
        firstRender.current = false;
        setState({ data: d, loading: false });
      }
    }).catch((e) => {
      if (!cancelled) {
        firstRender.current = false;
        setState((p) => ({ ...p, error: e.message, loading: false }));
      }
    });
    return () => { cancelled = true; };
  }, [path, refreshKey]);
  return state;
}

// PipelineUptime displays a live-updating uptime for a single pipeline.
// Must be a separate component so the useLiveUptime hook is called at
// the top level (not inside a .map() callback).
function PipelineUptime({ label, startedAt, fallback }: { label: string; startedAt?: string; fallback: string }) {
  const uptime = useLiveUptime(startedAt);
  return <div className="text-xs text-slate-400">{label} {startedAt ? uptime : fallback}</div>;
}

// PipelineRowMeta shows the compact meta line for a pipeline row with
// live-updating uptime.
function PipelineRowMeta({ t, startedAt, uptimeFallback, written, cdcLagMs }: {
  t: TFunc; startedAt?: string; uptimeFallback: string; written: number; cdcLagMs?: number;
}) {
  const uptime = useLiveUptime(startedAt);
  return (
    <div className="mt-0.5 text-xs text-slate-400">
      {t('dash.uptime')} {startedAt ? uptime : uptimeFallback} · {written} {t('pipe.written')}
      {cdcLagMs && cdcLagMs > 0 ? ` · lag ${cdcLagMs}ms` : ''}
    </div>
  );
}

// LiveUptimeInline returns just the uptime span (no wrapper div) for
// inline display within existing layouts.
function LiveUptimeInline({ startedAt, fallback }: { startedAt?: string; fallback: string }) {
  const uptime = useLiveUptime(startedAt);
  return <>{startedAt ? uptime : fallback}</>;
}
function formatDuration(seconds: number): string {
  if (seconds < 0 || !isFinite(seconds)) return 'N/A';
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

// parseStartedAt extracts a timestamp (ms since epoch) from a pipeline's
// started_at field, which may be an ISO string or a Go time string.
function parseStartedAt(startedAt: string | undefined): number | null {
  if (!startedAt) return null;
  const t = new Date(startedAt).getTime();
  return isNaN(t) ? null : t;
}

// useLiveUptime returns a live-updating duration string computed from the
// given started_at timestamp. Updates every second on the client so the
// displayed uptime is always accurate, not stale from the last API refresh.
function useLiveUptime(startedAt: string | undefined): string {
  const [, force] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => force((n) => n + 1), 1000);
    return () => clearInterval(timer);
  }, []);
  const startMs = parseStartedAt(startedAt);
  if (startMs === null) return 'N/A';
  return formatDuration((Date.now() - startMs) / 1000);
}

// ════════════════════════════════════════════════
// Icons
// ════════════════════════════════════════════════
const Icon = {
  Dashboard: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>,
  Pipeline: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><path d="M3 12h4l3-9 4 18 3-9h4"/></svg>,
  Designer: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><path d="M12 2v20M2 12h20"/><circle cx="12" cy="12" r="3"/></svg>,
  DLQ: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><path d="M3 6h18M8 6V4a2 2 0 012-2h4a2 2 0 012 2v2M19 6l-1 14a2 2 0 01-2 2H8a2 2 0 01-2-2L5 6"/></svg>,
  Plugin: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/><path d="M10 7h4M7 10v4"/></svg>,
  Audit: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/></svg>,
  Gear: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><circle cx="12" cy="12" r="3"/><path d="M19 12a7 7 0 00-.1-1.3l2-1.5-2-3.4-2.4 1a7 7 0 00-2.2-1.3L16 3h-4l-.3 2.5a7 7 0 00-2.2 1.3l-2.4-1-2 3.4 2 1.5A7 7 0 005 12a7 7 0 00.1 1.3l-2 1.5 2 3.4 2.4-1a7 7 0 002.2 1.3L12 21h4l.3-2.5a7 7 0 002.2-1.3l2.4 1 2-3.4-2-1.5A7 7 0 0019 12z"/></svg>,
  Globe: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15 15 0 010 20M12 2a15 15 0 000 20"/></svg>,
  Flow: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><circle cx="5" cy="5" r="2"/><circle cx="19" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><circle cx="19" cy="19" r="2"/><path d="M7 5h10M5 7v10M19 7v10M7 19h10"/></svg>,
  Server: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><rect x="2" y="3" width="20" height="8" rx="2"/><rect x="2" y="13" width="20" height="8" rx="2"/><path d="M6 7h.01M6 17h.01"/></svg>,
  Clock: (p: any) => <svg {...p} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>,
};
const PAGE_ICONS: Record<string, (p: any) => JSX.Element> = {
  dashboard: Icon.Dashboard, pipelines: Icon.Pipeline, designer: Icon.Flow,
  dlq: Icon.DLQ, plugins: Icon.Plugin, audit: Icon.Audit,
  workers: Icon.Server, myPlugins: Icon.Plugin,
};

// ════════════════════════════════════════════════
// App
// ════════════════════════════════════════════════
type Page = 'dashboard' | 'pipelines' | 'designer' | 'dlq' | 'plugins' | 'audit' | 'workers' | 'myPlugins';
type Toast = { id: number; type: 'success' | 'error' | 'info'; msg: string };

function App() {
  const [lang, setLangState] = useState<Lang>(getLang());
  const [page, setPage] = useState<Page>('dashboard');
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedPipeline, setSelectedPipeline] = useState('');
  const [editTarget, setEditTarget] = useState(''); // pipeline name to load into designer
  const [token, setToken] = useState(getToken());
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [showSettings, setShowSettings] = useState(false);
  const [llmConfig, setLLMConfig] = useState({ base_url: '', model: '', api_key: '' });
  const toastId = useRef(0);
  const autoRefresh = useRef(setInterval(() => {}, 99999));

  const t = useCallback((key: string) => translate(key, lang), [lang]);

  const pipelines = useApi<{ pipelines: Pipeline[] }>('/api/v2/pipelines', refreshKey);
  const metrics = useApi<{ pipelines: MetricsPipeline[] }>('/api/v2/metrics', refreshKey);
  const plugins = useApi<PluginResponse>('/api/v2/plugins', refreshKey);
  const pluginSchema = useApi<any>('/api/v2/plugins/schema', refreshKey);
  const checkpoints = useApi<{ checkpoints: Checkpoint[] }>('/api/v2/checkpoints', refreshKey);
  const audit = useApi<{ events: AuditEvent[] }>('/api/v2/audit?limit=50', refreshKey);

  const selected = pipelines.data?.pipelines.find((p) => p.name === selectedPipeline) || pipelines.data?.pipelines[0];
  const selectedMetric = metrics.data?.pipelines.find((p) => p.name === selected?.name);

  const totals = useMemo(() => {
    const list = pipelines.data?.pipelines || [];
    return list.reduce((a, p) => ({
      read: a.read + p.stats.records_read, written: a.written + p.stats.records_written,
      failed: a.failed + p.stats.records_failed, dlq: a.dlq + p.stats.records_dlq,
      running: a.running + (p.status === 'running' ? 1 : 0),
    }), { read: 0, written: 0, failed: 0, dlq: 0, running: 0 });
  }, [pipelines.data]);

  useEffect(() => {
    clearInterval(autoRefresh.current);
    autoRefresh.current = setInterval(() => setRefreshKey((n) => n + 1), 5000);
    return () => clearInterval(autoRefresh.current);
  }, []);

  const toast = useCallback((type: Toast['type'], msg: string) => {
    const id = ++toastId.current;
    setToasts((ts) => [...ts, { id, type, msg }]);
    setTimeout(() => setToasts((ts) => ts.filter((x) => x.id !== id)), 4000);
  }, []);

  const runAction = useCallback(async (label: string, fn: () => Promise<unknown>) => {
    try { await fn(); toast('success', label); setRefreshKey((n) => n + 1); }
    catch (e) { toast('error', `${label}: ${e instanceof Error ? e.message : String(e)}`); }
  }, [toast]);

  const editPipeline = useCallback((name: string) => {
    setEditTarget(name);
    setPage('designer');
  }, []);

  const loadLLMConfig = useCallback(() => {
    api<{ llm_base_url?: string; llm_model?: string; llm_api_key?: string }>('/api/v2/settings')
      .then((d) => setLLMConfig({ base_url: d.llm_base_url || '', model: d.llm_model || '', api_key: d.llm_api_key || '' }))
      .catch(() => {});
  }, []);

  const switchLang = (l: Lang) => { setLangState(l); setLang(l); };

  const navItems: { id: Page; key: string }[] = [
    { id: 'dashboard', key: 'nav.dashboard' },
    { id: 'pipelines', key: 'nav.pipelines' },
    { id: 'designer', key: 'nav.designer' },
    { id: 'dlq', key: 'nav.dlq' },
    { id: 'plugins', key: 'nav.plugins' },
    { id: 'myPlugins', key: 'nav.myPlugins' },
    { id: 'workers', key: 'nav.workers' },
    { id: 'audit', key: 'nav.audit' },
  ];

  return (
    <div className="flex min-h-screen">
      {/* Sidebar */}
      <aside className="fixed inset-y-0 left-0 z-30 flex w-56 flex-col border-r border-slate-200 bg-white">
        <div className="flex h-16 items-center gap-2.5 border-b border-slate-100 px-5">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-indigo-600 text-white">
            <Icon.Pipeline className="h-4 w-4" />
          </div>
          <div>
            <div className="text-sm font-bold text-slate-900">{t('app.title')}</div>
            <div className="text-xs text-slate-400">{t('app.subtitle')}</div>
          </div>
        </div>
        <nav className="flex-1 space-y-1 p-3">
          {navItems.map((item) => {
            const IconComp = PAGE_ICONS[item.id];
            return (
              <div key={item.id} className={`sidebar-item ${page === item.id ? 'active' : ''}`} onClick={() => setPage(item.id)}>
                {IconComp && <IconComp className="sidebar-icon h-4 w-4" />}
                <span>{t(item.key)}</span>
                {item.id === 'pipelines' && totals.running > 0 && (
                  <span className="ml-auto rounded-full bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-700">{totals.running}</span>
                )}
              </div>
            );
          })}
        </nav>
        {/* Settings button — opens modal */}
        <div className="border-t border-slate-100 p-3">
          <div className="sidebar-item cursor-pointer" onClick={() => { setShowSettings(true); loadLLMConfig(); }}>
            <Icon.Gear className="h-4 w-4" />
            <span>{t('nav.settings')}</span>
          </div>
        </div>
      </aside>

      {/* Main */}
      <main className="ml-56 flex-1">
        <header className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-slate-200 bg-white/80 px-8 backdrop-blur">
          <h1 className="text-lg font-semibold text-slate-900">
            {page === 'dlq' ? t('top.dlqWorkbench') : t(`nav.${page}`)}
          </h1>
          <div className="flex items-center gap-3">
            <span className="text-xs text-slate-400">{t('top.autorefresh')}</span>
            <div className={`status-dot ${pipelines.data?.pipelines.some(p => p.status === 'running') ? 'status-running' : 'status-stopped'}`} />
            {/* Quick lang toggle */}
            <button className="btn btn-ghost btn-sm flex items-center gap-1" onClick={() => switchLang(lang === 'en' ? 'zh' : 'en')} title={t('settings.language')}>
              <Icon.Globe className="h-4 w-4" />
              {lang === 'en' ? '中文' : 'EN'}
            </button>
            <button className="btn btn-secondary btn-sm" onClick={() => runAction(t('toast.reloadSpecs'), () => api('/api/v2/specs/reload', { method: 'POST' }))}>
              {t('top.reloadSpecs')}
            </button>
          </div>
        </header>

        {/* Toasts */}
        {toasts.length > 0 && (
          <div className="fixed right-4 top-20 z-50 space-y-2">
            {toasts.map((ts) => (
              <div key={ts.id} className={`toast-enter flex items-center gap-2 rounded-lg px-4 py-3 text-sm shadow-lg ${
                ts.type === 'success' ? 'bg-emerald-600 text-white' : ts.type === 'error' ? 'bg-rose-600 text-white' : 'bg-slate-800 text-white'
              }`}>
                <span>{ts.type === 'success' ? '✓' : ts.type === 'error' ? '✗' : 'ℹ'}</span>
                <span>{ts.msg}</span>
              </div>
            ))}
          </div>
        )}

        <div className="p-8">
          {page === 'dashboard' && <DashboardPage t={t} lang={lang} totals={totals} pipelines={pipelines} metrics={metrics} selected={selected} selectedMetric={selectedMetric} onSelect={setSelectedPipeline} />}
          {page === 'pipelines' && <PipelinesPage t={t} lang={lang} pipelines={pipelines} metrics={metrics} selected={selected} selectedMetric={selectedMetric} onSelect={setSelectedPipeline} onAction={runAction} checkpoints={checkpoints} onResetCheckpoint={(name: string) => runAction(`${t('toast.resetCheckpoint')}: ${name}`, () => api(`/api/v2/pipelines/${name}/checkpoint/reset`, { method: 'POST' }))} onEdit={editPipeline} refreshKey={refreshKey} onShowToast={toast} />}
          {page === 'designer' && <DagEditorPage t={t} lang={lang} plugins={plugins} schema={pluginSchema} onAction={runAction} editTarget={editTarget} />}
          {page === 'dlq' && <DLQPage t={t} lang={lang} pipelines={pipelines} selected={selected} onSelect={setSelectedPipeline} onAction={runAction} />}
          {page === 'plugins' && <PluginsPage t={t} lang={lang} plugins={plugins} />}
          {page === 'myPlugins' && <MyPluginsPage t={t} lang={lang} />}
          {page === 'workers' && <WorkersPage t={t} lang={lang} />}
          {page === 'audit' && <AuditPage t={t} lang={lang} audit={audit} />}
        </div>
      </main>

      {/* Settings Modal */}
      {showSettings && <SettingsModal t={t} lang={lang} token={token} setToken={setToken} switchLang={switchLang} llmConfig={llmConfig} setLLMConfig={setLLMConfig} onClose={() => setShowSettings(false)} onSaveToken={() => { window.localStorage.setItem('etl_api_token', token); setRefreshKey((n) => n + 1); toast('success', t('settings.tokenSaved')); }} onSaveLLM={() => {
        api('/api/v2/settings', { method: 'POST', body: JSON.stringify({ llm_base_url: llmConfig.base_url, llm_model: llmConfig.model, llm_api_key: llmConfig.api_key }) })
          .then(() => toast('success', t('settings.llmSaved')))
          .catch((e) => toast('error', e.message));
      }} />}
    </div>
  );
}

type TFunc = (key: string) => string;

// ════════════════════════════════════════════════
// Settings Modal
// ════════════════════════════════════════════════
function SettingsModal({ t, lang, token, setToken, switchLang, llmConfig, setLLMConfig, onClose, onSaveToken, onSaveLLM }: {
  t: TFunc; lang: Lang; token: string; setToken: (v: string) => void; switchLang: (l: Lang) => void;
  llmConfig: { base_url: string; model: string; api_key: string };
  setLLMConfig: (c: { base_url: string; model: string; api_key: string }) => void;
  onClose: () => void; onSaveToken: () => void; onSaveLLM: () => void;
}) {
  const [tab, setTab] = useState<'general' | 'llm' | 'worker'>('general');
  const [workerLabels, setWorkerLabels] = useState('');

  // Load worker labels from API
  useEffect(() => {
    api<{ labels?: Record<string, string> }>('/api/v2/workers/standalone-worker').then((d: any) => {
      if (d?.labels) setWorkerLabels(Object.entries(d.labels).map(([k, v]) => `${k}=${v}`).join(', '));
    }).catch(() => {});
  }, []);

  const saveWorkerLabels = () => {
    const labels: Record<string, string> = {};
    workerLabels.split(',').forEach((pair) => {
      const [k, v] = pair.trim().split('=');
      if (k && v) labels[k.trim()] = v.trim();
    });
    api('/api/v2/workers', { method: 'POST', body: JSON.stringify({ id: 'standalone-worker', host: 'localhost', port: 0, slots: 4, labels }) })
      .then(() => alert(t('settings.workerLabelsSaved')))
      .catch((e) => alert(e.message));
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm" onClick={onClose}>
      <div className="w-full max-w-2xl rounded-2xl bg-white shadow-2xl" onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-6 py-4">
          <h2 className="text-base font-semibold text-slate-900">{t('nav.settings')}</h2>
          <button className="btn btn-ghost btn-sm" onClick={onClose}>✕</button>
        </div>
        {/* Tabs */}
        <div className="flex gap-1 border-b border-slate-200 px-6">
          {([
            { id: 'general', label: t('settings.tabGeneral') },
            { id: 'llm', label: t('settings.tabLLM') },
            { id: 'worker', label: t('settings.tabWorker') },
          ] as const).map((tb) => (
            <button key={tb.id} className={`border-b-2 px-4 py-2.5 text-sm font-medium transition-colors ${tab === tb.id ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-slate-500 hover:text-slate-700'}`} onClick={() => setTab(tb.id)}>
              {tb.label}
            </button>
          ))}
        </div>
        {/* Content */}
        <div className="max-h-[60vh] overflow-y-auto px-6 py-5">
          {tab === 'general' && (
            <div className="space-y-5">
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-700">{t('settings.language')}</label>
                <div className="flex gap-2">
                  <button className={`btn btn-sm flex-1 ${lang === 'en' ? 'btn-primary' : 'btn-secondary'}`} onClick={() => switchLang('en')}>English</button>
                  <button className={`btn btn-sm flex-1 ${lang === 'zh' ? 'btn-primary' : 'btn-secondary'}`} onClick={() => switchLang('zh')}>中文</button>
                </div>
              </div>
              <div>
                <label className="mb-2 block text-sm font-medium text-slate-700">{t('settings.apiToken')}</label>
                <input className="input w-full" value={token} onChange={(e) => setToken(e.target.value)} placeholder={t('settings.tokenPlaceholder')} />
                <button className="btn btn-secondary btn-sm mt-2" onClick={onSaveToken}>{t('settings.saveToken')}</button>
              </div>
            </div>
          )}
          {tab === 'llm' && (
            <div className="space-y-4">
              <p className="text-sm text-slate-500">{t('settings.llmDesc')}</p>
              <div>
                <label className="mb-1.5 block text-sm font-medium text-slate-700">{t('settings.llmBaseUrl')}</label>
                <input className="input w-full" value={llmConfig.base_url} onChange={(e) => setLLMConfig({ ...llmConfig, base_url: e.target.value })} placeholder="https://api.openai.com/v1" />
              </div>
              <div>
                <label className="mb-1.5 block text-sm font-medium text-slate-700">{t('settings.llmModel')}</label>
                <input className="input w-full" value={llmConfig.model} onChange={(e) => setLLMConfig({ ...llmConfig, model: e.target.value })} placeholder="gpt-4o" />
              </div>
              <div>
                <label className="mb-1.5 block text-sm font-medium text-slate-700">{t('settings.llmApiKey')}</label>
                <input className="input w-full" type="password" value={llmConfig.api_key} onChange={(e) => setLLMConfig({ ...llmConfig, api_key: e.target.value })} placeholder="sk-..." />
              </div>
              <div className="rounded-lg bg-indigo-50 px-4 py-3 text-xs text-indigo-600">
                💡 {t('settings.llmHint')}
              </div>
              <button className="btn btn-primary" onClick={onSaveLLM}>{t('settings.llmSave')}</button>
            </div>
          )}
          {tab === 'worker' && (
            <div className="space-y-4">
              <p className="text-sm text-slate-500">{t('settings.workerDesc')}</p>
              <div>
                <label className="mb-1.5 block text-sm font-medium text-slate-700">{t('settings.workerLabels')}</label>
                <input className="input w-full" value={workerLabels} onChange={(e) => setWorkerLabels(e.target.value)} placeholder="zone=us-east, gpu=true, highmem=true" />
                <p className="mt-1 text-xs text-slate-400">{t('settings.workerLabelsHint')}</p>
              </div>
              <button className="btn btn-primary" onClick={saveWorkerLabels}>{t('settings.workerLabelsSave')}</button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════
// Dashboard
// ════════════════════════════════════════════════
function DashboardPage({ t, totals, pipelines, selected, selectedMetric, onSelect }: { t: TFunc; lang: Lang; totals: any; pipelines: any; metrics: any; selected: any; selectedMetric: any; onSelect: (n: string) => void }) {
  const pList = pipelines.data?.pipelines || [];
  const runningCount = pList.filter((p: Pipeline) => p.status === 'running').length;
  const failedCount = pList.filter((p: Pipeline) => p.status === 'failed').length;
  const health = pList.length > 0 ? Math.round((runningCount / pList.length) * 100) : 100;
  const cards = [
    { label: t('dash.running'), value: totals.running, sub: `${pList.length} ${t('dash.totalPipelines')} · ${health}% healthy`, color: 'text-emerald-600', dot: 'bg-emerald-500', icon: '🟢' },
    { label: t('dash.recordsRead'), value: totals.read, sub: t('dash.allTime'), color: 'text-blue-600', dot: 'bg-blue-500', icon: '📖' },
    { label: t('dash.recordsWritten'), value: totals.written, sub: t('dash.allTime'), color: 'text-indigo-600', dot: 'bg-indigo-500', icon: '✅' },
    { label: t('dash.failed'), value: totals.failed, sub: totals.failed > 0 ? `${failedCount} pipeline(s) failed` : t('dash.healthy'), color: totals.failed > 0 ? 'text-rose-600' : 'text-slate-600', dot: totals.failed > 0 ? 'bg-rose-500' : 'bg-slate-300', icon: totals.failed > 0 ? '🚨' : '✅' },
    { label: t('dash.dlq'), value: totals.dlq, sub: totals.dlq > 0 ? t('dash.needsAttention') : t('dash.empty'), color: totals.dlq > 0 ? 'text-amber-600' : 'text-slate-600', dot: totals.dlq > 0 ? 'bg-amber-500' : 'bg-slate-300', icon: totals.dlq > 0 ? '📦' : '✓' },
  ];
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-5">
        {cards.map((c) => (
          <div key={c.label} className="card card-body hover:shadow-md transition-shadow">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{c.label}</span>
              <span className="text-lg">{c.icon}</span>
            </div>
            <div className={`mt-2 text-3xl font-bold ${c.color}`}>{c.value.toLocaleString()}</div>
            <div className="mt-1 text-xs text-slate-400">{c.sub}</div>
          </div>
        ))}
      </div>
      <div className="grid gap-6 lg:grid-cols-3">
        <div className="card lg:col-span-2">
          <div className="card-header"><h2 className="text-sm font-semibold">{t('dash.pipelineOverview')}</h2></div>
          <div className="card-body space-y-2">
            {pList.map((p: Pipeline) => (
              <div key={p.name} className={`pipeline-row ${selected?.name === p.name ? 'selected' : ''}`} onClick={() => onSelect(p.name)}>
                <span className={`status-dot status-${p.status}`} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 truncate">
                    <span className="text-sm font-medium">{p.name}</span>
                    <span className={`badge text-[10px] px-1.5 ${
                      p.status === 'running' ? 'badge-emerald' : p.status === 'failed' ? 'badge-rose' : p.status === 'completed' ? 'badge-blue' : 'badge-slate'
                    }`}>{p.status === 'running' ? 'Running' : p.status === 'failed' ? 'Failed' : p.status === 'completed' ? 'Done' : p.status}</span>
                  </div>
                  <PipelineUptime label={t('dash.uptime')} startedAt={p.stats.started_at} fallback={p.stats.uptime || t('common.na')} />
                </div>
                <div className="hidden items-center gap-2 sm:flex">
                  <span className="badge badge-blue">{p.stats.records_written} {t('pipe.written')}</span>
                  {p.stats.records_failed > 0 && <span className="badge badge-rose">{p.stats.records_failed} {t('dash.failed')}</span>}
                  {p.stats.records_dlq > 0 && <span className="badge badge-amber">{p.stats.records_dlq} {t('dash.dlq')}</span>}
                </div>
              </div>
            ))}
            {!pList.length && <Empty text={t('dash.noPipelines')} />}
          </div>
        </div>
        <div className="card">
          <div className="card-header"><h2 className="text-sm font-semibold">{t('dash.keyMetrics')} {selected?.name ? `· ${selected.name}` : ''}</h2></div>
          <div className="card-body space-y-4">
            {selectedMetric ? (
              <>
                <Progress label={t('metric.writeSuccess')} value={ratio(selectedMetric.records_written, selectedMetric.records_written + selectedMetric.records_failed)} />
                <div className="grid grid-cols-2 gap-3">
                  <Mini label={t('metric.readLatency')} value={`${selectedMetric.source_read_latency_ms.toFixed(1)}ms`} />
                  <Mini label={t('metric.writeLatency')} value={`${selectedMetric.sink_write_latency_ms.toFixed(1)}ms`} />
                  <Mini label={t('metric.avgBatch')} value={String(selectedMetric.avg_batch_size)} />
                  <Mini label={t('metric.cpAge')} value={`${selectedMetric.checkpoint_age_seconds}s`} />
                  {selectedMetric.cdc_lag_ms > 0 && <Mini label={t('metric.cdcLag')} value={`${selectedMetric.cdc_lag_ms}ms`} />}
                  <Mini label={t('metric.dlqFiles')} value={String(selectedMetric.dlq_file_count)} />
                </div>
              </>
            ) : <Empty text={t('dash.selectPipeline')} />}
          </div>
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════
// Reusable Modal
// ════════════════════════════════════════════════
function Modal({ title, onClose, children, width = 'max-w-3xl' }: { title: string; onClose: () => void; children: React.ReactNode; width?: string }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm" onClick={onClose}>
      <div className={`w-full ${width} rounded-2xl bg-white shadow-2xl flex flex-col max-h-[85vh]`} onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between border-b border-slate-200 px-6 py-4 shrink-0">
          <h2 className="text-base font-semibold text-slate-900">{title}</h2>
          <button className="btn btn-ghost btn-sm" onClick={onClose}>✕</button>
        </div>
        <div className="overflow-y-auto px-6 py-5 flex-1">{children}</div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════
// Pipeline Log Modal (independent modal, co-located with start/stop)
// ════════════════════════════════════════════════
function PipelineLogModal({ t, name, onClose }: { t: (k: string) => string; name: string; onClose: () => void }) {
  return (
    <Modal title={`${t('pipe.logs')} · ${name}`} onClose={onClose} width="max-w-4xl">
      <div style={{ height: '60vh' }}>
        <PipelineLogDrawer t={t} name={name} />
      </div>
    </Modal>
  );
}

// ════════════════════════════════════════════════
// Pipeline Runtime Preview Modal (node-level data)
// ════════════════════════════════════════════════
function PipelinePreviewModal({ t, name, onClose }: { t: (k: string) => string; name: string; onClose: () => void }) {
  const { data, error, loading } = useApi<PreviewResponse>(`/api/v2/pipelines/${name}/preview`, 0);

  const renderStage = (title: string, entries?: PipelineLogEntry[]) => (
    <div className="card">
      <div className="card-header py-2"><h3 className="text-xs font-semibold">{title}</h3></div>
      <div className="card-body p-0 max-h-48 overflow-y-auto bg-slate-900 font-mono text-xs">
        {entries && entries.length > 0 ? entries.map((e, i) => (
          <div key={i} className="flex gap-2 px-3 py-1 hover:bg-white/5 border-b border-slate-800">
            <span className="text-slate-500 shrink-0">{e.timestamp.slice(11, 23)}</span>
            <span className={`shrink-0 w-10 ${e.level === 'ERROR' ? 'text-rose-400' : e.level === 'WARN' ? 'text-amber-300' : 'text-emerald-400'}`}>{e.level}</span>
            <span className="text-slate-300 truncate">{e.message}</span>
          </div>
        )) : <div className="text-slate-600 text-center py-6">—</div>}
      </div>
    </div>
  );

  return (
    <Modal title={`${t('pipe.previewTitle')} · ${name}`} onClose={onClose} width="max-w-4xl">
      {loading ? <Empty text={t('common.loading')} /> :
       error ? <ErrorBox message={error} /> :
       !data || (data.total_logs === 0 && (data.shard_logs?.length ?? 0) === 0) ? <Empty text={t('pipe.noPreview')} /> :
        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {renderStage(t('pipe.previewSource'), data.stages?.source)}
            {renderStage(t('pipe.previewTransform'), data.stages?.transform)}
            {renderStage(t('pipe.previewSink'), data.stages?.sink)}
          </div>
          <div className="text-xs text-slate-400">{t('pipe.previewShardLogs')} ({data.shard_logs?.length || 0})</div>
          {data.shard_logs?.map((s, i) => (
            <div key={i} className="card">
              <div className="card-header py-2"><h3 className="text-xs font-semibold">#{s.shard}</h3></div>
              <div className="card-body p-0 max-h-32 overflow-y-auto bg-slate-900 font-mono text-xs">
                {s.entries?.map((e, j) => (
                  <div key={j} className="flex gap-2 px-3 py-1 border-b border-slate-800">
                    <span className="text-slate-500">{e.timestamp.slice(11, 23)}</span>
                    <span className="text-slate-300">{e.message}</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      }
    </Modal>
  );
}

// ════════════════════════════════════════════════
// Pipeline DAG + Logs Combined Modal (replaces Preview + DAG)
// ════════════════════════════════════════════════
function PipelineDAGModal({ t, name, onClose }: { t: (k: string) => string; name: string; onClose: () => void }) {
  const { data, error, loading } = useApi<DAGResponse>(`/api/v2/pipelines/${name}/dag`, 0);
  const [selectedNode, setSelectedNode] = useState<DAGNode | null>(null);
  const [tab, setTab] = useState<'dag' | 'logs'>('dag');

  const kindColor: Record<string, string> = {
    source:       'bg-sky-100 text-sky-700 border-sky-300',
    transform:    'bg-violet-100 text-violet-700 border-violet-300',
    sink:         'bg-emerald-100 text-emerald-700 border-emerald-300',
    fanout:       'bg-amber-100 text-amber-700 border-amber-300',
    router:       'bg-rose-100 text-rose-700 border-rose-300',
    tap:          'bg-cyan-100 text-cyan-700 border-cyan-300',
    rate_limiter: 'bg-lime-100 text-lime-700 border-lime-300',
    enricher:     'bg-pink-100 text-pink-700 border-pink-300',
    lookup:       'bg-purple-100 text-purple-700 border-purple-300',
  };

  return (
    <Modal title={`${t('pipe.dagTitle')} · ${name}`} onClose={onClose} width="max-w-5xl">
      {/* Tabs */}
      <div className="mb-4 flex gap-1 border-b border-slate-200">
        <button className={`border-b-2 px-4 py-2 text-sm font-medium transition-colors ${tab === 'dag' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-slate-500 hover:text-slate-700'}`} onClick={() => setTab('dag')}>
          🔀 {t('pipe.dag')}
        </button>
        <button className={`border-b-2 px-4 py-2 text-sm font-medium transition-colors ${tab === 'logs' ? 'border-indigo-600 text-indigo-600' : 'border-transparent text-slate-500 hover:text-slate-700'}`} onClick={() => setTab('logs')}>
          📋 {t('pipe.logs')}
        </button>
      </div>

      {tab === 'dag' ? (
        loading ? <Empty text={t('common.loading')} /> :
        error ? <ErrorBox message={error} /> : !data ? <Empty text={t('ui.noData')} /> : (
          <div className="grid grid-cols-1 lg:grid-cols-[1fr_320px] gap-4">
            {/* DAG visualization */}
            <div className="space-y-3">
              <div className="flex gap-4 text-xs">
                <span>📊 {data.dag.nodes?.length || 0} {t('pipe.dagNodes')}</span>
                <span>🔗 {data.dag.edges?.length || 0} {t('pipe.dagEdges')}</span>
                {data.schedule && <span>⏰ {data.schedule.type}{data.schedule.cron ? `: ${data.schedule.cron}` : ''}</span>}
              </div>
              {/* Nodes */}
              <div className="flex flex-wrap gap-2">
                {(data.dag.nodes || []).map((n) => (
                  <div key={n.id}
                    className={`cursor-pointer rounded-lg border-2 px-3 py-2 text-xs font-medium transition-all hover:shadow-md ${kindColor[n.kind] || 'bg-slate-100 border-slate-300'} ${selectedNode?.id === n.id ? 'ring-2 ring-indigo-400' : ''}`}
                    onClick={() => setSelectedNode(n)}>
                    <div className="font-bold">{n.id}</div>
                    <div className="opacity-70">{n.kind} · {n.plugin}</div>
                  </div>
                ))}
              </div>
              {/* Edges */}
              {(data.dag.edges || []).length > 0 && (
                <div className="card">
                  <div className="card-header py-2"><h3 className="text-xs font-semibold">{t('pipe.dagEdges')}</h3></div>
                  <div className="card-body space-y-1">
                    {(data.dag.edges || []).map((e, i) => (
                      <div key={i} className="text-xs text-slate-600">
                        <span className="font-mono">{e.from}</span>
                        <span className="mx-2 text-slate-400">→</span>
                        <span className="font-mono">{e.to}</span>
                        {e.condition && <span className="ml-2 badge badge-amber text-[10px]">{e.condition.field} {e.condition.operator} {String(e.condition.value)}</span>}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
            {/* Node config inspector */}
            <div className="card">
              <div className="card-header py-2"><h3 className="text-xs font-semibold">{t('pipe.dagConfig')}</h3></div>
              <div className="card-body">
                {selectedNode ? (
                  <div className="space-y-2">
                    <div className="text-sm font-bold text-slate-900">{selectedNode.id}</div>
                    <div className="text-xs text-slate-500">{selectedNode.kind} · {selectedNode.plugin}</div>
                    <pre className="mt-2 overflow-x-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-300 max-h-64">
{JSON.stringify(selectedNode.config || {}, null, 2)}
                    </pre>
                  </div>
                ) : <Empty text={t('pipe.dagNoConfig')} />}
              </div>
            </div>
          </div>
        )
      ) : (
        /* Logs tab — embedded real-time log viewer */
        <div style={{ height: '55vh' }}>
          <PipelineLogDrawer t={t} name={name} />
        </div>
      )}
    </Modal>
  );
}

// ════════════════════════════════════════════════
// Pipeline Version Management Modal
// ════════════════════════════════════════════════
function PipelineVersionsModal({ t, name, onClose, onAction }: { t: (k: string) => string; name: string; onClose: () => void; onAction: (label: string, fn: () => Promise<unknown>) => void }) {
  const { data, error, loading } = useApi<{ versions: PipelineVersion[] }>(`/api/v2/pipelines/${name}/versions`, 0);
  const [diffData, setDiffData] = useState<{ version: number; current: string; historical: string } | null>(null);

  const doRollback = async (version: number) => {
    if (!confirm(t('pipe.confirmRollback').replace('{version}', String(version)))) return;
    onAction(t('pipe.rolledBack').replace('{version}', String(version)), () =>
      api(`/api/v2/pipelines/${name}/versions/${version}/rollback`, { method: 'POST' })
    );
    onClose();
  };

  const doDiff = async (version: number) => {
    try {
      const d = await api<{ version: { version: number; spec_yaml: string }; current: string; historical: string }>(
        `/api/v2/pipelines/${name}/versions/${version}/diff`
      );
      setDiffData({ version: d.version.version, current: d.current, historical: d.version.spec_yaml });
    } catch { /* ignore */ }
  };

  return (
    <Modal title={`${t('pipe.versionsTitle')} · ${name}`} onClose={onClose} width="max-w-4xl">
      {loading ? <Empty text={t('common.loading')} /> :
       error ? <ErrorBox message={error} /> :
       !data?.versions?.length ? <Empty text={t('pipe.noVersions')} /> :
        <div className="space-y-3">
          {diffData && (
            <div className="card border-indigo-200">
              <div className="card-header py-2 flex items-center justify-between">
                <h3 className="text-xs font-semibold">{t('pipe.versionDiff')} · v{diffData.version}</h3>
                <button className="btn btn-ghost btn-sm" onClick={() => setDiffData(null)}>✕</button>
              </div>
              <div className="card-body grid grid-cols-2 gap-3">
                <div>
                  <div className="text-xs font-semibold text-slate-500 mb-1">{t('pipe.diffHistorical')} (v{diffData.version})</div>
                  <pre className="overflow-x-auto rounded-lg bg-rose-50 p-2 text-xs max-h-64 overflow-y-auto">{diffData.historical || '(empty)'}</pre>
                </div>
                <div>
                  <div className="text-xs font-semibold text-slate-500 mb-1">{t('pipe.diffCurrent')}</div>
                  <pre className="overflow-x-auto rounded-lg bg-emerald-50 p-2 text-xs max-h-64 overflow-y-auto">{diffData.current || '(empty)'}</pre>
                </div>
              </div>
            </div>
          )}
          {(data.versions || []).map((v) => (
            <div key={v.version} className="flex items-center justify-between rounded-lg border border-slate-200 bg-slate-50 px-4 py-2.5">
              <div>
                <div className="text-sm font-semibold">v{v.version}</div>
                <div className="text-xs text-slate-400">{fmtTime(v.created_at)}</div>
              </div>
              <div className="flex gap-2">
                <button className="btn btn-secondary btn-sm" onClick={() => doDiff(v.version)}>{t('pipe.versionDiff')}</button>
                <button className="btn btn-danger btn-sm" onClick={() => doRollback(v.version)}>{t('pipe.rollback')}</button>
              </div>
            </div>
          ))}
        </div>
      }
    </Modal>
  );
}

// ════════════════════════════════════════════════
// Spec Import Modal
// ════════════════════════════════════════════════
function SpecImportModal({ t, onClose, onImported }: { t: (k: string) => string; onClose: () => void; onImported: (name: string) => void }) {
  const [yamlText, setYamlText] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');
  const fileRef = useRef<HTMLInputElement>(null);

  const doImport = async () => {
    setBusy(true); setErr('');
    try {
      const res = await fetch('/api/v2/specs/import', {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain', ...(getToken() ? { 'X-API-Token': getToken() } : {}) },
        body: yamlText,
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      onImported(data.name || '');
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => setYamlText(String(reader.result || ''));
    reader.readAsText(file);
  };

  return (
    <Modal title={t('pipe.importTitle')} onClose={onClose} width="max-w-2xl">
      <div className="space-y-4">
        <div>
          <input ref={fileRef} type="file" accept=".yaml,.yml" className="hidden" onChange={onFileChange} />
          <button className="btn btn-secondary btn-sm" onClick={() => fileRef.current?.click()}>📁 {t('pipe.importSelectFile')}</button>
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-slate-500">{t('pipe.importOrPaste')}</label>
          <textarea className="input w-full font-mono text-xs" rows={14} value={yamlText}
            onChange={(e) => setYamlText(e.target.value)}
            placeholder={'name: my-pipeline\nsource:\n  type: file\n  config:\n    path: ./input.csv\nsink:\n  type: file\n  config:\n    path: ./output.csv'} />
        </div>
        {err && <ErrorBox message={err} />}
        <button className="btn btn-primary" onClick={doImport} disabled={busy || !yamlText.trim()}>
          {busy ? '...' : t('pipe.importBtn')}
        </button>
      </div>
    </Modal>
  );
}

// ════════════════════════════════════════════════
// Pipeline Action Dropdown Menu
// ════════════════════════════════════════════════
function PipelineActionMenu({ p, onLogs, onDAG, onEdit, onDelete, onExport }: {
  p: Pipeline;
  onLogs: () => void; onDAG: () => void; onEdit: () => void; onDelete: () => void; onExport: () => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const items = [
    { icon: '📋', label: t('action.logs'), onClick: onLogs },
    { icon: '🔀', label: t('action.dagAndLogs'), onClick: onDAG },
    { icon: '✏️', label: t('action.edit'), onClick: onEdit },
    { icon: '📤', label: t('action.exportYaml'), onClick: onExport },
    { icon: '🗑', label: t('action.delete'), onClick: onDelete, danger: true },
  ];

  return (
    <div className="relative" ref={ref}>
      <button className="btn btn-ghost btn-sm" onClick={() => setOpen(!open)}>
        ⋯<span className="hidden lg:inline"> {t('ui.more')}</span>
      </button>
      {open && (
        <div className="absolute right-0 top-full z-30 mt-1 w-44 rounded-lg border border-slate-200 bg-white py-1 shadow-lg">
          {items.map((item) => (
            <button
              key={item.label}
              className={`flex w-full items-center gap-2.5 px-3 py-1.5 text-left text-sm transition-colors hover:bg-slate-50 ${item.danger ? 'text-rose-600 hover:bg-rose-50' : 'text-slate-700'}`}
              onClick={() => { setOpen(false); item.onClick(); }}
            >
              <span className="w-4 text-center">{item.icon}</span>
              <span>{item.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════
// Pipelines
// ════════════════════════════════════════════════

// Constants hoisted out of the component to avoid re-creation on every render.
const STATUS_BADGE: Record<string, string> = {
  running: 'badge-emerald', completed: 'badge-blue', failed: 'badge-rose',
  stopped: 'badge-slate', starting: 'badge-amber', paused: 'badge-amber',
  error: 'badge-rose',
};
const STATUS_LABEL: Record<string, string> = {
  running: 'Running', completed: 'Completed', failed: 'Failed',
  stopped: 'Stopped', starting: 'Starting...', paused: 'Paused',
  error: 'Error',
};
function statusLabel(t: TFunc, status: string): string {
  const key = `status.${status}`;
  const translated = t(key);
  return translated !== key ? translated : STATUS_LABEL[status] || status;
}

// PipelineRow is memoized so it only re-renders when its props actually change,
// not when the parent PipelinesPage re-renders due to 5s API refresh.
// Custom comparison: compare by value (name, status, key stats) instead of
// object reference, so that a 5s API refresh that returns equivalent data
// does NOT trigger a DOM re-render of unchanged rows.
const PipelineRow = React.memo(function PipelineRow({ p, m, compact, selected, t, onSelect, onAction, onShowLogs, onShowDAG, onEdit, onExport, onDelete }: {
  p: Pipeline; m?: MetricsPipeline; compact: boolean; selected: boolean; t: TFunc;
  onSelect: (n: string) => void; onAction: (msg: string, fn: () => Promise<any>) => void;
  onShowLogs: () => void; onShowDAG: () => void; onEdit: () => void; onExport: () => void; onDelete: () => void;
}) {
  return (
    <div className={`pipeline-row ${compact ? 'py-2' : ''} ${selected ? 'selected' : ''}`} onClick={() => onSelect(p.name)}>
      <span className={`status-dot status-${p.status}`} />
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2 truncate">
          <span className="text-sm font-semibold">{p.name}</span>
          <span className={`badge text-[10px] px-1.5 ${STATUS_BADGE[p.status] || 'badge-slate'}`}>{statusLabel(t, p.status)}</span>
          {p.parallelism && p.parallelism > 1 && <span className="badge badge-purple text-[10px] px-1">×{p.parallelism}</span>}
          {!compact && (p.tags || []).map((tag: string) => <span key={tag} className="badge badge-blue text-[10px] px-1">{tag}</span>)}
        </div>
        {!compact && <PipelineRowMeta t={t} startedAt={p.stats.started_at} uptimeFallback={p.stats.uptime || 'N/A'} written={p.stats.records_written} cdcLagMs={m?.cdc_lag_ms} />}
        {compact && m && m.cdc_lag_ms > 0 && <span className="text-[10px] text-amber-600 ml-1">lag {m.cdc_lag_ms}ms</span>}
      </div>
      <div className="hidden items-center gap-1.5 sm:flex">
        {p.stats.records_failed > 0 && <span className="badge badge-rose text-[10px] px-1">{p.stats.records_failed}</span>}
        {m && m.dlq_file_count > 0 && <span className="badge badge-amber text-[10px] px-1">{m.dlq_file_count}</span>}
        {!compact && p.stats.records_written > 0 && <span className="badge badge-emerald text-[10px] px-1">{p.stats.records_written}</span>}
      </div>
      <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
        <button className={`btn btn-sm ${p.status === 'running' ? 'btn-ghost opacity-40' : 'btn-secondary'}`} disabled={p.status === 'running'} onClick={() => onAction(`Start ${p.name}`, () => api(`/api/v2/pipelines/${p.name}/start`, { method: 'POST' }))}>
          ▶
        </button>
        <button className={`btn btn-sm ${p.status !== 'running' ? 'btn-ghost opacity-40' : 'btn-secondary'}`} disabled={p.status !== 'running'} onClick={() => onAction(`Stop ${p.name}`, () => api(`/api/v2/pipelines/${p.name}/stop`, { method: 'POST' }))}>
          ⏹
        </button>
        <PipelineActionMenu p={p} onLogs={onShowLogs} onDAG={onShowDAG} onEdit={onEdit} onDelete={onDelete} onExport={onExport} />
      </div>
    </div>
  );
}, (prev, next) => {
  // Custom comparison: skip re-render if the data that affects the DOM
  // hasn't meaningfully changed, even if object references differ.
  const p1 = prev.p, p2 = next.p;
  if (p1.name !== p2.name ||
      p1.status !== p2.status ||
      p1.stats.records_written !== p2.stats.records_written ||
      p1.stats.records_failed !== p2.stats.records_failed ||
      p1.stats.started_at !== p2.stats.started_at ||
      p1.parallelism !== p2.parallelism ||
      prev.selected !== next.selected ||
      prev.compact !== next.compact) {
    return false; // re-render
  }
  // Compare metrics that affect display
  const m1 = prev.m, m2 = next.m;
  if ((m1?.cdc_lag_ms || 0) !== (m2?.cdc_lag_ms || 0) ||
      (m1?.dlq_file_count || 0) !== (m2?.dlq_file_count || 0)) {
    return false; // re-render
  }
  return true; // skip re-render — data is equivalent
});

function PipelinesPage({ t, pipelines, metrics, selected, selectedMetric, onSelect, onAction, checkpoints, onResetCheckpoint, onEdit, refreshKey, onShowToast }: any) {
  const [showLogs, setShowLogs] = useState(false);
  const [showDAG, setShowDAG] = useState(false);
  const [showVersions, setShowVersions] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [tagFilter, setTagFilter] = useState('');
  const [search, setSearch] = useState('');
  const [sortKey, setSortKey] = useState('name');
  const [compact, setCompact] = useState(false);

  const allTags = useMemo(() => {
    const s = new Set<string>();
    (pipelines.data?.pipelines || []).forEach((p: Pipeline) => (p.tags || []).forEach((tag) => s.add(tag)));
    return Array.from(s).sort();
  }, [pipelines.data]);

  const filteredPipelines = useMemo(() => {
    let list = pipelines.data?.pipelines || [];
    if (tagFilter) list = list.filter((p: Pipeline) => (p.tags || []).includes(tagFilter));
    if (search) {
      const q = search.toLowerCase();
      list = list.filter((p: Pipeline) => p.name.toLowerCase().includes(q));
    }
    // Sort
    list = [...list].sort((a: Pipeline, b: Pipeline) => {
      const mA = metrics.data?.pipelines.find((x: MetricsPipeline) => x.name === a.name);
      const mB = metrics.data?.pipelines.find((x: MetricsPipeline) => x.name === b.name);
      switch (sortKey) {
        case 'name': return a.name.localeCompare(b.name);
        case 'status': return a.status.localeCompare(b.status);
        case 'written': return (b.stats.records_written || 0) - (a.stats.records_written || 0);
        case 'latency': return ((mB?.sink_write_latency_ms || 0) - (mA?.sink_write_latency_ms || 0));
        case 'uptime': return (b.stats.uptime || '').localeCompare(a.stats.uptime || '');
        default: return 0;
      }
    });
    return list;
  }, [pipelines.data, tagFilter, search, sortKey, metrics.data]);

  useEffect(() => { setShowLogs(false); setShowDAG(false); setShowVersions(false); }, [selected?.name]);

  const handleDelete = (p: Pipeline) => {
    if (!confirm(t('pipe.confirmDelete').replace('{name}', p.name))) return;
    onAction(t('pipe.deleted').replace('{name}', p.name), () =>
      api(`/api/v2/pipelines/${p.name}`, { method: 'DELETE' })
    );
  };

  const handleExport = async (p: Pipeline) => {
    try {
      const token = getToken();
      const headers: Record<string, string> = {};
      if (token) headers['X-API-Token'] = token;
      const res = await fetch(`/api/v2/pipelines/${p.name}/export`, { headers });
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = `${p.name}.yaml`; a.click();
      URL.revokeObjectURL(url);
      onShowToast?.('success', t('pipe.exportDownload').replace('{name}', p.name));
    } catch (e) {
      onShowToast?.('error', String(e));
    }
  };

  const batchAction = (action: 'start' | 'stop', filter: (p: Pipeline) => boolean) => {
    const targets = filteredPipelines.filter(filter);
    if (!targets.length) return;
    onShowToast?.('info', `${action === 'start' ? 'Starting' : 'Stopping'} ${targets.length} pipeline(s)...`);
    targets.forEach((p: Pipeline) => {
      onAction(`${action} ${p.name}`, () => api(`/api/v2/pipelines/${p.name}/${action}`, { method: 'POST' }));
    });
  };

  const loading = pipelines.loading || metrics.loading;
  const runningCount = filteredPipelines.filter((p: Pipeline) => p.status === 'running').length;
  const stoppedCount = filteredPipelines.filter((p: Pipeline) => p.status !== 'running').length;

  return (
    <>
    {showLogs && selected?.name && <PipelineLogModal t={t} name={selected.name} onClose={() => setShowLogs(false)} />}
    {showDAG && selected?.name && <PipelineDAGModal t={t} name={selected.name} onClose={() => setShowDAG(false)} />}
    {showVersions && selected?.name && <PipelineVersionsModal t={t} name={selected.name} onClose={() => setShowVersions(false)} onAction={onAction} />}
    {showImport && <SpecImportModal t={t} onClose={() => setShowImport(false)} onImported={(name: string) => onShowToast?.('success', t('pipe.importSuccess').replace('{name}', name))} />}

    {loading && !pipelines.data && (
      <div className="grid gap-6 xl:grid-cols-[1fr_400px]">
        <div className="card">
          <div className="card-body space-y-3 p-6">
            {[1,2,3,4,5].map((i) => (<div key={i} className="h-14 rounded-lg bg-slate-100 animate-pulse" />))}
          </div>
        </div>
        <div className="space-y-6">
          <div className="card"><div className="card-body p-6"><div className="h-48 rounded-lg bg-slate-100 animate-pulse" /></div></div>
          <div className="card"><div className="card-body p-6"><div className="h-32 rounded-lg bg-slate-100 animate-pulse" /></div></div>
        </div>
      </div>
    )}

    {!loading && (
    <div className="grid gap-6 xl:grid-cols-[1fr_400px]">
      <div className="card" style={{ overflow: 'auto' }}>
        <div className="card-header flex items-center justify-between flex-wrap gap-2">
          <h2 className="text-sm font-semibold">{t('pipe.allPipelines')}</h2>
          <div className="flex items-center gap-2 flex-wrap">
            <input className="input w-36 text-xs py-1.5" placeholder={'🔍 ' + t('pipe.search')} value={search} onChange={(e) => setSearch(e.target.value)} />
            {allTags.length > 0 && (
              <select className="input w-32 text-xs py-1" value={tagFilter} onChange={(e) => setTagFilter(e.target.value)}>
                <option value="">🏷 {t('pipe.allTags')}</option>
                {allTags.map((tag) => <option key={tag} value={tag}>{tag}</option>)}
              </select>
            )}
            <select className="input w-28 text-xs py-1" value={sortKey} onChange={(e) => setSortKey(e.target.value)}>
              <option value="name">{'↕ ' + t('pipe.sortName')}</option>
              <option value="status">{'↕ ' + t('pipe.sortStatus')}</option>
              <option value="written">{'↕ ' + t('pipe.sortWritten')}</option>
              <option value="latency">{'↕ ' + t('pipe.sortLatency')}</option>
              <option value="uptime">{'↕ ' + t('pipe.sortUptime')}</option>
            </select>
            <button className={`btn btn-ghost btn-sm text-xs ${compact ? 'text-indigo-600' : ''}`} onClick={() => setCompact(!compact)} title={t(compact ? 'pipe.expandedMode' : 'pipe.compactMode')}>
              {compact ? '⛶' : '⊞'}
            </button>
            <button className="btn btn-secondary btn-sm" onClick={() => setShowImport(true)}>📥 {t('pipe.import')}</button>
          </div>
        </div>
        {/* Batch actions bar */}
        <div className="flex items-center gap-2 px-4 py-2 border-b border-slate-100 bg-slate-50/50">
          <span className="text-xs text-slate-400">{filteredPipelines.length} {t('pipe.pipelines')}</span>
          <span className="text-[11px] bg-emerald-100 text-emerald-700 px-1.5 py-0.5 rounded-full font-medium">{runningCount} {t('pipe.running')}</span>
          {stoppedCount > 0 && <span className="text-[11px] bg-slate-200 text-slate-600 px-1.5 py-0.5 rounded-full">{stoppedCount} {t('pipe.stopped')}</span>}
          <div className="flex-1" />
          <button className="btn btn-ghost btn-sm text-xs" onClick={() => batchAction('start', (p: Pipeline) => p.status !== 'running')}>{'▶ ' + t('pipe.startAll')}</button>
          <button className="btn btn-ghost btn-sm text-xs" onClick={() => batchAction('stop', (p: Pipeline) => p.status === 'running')}>{'⏹ ' + t('pipe.stopAll')}</button>
        </div>
        <div className="card-body space-y-1.5">
          {filteredPipelines.map((p: Pipeline) => {
            const m = metrics.data?.pipelines.find((x: MetricsPipeline) => x.name === p.name);
            return (
              <PipelineRow
                key={p.name}
                p={p}
                m={m}
                compact={compact}
                selected={selected?.name === p.name}
                t={t}
                onSelect={onSelect}
                onAction={onAction}
                onShowLogs={() => { onSelect(p.name); setShowLogs(true); }}
                onShowDAG={() => { onSelect(p.name); setShowDAG(true); }}
                onEdit={() => onEdit(p.name)}
                onExport={() => handleExport(p)}
                onDelete={() => handleDelete(p)}
              />
            );
          })}
          {!filteredPipelines.length && <Empty text={search ? `No pipelines matching "${search}"` : tagFilter ? `No pipelines with tag "${tagFilter}"` : t('pipe.noPipelines')} />}
        </div>
      </div>

      {/* Details Panel */}
      <div className="space-y-6">
        <div className="card">
          <div className="card-header flex items-center justify-between flex-wrap gap-2">
            <h2 className="text-sm font-semibold">{t('pipe.details')} {selected?.name ? `· ${selected.name}` : ''}</h2>
            <div className="flex gap-1.5 flex-wrap">
              {selected?.name && <button className="btn btn-secondary btn-sm" onClick={() => setShowDAG(true)}>{'🔀 ' + t('pipe.dagBtn')}</button>}
              {selected?.name && <button className="btn btn-secondary btn-sm" onClick={() => setShowVersions(true)}>📜 {t('pipe.versions')}</button>}
            </div>
          </div>
          <div className="card-body">
            {selectedMetric ? (
              <div className="space-y-4">
                {/* Status and uptime bar */}
                {selected && (
                  <div className="flex items-center gap-3 p-3 rounded-xl bg-slate-50 border border-slate-100">
                    <span className={`status-dot status-${selected.status}`} />
                    <div>
                      <div className="text-sm font-semibold">{statusLabel(t, selected.status)}</div>
                      <div className="text-xs text-slate-400">{t('pipe.uptimeLabel')} <LiveUptimeInline startedAt={selected.stats.started_at} fallback={selected.stats.uptime || t('common.na')} /></div>
                    </div>
                    <div className="flex-1" />
                    <div className="text-right text-xs text-slate-400">
                      <div>{t('pipe.readLabel')} {selected.stats.records_read || 0}</div>
                      <div>{t('pipe.writtenLabel')} {selected.stats.records_written || 0}</div>
                    </div>
                  </div>
                )}
                <Progress label={t('metric.writeSuccessRate')} value={ratio(selectedMetric.records_written, selectedMetric.records_written + selectedMetric.records_failed)} />
                <Progress label={t('metric.dlqPressure')} value={ratio(selectedMetric.records_dlq, Math.max(1, selectedMetric.records_read))} danger />
                <div className="grid grid-cols-2 gap-3 border-t border-slate-100 pt-4">
                  <Mini label={t('metric.readLatency')} value={`${selectedMetric.source_read_latency_ms.toFixed(1)}ms`} />
                  <Mini label={t('metric.writeLatency')} value={`${selectedMetric.sink_write_latency_ms.toFixed(1)}ms`} />
                  <Mini label={t('metric.lastBatch')} value={String(selectedMetric.last_batch_size)} />
                  <Mini label={t('metric.avgBatch')} value={String(selectedMetric.avg_batch_size)} />
                  <Mini label={t('metric.batchCount')} value={String(selectedMetric.batch_count)} />
                  <Mini label={t('metric.cpAge')} value={`${selectedMetric.checkpoint_age_seconds}s`} />
                  {selectedMetric.cdc_lag_ms > 0 && <Mini label={t('metric.cdcLag')} value={`${selectedMetric.cdc_lag_ms}ms`} />}
                  <Mini label={t('metric.dlqFiles')} value={String(selectedMetric.dlq_file_count)} />
                </div>
                {selected.stats.last_error && <ErrorBox message={selected.stats.last_error} />}

                {selected.shard_count && selected.shard_count > 1 && (
                  <>
                    <h3 className="text-sm font-semibold pt-2 border-t">{t('pipe.shardsLabel')} ({selected.shard_count})</h3>
                    <ShardsInline t={t} name={selected.name} refreshKey={refreshKey} />
                  </>
                )}
              </div>
            ) : <Empty text={t('dash.selectPipeline')} />}
          </div>
        </div>
        {/* Checkpoints */}
        <div className="card">
          <div className="card-header">
            <h2 className="text-sm font-semibold">{t('pipe.checkpoints')} {selected?.name ? `· ${selected.name}` : ''}</h2>
          </div>
          <div className="card-body space-y-2">
            {selected?.name ? (
              (() => {
                const selCps = (checkpoints.data?.checkpoints || []).filter((cp: Checkpoint) => cp.job_name === selected.name);
                return selCps.length > 0 ? selCps.map((cp: Checkpoint) => (
                  <div key={cp.job_name} className="flex items-center justify-between rounded-lg border border-slate-200 bg-slate-50 px-3 py-2.5">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-medium">{cp.job_name}</div>
                      <div className="text-xs text-slate-400">{cp.source} · {fmtTime(cp.timestamp)}</div>
                    </div>
                    <button className="btn btn-ghost btn-sm" onClick={() => onResetCheckpoint(cp.job_name)}>{t('pipe.reset')}</button>
                  </div>
                )) : <Empty text={t('pipe.noCheckpoints')} />;
              })()
            ) : <Empty text={t('dash.selectPipeline')} />}
          </div>
        </div>
      </div>
    </div>
    )}
    </>
  );
}

// ════════════════════════════════════════════════
// Inline Shards (for details panel)
// ════════════════════════════════════════════════
function ShardsInline({ t, name, refreshKey }: { t: (k: string) => string; name: string; refreshKey: number }) {
  const shards = useApi<{ shards: ShardInfo[] }>(`/api/v2/pipelines/${name}/shards`, refreshKey);
  return (
    <div className="space-y-2">
      {(shards.data?.shards || []).map((s) => (
        <div key={s.index} className="flex items-center justify-between rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
          <div className="flex items-center gap-2">
            <span className="text-xs font-semibold">#{s.index}</span>
            <span className={`status-dot status-${s.status}`} />
            <span className="text-xs text-slate-500">{s.status}</span>
          </div>
          <div className="flex gap-1.5">
            <span className="badge badge-blue text-[10px]">{s.stats.records_written} w</span>
            <span className="badge badge-slate text-[10px]">{s.stats.records_read} r</span>
          </div>
        </div>
      ))}
    </div>
  );
}
// ════════════════════════════════════════════════
// Pipeline Log Drawer (full-height log viewer)
// ════════════════════════════════════════════════
function PipelineLogDrawer({ t, name }: { t: (k: string) => string; name: string }) {
  const [entries, setEntries] = useState<PipelineLogEntry[]>([]);
  const lastSeq = useRef(0);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [paused, setPaused] = useState(false);

  useEffect(() => {
    let active = true;
    const fetchLogs = async () => {
      try {
        const data = await api<{ entries: PipelineLogEntry[]; last_seq: number }>(
          `/api/v2/pipelines/${name}/log?since=${lastSeq.current}`
        );
        if (!active) return;
        if (data.entries?.length) setEntries((prev) => [...prev.slice(-2000), ...data.entries]);
        lastSeq.current = data.last_seq;
      } catch { /* retry */ }
    };
    setEntries([]); lastSeq.current = 0;
    fetchLogs();
    const timer = setInterval(() => { if (!paused) fetchLogs(); }, 1000);
    return () => { active = false; clearInterval(timer); };
  }, [name, paused]);

  useEffect(() => {
    if (!paused && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries, paused]);

  const levelColor = (lvl: string) => {
    switch (lvl) {
      case 'ERROR': return 'text-rose-400';
      case 'WARN':  return 'text-amber-300';
      case 'DEBUG': return 'text-slate-500';
      default:      return 'text-emerald-400';
    }
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-2 px-4 py-1.5 border-b border-slate-100 bg-slate-900">
        <button className={`text-xs px-2 py-0.5 rounded ${paused ? 'bg-amber-600 text-white' : 'bg-slate-700 text-slate-300'}`}
          onClick={() => setPaused(!paused)}>{paused ? '▶ ' + t('log.resume') : '⏸ ' + t('log.pause')}</button>
        <button className="text-xs px-2 py-0.5 rounded bg-slate-700 text-slate-300"
          onClick={() => { setEntries([]); lastSeq.current = 0; }}>{'✕ ' + t('log.clear')}</button>
        <span className="text-xs text-slate-500 ml-auto">{entries.length} {t('log.lines')}</span>
      </div>
      <div ref={scrollRef} className="flex-1 bg-slate-900 text-xs font-mono p-3 overflow-y-auto leading-relaxed">
        {entries.length === 0 ? (
          <div className="text-slate-500 text-center py-10">{t('pipe.noLogs')}</div>
        ) : (
          entries.map((e, i) => (
            <div key={i} className="flex gap-2 hover:bg-white/5">
              <span className="text-slate-600 shrink-0">{e.timestamp.slice(11, 23)}</span>
              <span className={`shrink-0 w-12 ${levelColor(e.level)}`}>{e.level.padEnd(5)}</span>
              <span className={levelColor(e.level)}>{e.message}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
// DLQRow displays a single dead-letter queue entry with expandable record data.
function DLQRow({ item, onDelete, onReplay, t }: { item: DLQItem; onDelete: () => void; onReplay: () => void; t: TFunc }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="rounded-lg border border-slate-200 p-3 hover:border-slate-300 transition">
      <div className="flex items-center gap-3">
        <span className="badge badge-slate">{item.record.operation}</span>
        {item.error_class && <span className="badge badge-amber">{item.error_class}</span>}
        <span className="text-xs text-slate-400">{fmtTime(item.timestamp)}</span>
        <div className="flex-1 min-w-0">
          <div className="truncate text-xs font-mono text-rose-600">{item.error}</div>
        </div>
        <button className="btn btn-ghost btn-sm text-xs" onClick={() => setExpanded(!expanded)}>{expanded ? '▲' : '▼'} {t('dlq.data')}</button>
        <button className="btn btn-secondary btn-sm text-xs" onClick={onReplay} title={t('dlq.replayRecord')}>↻</button>
        <button className="btn btn-danger btn-sm text-xs" onClick={onDelete} title={t('dlq.deleteRecord')}>🗑</button>
      </div>
      {expanded && (
        <div className="mt-2 rounded-lg bg-slate-50 p-3">
          <pre className="text-xs overflow-x-auto whitespace-pre-wrap break-all">{JSON.stringify(item.record.data, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}

// ════════════════════════════════════════════════
// DLQ
// ════════════════════════════════════════════════
function DLQPage({ t, pipelines, selected, onSelect, onAction }: any) {
  const [filter, setFilter] = useState('');
  const [refreshKey, setRefreshKey] = useState(0);
  const query = filter ? `limit=50&contains=${encodeURIComponent(filter)}` : 'limit=50';
  const dlq = useApi<{ items: DLQItem[] }>(selected ? `/api/v2/dlq/${selected.name}?${query}` : '/api/v2/dlq/_missing', selected ? refreshKey : -1);

  const deleteOne = async (item: DLQItem) => {
    try {
      await api(`/api/v2/dlq/${selected.name}?from=${encodeURIComponent(item.timestamp)}&until=${encodeURIComponent(new Date(new Date(item.timestamp).getTime() + 2000).toISOString())}`, { method: 'DELETE' });
      setRefreshKey((n) => n + 1);
    } catch (e) { /* ignore */ }
  };

  return (
    <div className="grid gap-6 xl:grid-cols-[300px_1fr]">
      <div className="card">
        <div className="card-header"><h2 className="text-sm font-semibold">{t('dlq.selectPipeline')}</h2></div>
        <div className="card-body space-y-1">
          {(pipelines.data?.pipelines || []).map((p: Pipeline) => (
            <div key={p.name} className={`pipeline-row ${selected?.name === p.name ? 'selected' : ''} !p-3`} onClick={() => { onSelect(p.name); setRefreshKey(n => n + 1); }}>
              <span className={`status-dot status-${p.status}`} />
              <span className="truncate text-sm">{p.name}</span>
              {p.stats.records_dlq > 0 && <span className="badge badge-rose ml-auto">{p.stats.records_dlq}</span>}
            </div>
          ))}
        </div>
      </div>
      <div className="card">
        <div className="card-header flex items-center justify-between gap-4">
          <h2 className="text-sm font-semibold">{t('dlq.title')} {selected?.name ? `· ${selected.name}` : ''}</h2>
          <div className="flex items-center gap-2">
            <input className="input w-56" placeholder={t('dlq.filter')} value={filter} onChange={(e) => { setFilter(e.target.value); setRefreshKey(n => n + 1); }} />
            <button className="btn btn-secondary btn-sm" onClick={() => { setRefreshKey(n => n + 1); }}>{t('dlq.refresh')}</button>
            <button className="btn btn-secondary btn-sm" disabled={!selected} onClick={() => onAction(`${t('toast.replayDlq')}: ${selected.name}`, () => { const q = filter ? `?contains=${encodeURIComponent(filter)}` : ''; return api(`/api/v2/dlq/${selected.name}/replay${q}`, { method: 'POST' }).then(() => setRefreshKey(n => n + 1)); })}>{t('dlq.replay')}</button>
            <button className="btn btn-danger btn-sm" disabled={!selected} onClick={() => { if (confirm(t('dlq.confirmDeleteAll'))) { onAction(`${t('toast.deleteDlq')}: ${selected.name}`, () => { const q = filter ? `?contains=${encodeURIComponent(filter)}` : ''; return api(`/api/v2/dlq/${selected.name}${q}`, { method: 'DELETE' }).then(() => setRefreshKey(n => n + 1)); }); } }}>{t('dlq.deleteAll')}</button>
          </div>
        </div>
        <div className="card-body">
          {dlq.error ? <ErrorBox message={dlq.error} /> :
            dlq.data?.items?.length ? (
              <div className="space-y-2">
                {dlq.data.items.map((item, i) => (
                  <DLQRow key={i} t={t} item={item} onDelete={() => deleteOne(item)} onReplay={() => onAction(`Replay: ${selected.name}`, () => api(`/api/v2/dlq/${selected.name}/replay?from=${encodeURIComponent(item.timestamp)}&until=${encodeURIComponent(new Date(new Date(item.timestamp).getTime() + 2000).toISOString())}`, { method: 'POST' }).then(() => setRefreshKey(n => n + 1)))} />
                ))}
              </div>
            ) : <Empty text={t('dlq.noRecords')} />
          }
        </div>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════
// Plugins
// ════════════════════════════════════════════════
function PluginsPage({ t, plugins }: any) {
  const meta = plugins.data?.metadata;
  if (!meta) return <div className="card card-body"><Empty text={t('plugin.loading')} /></div>;
  const rows = [
    ...Object.entries(meta.sources || {}).map(([n, i]) => ({ kind: 'source', name: n, info: i })),
    ...Object.entries(meta.sinks || {}).map(([n, i]) => ({ kind: 'sink', name: n, info: i })),
    ...Object.entries(meta.transforms || {}).map(([n, i]) => ({ kind: 'transform', name: n, info: i })),
  ];
  const kindTone: Record<string, string> = { source: 'badge-cyan', sink: 'badge-emerald', transform: 'badge-violet' };
  return (
    <div className="card">
      <div className="card-header flex items-center justify-between">
        <h2 className="text-sm font-semibold">{t('plugin.matrix')}</h2>
        <div className="flex gap-3 text-xs text-slate-400">
          <span>{plugins.data?.sources.length || 0} {t('plugin.sources')}</span>
          <span>{plugins.data?.sinks.length || 0} {t('plugin.sinks')}</span>
          <span>{plugins.data?.transforms.length || 0} {t('plugin.transforms')}</span>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="tbl">
          <thead><tr><th>{t('plugin.kind')}</th><th>{t('plugin.plugin')}</th><th>{t('plugin.maturity')}</th><th>{t('plugin.requiredFields')}</th><th>{t('plugin.capabilities')}</th></tr></thead>
          <tbody>
            {rows.map((r) => (
              <tr key={`${r.kind}-${r.name}`}>
                <td><span className={`badge ${kindTone[r.kind]}`}>{r.kind}</span></td>
                <td className="font-medium">{r.name}</td>
                <td><span className={`badge ${r.info.maturity === 'stable' ? 'badge-emerald' : r.info.maturity === 'beta' ? 'badge-blue' : 'badge-amber'}`}>{r.info.maturity || 'unknown'}</span></td>
                <td><div className="flex flex-wrap gap-1">{(r.info.required || []).map((f: string) => <span key={f} className="badge badge-rose">{f}</span>)}</div></td>
                <td><div className="flex flex-wrap gap-1">{(r.info.capabilities || []).map((c: string) => <span key={c} className="badge badge-slate">{c}</span>)}</div></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════
// Audit
// ════════════════════════════════════════════════
function AuditPage({ t, audit }: any) {
  return (
    <div className="card">
      <div className="card-header"><h2 className="text-sm font-semibold">{t('audit.trail')}</h2></div>
      <div className="overflow-x-auto">
        <table className="tbl">
          <thead><tr><th>{t('audit.action')}</th><th>{t('audit.method')}</th><th>{t('audit.path')}</th><th>{t('audit.time')}</th></tr></thead>
          <tbody>
            {(audit.data?.events || []).map((e: AuditEvent, i: number) => (
              <tr key={i}>
                <td><span className="badge badge-indigo">{e.action || 'event'}</span></td>
                <td><span className="badge badge-slate">{e.method || t('common.na')}</span></td>
                <td className="text-xs">{e.target || e.path || t('common.na')}</td>
                <td className="text-xs text-slate-400">{fmtTime(e.timestamp)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {!audit.data?.events?.length && <div className="p-8"><Empty text={t('audit.noEvents')} /></div>}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════
// Shared
// ════════════════════════════════════════════════
function Progress({ label, value, danger }: { label: string; value: number; danger?: boolean }) {
  const pct = Math.max(0, Math.min(100, Math.round(value * 100)));
  return (
    <div>
      <div className="mb-1.5 flex justify-between text-xs">
        <span className="font-medium text-slate-600">{label}</span>
        <span className="text-slate-400">{pct}%</span>
      </div>
      <div className="progress-track">
        <div className={`progress-fill ${danger ? 'bg-amber-500' : 'bg-indigo-500'}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function Mini({ label, value }: { label: string; value: string }) {
  return <div className="rounded-lg bg-slate-50 px-3 py-2"><div className="text-xs text-slate-400">{label}</div><div className="mt-0.5 text-sm font-semibold text-slate-700">{value}</div></div>;
}

function Empty({ text }: { text: string }) {
  return <div className="rounded-lg border border-dashed border-slate-200 py-10 text-center text-sm text-slate-400">{text}</div>;
}

function ErrorBox({ message }: { message: string }) {
  return <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2.5 text-sm text-rose-700">{message}</div>;
}

function ratio(a: number, b: number) { return b <= 0 ? 0 : a / b; }
function fmtTime(v?: string) { if (!v || v.startsWith('0001-')) return 'n/a'; return new Date(v).toLocaleString(); }

// ════════════════════════════════════════════════
// Pipeline Log Viewer
// ════════════════════════════════════════════════
function PipelineLogViewer({ t, name }: { t: (k: string) => string; name: string }) {
  const [entries, setEntries] = useState<PipelineLogEntry[]>([]);
  const lastSeq = useRef(0);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [paused, setPaused] = useState(false);

  useEffect(() => {
    let active = true;
    const fetchLogs = async () => {
      try {
        const data = await api<{ entries: PipelineLogEntry[]; last_seq: number }>(
          `/api/v2/pipelines/${name}/log?since=${lastSeq.current}`
        );
        if (!active) return;
        if (data.entries?.length) {
          setEntries((prev) => [...prev, ...data.entries]);
        }
        lastSeq.current = data.last_seq;
      } catch { /* retry */ }
    };
    fetchLogs();
    const timer = setInterval(() => { if (!paused) fetchLogs(); }, 1000);
    return () => { active = false; clearInterval(timer); };
  }, [name, paused]);

  useEffect(() => {
    if (!paused && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries, paused]);

  const levelColor = (lvl: string) => {
    switch (lvl) {
      case 'ERROR': return 'text-rose-400';
      case 'WARN':  return 'text-amber-400';
      case 'DEBUG': return 'text-slate-500';
      default:      return 'text-emerald-400';
    }
  };

  return (
    <div className="card">
      <div className="card-header flex items-center justify-between">
        <h2 className="text-sm font-semibold">{t('pipe.logs')}</h2>
        <div className="flex gap-2">
          <button className={`btn btn-ghost btn-xs ${paused ? 'text-amber-500' : ''}`} onClick={() => setPaused(!paused)}>
            {paused ? '▶' : '⏸'}
          </button>
          <button className="btn btn-ghost btn-xs" onClick={() => { setEntries([]); lastSeq.current = 0; }}>✕</button>
        </div>
      </div>
      <div ref={scrollRef} className="bg-slate-900 text-xs font-mono rounded-lg p-3 h-40 overflow-y-auto space-y-0.5">
        {entries.length === 0 ? (
          <div className="text-slate-500 text-center py-6">{t('pipe.noLogs')}</div>
        ) : (
          entries.map((e, i) => (
            <div key={i} className="flex gap-2 leading-relaxed">
              <span className="text-slate-600 shrink-0">{e.timestamp.slice(11, 19)}</span>
              <span className={`shrink-0 w-10 ${levelColor(e.level)}`}>{e.level}</span>
              <span className={levelColor(e.level)}>{e.message}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

createRoot(document.getElementById('root')!).render(<App />);

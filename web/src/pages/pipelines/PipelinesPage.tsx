import React, { useEffect, useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { EmptyState } from '@/components/shared/empty-state';
import { PipelineHealthBadge, HealthDot } from '@/components/shared/pipeline-health-badge';
import { PipelinePath } from '@/components/shared/pipeline-path';
import { confirmAction } from '@/components/shared/confirm-dialog';
import {
  api,
  getToken,
  normalizePipelines,
  pipelineKey,
  pipelineRef,
} from '@/lib/api';
import { PipelineRowMeta } from '@/lib/uptime';
import {
  deriveModeLabel,
  derivePipelineHealth,
  formatLag,
} from '@/lib/pipeline-health';
import { cn } from '@/lib/utils';
import type {
  ApiState,
  Checkpoint,
  MetricsPipeline,
  Pipeline,
  TFunc,
} from '@/lib/types';
import type { ToastFn } from '@/lib/toast';
import {
  CheckSquare,
  LayoutList,
  MoreHorizontal,
  Play,
  Search,
  Square,
  SquareStack,
} from 'lucide-react';
import {
  PipelineLogModal,
  PipelineVersionsModal,
  SpecImportModal,
} from './pipeline-modals';

function PipelineActionMenu({
  t,
  onLogs,
  onViewTopology,
  onDelete,
  onExport,
  onStart,
  onStop,
  status,
}: {
  t: TFunc;
  onLogs: () => void;
  onViewTopology: () => void;
  onDelete: () => void;
  onExport: () => void;
  onStart: () => void;
  onStop: () => void;
  status: string;
}) {
  // List menu: ops + read-only topology. Designer is only from detail Topology/Spec.
  const items = [
    status !== 'running'
      ? { label: t('pipe.start'), onClick: onStart }
      : { label: t('pipe.stop'), onClick: onStop },
    { label: t('action.logs'), onClick: onLogs },
    { label: t('pipe.viewTopology'), onClick: onViewTopology },
    { label: t('action.exportYaml'), onClick: onExport },
    { label: t('action.delete'), onClick: onDelete, danger: true },
  ];

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="sm" aria-label={t('ui.more')}>
          <MoreHorizontal className="h-4 w-4" />
          <span className="hidden lg:inline"> {t('ui.more')}</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-44">
        {items.map((item) => (
          <DropdownMenuItem
            key={item.label}
            className={item.danger ? 'text-rose-600 focus:text-rose-600' : ''}
            onClick={item.onClick}
          >
            <span>{item.label}</span>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

const PipelineRow = React.memo(
  function PipelineRow({
    p,
    m,
    compact,
    selected,
    checked,
    t,
    onSelect,
    onToggleCheck,
    onOpenDetail,
    onAction,
    onShowLogs,
    onExport,
    onDelete,
  }: {
    p: Pipeline;
    m?: MetricsPipeline;
    compact: boolean;
    selected: boolean;
    checked: boolean;
    t: TFunc;
    onSelect: (n: string) => void;
    onToggleCheck: (n: string) => void;
    onOpenDetail?: (n: string, tab?: string) => void;
    onAction: (msg: string, fn: () => Promise<any>) => void;
    onShowLogs: () => void;
    onExport: () => void;
    onDelete: () => void;
  }) {
    const ref = pipelineRef(p);
    const health = derivePipelineHealth(p, m);
    const primaryLabel =
      health === 'failed' || health === 'degraded'
        ? t('dash.actionView')
        : t('pipe.openDetail');
    return (
      <div
        className={cn(
          'pipeline-row grid grid-cols-1 items-center gap-3 md:grid-cols-[auto_minmax(160px,.9fr)_minmax(200px,1.4fr)_100px_110px_minmax(9.5rem,auto)]',
          compact && 'py-2',
          selected && 'selected',
        )}
        onClick={() => onSelect(pipelineKey(p))}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            onSelect(pipelineKey(p));
          }
        }}
        onDoubleClick={() => {
          onSelect(pipelineKey(p));
          onOpenDetail?.(pipelineKey(p));
        }}
      >
        <div
          className="flex items-center gap-2"
          onClick={(e) => e.stopPropagation()}
        >
          <input
            type="checkbox"
            className="h-4 w-4 rounded border-input"
            checked={checked}
            aria-label={`Select ${p.name}`}
            onChange={() => onToggleCheck(pipelineKey(p))}
          />
          <HealthDot health={health} />
        </div>
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="truncate text-sm font-semibold">{p.name}</span>
            <PipelineHealthBadge health={health} t={t} />
            {p.parallelism && p.parallelism > 1 && (
              <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground">
                ×{p.parallelism}
              </span>
            )}
            {!compact &&
              (p.tags || []).map((tag) => (
                <span
                  key={tag}
                  className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground"
                >
                  {tag}
                </span>
              ))}
          </div>
          {compact && m && m.cdc_lag_ms > 0 && (
            <span className="text-[10px] text-amber-600">lag {formatLag(m.cdc_lag_ms)}</span>
          )}
        </div>
        <div className="min-w-0">
          {!compact && <PipelinePath pipeline={p} />}
          {!compact && (
            <PipelineRowMeta
              t={t}
              startedAt={p.stats.started_at}
              uptimeFallback={p.stats.uptime || 'N/A'}
              written={p.stats.records_written}
              cdcLagMs={m?.cdc_lag_ms}
            />
          )}
        </div>
        <div className="hidden sm:block">
          <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground">
            {deriveModeLabel(p, m)}
          </span>
        </div>
        <div className="hidden min-w-[88px] flex-col items-start sm:flex">
          {p.stats.records_dlq > 0 ? (
            <>
              <span className="tabular text-xs font-semibold text-rose-600">
                {p.stats.records_dlq.toLocaleString()}
              </span>
              <span className="text-[10px] text-muted-foreground">DLQ</span>
            </>
          ) : p.stats.records_failed > 0 ? (
            <>
              <span className="tabular text-xs font-semibold text-rose-600">
                {p.stats.records_failed.toLocaleString()}
              </span>
              <span className="text-[10px] text-muted-foreground">{t('dash.failed')}</span>
            </>
          ) : m && m.cdc_lag_ms > 0 ? (
            <>
              <span className="tabular text-xs font-semibold">{formatLag(m.cdc_lag_ms)}</span>
              <span className="text-[10px] text-muted-foreground">CDC lag</span>
            </>
          ) : (
            <>
              <span className="tabular text-xs font-semibold">
                {(p.stats.records_written || 0).toLocaleString()}
              </span>
              <span className="text-[10px] text-muted-foreground">{t('pipe.written')}</span>
            </>
          )}
        </div>
        <div
          className="flex items-center justify-end gap-1 justify-self-end md:min-w-[9.5rem]"
          onClick={(e) => e.stopPropagation()}
        >
          <Button
            size="sm"
            variant={health === 'failed' || health === 'degraded' ? 'default' : 'secondary'}
            onClick={() => {
              onSelect(pipelineKey(p));
              onOpenDetail?.(pipelineKey(p));
            }}
          >
            {primaryLabel}
          </Button>
          <PipelineActionMenu
            t={t}
            status={p.status}
            onStart={() =>
              onAction(`Start ${p.name}`, () =>
                api(`/api/v2/pipelines/${ref}/start`, { method: 'POST' }),
              )
            }
            onStop={() =>
              onAction(`Stop ${p.name}`, () =>
                api(`/api/v2/pipelines/${ref}/stop`, { method: 'POST' }),
              )
            }
            onLogs={onShowLogs}
            onViewTopology={() => {
              onSelect(pipelineKey(p));
              onOpenDetail?.(pipelineKey(p), 'topology');
            }}
            onDelete={onDelete}
            onExport={onExport}
          />
        </div>
      </div>
    );
  },
  (prev, next) => {
    const p1 = prev.p,
      p2 = next.p;
    if (
      p1.name !== p2.name ||
      p1.id !== p2.id ||
      p1.status !== p2.status ||
      p1.stats.records_written !== p2.stats.records_written ||
      p1.stats.records_failed !== p2.stats.records_failed ||
      p1.stats.records_dlq !== p2.stats.records_dlq ||
      p1.stats.started_at !== p2.stats.started_at ||
      p1.parallelism !== p2.parallelism ||
      prev.selected !== next.selected ||
      prev.checked !== next.checked ||
      prev.compact !== next.compact ||
      prev.t !== next.t
    ) {
      return false;
    }
    const m1 = prev.m,
      m2 = next.m;
    if (
      (m1?.cdc_lag_ms || 0) !== (m2?.cdc_lag_ms || 0) ||
      (m1?.dlq_file_count || 0) !== (m2?.dlq_file_count || 0)
    ) {
      return false;
    }
    return true;
  },
);

function readListFilters() {
  const raw = (window.location.hash || '').split('?')[1] || '';
  const qs = new URLSearchParams(raw);
  return {
    search: qs.get('q') || '',
    status: qs.get('status') || '',
    mode: qs.get('mode') || '',
    tag: qs.get('tag') || '',
    sort: qs.get('sort') || 'name',
  };
}

function writeListFilters(next: {
  search: string;
  status: string;
  mode: string;
  tag: string;
  sort: string;
}) {
  const base = (window.location.hash || '#/pipelines').split('?')[0] || '#/pipelines';
  const qs = new URLSearchParams();
  if (next.search) qs.set('q', next.search);
  if (next.status) qs.set('status', next.status);
  if (next.mode) qs.set('mode', next.mode);
  if (next.tag) qs.set('tag', next.tag);
  if (next.sort && next.sort !== 'name') qs.set('sort', next.sort);
  const q = qs.toString();
  const hash = q ? `${base}?${q}` : base;
  if (window.location.hash !== hash) {
    window.history.replaceState(null, '', hash);
  }
}

type Props = {
  t: TFunc;
  lang?: string;
  pipelines: ApiState<{ pipelines: Pipeline[] }>;
  metrics: ApiState<{ pipelines: MetricsPipeline[] }>;
  selected?: Pipeline;
  selectedMetric?: MetricsPipeline;
  onSelect: (n: string) => void;
  onOpenDetail?: (key: string, tab?: string) => void;
  onOpenWizard?: () => void;
  onAction: (label: string, fn: () => Promise<unknown>) => void;
  checkpoints: ApiState<{ checkpoints: Checkpoint[] }>;
  onResetCheckpoint: (ref: string, label?: string) => void;
  /** @deprecated Topology edit is only from detail Topology tab → designer. Kept optional for call-site compat. */
  onEdit?: (ref: string) => void;
  refreshKey: number;
  onShowToast?: ToastFn;
  plugins: ApiState<any>;
  pluginSchema: ApiState<any>;
  forceWizard?: boolean;
  onWizardClose?: () => void;
};

export function PipelinesPage({
  t,
  pipelines,
  metrics,
  selected,
  onSelect,
  onOpenDetail,
  onOpenWizard,
  onAction,
  onShowToast,
}: Props) {
  const initial = readListFilters();
  // Modal logs kept as fallback when detail route is unavailable.
  const [showLogs, setShowLogs] = useState(false);
  const [showVersions, setShowVersions] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [tagFilter, setTagFilter] = useState(initial.tag);
  const [search, setSearch] = useState(initial.search);
  const [statusFilter, setStatusFilter] = useState(initial.status);
  const [modeFilter, setModeFilter] = useState(initial.mode);
  const [sortKey, setSortKey] = useState(initial.sort);
  const [compact, setCompact] = useState(false);
  const [checked, setChecked] = useState<Record<string, boolean>>({});

  useEffect(() => {
    writeListFilters({
      search,
      status: statusFilter,
      mode: modeFilter,
      tag: tagFilter,
      sort: sortKey,
    });
  }, [search, statusFilter, modeFilter, tagFilter, sortKey]);

  const allTags = useMemo(() => {
    const s = new Set<string>();
    normalizePipelines(pipelines.data).forEach((p) => (p.tags || []).forEach((tag) => s.add(tag)));
    return Array.from(s).sort();
  }, [pipelines.data]);

  const filteredPipelines = useMemo(() => {
    let list = normalizePipelines(pipelines.data);
    if (tagFilter) list = list.filter((p) => (p.tags || []).includes(tagFilter));
    if (search) {
      const q = search.toLowerCase();
      list = list.filter((p) => p.name.toLowerCase().includes(q));
    }
    if (statusFilter) {
      list = list.filter((p) => {
        const m = (metrics.data?.pipelines || []).find(
          (x) => (x.id && x.id === p.id) || x.name === p.name,
        );
        const h = derivePipelineHealth(p, m);
        return h === statusFilter || p.status === statusFilter;
      });
    }
    if (modeFilter) {
      list = list.filter((p) => {
        const m = (metrics.data?.pipelines || []).find(
          (x) => (x.id && x.id === p.id) || x.name === p.name,
        );
        return deriveModeLabel(p, m).toLowerCase() === modeFilter.toLowerCase();
      });
    }
    list = [...list].sort((a, b) => {
      const mA = (metrics.data?.pipelines || []).find(
        (x) => (x.id && x.id === a.id) || x.name === a.name,
      );
      const mB = (metrics.data?.pipelines || []).find(
        (x) => (x.id && x.id === b.id) || x.name === b.name,
      );
      switch (sortKey) {
        case 'name':
          return a.name.localeCompare(b.name);
        case 'status':
          return a.status.localeCompare(b.status);
        case 'written':
          return (b.stats.records_written || 0) - (a.stats.records_written || 0);
        case 'latency':
          return (mB?.sink_write_latency_ms || 0) - (mA?.sink_write_latency_ms || 0);
        case 'uptime':
          return (b.stats.uptime || '').localeCompare(a.stats.uptime || '');
        default:
          return 0;
      }
    });
    return list;
  }, [pipelines.data, tagFilter, search, statusFilter, modeFilter, sortKey, metrics.data]);

  useEffect(() => {
    setShowLogs(false);
    setShowVersions(false);
  }, [selected?.id, selected?.name]);

  const handleDelete = (p: Pipeline) => {
    if (!confirmAction(t('pipe.confirmDelete').replace('{name}', p.name))) return;
    onAction(t('pipe.deleted').replace('{name}', p.name), () =>
      api(`/api/v2/pipelines/${pipelineRef(p)}`, { method: 'DELETE' }),
    );
  };

  const handleExport = async (p: Pipeline) => {
    try {
      const token = getToken();
      const headers: Record<string, string> = {};
      if (token) headers['X-API-Token'] = token;
      const res = await fetch(`/api/v2/pipelines/${pipelineRef(p)}/export`, { headers });
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${p.name}.yaml`;
      a.click();
      URL.revokeObjectURL(url);
      onShowToast?.('success', t('pipe.exportDownload').replace('{name}', p.name));
    } catch (e) {
      onShowToast?.('error', String(e));
    }
  };

  const selectedKeys = Object.keys(checked).filter((k) => checked[k]);
  const selectedPipes = filteredPipelines.filter((p) => selectedKeys.includes(pipelineKey(p)));

  const batchAction = (action: 'start' | 'stop', targets: Pipeline[]) => {
    if (!targets.length) return;
    onShowToast?.(
      'info',
      `${action === 'start' ? t('ui.starting') : t('ui.stopping')} ${targets.length} ${t('ui.pipelines')}...`,
    );
    targets.forEach((p) => {
      onAction(`${action} ${p.name}`, () =>
        api(`/api/v2/pipelines/${pipelineRef(p)}/${action}`, { method: 'POST' }),
      );
    });
  };

  const loading = pipelines.loading || metrics.loading;
  const runningCount = filteredPipelines.filter((p) => p.status === 'running').length;
  const stoppedCount = filteredPipelines.filter((p) => p.status !== 'running').length;

  const healthOptions: { value: string; label: string }[] = [
    { value: '', label: t('pipe.allStatuses') },
    { value: 'healthy', label: t('health.healthy') },
    { value: 'degraded', label: t('health.degraded') },
    { value: 'failed', label: t('health.failed') },
    { value: 'running', label: t('pipe.running') },
    { value: 'stopped', label: t('pipe.stopped') },
    { value: 'scheduled', label: t('health.scheduled') },
  ];

  const modeOptions = [
    { value: '', label: t('pipe.allModes') },
    { value: 'CDC', label: 'CDC' },
    { value: 'batch', label: 'batch' },
    { value: 'streaming', label: 'streaming' },
    { value: 'DAG', label: 'DAG' },
    { value: 'scheduled', label: 'scheduled' },
  ];

  return (
    <>
      {showLogs && selected?.name && (
        <PipelineLogModal
          t={t}
          name={selected.name}
          refId={pipelineRef(selected)}
          onClose={() => setShowLogs(false)}
        />
      )}
      {showVersions && selected?.name && (
        <PipelineVersionsModal
          t={t}
          name={selected.name}
          refId={pipelineRef(selected)}
          onClose={() => setShowVersions(false)}
          onAction={onAction}
        />
      )}
      {showImport && (
        <SpecImportModal
          t={t}
          onClose={() => setShowImport(false)}
          onImported={(name) =>
            onShowToast?.('success', t('pipe.importSuccess').replace('{name}', name))
          }
        />
      )}

      {loading && !pipelines.data && (
        <Card>
          <CardContent className="space-y-3 p-6">
            {[1, 2, 3, 4, 5].map((i) => (
              <div key={i} className="h-14 animate-pulse rounded-lg bg-muted" />
            ))}
          </CardContent>
        </Card>
      )}

      {!loading && (
        <div className="space-y-4" data-testid="pipelines-list-fullwidth">
          <Card className="overflow-auto">
            <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-2 space-y-0 pb-3">
              <CardTitle className="text-sm">{t('pipe.allPipelines')}</CardTitle>
              <div className="flex flex-wrap items-center gap-2">
                <div className="relative">
                  <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    className="h-8 w-40 pl-7 text-xs"
                    placeholder={t('pipe.search')}
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                    aria-label={t('pipe.search')}
                  />
                </div>
                <select
                  className="h-8 w-32 rounded-md border border-input bg-background px-2 text-xs"
                  value={statusFilter}
                  onChange={(e) => setStatusFilter(e.target.value)}
                  aria-label={t('pipe.filterStatus')}
                >
                  {healthOptions.map((o) => (
                    <option key={o.value || 'all'} value={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
                <select
                  className="h-8 w-28 rounded-md border border-input bg-background px-2 text-xs"
                  value={modeFilter}
                  onChange={(e) => setModeFilter(e.target.value)}
                  aria-label={t('pipe.filterMode')}
                >
                  {modeOptions.map((o) => (
                    <option key={o.value || 'all-modes'} value={o.value}>
                      {o.label}
                    </option>
                  ))}
                </select>
                {allTags.length > 0 && (
                  <select
                    className="h-8 w-32 rounded-md border border-input bg-background px-2 text-xs"
                    value={tagFilter}
                    onChange={(e) => setTagFilter(e.target.value)}
                    aria-label={t('pipe.allTags')}
                  >
                    <option value="">{t('pipe.allTags')}</option>
                    {allTags.map((tag) => (
                      <option key={tag} value={tag}>
                        {tag}
                      </option>
                    ))}
                  </select>
                )}
                <select
                  className="h-8 w-28 rounded-md border border-input bg-background px-2 text-xs"
                  value={sortKey}
                  onChange={(e) => setSortKey(e.target.value)}
                  aria-label={t('pipe.sortName')}
                >
                  <option value="name">{t('pipe.sortName')}</option>
                  <option value="status">{t('pipe.sortStatus')}</option>
                  <option value="written">{t('pipe.sortWritten')}</option>
                  <option value="latency">{t('pipe.sortLatency')}</option>
                  <option value="uptime">{t('pipe.sortUptime')}</option>
                </select>
                <Button
                  variant="ghost"
                  size="sm"
                  className={cn('text-xs', compact && 'text-primary')}
                  onClick={() => setCompact(!compact)}
                  title={t(compact ? 'pipe.expandedMode' : 'pipe.compactMode')}
                  aria-label={t(compact ? 'pipe.expandedMode' : 'pipe.compactMode')}
                >
                  {compact ? <LayoutList className="h-4 w-4" /> : <SquareStack className="h-4 w-4" />}
                </Button>
                <Button
                  data-testid="open-first-task-wizard"
                  size="sm"
                  onClick={() => onOpenWizard?.()}
                >
                  {t('pipe.createWizard')}
                </Button>
                <Button variant="secondary" size="sm" onClick={() => setShowImport(true)}>
                  {t('pipe.import')}
                </Button>
              </div>
            </CardHeader>

            {selectedPipes.length > 0 ? (
              <div
                className="flex flex-wrap items-center gap-2 border-b border-border bg-primary/5 px-4 py-2"
                data-testid="pipelines-selection-toolbar"
              >
                <CheckSquare className="h-4 w-4 text-primary" />
                <span className="text-xs font-semibold">
                  {t('pipe.selectedCount').replace('{n}', String(selectedPipes.length))}
                </span>
                <span className="text-xs text-muted-foreground">
                  {t('pipe.selectedImpact')
                    .replace('{running}', String(selectedPipes.filter((p) => p.status === 'running').length))
                    .replace('{stopped}', String(selectedPipes.filter((p) => p.status !== 'running').length))}
                </span>
                <div className="flex-1" />
                <Button
                  variant="secondary"
                  size="sm"
                  className="text-xs"
                  onClick={() =>
                    batchAction(
                      'start',
                      selectedPipes.filter((p) => p.status !== 'running'),
                    )
                  }
                >
                  <Play className="h-3.5 w-3.5" /> {t('pipe.start')}
                </Button>
                <Button
                  variant="secondary"
                  size="sm"
                  className="text-xs"
                  onClick={() =>
                    batchAction(
                      'stop',
                      selectedPipes.filter((p) => p.status === 'running'),
                    )
                  }
                >
                  <Square className="h-3.5 w-3.5" /> {t('pipe.stop')}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="text-xs"
                  onClick={() => setChecked({})}
                >
                  {t('pipe.clearSelection')}
                </Button>
              </div>
            ) : (
              <div className="flex flex-wrap items-center gap-2 border-b border-border bg-muted/40 px-4 py-2">
                <span className="text-xs text-muted-foreground">
                  {filteredPipelines.length} {t('pipe.pipelines')}
                </span>
                <span className="rounded-full bg-emerald-100 px-1.5 py-0.5 text-[11px] font-medium text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300">
                  {runningCount} {t('pipe.running')}
                </span>
                {stoppedCount > 0 && (
                  <span className="rounded-full bg-slate-200 px-1.5 py-0.5 text-[11px] text-slate-600 dark:bg-slate-800 dark:text-slate-300">
                    {stoppedCount} {t('pipe.stopped')}
                  </span>
                )}
                <div className="flex-1" />
                <Button
                  variant="ghost"
                  size="sm"
                  className="text-xs"
                  onClick={() =>
                    batchAction(
                      'start',
                      filteredPipelines.filter((p) => p.status !== 'running'),
                    )
                  }
                >
                  <Play className="h-3.5 w-3.5" /> {t('pipe.startAll')}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="text-xs"
                  onClick={() =>
                    batchAction(
                      'stop',
                      filteredPipelines.filter((p) => p.status === 'running'),
                    )
                  }
                >
                  <Square className="h-3.5 w-3.5" /> {t('pipe.stopAll')}
                </Button>
              </div>
            )}

            <CardContent className="space-y-1.5 pt-4">
              {filteredPipelines.map((p) => {
                const m = (metrics.data?.pipelines || []).find(
                  (x) => (x.id && x.id === p.id) || x.name === p.name,
                );
                const key = pipelineKey(p);
                return (
                  <PipelineRow
                    key={key}
                    p={p}
                    m={m}
                    compact={compact}
                    selected={pipelineKey(selected) === key}
                    checked={Boolean(checked[key])}
                    t={t}
                    onSelect={onSelect}
                    onToggleCheck={(k) =>
                      setChecked((prev) => ({ ...prev, [k]: !prev[k] }))
                    }
                    onOpenDetail={onOpenDetail}
                    onAction={onAction}
                    onShowLogs={() => {
                      onSelect(key);
                      // Prefer full-page detail Logs tab (aligned shell); modal is fallback.
                      if (onOpenDetail) {
                        onOpenDetail(key, 'logs');
                      } else {
                        setShowLogs(true);
                      }
                    }}
                    onExport={() => handleExport(p)}
                    onDelete={() => handleDelete(p)}
                  />
                );
              })}
              {!filteredPipelines.length && (
                <EmptyState
                  text={
                    search
                      ? `No pipelines matching "${search}"`
                      : tagFilter || statusFilter || modeFilter
                        ? t('pipe.emptyFiltered')
                        : t('pipe.emptyTitle')
                  }
                  hint={
                    search || tagFilter || statusFilter || modeFilter
                      ? t('pipe.emptyFilteredHint')
                      : t('pipe.emptyHint')
                  }
                >
                  {!search && !tagFilter && !statusFilter && !modeFilter && (
                    <Button
                      data-testid="empty-open-wizard"
                      size="sm"
                      onClick={() => onOpenWizard?.()}
                    >
                      {t('pipe.createWizard')}
                    </Button>
                  )}
                </EmptyState>
              )}
            </CardContent>
          </Card>
        </div>
      )}
    </>
  );
}

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
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { Progress, MiniStat } from '@/components/shared/progress';
import {
  StatusBadge,
  StatusDot,
  ToneBadge,
  statusLabel,
} from '@/components/shared/status-badge';
import { confirmAction } from '@/components/shared/confirm-dialog';
import {
  api,
  getToken,
  normalizePipelines,
  pipelineKey,
  pipelineRef,
} from '@/lib/api';
import { ratio, fmtTime } from '@/lib/format';
import { LiveUptimeInline, PipelineRowMeta } from '@/lib/uptime';
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
  PipelineDAGModal,
  PipelineLogModal,
  PipelineVersionsModal,
  SpecImportModal,
} from './pipeline-modals';
import { FirstTaskWizard } from './first-task-wizard';
import { ShardsInline } from './shards-inline';

function PipelineActionMenu({
  t,
  onLogs,
  onDAG,
  onEdit,
  onDelete,
  onExport,
}: {
  t: TFunc;
  onLogs: () => void;
  onDAG: () => void;
  onEdit: () => void;
  onDelete: () => void;
  onExport: () => void;
}) {
  const items = [
    { icon: '📋', label: t('action.logs'), onClick: onLogs },
    { icon: '🔀', label: t('action.dagAndLogs'), onClick: onDAG },
    { icon: '✏️', label: t('action.edit'), onClick: onEdit },
    { icon: '📤', label: t('action.exportYaml'), onClick: onExport },
    { icon: '🗑', label: t('action.delete'), onClick: onDelete, danger: true },
  ];

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="sm">
          ⋯<span className="hidden lg:inline"> {t('ui.more')}</span>
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-44">
        {items.map((item) => (
          <DropdownMenuItem
            key={item.label}
            className={item.danger ? 'text-rose-600 focus:text-rose-600' : ''}
            onClick={item.onClick}
          >
            <span className="w-4 text-center">{item.icon}</span>
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
    t,
    onSelect,
    onAction,
    onShowLogs,
    onShowDAG,
    onEdit,
    onExport,
    onDelete,
  }: {
    p: Pipeline;
    m?: MetricsPipeline;
    compact: boolean;
    selected: boolean;
    t: TFunc;
    onSelect: (n: string) => void;
    onAction: (msg: string, fn: () => Promise<any>) => void;
    onShowLogs: () => void;
    onShowDAG: () => void;
    onEdit: () => void;
    onExport: () => void;
    onDelete: () => void;
  }) {
    const ref = pipelineRef(p);
    return (
      <div
        className={cn('pipeline-row', compact && 'py-2', selected && 'selected')}
        onClick={() => onSelect(pipelineKey(p))}
      >
        <StatusDot status={p.status} />
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 truncate">
            <span className="text-sm font-semibold">{p.name}</span>
            <StatusBadge status={p.status} t={t} />
            {p.parallelism && p.parallelism > 1 && (
              <ToneBadge tone="purple" className="px-1 text-[10px]">
                ×{p.parallelism}
              </ToneBadge>
            )}
            {!compact &&
              (p.tags || []).map((tag) => (
                <ToneBadge key={tag} tone="blue" className="px-1 text-[10px]">
                  {tag}
                </ToneBadge>
              ))}
          </div>
          {!compact && (
            <PipelineRowMeta
              t={t}
              startedAt={p.stats.started_at}
              uptimeFallback={p.stats.uptime || 'N/A'}
              written={p.stats.records_written}
              cdcLagMs={m?.cdc_lag_ms}
            />
          )}
          {compact && m && m.cdc_lag_ms > 0 && (
            <span className="ml-1 text-[10px] text-amber-600">lag {m.cdc_lag_ms}ms</span>
          )}
        </div>
        <div className="hidden items-center gap-1.5 sm:flex">
          {p.stats.records_failed > 0 && (
            <ToneBadge tone="rose" className="px-1 text-[10px]">
              {p.stats.records_failed}
            </ToneBadge>
          )}
          {m && m.dlq_file_count > 0 && (
            <ToneBadge tone="amber" className="px-1 text-[10px]">
              {m.dlq_file_count}
            </ToneBadge>
          )}
          {!compact && p.stats.records_written > 0 && (
            <ToneBadge tone="emerald" className="px-1 text-[10px]">
              {p.stats.records_written}
            </ToneBadge>
          )}
        </div>
        <div className="flex items-center gap-1" onClick={(e) => e.stopPropagation()}>
          <Button
            size="sm"
            variant={p.status === 'running' ? 'ghost' : 'secondary'}
            className={p.status === 'running' ? 'opacity-40' : ''}
            disabled={p.status === 'running'}
            onClick={() =>
              onAction(`Start ${p.name}`, () =>
                api(`/api/v2/pipelines/${ref}/start`, { method: 'POST' }),
              )
            }
          >
            ▶
          </Button>
          <Button
            size="sm"
            variant={p.status !== 'running' ? 'ghost' : 'secondary'}
            className={p.status !== 'running' ? 'opacity-40' : ''}
            disabled={p.status !== 'running'}
            onClick={() =>
              onAction(`Stop ${p.name}`, () =>
                api(`/api/v2/pipelines/${ref}/stop`, { method: 'POST' }),
              )
            }
          >
            ⏹
          </Button>
          <PipelineActionMenu
            t={t}
            onLogs={onShowLogs}
            onDAG={onShowDAG}
            onEdit={onEdit}
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
      p1.stats.started_at !== p2.stats.started_at ||
      p1.parallelism !== p2.parallelism ||
      prev.selected !== next.selected ||
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

type Props = {
  t: TFunc;
  lang?: string;
  pipelines: ApiState<{ pipelines: Pipeline[] }>;
  metrics: ApiState<{ pipelines: MetricsPipeline[] }>;
  selected?: Pipeline;
  selectedMetric?: MetricsPipeline;
  onSelect: (n: string) => void;
  onAction: (label: string, fn: () => Promise<unknown>) => void;
  checkpoints: ApiState<{ checkpoints: Checkpoint[] }>;
  onResetCheckpoint: (ref: string, label?: string) => void;
  onEdit: (ref: string) => void;
  refreshKey: number;
  onShowToast?: ToastFn;
  plugins: ApiState<any>;
  pluginSchema: ApiState<any>;
};

export function PipelinesPage({
  t,
  pipelines,
  metrics,
  selected,
  selectedMetric,
  onSelect,
  onAction,
  checkpoints,
  onResetCheckpoint,
  onEdit,
  refreshKey,
  onShowToast,
  plugins,
  pluginSchema,
}: Props) {
  const [showLogs, setShowLogs] = useState(false);
  const [showDAG, setShowDAG] = useState(false);
  const [showVersions, setShowVersions] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [showWizard, setShowWizard] = useState(false);
  const [tagFilter, setTagFilter] = useState('');
  const [search, setSearch] = useState('');
  const [sortKey, setSortKey] = useState('name');
  const [compact, setCompact] = useState(false);

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
  }, [pipelines.data, tagFilter, search, sortKey, metrics.data]);

  useEffect(() => {
    setShowLogs(false);
    setShowDAG(false);
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

  const batchAction = (action: 'start' | 'stop', filter: (p: Pipeline) => boolean) => {
    const targets = filteredPipelines.filter(filter);
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
      {showDAG && selected?.name && (
        <PipelineDAGModal
          t={t}
          name={selected.name}
          refId={pipelineRef(selected)}
          onClose={() => setShowDAG(false)}
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
      {showWizard && (
        <FirstTaskWizard
          t={t}
          plugins={plugins}
          schema={pluginSchema}
          onClose={() => setShowWizard(false)}
          onCreated={(name) => {
            onShowToast?.('success', `Pipeline created: ${name}`);
            setShowWizard(false);
          }}
        />
      )}

      {loading && !pipelines.data && (
        <div className="grid gap-6 xl:grid-cols-[1fr_400px]">
          <Card>
            <CardContent className="space-y-3 p-6">
              {[1, 2, 3, 4, 5].map((i) => (
                <div key={i} className="h-14 animate-pulse rounded-lg bg-muted" />
              ))}
            </CardContent>
          </Card>
          <div className="space-y-6">
            <Card>
              <CardContent className="p-6">
                <div className="h-48 animate-pulse rounded-lg bg-muted" />
              </CardContent>
            </Card>
            <Card>
              <CardContent className="p-6">
                <div className="h-32 animate-pulse rounded-lg bg-muted" />
              </CardContent>
            </Card>
          </div>
        </div>
      )}

      {!loading && (
        <div className="grid gap-6 xl:grid-cols-[1fr_400px]">
          <Card className="overflow-auto">
            <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-2 space-y-0 pb-3">
              <CardTitle className="text-sm">{t('pipe.allPipelines')}</CardTitle>
              <div className="flex flex-wrap items-center gap-2">
                <Input
                  className="h-8 w-36 text-xs"
                  placeholder={'🔍 ' + t('pipe.search')}
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
                {allTags.length > 0 && (
                  <select
                    className="h-8 w-32 rounded-md border border-input bg-background px-2 text-xs"
                    value={tagFilter}
                    onChange={(e) => setTagFilter(e.target.value)}
                  >
                    <option value="">🏷 {t('pipe.allTags')}</option>
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
                >
                  <option value="name">{'↕ ' + t('pipe.sortName')}</option>
                  <option value="status">{'↕ ' + t('pipe.sortStatus')}</option>
                  <option value="written">{'↕ ' + t('pipe.sortWritten')}</option>
                  <option value="latency">{'↕ ' + t('pipe.sortLatency')}</option>
                  <option value="uptime">{'↕ ' + t('pipe.sortUptime')}</option>
                </select>
                <Button
                  variant="ghost"
                  size="sm"
                  className={cn('text-xs', compact && 'text-primary')}
                  onClick={() => setCompact(!compact)}
                  title={t(compact ? 'pipe.expandedMode' : 'pipe.compactMode')}
                >
                  {compact ? '⛶' : '⊞'}
                </Button>
                <Button
                  data-testid="open-first-task-wizard"
                  size="sm"
                  onClick={() => setShowWizard(true)}
                >
                  ✨ {t('pipe.createWizard')}
                </Button>
                <Button variant="secondary" size="sm" onClick={() => setShowImport(true)}>
                  📥 {t('pipe.import')}
                </Button>
              </div>
            </CardHeader>

            <div className="flex items-center gap-2 border-b border-border bg-muted/40 px-4 py-2">
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
                onClick={() => batchAction('start', (p) => p.status !== 'running')}
              >
                {'▶ ' + t('pipe.startAll')}
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="text-xs"
                onClick={() => batchAction('stop', (p) => p.status === 'running')}
              >
                {'⏹ ' + t('pipe.stopAll')}
              </Button>
            </div>

            <CardContent className="space-y-1.5 pt-4">
              {filteredPipelines.map((p) => {
                const m = (metrics.data?.pipelines || []).find(
                  (x) => (x.id && x.id === p.id) || x.name === p.name,
                );
                return (
                  <PipelineRow
                    key={pipelineKey(p)}
                    p={p}
                    m={m}
                    compact={compact}
                    selected={pipelineKey(selected) === pipelineKey(p)}
                    t={t}
                    onSelect={onSelect}
                    onAction={onAction}
                    onShowLogs={() => {
                      onSelect(pipelineKey(p));
                      setShowLogs(true);
                    }}
                    onShowDAG={() => {
                      onSelect(pipelineKey(p));
                      setShowDAG(true);
                    }}
                    onEdit={() => onEdit(pipelineKey(p))}
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
                      : tagFilter
                        ? `No pipelines with tag "${tagFilter}"`
                        : t('pipe.emptyTitle')
                  }
                  hint={search || tagFilter ? '' : t('pipe.emptyHint')}
                >
                  {!search && !tagFilter && (
                    <Button
                      data-testid="empty-open-wizard"
                      size="sm"
                      onClick={() => setShowWizard(true)}
                    >
                      ✨ {t('pipe.createWizard')}
                    </Button>
                  )}
                </EmptyState>
              )}
            </CardContent>
          </Card>

          <div className="space-y-6">
            <Card>
              <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-2 space-y-0 pb-3">
                <CardTitle className="text-sm">
                  {t('pipe.details')} {selected?.name ? `· ${selected.name}` : ''}
                </CardTitle>
                <div className="flex flex-wrap gap-1.5">
                  {selected?.name && (
                    <Button variant="secondary" size="sm" onClick={() => setShowDAG(true)}>
                      {'🔀 ' + t('pipe.dagBtn')}
                    </Button>
                  )}
                  {selected?.name && (
                    <Button variant="secondary" size="sm" onClick={() => setShowVersions(true)}>
                      📜 {t('pipe.versions')}
                    </Button>
                  )}
                </div>
              </CardHeader>
              <CardContent>
                {selectedMetric ? (
                  <div className="space-y-4">
                    {selected && (
                      <div className="flex items-center gap-3 rounded-xl border border-border bg-muted/40 p-3">
                        <StatusDot status={selected.status} />
                        <div>
                          <div className="text-sm font-semibold">
                            {statusLabel(t, selected.status)}
                          </div>
                          <div className="text-xs text-muted-foreground">
                            {t('pipe.uptimeLabel')}{' '}
                            <LiveUptimeInline
                              startedAt={selected.stats.started_at}
                              fallback={selected.stats.uptime || t('common.na')}
                            />
                          </div>
                        </div>
                        <div className="flex-1" />
                        <div className="text-right text-xs text-muted-foreground">
                          <div>
                            {t('pipe.readLabel')} {selected.stats.records_read || 0}
                          </div>
                          <div>
                            {t('pipe.writtenLabel')} {selected.stats.records_written || 0}
                          </div>
                        </div>
                      </div>
                    )}
                    <Progress
                      label={t('metric.writeSuccessRate')}
                      value={ratio(
                        selectedMetric.records_written,
                        selectedMetric.records_written + selectedMetric.records_failed,
                      )}
                    />
                    <Progress
                      label={t('metric.dlqPressure')}
                      value={ratio(
                        selectedMetric.records_dlq,
                        Math.max(1, selectedMetric.records_read),
                      )}
                      danger
                    />
                    <div className="grid grid-cols-2 gap-3 border-t border-border pt-4">
                      <MiniStat
                        label={t('metric.readLatency')}
                        value={`${selectedMetric.source_read_latency_ms.toFixed(1)}ms`}
                      />
                      <MiniStat
                        label={t('metric.writeLatency')}
                        value={`${selectedMetric.sink_write_latency_ms.toFixed(1)}ms`}
                      />
                      <MiniStat
                        label={t('metric.lastBatch')}
                        value={String(selectedMetric.last_batch_size)}
                      />
                      <MiniStat
                        label={t('metric.avgBatch')}
                        value={String(selectedMetric.avg_batch_size)}
                      />
                      <MiniStat
                        label={t('metric.batchCount')}
                        value={String(selectedMetric.batch_count)}
                      />
                      <MiniStat
                        label={t('metric.cpAge')}
                        value={`${selectedMetric.checkpoint_age_seconds}s`}
                      />
                      {selectedMetric.cdc_lag_ms > 0 && (
                        <MiniStat
                          label={t('metric.cdcLag')}
                          value={`${selectedMetric.cdc_lag_ms}ms`}
                        />
                      )}
                      <MiniStat
                        label={t('metric.dlqFiles')}
                        value={String(selectedMetric.dlq_file_count)}
                      />
                    </div>
                    {selected?.stats?.last_error && (
                      <ErrorBox message={selected.stats.last_error} />
                    )}
                    {selected?.shard_count && selected.shard_count > 1 && (
                      <>
                        <h3 className="border-t border-border pt-2 text-sm font-semibold">
                          {t('pipe.shardsLabel')} ({selected.shard_count})
                        </h3>
                        <ShardsInline
                          t={t}
                          name={pipelineRef(selected)}
                          refreshKey={refreshKey}
                        />
                      </>
                    )}
                  </div>
                ) : (
                  <EmptyState text={t('dash.selectPipeline')} />
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-sm">
                  {t('pipe.checkpoints')} {selected?.name ? `· ${selected.name}` : ''}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                {selected?.name ? (
                  (() => {
                    const selectedJob = pipelineKey(selected);
                    const selCps = (checkpoints.data?.checkpoints || []).filter(
                      (cp) => cp.job_name === selectedJob,
                    );
                    return selCps.length > 0 ? (
                      selCps.map((cp) => (
                        <div
                          key={cp.job_name}
                          className="flex items-center justify-between rounded-lg border border-border bg-muted/40 px-3 py-2.5"
                        >
                          <div className="min-w-0">
                            <div className="truncate text-sm font-medium">{cp.job_name}</div>
                            <div className="text-xs text-muted-foreground">
                              {cp.source} · {fmtTime(cp.timestamp)}
                            </div>
                          </div>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => onResetCheckpoint(cp.job_name, selected.name)}
                          >
                            {t('pipe.reset')}
                          </Button>
                        </div>
                      ))
                    ) : (
                      <EmptyState text={t('pipe.noCheckpoints')} />
                    );
                  })()
                ) : (
                  <EmptyState text={t('dash.selectPipeline')} />
                )}
              </CardContent>
            </Card>
          </div>
        </div>
      )}
    </>
  );
}

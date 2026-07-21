import { useEffect, useMemo, useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { StatusDot, ToneBadge } from '@/components/shared/status-badge';
import { confirmAction } from '@/components/shared/confirm-dialog';
import { api, normalizePipelines, pipelineKey, pipelineRef, useApi } from '@/lib/api';
import { fmtTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { ApiState, DLQItem, Pipeline, TFunc } from '@/lib/types';
import type { Lang } from '@/i18n';
import {
  ChevronDown,
  ChevronRight,
  Play,
  RefreshCw,
  RotateCcw,
  Trash2,
} from 'lucide-react';

type Props = {
  t: TFunc;
  lang: Lang;
  pipelines: ApiState<{ pipelines: Pipeline[] }>;
  selected?: Pipeline;
  onSelect: (n: string) => void;
  onAction: (label: string, fn: () => Promise<unknown>) => void;
};

type Aggregate = {
  key: string;
  error_class: string;
  dag_node: string;
  count: number;
  sample: string;
  items: DLQItem[];
  latest: string;
};

function SampleRow({
  item,
  onDelete,
  onReplay,
  replayDisabled,
  replayDisabledReason,
  t,
}: {
  item: DLQItem;
  onDelete: () => void;
  onReplay: () => void;
  replayDisabled?: boolean;
  replayDisabledReason?: string;
  t: TFunc;
}) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="flex flex-wrap items-center gap-2">
        <ToneBadge tone="slate">{item.record.operation}</ToneBadge>
        {item.id ? <ToneBadge tone="slate">#{item.id}</ToneBadge> : null}
        <span className="text-xs text-muted-foreground">{fmtTime(item.timestamp)}</span>
        <div className="min-w-0 flex-1">
          <div className="truncate font-mono text-xs text-rose-600 dark:text-rose-400">
            {item.error}
          </div>
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="text-xs"
          onClick={() => setExpanded(!expanded)}
          aria-label={t('dlq.data')}
        >
          {expanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
          {t('dlq.data')}
        </Button>
        <Button
          variant="secondary"
          size="sm"
          className="text-xs"
          disabled={replayDisabled}
          onClick={onReplay}
          title={replayDisabledReason || t('dlq.replayRecord')}
          aria-label={t('dlq.replayRecord')}
        >
          <RotateCcw className="h-3.5 w-3.5" />
        </Button>
        <Button
          variant="destructive"
          size="sm"
          className="text-xs"
          onClick={onDelete}
          title={t('dlq.deleteRecord')}
          aria-label={t('dlq.deleteRecord')}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
      {replayDisabledReason && (
        <div className="mt-2 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300">
          {replayDisabledReason}
        </div>
      )}
      {expanded && (
        <div className="mt-2 rounded-lg bg-muted/40 p-3">
          <div className="mb-2 grid gap-1 text-[11px] text-muted-foreground md:grid-cols-3">
            {item.record_hash && <div className="truncate font-mono">hash {item.record_hash}</div>}
            {!!item.pipeline_version && <div>version {item.pipeline_version}</div>}
            {item.dag_node && (
              <div>
                {t('dlq.dagNode')} {item.dag_node}
              </div>
            )}
          </div>
          <pre className="overflow-x-auto whitespace-pre-wrap break-all text-xs">
            {JSON.stringify(item.record.data, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

export function DLQPage({ t, pipelines, selected, onSelect, onAction }: Props) {
  const [filter, setFilter] = useState('');
  const [pipeFilter, setPipeFilter] = useState('');
  const [showBacklogOnly, setShowBacklogOnly] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [lastReplayResult, setLastReplayResult] = useState<{
    label: string;
    replayed: number;
    remaining: number;
    failed?: number;
    dryRun?: boolean;
  } | null>(null);
  const [expandedAgg, setExpandedAgg] = useState<Record<string, boolean>>({});
  const [showConfirm, setShowConfirm] = useState(false);
  const [dryRunOnly, setDryRunOnly] = useState(false);
  const query = filter ? `limit=50&contains=${encodeURIComponent(filter)}` : 'limit=50';
  const selectedRef = pipelineRef(selected);
  const dlq = useApi<{ items: DLQItem[] }>(
    selected ? `/api/v2/dlq/${selectedRef}?${query}` : '/api/v2/dlq/_missing',
    selected ? refreshKey : -1,
  );
  const dlqItems = dlq.data?.items || [];
  const isDAG = Boolean(selected?.dag);
  const missingDagNodeCount = isDAG ? dlqItems.filter((item) => !item.dag_node).length : 0;
  const bulkReplayDisabled = !selected || missingDagNodeCount > 0 || dlqItems.length === 0;
  const hasBacklog = dlqItems.length > 0;

  const aggregates: Aggregate[] = useMemo(() => {
    const acc: Record<string, Aggregate> = {};
    for (const item of dlqItems) {
      const key = `${item.error_class || 'unknown'}::${item.dag_node || '-'}`;
      if (!acc[key]) {
        acc[key] = {
          key,
          error_class: item.error_class || 'unknown',
          dag_node: item.dag_node || '',
          count: 0,
          sample: item.error,
          items: [],
          latest: item.timestamp,
        };
      }
      acc[key].count += 1;
      acc[key].items.push(item);
      if (item.timestamp > acc[key].latest) acc[key].latest = item.timestamp;
    }
    return Object.values(acc).sort((a, b) => b.count - a.count);
  }, [dlqItems]);

  useEffect(() => {
    setLastReplayResult(null);
    setShowConfirm(false);
  }, [selectedRef]);

  const sinkMode =
    (selected as any)?.sink?.config?.batch_mode ||
    (selected as any)?.spec?.sink?.config?.batch_mode ||
    'unknown';

  const replayResultToast = (label: string, result: { replayed?: number }, dryRun = false) => {
    const replayed = Number(result.replayed) || 0;
    const remaining = Math.max(0, dlqItems.length - (dryRun ? 0 : replayed));
    setLastReplayResult({
      label,
      replayed,
      remaining,
      dryRun,
    });
    const message = dryRun
      ? `${label} · dry-run · would replay ${replayed || dlqItems.length}`
      : `${label} · ${t('dlq.replayed')}: ${replayed}`;
    return { toastMessage: message };
  };

  const deleteOne = async (item: DLQItem) => {
    try {
      if (item.id) {
        await api(`/api/v2/dlq/${selectedRef}/${item.id}`, { method: 'DELETE' });
      } else {
        await api(
          `/api/v2/dlq/${selectedRef}?from=${encodeURIComponent(item.timestamp)}&until=${encodeURIComponent(new Date(new Date(item.timestamp).getTime() + 2000).toISOString())}`,
          { method: 'DELETE' },
        );
      }
      setRefreshKey((n) => n + 1);
    } catch {
      /* ignore */
    }
  };

  const runBulkReplay = async (dryRun: boolean) => {
    if (!selected) return;
    const q = filter ? `?contains=${encodeURIComponent(filter)}` : '';
    if (dryRun) {
      // No dedicated dry-run API — surface impact panel only
      setLastReplayResult({
        label: `${t('toast.replayDlq')}: ${selected.name}`,
        replayed: dlqItems.length,
        remaining: dlqItems.length,
        dryRun: true,
      });
      setDryRunOnly(false);
      return;
    }
    await onAction(`${t('toast.replayDlq')}: ${selected.name}`, async () => {
      const result = await api<{ replayed?: number }>(
        `/api/v2/dlq/${selectedRef}/replay${q}`,
        { method: 'POST' },
      );
      setRefreshKey((n) => n + 1);
      setShowConfirm(false);
      return replayResultToast(`${t('toast.replayDlq')}: ${selected.name}`, result, false);
    });
  };

  const allPipes = normalizePipelines(pipelines.data);
  const sortedPipes = useMemo(() => {
    return [...allPipes].sort((a, b) => {
      const da = a.stats.records_dlq || 0;
      const db = b.stats.records_dlq || 0;
      if (db !== da) return db - da;
      return a.name.localeCompare(b.name);
    });
  }, [allPipes]);
  const filteredPipes = useMemo(() => {
    const q = pipeFilter.trim().toLowerCase();
    if (!q) return sortedPipes;
    return sortedPipes.filter((p) => p.name.toLowerCase().includes(q));
  }, [sortedPipes, pipeFilter]);
  const backlogOnly = showBacklogOnly
    ? filteredPipes.filter((p) => (p.stats.records_dlq || 0) > 0)
    : filteredPipes;

  return (
    <div className="grid gap-6 xl:grid-cols-[minmax(220px,280px)_minmax(0,1fr)_minmax(260px,300px)]">
      <Card className="flex min-h-0 max-h-[min(72vh,720px)] flex-col overflow-hidden xl:sticky xl:top-4">
        <CardHeader className="shrink-0 space-y-3 pb-3">
          <div className="flex items-center justify-between gap-2">
            <CardTitle className="text-sm">{t('dlq.selectPipeline')}</CardTitle>
            <span className="tabular text-[11px] text-muted-foreground">
              {backlogOnly.length}/{allPipes.length}
            </span>
          </div>
          <Input
            className="h-8"
            placeholder={t('dlq.searchPipeline')}
            value={pipeFilter}
            onChange={(e) => setPipeFilter(e.target.value)}
            data-testid="dlq-pipeline-filter"
          />
          <label className="flex cursor-pointer items-center gap-2 text-xs text-muted-foreground">
            <input
              type="checkbox"
              className="h-3.5 w-3.5 rounded border-input"
              checked={showBacklogOnly}
              onChange={(e) => setShowBacklogOnly(e.target.checked)}
            />
            {t('dlq.backlogOnly')}
          </label>
        </CardHeader>
        <CardContent className="min-h-0 flex-1 space-y-1 overflow-y-auto overscroll-contain pr-1">
          {backlogOnly.length === 0 ? (
            <EmptyState text={t('dlq.noPipelineMatch')} />
          ) : (
            backlogOnly.map((p) => {
              const key = pipelineKey(p);
              const active = pipelineKey(selected) === key;
              const dlq = p.stats.records_dlq || 0;
              return (
                <button
                  type="button"
                  key={key}
                  className={cn(
                    // Keep .pipeline-row for e2e selection helpers; layout is flex here (not list grid).
                    'pipeline-row flex w-full min-w-0 items-center gap-2 !rounded-lg !p-2.5',
                    'hover:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
                    active && 'selected border-primary/30 bg-accent/60',
                  )}
                  onClick={() => {
                    onSelect(key);
                    setRefreshKey((n) => n + 1);
                  }}
                >
                  <StatusDot status={p.status} />
                  <span className="min-w-0 flex-1 truncate text-sm font-medium" title={p.name}>
                    {p.name}
                  </span>
                  {dlq > 0 ? (
                    <ToneBadge tone="rose" className="ml-auto shrink-0 tabular">
                      {dlq}
                    </ToneBadge>
                  ) : (
                    <span className="ml-auto shrink-0 tabular text-[10px] text-muted-foreground">0</span>
                  )}
                </button>
              );
            })
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-4 space-y-0 pb-3">
          <CardTitle className="text-sm">
            {t('dlq.title')} {selected?.name ? `· ${selected.name}` : ''}
          </CardTitle>
          <div className="flex flex-wrap items-center gap-2">
            <Input
              className="w-56"
              placeholder={t('dlq.filter')}
              value={filter}
              onChange={(e) => {
                setFilter(e.target.value);
                setRefreshKey((n) => n + 1);
              }}
            />
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setRefreshKey((n) => n + 1)}
              aria-label={t('dlq.refresh')}
            >
              <RefreshCw className="h-3.5 w-3.5" />
              {t('dlq.refresh')}
            </Button>
            {hasBacklog && (
              <Button
                variant="secondary"
                size="sm"
                disabled={bulkReplayDisabled}
                title={missingDagNodeCount > 0 ? t('dlq.dagBulkBlocked') : t('dlq.replay')}
                onClick={() => {
                  setShowConfirm(true);
                  setDryRunOnly(false);
                }}
              >
                <RotateCcw className="h-3.5 w-3.5" />
                {t('dlq.replay')}
              </Button>
            )}
            {hasBacklog && (
              <Button
                variant="destructive"
                size="sm"
                disabled={!selected}
                onClick={() => {
                  if (!confirmAction(t('dlq.confirmDeleteAll'))) return;
                  onAction(`${t('toast.deleteDlq')}: ${selected!.name}`, () => {
                    const q = filter ? `?contains=${encodeURIComponent(filter)}` : '';
                    return api(`/api/v2/dlq/${selectedRef}${q}`, { method: 'DELETE' }).then(() =>
                      setRefreshKey((n) => n + 1),
                    );
                  });
                }}
              >
                <Trash2 className="h-3.5 w-3.5" />
                {t('dlq.deleteAll')}
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {isDAG && (
            <div
              className={cn(
                'mb-3 rounded border px-3 py-2 text-xs',
                missingDagNodeCount > 0
                  ? 'border-amber-200 bg-amber-50 text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300'
                  : 'border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-300',
              )}
            >
              {missingDagNodeCount > 0 ? t('dlq.dagNodeMissing') : t('dlq.dagReplaySupported')}
            </div>
          )}
          {lastReplayResult && (
            <div
              data-testid="dlq-replay-result"
              className="mb-3 rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-300"
            >
              <div className="font-semibold">
                {lastReplayResult.dryRun ? t('dlq.dryRunResult') : lastReplayResult.label}
              </div>
              <div className="mt-1 grid gap-1 sm:grid-cols-3">
                <div>
                  {t('dlq.resultSuccess')}:{' '}
                  <span className="tabular font-semibold">{lastReplayResult.replayed}</span>
                </div>
                <div>
                  {t('dlq.resultFailed')}:{' '}
                  <span className="tabular font-semibold">{lastReplayResult.failed || 0}</span>
                </div>
                <div>
                  {t('dlq.remainingHint')}:{' '}
                  <span className="tabular font-semibold">
                    {lastReplayResult.remaining.toLocaleString()}
                  </span>
                </div>
              </div>
            </div>
          )}

          <div className="mb-2 text-xs font-semibold text-muted-foreground">
            {t('dlq.aggregate')}
          </div>
          {dlq.error ? (
            <ErrorBox message={dlq.error} />
          ) : aggregates.length ? (
            <div className="space-y-2">
              {aggregates.map((g) => {
                const open = expandedAgg[g.key];
                return (
                  <div key={g.key} className="overflow-hidden rounded-lg border border-border">
                    <button
                      type="button"
                      className="flex w-full items-center justify-between gap-3 px-3 py-3 text-left text-xs transition hover:bg-muted/40"
                      onClick={() =>
                        setExpandedAgg((prev) => ({ ...prev, [g.key]: !prev[g.key] }))
                      }
                    >
                      <div className="flex min-w-0 items-center gap-2">
                        {open ? (
                          <ChevronDown className="h-3.5 w-3.5 shrink-0" />
                        ) : (
                          <ChevronRight className="h-3.5 w-3.5 shrink-0" />
                        )}
                        <div className="min-w-0">
                          <div className="truncate font-medium">{g.sample}</div>
                          <div className="mt-0.5 text-muted-foreground">
                            {g.error_class}
                            {g.dag_node ? ` · node ${g.dag_node}` : ''}
                            {' · '}
                            {fmtTime(g.latest)}
                          </div>
                        </div>
                      </div>
                      <span className="tabular text-sm font-semibold">{g.count}</span>
                    </button>
                    {open && (
                      <div className="space-y-2 border-t border-border bg-muted/20 p-3">
                        {g.items.map((item, i) => {
                          const replayDisabledReason =
                            isDAG && !item.dag_node ? t('dlq.dagNodeMissingRecord') : undefined;
                          return (
                            <SampleRow
                              key={item.id || `${g.key}-${i}`}
                              t={t}
                              item={item}
                              onDelete={() => deleteOne(item)}
                              onReplay={() =>
                                onAction(
                                  `${t('toast.replayDlq')}: ${selected!.name}${item.id ? ` #${item.id}` : ''}`,
                                  async () => {
                                    const url = item.id
                                      ? `/api/v2/dlq/${selectedRef}/${item.id}/replay`
                                      : `/api/v2/dlq/${selectedRef}/replay?from=${encodeURIComponent(item.timestamp)}&until=${encodeURIComponent(new Date(new Date(item.timestamp).getTime() + 2000).toISOString())}`;
                                    const result = await api<{ replayed?: number }>(url, {
                                      method: 'POST',
                                    });
                                    setRefreshKey((n) => n + 1);
                                    return replayResultToast(
                                      `${t('toast.replayDlq')}: ${selected!.name}${item.id ? ` #${item.id}` : ''}`,
                                      result,
                                    );
                                  },
                                )
                              }
                              replayDisabled={Boolean(replayDisabledReason)}
                              replayDisabledReason={replayDisabledReason}
                            />
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          ) : (
            <EmptyState
              text={t('dlq.noRecords')}
              hint={t('dlq.emptyHint')}
              className="border-emerald-200 bg-emerald-50/40 dark:border-emerald-900 dark:bg-emerald-950/20"
            />
          )}
        </CardContent>
      </Card>

      {/* Replay confirm panel */}
      <Card data-testid="dlq-replay-panel">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">{t('dlq.confirmPanel')}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          {!selected ? (
            <EmptyState text={t('dash.selectPipeline')} />
          ) : !hasBacklog ? (
            <div className="rounded-lg border border-emerald-200 bg-emerald-50/50 p-3 text-xs text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/30 dark:text-emerald-200">
              {t('dlq.emptyHint')}
            </div>
          ) : (
            <>
              <div className="space-y-2 rounded-lg border border-border bg-muted/30 p-3 text-xs">
                <div className="flex justify-between gap-2">
                  <span className="text-muted-foreground">{t('dlq.targetCount')}</span>
                  <span className="tabular font-semibold">{dlqItems.length}</span>
                </div>
                <div className="flex justify-between gap-2">
                  <span className="text-muted-foreground">{t('dlq.filterScope')}</span>
                  <span className="max-w-[140px] truncate font-mono">
                    {filter || t('dlq.filterAll')}
                  </span>
                </div>
                <div className="flex justify-between gap-2">
                  <span className="text-muted-foreground">{t('dlq.sinkIdempotency')}</span>
                  <span className="font-mono">{String(sinkMode)}</span>
                </div>
                <div className="text-muted-foreground">{t('dlq.replayBoundary')}</div>
              </div>
              {(showConfirm || dryRunOnly) && (
                <div className="confirm space-y-2 rounded-xl border border-rose-200 bg-rose-50/60 p-3 dark:border-rose-900 dark:bg-rose-950/30">
                  <div className="text-xs font-semibold text-rose-800 dark:text-rose-200">
                    {t('dlq.confirmTitle')}
                  </div>
                  <p className="text-[11px] text-muted-foreground">{t('dlq.confirmBody')}</p>
                  <div className="flex flex-wrap gap-2">
                    <Button
                      size="sm"
                      variant="secondary"
                      onClick={() => runBulkReplay(true)}
                      data-testid="dlq-dry-run"
                    >
                      <Play className="h-3.5 w-3.5" />
                      {t('dlq.dryRun')}
                    </Button>
                    <Button
                      size="sm"
                      disabled={bulkReplayDisabled}
                      onClick={() => runBulkReplay(false)}
                      data-testid="dlq-confirm-replay"
                    >
                      <RotateCcw className="h-3.5 w-3.5" />
                      {t('dlq.confirmReplay')}
                    </Button>
                    <Button size="sm" variant="ghost" onClick={() => setShowConfirm(false)}>
                      {t('common.cancel')}
                    </Button>
                  </div>
                </div>
              )}
              {!showConfirm && (
                <Button
                  className="w-full"
                  size="sm"
                  disabled={bulkReplayDisabled}
                  onClick={() => setShowConfirm(true)}
                >
                  {t('dlq.openConfirm')}
                </Button>
              )}
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

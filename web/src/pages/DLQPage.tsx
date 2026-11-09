import { useEffect, useMemo, useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { StatusDot, ToneBadge } from '@/components/shared/status-badge';
import { ConfirmDialog } from '@/components/shared/confirm-dialog';
import { api, normalizePipelines, pipelineKey, pipelineRef, useApi } from '@/lib/api';
import { pipelineDLQBacklog, replayBlockedReason } from '@/lib/dlq';
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
  const replayState = item.identity_context?.replay_state;
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="flex flex-wrap items-center gap-2">
        <ToneBadge tone="slate">{item.record.operation}</ToneBadge>
        {item.id ? <ToneBadge tone="slate">#{item.id}</ToneBadge> : null}
        <span className="text-xs text-muted-foreground">{fmtTime(item.timestamp)}</span>
        {replayState ? (
          <ToneBadge tone={replayState === 'sink_acked' ? 'emerald' : 'amber'}>
            {replayState}
          </ToneBadge>
        ) : null}
        {!!item.attempt && item.attempt > 1 ? (
          <ToneBadge tone="slate">
            {t('dlq.attempts')} {item.attempt}
          </ToneBadge>
        ) : null}
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
            {replayState && <div>replay_state {replayState}</div>}
            {item.identity_context?.replay_provenance && (
              <div>provenance {item.identity_context.replay_provenance}</div>
            )}
            {!!item.identity_context?.replay_attempt && (
              <div>attempts {item.identity_context.replay_attempt}</div>
            )}
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

  // UI-B.3: pending confirmation for destructive DLQ purge.
  const [pendingPurge, setPendingPurge] = useState(false);
  const [filter, setFilter] = useState('');
  // Debounced copy of the free-text filter: refetching on every keystroke made
  // the page issue one request per character.
  const [appliedFilter, setAppliedFilter] = useState('');
  const [errorClass, setErrorClass] = useState('');
  const [offset, setOffset] = useState(0);
  const [pipeFilter, setPipeFilter] = useState('');
  const [showBacklogOnly, setShowBacklogOnly] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [lastReplayResult, setLastReplayResult] = useState<{
    label: string;
    replayed: number;
    remaining: number;
    failed?: number;
    dryRun?: boolean;
    blocked?: number;
    blockedReasons?: string[];
  } | null>(null);
  const [expandedAgg, setExpandedAgg] = useState<Record<string, boolean>>({});
  const [showConfirm, setShowConfirm] = useState(false);
  const [dryRunOnly, setDryRunOnly] = useState(false);
  const pageSize = 50;
  const queryParams = new URLSearchParams();
  queryParams.set('limit', String(pageSize));
  if (offset > 0) queryParams.set('offset', String(offset));
  if (appliedFilter) queryParams.set('contains', appliedFilter);
  if (errorClass) queryParams.set('error_class', errorClass);
  const query = queryParams.toString();
  const selectedRef = pipelineRef(selected);
  const dlq = useApi<{ items: DLQItem[] }>(
    selected ? `/api/v2/dlq/${selectedRef}?${query}` : '/api/v2/dlq/_missing',
    selected ? refreshKey : -1,
  );
  const dlqItems = useMemo(() => dlq.data?.items || [], [dlq.data]);
  // error_class options are derived from the loaded page; the server-side filter
  // still applies to the whole backlog.
  const errorClasses = useMemo(
    () => Array.from(new Set(dlqItems.map((i) => i.error_class || 'unknown'))).sort(),
    [dlqItems],
  );
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
    const timer = window.setTimeout(() => setAppliedFilter(filter.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [filter]);

  useEffect(() => {
    setLastReplayResult(null);
    setShowConfirm(false);
    setOffset(0);
  }, [selectedRef, appliedFilter, errorClass]);

  useEffect(() => {
    setOffset(0);
  }, [selectedRef]);

  const sinkMode =
    (selected as any)?.sink?.config?.batch_mode ||
    (selected as any)?.spec?.sink?.config?.batch_mode ||
    'unknown';

  const replayResultToast = (label: string, result: { replayed?: number }) => {
    const replayed = Number(result.replayed) || 0;
    const remaining = Math.max(0, dlqItems.length - replayed);
    setLastReplayResult({ label, replayed, remaining });
    return { toastMessage: `${label} · ${t('dlq.replayed')}: ${replayed}` };
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

  const replayQuery = (() => {
    const params = new URLSearchParams();
    if (appliedFilter) params.set('contains', appliedFilter);
    if (errorClass) params.set('error_class', errorClass);
    const qs = params.toString();
    return qs ? `?${qs}` : '';
  })();

  // A real server-side dry run: the server applies the replay identity gate to
  // every selected row and reports what would replay vs. what is blocked. It
  // writes nothing to the sink and mutates no DLQ state.
  const runDryRun = async () => {
    if (!selected) return;
    await onAction(`${t('dlq.dryRun')}: ${selected.name}`, async () => {
      const result = await api<{
        dry_run?: boolean;
        replayable?: number;
        blocked?: number;
        items?: { replayable?: boolean; reason?: string }[];
      }>(`/api/v2/dlq/${selectedRef}/replay${replayQuery}${replayQuery ? '&' : '?'}dry_run=1`, {
        method: 'POST',
      });
      const blockedReasons = (result.items || [])
        .filter((i) => i.replayable === false && i.reason)
        .map((i) => String(i.reason))
        .slice(0, 5);
      setLastReplayResult({
        label: `${t('dlq.dryRunResult')}: ${selected.name}`,
        replayed: Number(result.replayable) || 0,
        failed: Number(result.blocked) || 0,
        remaining: dlqItems.length,
        dryRun: true,
        blockedReasons,
      });
      setDryRunOnly(false);
      return {
        toastMessage: `${t('dlq.dryRunResult')} · ${t('dlq.resultSuccess')}: ${Number(result.replayable) || 0} · ${t('dlq.resultFailed')}: ${Number(result.blocked) || 0}`,
      };
    });
  };

  const runBulkReplay = async () => {
    if (!selected) return;
    await onAction(`${t('toast.replayDlq')}: ${selected.name}`, async () => {
      const result = await api<{ replayed?: number }>(
        `/api/v2/dlq/${selectedRef}/replay${replayQuery}`,
        { method: 'POST' },
      );
      setRefreshKey((n) => n + 1);
      setShowConfirm(false);
      return replayResultToast(`${t('toast.replayDlq')}: ${selected.name}`, result);
    });
  };

  const allPipes = normalizePipelines(pipelines.data);
  const sortedPipes = useMemo(() => {
    return [...allPipes].sort((a, b) => {
      const da = pipelineDLQBacklog(a);
      const db = pipelineDLQBacklog(b);
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
    ? filteredPipes.filter((p) => pipelineDLQBacklog(p) > 0)
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
              const dlq = pipelineDLQBacklog(p);
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
              onChange={(e) => setFilter(e.target.value)}
              data-testid="dlq-record-filter"
            />
            <select
              className="h-8 rounded-md border border-input bg-background px-2 text-xs"
              value={errorClass}
              onChange={(e) => setErrorClass(e.target.value)}
              aria-label={t('dlq.class')}
              data-testid="dlq-error-class-filter"
            >
              <option value="">{t('dlq.allClasses')}</option>
              {errorClasses.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
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
                onClick={() => setPendingPurge(true)}
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
              {lastReplayResult.blockedReasons?.length ? (
                <ul className="mt-2 list-disc space-y-1 pl-4 text-[11px]">
                  {lastReplayResult.blockedReasons.map((reason, i) => (
                    <li key={i} className="break-all">
                      {reason}
                    </li>
                  ))}
                </ul>
              ) : null}
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
                          const stateReason = replayBlockedReason(item);
                          const replayDisabledReason =
                            isDAG && !item.dag_node
                              ? t('dlq.dagNodeMissingRecord')
                              : stateReason;
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
          {selected && (offset > 0 || dlqItems.length >= pageSize) && (
            <div
              className="mt-3 flex items-center justify-between gap-2 text-xs text-muted-foreground"
              data-testid="dlq-pager"
            >
              <span className="tabular">
                {t('dlq.pageRange')} {offset + 1}
                {dlqItems.length > 0 ? `–${offset + dlqItems.length}` : ''}
              </span>
              <div className="flex gap-2">
                <Button
                  variant="secondary"
                  size="sm"
                  disabled={offset === 0}
                  onClick={() => setOffset((n) => Math.max(0, n - pageSize))}
                >
                  {t('dlq.prevPage')}
                </Button>
                <Button
                  variant="secondary"
                  size="sm"
                  disabled={dlqItems.length < pageSize}
                  onClick={() => setOffset((n) => n + pageSize)}
                >
                  {t('dlq.nextPage')}
                </Button>
              </div>
            </div>
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
            <p className="text-xs leading-relaxed text-muted-foreground">
              {t('dlq.confirmIdle')}
            </p>
          ) : (
            <>
              <div className="space-y-2 rounded-lg border border-border bg-muted/30 p-3 text-xs">
                <div className="flex justify-between gap-2">
                  <span className="text-muted-foreground">{t('dlq.targetCount')}</span>
                  <span className="tabular font-semibold">
                    {dlqItems.length}
                    <span className="ml-1 font-normal text-muted-foreground">
                      {t('dlq.pageHint')}
                    </span>
                  </span>
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
                      onClick={runDryRun}
                      data-testid="dlq-dry-run"
                    >
                      <Play className="h-3.5 w-3.5" />
                      {t('dlq.dryRun')}
                    </Button>
                    <Button
                      size="sm"
                      disabled={bulkReplayDisabled}
                      onClick={runBulkReplay}
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
      <ConfirmDialog
        open={pendingPurge}
        onOpenChange={setPendingPurge}
        title={t('dlq.confirmDeleteAllTitle')}
        description={t('dlq.confirmDeleteAll')}
        confirmLabel={t('dlq.deleteAll')}
        cancelLabel={t('ui.cancel')}
        destructive
        onConfirm={() => {
          if (!selected) return;
          onAction(`${t('toast.deleteDlq')}: ${selected.name}`, () => {
            const q = filter ? `?contains=${encodeURIComponent(filter)}` : '';
            return api(`/api/v2/dlq/${selectedRef}${q}`, { method: 'DELETE' }).then(() =>
              setRefreshKey((n) => n + 1),
            );
          });
        }}
      />
    </div>
  );
}

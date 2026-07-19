import { useEffect, useState } from 'react';
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

type Props = {
  t: TFunc;
  lang: Lang;
  pipelines: ApiState<{ pipelines: Pipeline[] }>;
  selected?: Pipeline;
  onSelect: (n: string) => void;
  onAction: (label: string, fn: () => Promise<unknown>) => void;
};

function DLQRow({
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
    <div className="rounded-lg border border-border p-3 transition hover:border-muted-foreground/30">
      <div className="flex items-center gap-3">
        <ToneBadge tone="slate">{item.record.operation}</ToneBadge>
        {item.id ? <ToneBadge tone="slate">#{item.id}</ToneBadge> : null}
        {item.error_class ? <ToneBadge tone="amber">{item.error_class}</ToneBadge> : null}
        {item.dag_node ? (
          <ToneBadge tone="slate">
            {t('dlq.dagNode')} {item.dag_node}
          </ToneBadge>
        ) : null}
        <span className="text-xs text-muted-foreground">{fmtTime(item.timestamp)}</span>
        <div className="min-w-0 flex-1">
          <div className="truncate font-mono text-xs text-rose-600 dark:text-rose-400">
            {item.error}
          </div>
        </div>
        <Button variant="ghost" size="sm" className="text-xs" onClick={() => setExpanded(!expanded)}>
          {expanded ? '▲' : '▼'} {t('dlq.data')}
        </Button>
        <Button
          variant="secondary"
          size="sm"
          className="text-xs"
          disabled={replayDisabled}
          onClick={onReplay}
          title={replayDisabledReason || t('dlq.replayRecord')}
        >
          ↻
        </Button>
        <Button
          variant="destructive"
          size="sm"
          className="text-xs"
          onClick={onDelete}
          title={t('dlq.deleteRecord')}
        >
          🗑
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
  const [refreshKey, setRefreshKey] = useState(0);
  const [lastReplayResult, setLastReplayResult] = useState('');
  const query = filter ? `limit=50&contains=${encodeURIComponent(filter)}` : 'limit=50';
  const selectedRef = pipelineRef(selected);
  const dlq = useApi<{ items: DLQItem[] }>(
    selected ? `/api/v2/dlq/${selectedRef}?${query}` : '/api/v2/dlq/_missing',
    selected ? refreshKey : -1,
  );
  const dlqItems = dlq.data?.items || [];
  const isDAG = Boolean(selected?.dag);
  const missingDagNodeCount = isDAG ? dlqItems.filter((item) => !item.dag_node).length : 0;
  const bulkReplayDisabled = !selected || missingDagNodeCount > 0;

  useEffect(() => {
    setLastReplayResult('');
  }, [selectedRef]);

  const replayResultToast = (label: string, result: { replayed?: number }) => {
    const message = `${label} · ${t('dlq.replayed')}: ${Number(result.replayed) || 0}`;
    setLastReplayResult(message);
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

  return (
    <div className="grid gap-6 xl:grid-cols-[300px_1fr]">
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm">{t('dlq.selectPipeline')}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-1">
          {normalizePipelines(pipelines.data).map((p) => (
            <div
              key={pipelineKey(p)}
              className={cn(
                'pipeline-row !p-3',
                pipelineKey(selected) === pipelineKey(p) && 'selected',
              )}
              onClick={() => {
                onSelect(pipelineKey(p));
                setRefreshKey((n) => n + 1);
              }}
            >
              <StatusDot status={p.status} />
              <span className="truncate text-sm">{p.name}</span>
              {p.stats.records_dlq > 0 && (
                <ToneBadge tone="rose" className="ml-auto">
                  {p.stats.records_dlq}
                </ToneBadge>
              )}
            </div>
          ))}
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
            >
              {t('dlq.refresh')}
            </Button>
            <Button
              variant="secondary"
              size="sm"
              disabled={bulkReplayDisabled}
              title={missingDagNodeCount > 0 ? t('dlq.dagBulkBlocked') : t('dlq.replay')}
              onClick={() =>
                onAction(`${t('toast.replayDlq')}: ${selected!.name}`, async () => {
                  const q = filter ? `?contains=${encodeURIComponent(filter)}` : '';
                  const result = await api<{ replayed?: number }>(
                    `/api/v2/dlq/${selectedRef}/replay${q}`,
                    { method: 'POST' },
                  );
                  setRefreshKey((n) => n + 1);
                  return replayResultToast(`${t('toast.replayDlq')}: ${selected!.name}`, result);
                })
              }
            >
              {t('dlq.replay')}
            </Button>
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
              {t('dlq.deleteAll')}
            </Button>
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
              {lastReplayResult}
            </div>
          )}
          {dlq.error ? (
            <ErrorBox message={dlq.error} />
          ) : dlqItems.length ? (
            <div className="space-y-2">
              {dlqItems.map((item, i) => {
                const replayDisabledReason =
                  isDAG && !item.dag_node ? t('dlq.dagNodeMissingRecord') : undefined;
                return (
                  <DLQRow
                    key={item.id || i}
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
                          const result = await api<{ replayed?: number }>(url, { method: 'POST' });
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
          ) : (
            <EmptyState
              text={t('dlq.noRecords')}
              hint={t('dlq.emptyHint')}
              className="border-emerald-200 bg-emerald-50/40 dark:border-emerald-900 dark:bg-emerald-950/20"
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}

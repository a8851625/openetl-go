import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { EmptyState } from '@/components/shared/empty-state';
import { Progress, MiniStat } from '@/components/shared/progress';
import { StatusBadge, StatusDot, ToneBadge } from '@/components/shared/status-badge';
import { normalizePipelines, pipelineKey } from '@/lib/api';
import { ratio } from '@/lib/format';
import { PipelineUptime } from '@/lib/uptime';
import type {
  ApiState,
  MetricsPipeline,
  Pipeline,
  TFunc,
} from '@/lib/types';
import type { Lang } from '@/i18n';
import { cn } from '@/lib/utils';

type Totals = {
  read: number;
  written: number;
  failed: number;
  dlq: number;
  running: number;
};

type Props = {
  t: TFunc;
  lang: Lang;
  totals: Totals;
  pipelines: ApiState<{ pipelines: Pipeline[] }>;
  metrics: ApiState<{ pipelines: MetricsPipeline[] }>;
  selected?: Pipeline;
  selectedMetric?: MetricsPipeline;
  onSelect: (n: string) => void;
};

export function DashboardPage({ t, totals, pipelines, selected, selectedMetric, onSelect }: Props) {
  const pList = normalizePipelines(pipelines.data);
  const runningCount = pList.filter((p) => p.status === 'running').length;
  const failedCount = pList.filter((p) => p.status === 'failed').length;
  const health = pList.length > 0 ? Math.round((runningCount / pList.length) * 100) : 100;

  const cards = [
    {
      label: t('dash.running'),
      value: totals.running,
      sub: `${pList.length} ${t('dash.totalPipelines')} · ${health}% healthy`,
      color: 'text-emerald-600 dark:text-emerald-400',
      icon: '🟢',
    },
    {
      label: t('dash.recordsRead'),
      value: totals.read,
      sub: t('dash.allTime'),
      color: 'text-blue-600 dark:text-blue-400',
      icon: '📖',
    },
    {
      label: t('dash.recordsWritten'),
      value: totals.written,
      sub: t('dash.allTime'),
      color: 'text-indigo-600 dark:text-indigo-400',
      icon: '✅',
    },
    {
      label: t('dash.failed'),
      value: totals.failed,
      sub: totals.failed > 0 ? `${failedCount} pipeline(s) failed` : t('dash.healthy'),
      color: totals.failed > 0 ? 'text-rose-600 dark:text-rose-400' : 'text-muted-foreground',
      icon: totals.failed > 0 ? '🚨' : '✅',
    },
    {
      label: t('dash.dlq'),
      value: totals.dlq,
      sub: totals.dlq > 0 ? t('dash.needsAttention') : t('dash.empty'),
      color: totals.dlq > 0 ? 'text-amber-600 dark:text-amber-400' : 'text-muted-foreground',
      icon: totals.dlq > 0 ? '📦' : '✓',
    },
  ];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-5">
        {cards.map((c) => (
          <Card key={c.label} className="transition-shadow hover:shadow-md">
            <CardContent className="p-5">
              <div className="flex items-center justify-between">
                <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  {c.label}
                </span>
                <span className="text-lg">{c.icon}</span>
              </div>
              <div className={cn('mt-2 text-3xl font-bold', c.color)}>
                {c.value.toLocaleString()}
              </div>
              <div className="mt-1 text-xs text-muted-foreground">{c.sub}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">{t('dash.pipelineOverview')}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            {pList.map((p) => (
              <div
                key={pipelineKey(p)}
                className={cn(
                  'pipeline-row',
                  pipelineKey(selected) === pipelineKey(p) && 'selected',
                )}
                onClick={() => onSelect(pipelineKey(p))}
              >
                <StatusDot status={p.status} />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2 truncate">
                    <span className="text-sm font-medium">{p.name}</span>
                    <StatusBadge status={p.status} t={t} />
                  </div>
                  <PipelineUptime
                    label={t('dash.uptime')}
                    startedAt={p.stats.started_at}
                    fallback={p.stats.uptime || t('common.na')}
                  />
                </div>
                <div className="hidden items-center gap-2 sm:flex">
                  <ToneBadge tone="blue">
                    {p.stats.records_written} {t('pipe.written')}
                  </ToneBadge>
                  {p.stats.records_failed > 0 && (
                    <ToneBadge tone="rose">
                      {p.stats.records_failed} {t('dash.failed')}
                    </ToneBadge>
                  )}
                  {p.stats.records_dlq > 0 && (
                    <ToneBadge tone="amber">
                      {p.stats.records_dlq} {t('dash.dlq')}
                    </ToneBadge>
                  )}
                </div>
              </div>
            ))}
            {!pList.length && <EmptyState text={t('dash.noPipelines')} />}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm">
              {t('dash.keyMetrics')} {selected?.name ? `· ${selected.name}` : ''}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {selectedMetric ? (
              <>
                <Progress
                  label={t('metric.writeSuccess')}
                  value={ratio(
                    selectedMetric.records_written,
                    selectedMetric.records_written + selectedMetric.records_failed,
                  )}
                />
                <div className="grid grid-cols-2 gap-3">
                  <MiniStat
                    label={t('metric.readLatency')}
                    value={`${selectedMetric.source_read_latency_ms.toFixed(1)}ms`}
                  />
                  <MiniStat
                    label={t('metric.writeLatency')}
                    value={`${selectedMetric.sink_write_latency_ms.toFixed(1)}ms`}
                  />
                  <MiniStat label={t('metric.avgBatch')} value={String(selectedMetric.avg_batch_size)} />
                  <MiniStat
                    label={t('metric.cpAge')}
                    value={`${selectedMetric.checkpoint_age_seconds}s`}
                  />
                  {selectedMetric.cdc_lag_ms > 0 && (
                    <MiniStat label={t('metric.cdcLag')} value={`${selectedMetric.cdc_lag_ms}ms`} />
                  )}
                  <MiniStat
                    label={t('metric.dlqFiles')}
                    value={String(selectedMetric.dlq_file_count)}
                  />
                </div>
              </>
            ) : (
              <EmptyState text={t('dash.selectPipeline')} />
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

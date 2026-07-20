import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { EmptyState } from '@/components/shared/empty-state';
import { PipelineHealthBadge, HealthDot } from '@/components/shared/pipeline-health-badge';
import { PipelinePath } from '@/components/shared/pipeline-path';
import { normalizePipelines, pipelineKey } from '@/lib/api';
import {
  countHealth,
  deriveIssues,
  deriveModeLabel,
  derivePipelineHealth,
  findMetric,
  formatLag,
  type DerivedIssue,
} from '@/lib/pipeline-health';
import type { ApiState, MetricsPipeline, Pipeline, TFunc } from '@/lib/types';
import type { Lang } from '@/i18n';
import { cn } from '@/lib/utils';
import { AlertTriangle, ArrowRight, Plus } from 'lucide-react';

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
  onOpenPipeline?: (key: string, tab?: string) => void;
  onOpenIssues?: () => void;
  onOpenDLQ?: (key: string) => void;
  onCreatePipeline?: () => void;
  onOpenConnections?: () => void;
  timeRangeLabel?: string;
};

function IssueRow({
  issue,
  t,
  onAction,
}: {
  issue: DerivedIssue;
  t: TFunc;
  onAction: (issue: DerivedIssue) => void;
}) {
  return (
    <button
      type="button"
      className="grid w-full grid-cols-[10px_minmax(0,1fr)_auto] items-center gap-3 border-t border-border px-5 py-4 text-left transition hover:bg-muted/40 first:border-t-0"
      onClick={() => onAction(issue)}
    >
      <span
        className={cn(
          'h-2.5 w-2.5 rounded-full',
          issue.severity === 'blocking' ? 'bg-rose-500' : 'bg-amber-500',
        )}
        aria-hidden
      />
      <div className="min-w-0">
        <div className="truncate text-sm font-semibold">{issue.title}</div>
        <div className="mt-0.5 truncate text-xs text-muted-foreground">{issue.summary}</div>
      </div>
      <span className="flex items-center gap-1 text-xs font-semibold text-primary">
        {issue.action === 'dlq'
          ? t('dash.actionRepair')
          : issue.action === 'connections'
            ? t('dash.actionTestConn')
            : t('dash.actionView')}
        <ArrowRight className="h-3.5 w-3.5" />
      </span>
    </button>
  );
}

export function DashboardPage({
  t,
  totals,
  pipelines,
  metrics,
  onSelect,
  onOpenPipeline,
  onOpenIssues,
  onOpenDLQ,
  onCreatePipeline,
  onOpenConnections,
  timeRangeLabel,
}: Props) {
  const pList = normalizePipelines(pipelines.data);
  const mList = metrics.data?.pipelines || [];
  const issues = deriveIssues(pList, mList);
  const counts = countHealth(pList, mList);
  const healthyShare =
    counts.total > 0 ? Math.round((counts.healthy / counts.total) * 100) : 100;
  const range = timeRangeLabel || t('dash.range15m');

  const criticalPipes = pList
    .map((p) => {
      const m = findMetric(p, mList);
      return { p, m, health: derivePipelineHealth(p, m) };
    })
    .filter((x) => x.health === 'failed' || x.health === 'degraded')
    .slice(0, 6);

  const openIssue = (issue: DerivedIssue) => {
    onSelect(issue.pipelineKey);
    if (issue.action === 'dlq') onOpenDLQ?.(issue.pipelineKey);
    else if (issue.action === 'connections') onOpenConnections?.();
    else if (issue.action === 'issues') onOpenIssues?.();
    else onOpenPipeline?.(issue.pipelineKey, 'issues');
  };

  if (!pList.length && !pipelines.loading) {
    return (
      <div className="space-y-6">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <div className="text-xs font-bold uppercase tracking-[0.08em] text-primary">
              {t('dash.eyebrow')}
            </div>
            <h2 className="mt-1 text-2xl font-semibold tracking-tight md:text-3xl">
              {t('dash.emptyTitle')}
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">{t('dash.emptyHint')}</p>
          </div>
          <Button onClick={onCreatePipeline}>
            <Plus className="h-4 w-4" />
            {t('nav.createPipeline')}
          </Button>
        </div>
        <div className="grid gap-4 md:grid-cols-3">
          {[
            { n: '1', title: t('dash.stepConn'), desc: t('dash.stepConnDesc'), go: onOpenConnections },
            {
              n: '2',
              title: t('dash.stepPipe'),
              desc: t('dash.stepPipeDesc'),
              go: onCreatePipeline,
            },
            { n: '3', title: t('dash.stepVerify'), desc: t('dash.stepVerifyDesc'), go: onOpenIssues },
          ].map((s) => (
            <Card key={s.n} className="cursor-pointer transition hover:border-primary/40" onClick={s.go}>
              <CardContent className="p-5">
                <div className="text-xs font-bold text-primary">STEP {s.n}</div>
                <div className="mt-2 font-semibold">{s.title}</div>
                <p className="mt-1 text-xs text-muted-foreground">{s.desc}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-primary">
            {t('dash.eyebrow')}
          </div>
          <h2 className="mt-1 text-2xl font-semibold tracking-tight md:text-3xl">
            {t('dash.heroTitle')}
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {t('dash.heroSub')
              .replace('{issues}', String(issues.length))
              .replace('{healthy}', String(counts.healthy))
              .replace('{total}', String(counts.total))}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <span className="rounded-md border border-border bg-card px-3 py-2 text-xs text-muted-foreground">
            {range}
          </span>
          <Button onClick={onCreatePipeline}>
            <Plus className="h-4 w-4" />
            {t('nav.createPipeline')}
          </Button>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.7fr)_minmax(260px,0.8fr)]">
        <Card className="overflow-hidden">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-semibold">{t('dash.needsAction')}</CardTitle>
            <button
              type="button"
              className="text-xs font-semibold text-primary"
              onClick={onOpenIssues}
            >
              {issues.length} open →
            </button>
          </CardHeader>
          <CardContent className="p-0">
            {issues.length === 0 ? (
              <div className="px-5 py-10">
                <EmptyState text={t('dash.noIssues')} />
              </div>
            ) : (
              issues.slice(0, 5).map((issue) => (
                <IssueRow key={issue.id} issue={issue} t={t} onAction={openIssue} />
              ))
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-semibold">{t('dash.runHealth')}</CardTitle>
            <span className="text-xs text-muted-foreground">{range}</span>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <div className="tabular text-4xl font-bold tracking-tight">
                {counts.healthy}{' '}
                <span className="text-sm font-medium text-muted-foreground">
                  / {counts.total} {t('dash.healthy').toLowerCase()}
                </span>
              </div>
              <div className="progress-track mt-4">
                <div className="progress-fill" style={{ width: `${healthyShare}%` }} />
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3 text-sm">
              <div>
                <span className="tabular font-semibold">{counts.healthy}</span>{' '}
                <span className="text-muted-foreground">{t('health.healthy')}</span>
              </div>
              <div>
                <span className="tabular font-semibold">{counts.degraded}</span>{' '}
                <span className="text-muted-foreground">{t('health.degraded')}</span>
              </div>
              <div>
                <span className="tabular font-semibold">{counts.failed}</span>{' '}
                <span className="text-muted-foreground">{t('health.failed')}</span>
              </div>
              <div>
                <span className="tabular font-semibold">{counts.paused + counts.stopped}</span>{' '}
                <span className="text-muted-foreground">{t('health.paused')}</span>
              </div>
            </div>
            <p className="text-xs leading-relaxed text-muted-foreground">{t('dash.healthNote')}</p>
          </CardContent>
        </Card>
      </div>

      {/* Secondary cumulative metrics — explicit time scope */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {[
          { label: t('dash.recordsRead'), value: totals.read, scope: t('dash.allTime') },
          { label: t('dash.recordsWritten'), value: totals.written, scope: t('dash.allTime') },
          {
            label: t('dash.failedRecords'),
            value: totals.failed,
            scope: t('dash.allTime'),
            warn: totals.failed > 0,
          },
          {
            label: t('dash.dlqBacklog'),
            value: totals.dlq,
            scope: t('dash.currentBacklog'),
            warn: totals.dlq > 0,
          },
        ].map((c) => (
          <Card key={c.label}>
            <CardContent className="p-4">
              <div className="text-xs text-muted-foreground">{c.label}</div>
              <div
                className={cn(
                  'mt-1 tabular text-2xl font-bold',
                  c.warn ? 'text-rose-600 dark:text-rose-400' : '',
                )}
              >
                {c.value.toLocaleString()}
              </div>
              <div className="mt-1 text-[11px] text-muted-foreground">{c.scope}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm font-semibold">{t('dash.criticalPipes')}</CardTitle>
          <button
            type="button"
            className="text-xs font-semibold text-primary"
            onClick={() => onOpenPipeline?.('')}
          >
            {t('dash.viewAllPipes')} →
          </button>
        </CardHeader>
        <CardContent className="space-y-0 p-0">
          {(criticalPipes.length ? criticalPipes : pList.slice(0, 4).map((p) => ({
            p,
            m: findMetric(p, mList),
            health: derivePipelineHealth(p, findMetric(p, mList)),
          }))).map(({ p, m, health }) => (
            <button
              type="button"
              key={pipelineKey(p)}
              className="grid w-full grid-cols-1 items-center gap-3 border-t border-border px-5 py-4 text-left transition hover:bg-muted/40 first:border-t-0 md:grid-cols-[minmax(160px,.8fr)_minmax(0,1.4fr)_120px_100px]"
              onClick={() => {
                onSelect(pipelineKey(p));
                onOpenPipeline?.(pipelineKey(p), health === 'failed' || health === 'degraded' ? 'issues' : 'overview');
              }}
            >
              <div className="flex min-w-0 items-center gap-2">
                <HealthDot health={health} />
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold">{p.name}</div>
                  <div className="mt-0.5 flex items-center gap-2">
                    <PipelineHealthBadge health={health} t={t} />
                    <span className="text-[11px] text-muted-foreground">
                      {deriveModeLabel(p, m)}
                    </span>
                  </div>
                </div>
              </div>
              <PipelinePath pipeline={p} />
              <div className="tabular text-xs">
                {(p.stats.records_dlq || 0) > 0 ? (
                  <>
                    <div className="font-semibold">{p.stats.records_dlq.toLocaleString()}</div>
                    <div className="text-muted-foreground">DLQ</div>
                  </>
                ) : m && m.cdc_lag_ms > 0 ? (
                  <>
                    <div className="font-semibold">{formatLag(m.cdc_lag_ms)}</div>
                    <div className="text-muted-foreground">CDC lag</div>
                  </>
                ) : (
                  <>
                    <div className="font-semibold">
                      {(p.stats.records_written || 0).toLocaleString()}
                    </div>
                    <div className="text-muted-foreground">{t('pipe.written')}</div>
                  </>
                )}
              </div>
              <div>
                {health === 'failed' || health === 'degraded' ? (
                  <span className="inline-flex items-center gap-1 rounded bg-rose-50 px-2 py-1 text-[11px] font-bold text-rose-700 dark:bg-rose-950/40 dark:text-rose-300">
                    <AlertTriangle className="h-3 w-3" />
                    {t('dash.actionView')}
                  </span>
                ) : (
                  <span className="inline-flex rounded bg-emerald-50 px-2 py-1 text-[11px] font-bold text-emerald-700 dark:bg-emerald-950/40 dark:text-emerald-300">
                    {t('health.healthy')}
                  </span>
                )}
              </div>
            </button>
          ))}
          {!pList.length && (
            <div className="p-8">
              <EmptyState text={t('dash.noPipelines')} />
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

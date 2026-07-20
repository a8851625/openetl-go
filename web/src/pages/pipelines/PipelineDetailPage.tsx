import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { MiniStat, Progress } from '@/components/shared/progress';
import { PipelineHealthBadge, HealthDot } from '@/components/shared/pipeline-health-badge';
import { PipelinePath } from '@/components/shared/pipeline-path';
import { confirmAction } from '@/components/shared/confirm-dialog';
import { api, pipelineKey, pipelineRef } from '@/lib/api';
import { fmtTime, ratio } from '@/lib/format';
import {
  deriveModeLabel,
  derivePipelineHealth,
  formatLag,
} from '@/lib/pipeline-health';
import type { DetailTab } from '@/lib/routing';
import type { Checkpoint, MetricsPipeline, Pipeline, TFunc } from '@/lib/types';
import { ArrowLeft, GitBranch, Play, Square } from 'lucide-react';
import { PipelineRowMeta } from '@/lib/uptime';
import { cn } from '@/lib/utils';

type Props = {
  t: TFunc;
  pipeline?: Pipeline;
  metric?: MetricsPipeline;
  checkpoints: Checkpoint[];
  tab: DetailTab;
  onTabChange: (tab: DetailTab) => void;
  onBack: () => void;
  onAction: (label: string, fn: () => Promise<unknown>) => void;
  onResetCheckpoint: (ref: string, label?: string) => void;
  onEdit: (ref: string) => void;
  onOpenDLQ: (key: string) => void;
  onOpenDesigner: (ref: string) => void;
};

export function PipelineDetailPage({
  t,
  pipeline,
  metric,
  checkpoints,
  tab,
  onTabChange,
  onBack,
  onAction,
  onResetCheckpoint,
  onEdit,
  onOpenDLQ,
  onOpenDesigner,
}: Props) {
  const [resetName, setResetName] = useState('');
  const [versions, setVersions] = useState<{ version: number; created_at: string }[]>([]);
  const [specYaml, setSpecYaml] = useState('');
  const [specError, setSpecError] = useState('');

  useEffect(() => {
    if (!pipeline) return;
    const ref = pipelineRef(pipeline);
    if (tab === 'spec') {
      setSpecError('');
      api<{ yaml?: string; spec?: string } | string>(`/api/v2/pipelines/${ref}/export`)
        .then((d) => {
          if (typeof d === 'string') setSpecYaml(d);
          else setSpecYaml((d as any).yaml || (d as any).spec || JSON.stringify(d, null, 2));
        })
        .catch(async () => {
          // export may return raw yaml text
          try {
            const token = localStorage.getItem('etl_api_token') || '';
            const headers: Record<string, string> = {};
            if (token) headers['X-API-Token'] = token;
            const res = await fetch(`/api/v2/pipelines/${ref}/export`, { headers });
            setSpecYaml(await res.text());
          } catch (e) {
            setSpecError(String(e));
          }
        });
      api<{ versions?: { version: number; created_at: string }[] }>(
        `/api/v2/pipelines/${ref}/versions`,
      )
        .then((d) => setVersions(d.versions || []))
        .catch(() => setVersions([]));
    }
  }, [pipeline?.id, pipeline?.name, tab]);

  if (!pipeline) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" size="sm" onClick={onBack}>
          <ArrowLeft className="h-4 w-4" /> {t('pipe.backToList')}
        </Button>
        <EmptyState text={t('pipe.notFound')} />
      </div>
    );
  }

  const health = derivePipelineHealth(pipeline, metric);
  const ref = pipelineRef(pipeline);
  const key = pipelineKey(pipeline);
  const cps = checkpoints.filter(
    (c) => c.job_name === pipeline.name || c.id.includes(pipeline.name),
  );

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <Button variant="ghost" size="sm" onClick={onBack}>
          <ArrowLeft className="h-4 w-4" /> {t('pipe.backToList')}
        </Button>
        <PipelineHealthBadge health={health} t={t} />
        <span className="rounded bg-muted px-2 py-0.5 text-[11px] font-medium">
          {deriveModeLabel(pipeline, metric)}
        </span>
        {(pipeline.tags || []).map((tag) => (
          <span key={tag} className="rounded bg-muted px-2 py-0.5 text-[11px] text-muted-foreground">
            {tag}
          </span>
        ))}
      </div>

      <Card>
        <CardContent className="space-y-4 p-5">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="min-w-0 space-y-2">
              <div className="text-xs text-muted-foreground">/pipelines/{key}</div>
              <div className="flex items-center gap-2">
                <HealthDot health={health} />
                <h2 className="text-2xl font-semibold tracking-tight">{pipeline.name}</h2>
              </div>
              <PipelinePath pipeline={pipeline} />
              <PipelineRowMeta
                t={t}
                startedAt={pipeline.stats.started_at}
                uptimeFallback={pipeline.stats.uptime || 'N/A'}
                written={pipeline.stats.records_written}
                cdcLagMs={metric?.cdc_lag_ms}
              />
            </div>
            <div className="flex flex-wrap gap-2">
              <Button
                size="sm"
                variant="secondary"
                disabled={pipeline.status === 'running'}
                onClick={() =>
                  onAction(`Start ${pipeline.name}`, () =>
                    api(`/api/v2/pipelines/${ref}/start`, { method: 'POST' }),
                  )
                }
              >
                <Play className="h-3.5 w-3.5" /> {t('pipe.start')}
              </Button>
              <Button
                size="sm"
                variant="outline"
                className="text-rose-700"
                disabled={pipeline.status !== 'running'}
                onClick={() =>
                  onAction(`Stop ${pipeline.name}`, () =>
                    api(`/api/v2/pipelines/${ref}/stop`, { method: 'POST' }),
                  )
                }
              >
                <Square className="h-3.5 w-3.5" /> {t('pipe.stop')}
              </Button>
              <Button size="sm" variant="outline" onClick={() => onOpenDesigner(ref)}>
                <GitBranch className="h-3.5 w-3.5" /> {t('pipe.advancedDag')}
              </Button>
              <Button size="sm" onClick={() => onOpenDLQ(key)}>
                {t('pipe.handleIssues')}
              </Button>
            </div>
          </div>

          <Tabs
            value={tab}
            onValueChange={(v) => onTabChange(v as DetailTab)}
          >
            <TabsList className="h-auto flex-wrap justify-start">
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="runs">Runs</TabsTrigger>
              <TabsTrigger value="issues">Issues</TabsTrigger>
              <TabsTrigger value="checkpoints">Checkpoints</TabsTrigger>
              <TabsTrigger value="spec">Spec & versions</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="mt-4 space-y-4">
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                <MiniStat
                  label={t('pipe.written')}
                  value={(pipeline.stats.records_written || 0).toLocaleString()}
                />
                <MiniStat
                  label={t('dash.failedRecords')}
                  value={(pipeline.stats.records_failed || 0).toLocaleString()}
                />
                <MiniStat
                  label={t('dash.dlqBacklog')}
                  value={(pipeline.stats.records_dlq || 0).toLocaleString()}
                />
                <MiniStat
                  label={t('metric.cdcLag')}
                  value={metric ? formatLag(metric.cdc_lag_ms) : '—'}
                />
              </div>
              {metric && (
                <div className="grid gap-4 md:grid-cols-2">
                  <Card className="border-dashed">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">{t('metric.writeSuccess')}</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <Progress
                        label={t('metric.writeSuccessRate')}
                        value={ratio(
                          metric.records_written,
                          metric.records_written + metric.records_failed,
                        )}
                      />
                      <div className="mt-3 grid grid-cols-2 gap-2">
                        <MiniStat
                          label={t('metric.readLatency')}
                          value={`${metric.source_read_latency_ms.toFixed(1)}ms`}
                        />
                        <MiniStat
                          label={t('metric.writeLatency')}
                          value={`${metric.sink_write_latency_ms.toFixed(1)}ms`}
                        />
                        <MiniStat label={t('metric.avgBatch')} value={String(metric.avg_batch_size)} />
                        <MiniStat
                          label={t('metric.cpAge')}
                          value={`${metric.checkpoint_age_seconds}s`}
                        />
                      </div>
                    </CardContent>
                  </Card>
                  <Card className="border-dashed">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">{t('pipe.recentIssues')}</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-2 text-sm">
                      {pipeline.stats.last_error ? (
                        <div className="rounded-lg border border-rose-200 bg-rose-50 p-3 text-rose-800 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-200">
                          {pipeline.stats.last_error}
                        </div>
                      ) : (pipeline.stats.records_dlq || 0) > 0 ? (
                        <div className="rounded-lg border border-amber-200 bg-amber-50 p-3 text-amber-900 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-200">
                          DLQ backlog {pipeline.stats.records_dlq.toLocaleString()}
                        </div>
                      ) : (
                        <EmptyState text={t('issues.empty')} />
                      )}
                      <Button variant="link" className="h-auto p-0" onClick={() => onOpenDLQ(key)}>
                        {t('pipe.goDlq')} →
                      </Button>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>

            <TabsContent value="runs" className="mt-4">
              <div className="overflow-x-auto rounded-lg border border-border">
                <table className="w-full text-left text-sm">
                  <thead className="bg-muted/50 text-xs text-muted-foreground">
                    <tr>
                      <th className="px-3 py-2">Status</th>
                      <th className="px-3 py-2">Read</th>
                      <th className="px-3 py-2">Written</th>
                      <th className="px-3 py-2">Failed</th>
                      <th className="px-3 py-2">DLQ</th>
                      <th className="px-3 py-2">Started</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-t border-border">
                      <td className="px-3 py-2">
                        <PipelineHealthBadge health={health} t={t} />
                      </td>
                      <td className="tabular px-3 py-2">
                        {(pipeline.stats.records_read || 0).toLocaleString()}
                      </td>
                      <td className="tabular px-3 py-2">
                        {(pipeline.stats.records_written || 0).toLocaleString()}
                      </td>
                      <td className="tabular px-3 py-2">
                        {(pipeline.stats.records_failed || 0).toLocaleString()}
                      </td>
                      <td className="tabular px-3 py-2">
                        {(pipeline.stats.records_dlq || 0).toLocaleString()}
                      </td>
                      <td className="px-3 py-2 text-muted-foreground">
                        {pipeline.stats.started_at ? fmtTime(pipeline.stats.started_at) : '—'}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <p className="mt-2 text-xs text-muted-foreground">{t('pipe.runsNote')}</p>
            </TabsContent>

            <TabsContent value="issues" className="mt-4 space-y-3">
              {(pipeline.stats.records_failed || 0) > 0 ||
              (pipeline.stats.records_dlq || 0) > 0 ||
              pipeline.stats.last_error ? (
                <>
                  {pipeline.stats.last_error && (
                    <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-rose-200 bg-rose-50 p-4 dark:border-rose-900 dark:bg-rose-950/40">
                      <div>
                        <div className="text-sm font-semibold text-rose-800 dark:text-rose-200">
                          {pipeline.stats.last_error}
                        </div>
                        <div className="mt-1 text-xs text-rose-700/80 dark:text-rose-300/80">
                          {t('dash.failedRecords')}:{' '}
                          {(pipeline.stats.records_failed || 0).toLocaleString()}
                        </div>
                      </div>
                      <Button size="sm" onClick={() => onOpenDLQ(key)}>
                        {t('issues.repair')}
                      </Button>
                    </div>
                  )}
                  {(pipeline.stats.records_dlq || 0) > 0 && (
                    <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-amber-200 bg-amber-50 p-4 dark:border-amber-900 dark:bg-amber-950/40">
                      <div>
                        <div className="text-sm font-semibold">
                          DLQ backlog {(pipeline.stats.records_dlq || 0).toLocaleString()}
                        </div>
                        <div className="mt-1 text-xs text-muted-foreground">
                          {t('pipe.dlqHint')}
                        </div>
                      </div>
                      <Button size="sm" onClick={() => onOpenDLQ(key)}>
                        {t('pipe.goDlq')}
                      </Button>
                    </div>
                  )}
                </>
              ) : (
                <EmptyState text={t('issues.empty')} />
              )}
            </TabsContent>

            <TabsContent value="checkpoints" className="mt-4 space-y-4">
              {cps.length === 0 ? (
                <EmptyState text={t('pipe.noCheckpoints')} />
              ) : (
                <div className="space-y-2">
                  {cps.map((c) => (
                    <div
                      key={c.id}
                      className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border p-3 text-sm"
                    >
                      <div>
                        <div className="font-medium">{c.source}</div>
                        <div className="mt-0.5 font-mono text-xs text-muted-foreground">
                          {typeof c.position === 'string'
                            ? c.position
                            : JSON.stringify(c.position)}
                        </div>
                      </div>
                      <div className="text-xs text-muted-foreground">{fmtTime(c.timestamp)}</div>
                    </div>
                  ))}
                </div>
              )}
              <div className="rounded-xl border border-rose-200 bg-rose-50/60 p-4 dark:border-rose-900 dark:bg-rose-950/30">
                <div className="text-sm font-semibold text-rose-800 dark:text-rose-200">
                  {t('pipe.resetCheckpoint')}
                </div>
                <p className="mt-1 text-xs text-muted-foreground">{t('pipe.resetHint')}</p>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Input
                    className="max-w-xs"
                    placeholder={t('pipe.resetConfirmPh').replace('{name}', pipeline.name)}
                    value={resetName}
                    onChange={(e) => setResetName(e.target.value)}
                  />
                  <Button
                    variant="destructive"
                    size="sm"
                    disabled={resetName !== pipeline.name}
                    onClick={() => {
                      if (!confirmAction(t('pipe.confirmReset').replace('{name}', pipeline.name)))
                        return;
                      onResetCheckpoint(ref, pipeline.name);
                      setResetName('');
                    }}
                  >
                    {t('pipe.reset')}
                  </Button>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="spec" className="mt-4 space-y-4">
              <div className="flex flex-wrap gap-2">
                <Button size="sm" variant="secondary" onClick={() => onEdit(ref)}>
                  {t('pipe.edit')}
                </Button>
                <Button size="sm" variant="outline" onClick={() => onOpenDesigner(ref)}>
                  {t('pipe.advancedDag')}
                </Button>
              </div>
              {specError && <ErrorBox message={specError} />}
              {specYaml ? (
                <pre className="max-h-[420px] overflow-auto rounded-lg border border-border bg-muted/30 p-4 text-xs">
                  {specYaml}
                </pre>
              ) : (
                !specError && <EmptyState text={t('pipe.loadingSpec')} />
              )}
              {versions.length > 0 && (
                <div>
                  <div className="mb-2 text-sm font-semibold">{t('pipe.versions')}</div>
                  <div className="space-y-1">
                    {versions.map((v) => (
                      <div
                        key={v.version}
                        className={cn(
                          'flex items-center justify-between rounded-md border border-border px-3 py-2 text-xs',
                        )}
                      >
                        <span className="tabular font-medium">v{v.version}</span>
                        <span className="text-muted-foreground">{fmtTime(v.created_at)}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              <p className="text-xs text-muted-foreground">{t('pipe.specNote')}</p>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
    </div>
  );
}

import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { EmptyState } from '@/components/shared/empty-state';
import { normalizePipelines } from '@/lib/api';
import { deriveIssues, type DerivedIssue } from '@/lib/pipeline-health';
import type { ApiState, MetricsPipeline, Pipeline, TFunc } from '@/lib/types';
import { cn } from '@/lib/utils';
import { ArrowRight } from 'lucide-react';

type Props = {
  t: TFunc;
  pipelines: ApiState<{ pipelines: Pipeline[] }>;
  metrics: ApiState<{ pipelines: MetricsPipeline[] }>;
  onSelect: (key: string) => void;
  onOpenPipeline: (key: string, tab?: string) => void;
  onOpenDLQ: (key: string) => void;
  onOpenConnections: () => void;
};

export function IssuesPage({
  t,
  pipelines,
  metrics,
  onSelect,
  onOpenPipeline,
  onOpenDLQ,
  onOpenConnections,
}: Props) {
  const pList = normalizePipelines(pipelines.data);
  const mList = metrics.data?.pipelines || [];
  const issues = deriveIssues(pList, mList);
  const blocking = issues.filter((i) => i.severity === 'blocking').length;
  const warning = issues.filter((i) => i.severity === 'warning').length;
  const dlqTotal = pList.reduce((a, p) => a + (p.stats?.records_dlq || 0), 0);

  const act = (issue: DerivedIssue) => {
    onSelect(issue.pipelineKey);
    const issueQ = `?issue=${encodeURIComponent(issue.id)}`;
    if (issue.action === 'dlq') {
      onOpenDLQ(issue.pipelineKey);
      // keep issue param discoverable for deep links
      if (!window.location.hash.includes('issue=')) {
        window.history.replaceState(null, '', `${window.location.hash.split('?')[0]}${issueQ}`);
      }
    } else if (issue.action === 'connections') onOpenConnections();
    else onOpenPipeline(issue.pipelineKey, 'issues');
  };

  return (
    <div className="space-y-6">
      <div>
        <div className="text-xs font-bold uppercase tracking-[0.08em] text-primary">
          {t('issues.eyebrow')}
        </div>
        <h2 className="mt-1 text-2xl font-semibold tracking-tight">{t('issues.title')}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{t('issues.subtitle')}</p>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <Card>
          <CardContent className="p-4">
            <div className="text-xs text-muted-foreground">blocking</div>
            <div className="mt-1 tabular text-3xl font-bold text-rose-600">{blocking}</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-xs text-muted-foreground">degraded</div>
            <div className="mt-1 tabular text-3xl font-bold text-amber-600">{warning}</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-4">
            <div className="text-xs text-muted-foreground">DLQ backlog</div>
            <div className="mt-1 tabular text-3xl font-bold">{dlqTotal.toLocaleString()}</div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">{t('issues.inbox')}</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {!issues.length && (
            <div className="p-8">
              <EmptyState text={t('issues.empty')} />
            </div>
          )}
          {issues.map((issue) => (
            <div
              key={issue.id}
              className="grid grid-cols-1 items-center gap-3 border-t border-border px-5 py-4 md:grid-cols-[10px_minmax(0,1fr)_auto]"
            >
              <span
                className={cn(
                  'hidden h-2.5 w-2.5 rounded-full md:block',
                  issue.severity === 'blocking' ? 'bg-rose-500' : 'bg-amber-500',
                )}
              />
              <div className="min-w-0">
                <div className="text-sm font-semibold">{issue.title}</div>
                <div className="mt-0.5 text-xs text-muted-foreground">
                  <span className="font-medium text-foreground/80">{issue.pipelineName}</span>
                  {' · '}
                  {issue.summary}
                  {issue.node ? ` · node ${issue.node}` : ''}
                  {issue.field ? `.${issue.field}` : ''}
                </div>
                <div className="mt-1 text-[11px] text-muted-foreground">
                  {issue.metricLabel ? `${issue.metricLabel}: ${issue.metricValue ?? '—'}` : ''}
                  {issue.severity === 'blocking' ? ' · blocking' : ' · warning'}
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    onSelect(issue.pipelineKey);
                    onOpenPipeline(issue.pipelineKey, issue.node || issue.field ? 'issues' : 'overview');
                  }}
                >
                  {t('issues.locate')}
                </Button>
                <Button size="sm" onClick={() => act(issue)}>
                  {issue.action === 'dlq' ? t('issues.repair') : t('issues.diagnose')}
                  <ArrowRight className="h-3.5 w-3.5" />
                </Button>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}

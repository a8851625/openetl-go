import { useState } from 'react';
import { useApi } from '@/lib/api';
import type { DAGNode, DAGResponse, TFunc } from '@/lib/types';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { ToneBadge } from '@/components/shared/status-badge';
import { cn } from '@/lib/utils';
import { GitBranch, Pencil } from 'lucide-react';

const kindTone: Record<string, string> = {
  source:
    'bg-emerald-50 text-emerald-800 border-emerald-200 dark:bg-emerald-950/40 dark:text-emerald-300 dark:border-emerald-800',
  transform:
    'bg-violet-50 text-violet-800 border-violet-200 dark:bg-violet-950/40 dark:text-violet-300 dark:border-violet-800',
  sink: 'bg-sky-50 text-sky-800 border-sky-200 dark:bg-sky-950/40 dark:text-sky-300 dark:border-sky-800',
  fanout:
    'bg-amber-50 text-amber-800 border-amber-200 dark:bg-amber-950/40 dark:text-amber-300 dark:border-amber-800',
  router:
    'bg-rose-50 text-rose-800 border-rose-200 dark:bg-rose-950/40 dark:text-rose-300 dark:border-rose-800',
  tap: 'bg-muted text-foreground border-border',
  rate_limiter:
    'bg-lime-50 text-lime-800 border-lime-200 dark:bg-lime-950/40 dark:text-lime-300 dark:border-lime-800',
  enricher:
    'bg-pink-50 text-pink-800 border-pink-200 dark:bg-pink-950/40 dark:text-pink-300 dark:border-pink-800',
  lookup: 'bg-muted text-muted-foreground border-border',
};

type Props = {
  t: TFunc;
  pipelineRef: string;
  /** Optional: open full designer for editing. */
  onEdit?: () => void;
  className?: string;
  compact?: boolean;
};

/**
 * Read-only topology preview (nodes + edges + node config).
 * Editing belongs solely in the Advanced DAG designer.
 */
export function PipelineDagReadonly({ t, pipelineRef, onEdit, className, compact }: Props) {
  const { data, error, loading } = useApi<DAGResponse>(
    `/api/v2/pipelines/${encodeURIComponent(pipelineRef)}/dag`,
    0,
  );
  const [selectedNode, setSelectedNode] = useState<DAGNode | null>(null);

  const nodes = data?.dag?.nodes || [];
  const edges = data?.dag?.edges || [];

  return (
    <div className={cn('space-y-4', className)} data-testid="pipeline-dag-readonly">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <h3 className="flex items-center gap-2 text-sm font-semibold">
            <GitBranch className="h-4 w-4 text-primary" />
            {t('pipe.topologyReadonly')}
          </h3>
          <p className="max-w-2xl text-xs text-muted-foreground">{t('pipe.topologyHint')}</p>
        </div>
        {onEdit && (
          <Button size="sm" variant="outline" onClick={onEdit} data-testid="open-dag-designer">
            <Pencil className="h-3.5 w-3.5" /> {t('pipe.editInDesigner')}
          </Button>
        )}
      </div>

      {loading ? (
        <EmptyState text={t('common.loading')} />
      ) : error ? (
        <ErrorBox message={error} />
      ) : !data ? (
        <EmptyState text={t('ui.noData')} />
      ) : nodes.length === 0 ? (
        <EmptyState text={t('pipe.topologyEmpty')} />
      ) : (
        <div
          className={cn(
            'grid gap-4',
            compact ? 'grid-cols-1' : 'grid-cols-1 lg:grid-cols-[minmax(0,1fr)_minmax(240px,320px)]',
          )}
        >
          <div className="space-y-3">
            <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
              <span>
                {nodes.length} {t('pipe.dagNodes')}
              </span>
              <span>
                {edges.length} {t('pipe.dagEdges')}
              </span>
              {data.schedule?.type && (
                <span>
                  {t('pipe.dagSchedule')}: {data.schedule.type}
                  {data.schedule.cron ? ` · ${data.schedule.cron}` : ''}
                </span>
              )}
            </div>

            {/* Flow strip: source → … → sink order by edges when possible */}
            <div className="flex flex-wrap items-stretch gap-2">
              {nodes.map((n, idx) => (
                <div key={n.id} className="flex items-center gap-2">
                  {idx > 0 && (
                    <span className="hidden text-muted-foreground sm:inline" aria-hidden>
                      →
                    </span>
                  )}
                  <button
                    type="button"
                    className={cn(
                      'min-w-[7.5rem] rounded-xl border px-3 py-2.5 text-left text-xs transition hover:shadow-sm',
                      kindTone[n.kind] || 'border-border bg-card',
                      selectedNode?.id === n.id && 'ring-2 ring-primary ring-offset-1 ring-offset-background',
                    )}
                    onClick={() => setSelectedNode(n)}
                  >
                    <div className="font-semibold leading-tight">{n.id}</div>
                    <div className="mt-0.5 opacity-75">
                      {n.kind} · {n.plugin}
                    </div>
                  </button>
                </div>
              ))}
            </div>

            {edges.length > 0 && (
              <Card>
                <CardHeader className="py-2">
                  <CardTitle className="text-xs">{t('pipe.dagEdges')}</CardTitle>
                </CardHeader>
                <CardContent className="space-y-1.5">
                  {edges.map((e, i) => (
                    <div
                      key={e.id || `${e.from}-${e.to}-${i}`}
                      className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground"
                    >
                      <span className="font-mono font-medium text-foreground">{e.from}</span>
                      <span>→</span>
                      <span className="font-mono font-medium text-foreground">{e.to}</span>
                      {e.condition && (
                        <ToneBadge tone="amber" className="text-[10px]">
                          {e.condition.field} {e.condition.operator} {String(e.condition.value)}
                        </ToneBadge>
                      )}
                    </div>
                  ))}
                </CardContent>
              </Card>
            )}
          </div>

          {!compact && (
            <Card>
              <CardHeader className="py-2">
                <CardTitle className="text-xs">{t('pipe.dagConfig')}</CardTitle>
              </CardHeader>
              <CardContent>
                {selectedNode ? (
                  <div className="space-y-2">
                    <div className="text-sm font-semibold">{selectedNode.id}</div>
                    <div className="text-xs text-muted-foreground">
                      {selectedNode.kind} · {selectedNode.plugin}
                    </div>
                    <pre className="mt-2 max-h-72 overflow-auto rounded-lg border border-border bg-muted/40 p-3 font-mono text-[11px] leading-relaxed text-foreground">
                      {JSON.stringify(selectedNode.config || {}, null, 2)}
                    </pre>
                  </div>
                ) : (
                  <EmptyState text={t('pipe.dagNoConfig')} />
                )}
              </CardContent>
            </Card>
          )}
        </div>
      )}
    </div>
  );
}

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { ToneBadge } from '@/components/shared/status-badge';
import { useApi } from '@/lib/api';
import { fmtTime } from '@/lib/format';
import { routeToHash, type TaskContext } from '@/lib/routing';
import { contextMatchesRecovery, recoveryContext } from '@/lib/recovery';
import type { RecoveryEvent, RecoveryOperation, TFunc } from '@/lib/types';
import { useState } from 'react';

type Props = { t: TFunc; context?: TaskContext; pipelineId?: string; refreshKey?: number };

export function RecoveryRecords({ t, context, pipelineId, refreshKey = 0 }: Props) {
  const [retry, setRetry] = useState(0);
  const params = new URLSearchParams({ limit: '50' });
  const owner = context?.pipelineId || pipelineId;
  if (owner) params.set('pipeline_id', owner);
  if (context?.state) params.set('state', context.state);
  const list = useApi<{ events: RecoveryEvent[]; unassociated_operations: RecoveryOperation[] }>(
    `/api/v2/recovery-events?${params}`, refreshKey + retry,
  );
  const detail = useApi<{ event: RecoveryEvent; operations: RecoveryOperation[] }>(
    context?.recoveryId ? `/api/v2/recovery-events/${context.recoveryId}` : '/api/v2/recovery-events/_missing',
    refreshKey + retry, Boolean(context?.recoveryId),
  );
  const event = context?.recoveryId && detail.data?.event.id === context.recoveryId ? detail.data.event : undefined;
  const mismatch = event && !contextMatchesRecovery(context, event);
  const error = context?.recoveryId ? detail.error : list.error;
  const events = context?.recoveryId ? event ? [event] : []
    : (list.data?.events || []).filter((item) => (!owner || item.pipeline_id === owner) && contextMatchesRecovery(context, item) && (!context?.state || item.state === context.state));
  const operations = context?.recoveryId && event ? detail.data?.operations || [] : [];
  const unassociated = (list.data?.unassociated_operations || []).filter((operation) => !owner || operation.pipeline_id === owner);
  const stateLabel = (state: string) => t(`recovery.state.${state}`);

  return (
    <Card data-testid="recovery-records">
      <CardHeader className="flex flex-row items-start justify-between gap-3 pb-3">
        <div>
          <CardTitle className="text-sm">{t('recovery.title')}</CardTitle>
          <p className="mt-1 text-xs text-muted-foreground">{t('recovery.scope')}</p>
        </div>
        <Button variant="outline" size="sm" onClick={() => setRetry((n) => n + 1)}>{t('common.refresh')}</Button>
      </CardHeader>
      <CardContent className="space-y-4">
        {mismatch ? <ErrorBox message={t('recovery.contextMismatch')} />
          : error ? <ErrorBox message={`${t('recovery.unavailable')}: ${error}`} />
          : (context?.recoveryId ? detail.loading || detail.refreshing : list.loading || list.refreshing) && !events.length ? <p className="text-sm">{t('recovery.loading')}</p>
          : <>
            {!events.length && <EmptyState text={t('recovery.empty')} />}
            {events.map((record) => {
              const task = recoveryContext(record);
              const tone = record.state === 'resolved' ? 'emerald' : record.state === 'unknown' ? 'slate' : 'amber';
              return (
                <div key={record.id} className="space-y-3 border-b border-border pb-4 last:border-0 last:pb-0"
                  data-recovery-id={record.id} data-recovery-state={record.state}>
                  <div className="flex flex-wrap items-center gap-2">
                    <a className="text-sm font-semibold text-primary underline"
                      href={routeToHash({ page: 'issues', context: task })}>{record.pipeline_name}</a>
                    <ToneBadge tone={tone}>{stateLabel(record.state)}</ToneBadge>
                    {record.tracking_incomplete && <ToneBadge tone="amber">{t('recovery.incomplete')}</ToneBadge>}
                  </div>
                  <p className="text-xs text-muted-foreground">
                    {record.endpoint}{record.node_id ? ` · ${t('dlq.dagNode')} ${record.node_id}` : ''}
                    {' · '}{record.connection_known ? record.connection_name || t('recovery.inline') : t('recovery.connectionUnknown')}
                    {record.spec_version && record.spec_version > 0 ? ` · ${t('recovery.version')} ${record.spec_version}` : ''}
                  </p>
                  <div className="grid grid-cols-2 gap-2 text-xs sm:grid-cols-4">
                    <span>{t('recovery.captured')}: <b>{record.record_count}</b></span>
                    <span>{t('recovery.pending')}: <b>{record.dlq_open_rows}</b></span>
                    <span>{t('recovery.replayed')}: <b>{record.replayed_rows}</b></span>
                    <span>{t('recovery.discarded')}: <b>{record.abandoned_rows}</b></span>
                  </div>
                  <p className="break-all font-mono text-[11px] text-muted-foreground">{record.id} · {fmtTime(record.opened_at)}</p>
                  <p className="break-words text-xs">{record.first_error}</p>
                  <div className="flex flex-wrap gap-3 text-xs">
                    <a className="text-primary underline" href={routeToHash({ page: 'dlq', context: task })}>{t('nav.dlq')}</a>
                    <a className="text-primary underline" href={routeToHash({ page: 'pipeline-detail', id: record.pipeline_id, tab: 'issues', context: task })}>{t('nav.pipelines')}</a>
                    {record.connection_known && record.connection_name && (
                      <a className="text-primary underline" href={routeToHash({ page: 'connections', context: task })}>{t('nav.connections')}</a>
                    )}
                  </div>
                </div>
              );
            })}
            {operations.length > 0 && <div className="space-y-2" data-testid="recovery-operations">
              <h3 className="text-sm font-semibold">{t('recovery.outcomes')}</h3>
              {operations.map((operation) => <div key={operation.id} className="rounded border border-border p-3 text-xs"
                data-recovery-operation={operation.id} data-operation-state={operation.state}>
                <div className="flex flex-wrap gap-2 font-medium">
                  <span>{t(`recovery.kind.${operation.kind}`)}</span>
                  <span>· {stateLabel(operation.state)}</span>
                  <span>· #{operation.dlq_row_key || operation.dlq_row_id}</span>
                </div>
                <p className="mt-1 text-muted-foreground">{fmtTime(operation.started_at)}</p>
                {operation.sink_acknowledged_at && <p>{t('recovery.acknowledged')}: {fmtTime(operation.sink_acknowledged_at)}</p>}
                {operation.error && <p className="mt-1 break-words text-rose-600">{operation.error}</p>}
                <a className="mt-1 inline-block text-primary underline" href={routeToHash({ page: 'dlq', context: {
                  ...recoveryContext(event!), rowId: operation.dlq_row_key || String(operation.dlq_row_id),
                } })}>{t('recovery.row')}</a>
              </div>)}
            </div>}
            {!context?.recoveryId && unassociated.length > 0 && <div className="space-y-2 text-xs" data-testid="unassociated-recovery-outcomes">
              <h3 className="font-semibold">{t('recovery.unassociated')}</h3>
              {unassociated.slice(0, 20).map((operation) => <p key={operation.id}>
                {operation.pipeline_id} · #{operation.dlq_row_key || operation.dlq_row_id} · {stateLabel(operation.state)}
              </p>)}
            </div>}
          </>}
      </CardContent>
    </Card>
  );
}

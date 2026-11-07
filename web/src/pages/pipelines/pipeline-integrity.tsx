import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { MiniStat } from '@/components/shared/progress';
import { ToneBadge } from '@/components/shared/status-badge';
import { ConfirmDialog } from '@/components/shared/confirm-dialog';
import { apiErrorSummary } from '@/lib/api';
import { fetchReconcileCapabilities, resyncPipelineTables, runReconcile } from '@/lib/api';
import { fmtTime } from '@/lib/format';
import type {
  ReconcileCapabilities,
  ReconcileReport,
  ReconcileRowDiff,
  ReconcileTableReport,
  ResyncResult,
  TFunc,
} from '@/lib/types';

type Props = {
  t: TFunc;
  /** Pipeline id (the DLQ/reconcile APIs are addressed by pipeline id). */
  pipelineId: string;
  /** Current observed status, used to explain why resync may be refused. */
  status?: string;
};

function verdictTone(verdict: string): 'emerald' | 'amber' | 'rose' | 'slate' {
  switch (verdict) {
    case 'match':
      return 'emerald';
    case 'count_mismatch':
    case 'content_mismatch':
      return 'amber';
    case 'schema_mismatch':
    case 'missing_target_table':
    case 'error':
      return 'rose';
    default:
      return 'slate';
  }
}

function rowDiffTone(kind: string): 'rose' | 'amber' | 'blue' {
  switch (kind) {
    case 'missing_in_target':
      return 'rose';
    case 'extra_in_target':
      return 'blue';
    default:
      return 'amber';
  }
}

export function PipelineIntegrityPanel({ t, pipelineId, status }: Props) {
  const [capabilities, setCapabilities] = useState<ReconcileCapabilities | null>(null);
  const [report, setReport] = useState<ReconcileReport | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [tablesFilter, setTablesFilter] = useState('');
  const [sampleRows, setSampleRows] = useState('20');
  const [deep, setDeep] = useState(true);
  const [resyncInput, setResyncInput] = useState('');
  const [resyncBusy, setResyncBusy] = useState(false);
  const [resyncResult, setResyncResult] = useState<ResyncResult | null>(null);
  const [resyncError, setResyncError] = useState('');
  const [confirmResync, setConfirmResync] = useState(false);

  const parsedTables = tablesFilter
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  const resyncTables = resyncInput
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);

  async function loadCapabilities() {
    try {
      const doc = await fetchReconcileCapabilities(pipelineId);
      setCapabilities(doc);
    } catch (err) {
      // Capabilities are informational; a failure must not block the run button.
      setCapabilities(null);
      setError(apiErrorSummary(err, t('integrity.loadFailed')));
    }
  }

  async function onReconcile() {
    setBusy(true);
    setError('');
    try {
      const next = await runReconcile(pipelineId, {
        tables: parsedTables,
        deep,
        sample_rows: Number(sampleRows) || 0,
      });
      setReport(next);
      if (!capabilities) await loadCapabilities();
    } catch (err) {
      setError(apiErrorSummary(err, t('integrity.failed')));
    } finally {
      setBusy(false);
    }
  }

  async function onResync() {
    setConfirmResync(false);
    setResyncBusy(true);
    setResyncError('');
    try {
      const result = await resyncPipelineTables(pipelineId, resyncTables);
      setResyncResult(result);
    } catch (err) {
      setResyncError(apiErrorSummary(err, t('integrity.resyncFailed')));
    } finally {
      setResyncBusy(false);
    }
  }

  return (
    <div className="space-y-4" data-testid="pipeline-integrity">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm">{t('integrity.title')}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <p className="text-xs text-muted-foreground">{t('integrity.hint')}</p>
          <div className="flex flex-wrap items-end gap-2">
            <label className="flex flex-col gap-1 text-xs text-muted-foreground">
              {t('integrity.tables')}
              <Input
                className="w-56"
                data-testid="integrity-tables"
                placeholder={t('integrity.tablesPlaceholder')}
                value={tablesFilter}
                onChange={(event) => setTablesFilter(event.target.value)}
              />
            </label>
            <label className="flex flex-col gap-1 text-xs text-muted-foreground">
              {t('integrity.sampleRows')}
              <Input
                className="w-24"
                data-testid="integrity-sample-rows"
                value={sampleRows}
                onChange={(event) => setSampleRows(event.target.value.replace(/[^0-9]/g, ''))}
              />
            </label>
            <label className="flex items-center gap-2 pb-1 text-xs text-muted-foreground">
              <input
                type="checkbox"
                data-testid="integrity-deep"
                checked={deep}
                onChange={(event) => setDeep(event.target.checked)}
              />
              {t('integrity.deep')}
            </label>
            <Button
              size="sm"
              data-testid="integrity-run"
              disabled={busy}
              onClick={() => void onReconcile()}
            >
              {busy ? t('integrity.running') : t('integrity.run')}
            </Button>
          </div>
          {error ? (
            <div data-testid="integrity-error">
              <ErrorBox message={error} />
            </div>
          ) : null}
          {capabilities?.limits?.length ? (
            <details className="rounded-lg bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
              <summary className="cursor-pointer font-medium">{t('integrity.limits')}</summary>
              <ul className="mt-2 list-disc space-y-1 pl-5">
                {capabilities.limits.map((limit) => (
                  <li key={limit}>{limit}</li>
                ))}
              </ul>
            </details>
          ) : null}
        </CardContent>
      </Card>

      {report ? (
        <Card data-testid="integrity-report">
          <CardHeader className="flex flex-row items-center justify-between gap-2">
            <CardTitle className="text-sm">{t('integrity.report')}</CardTitle>
            <ToneBadge tone={verdictTone(report.verdict)} data-testid="integrity-verdict">
              {report.verdict}
            </ToneBadge>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
              <MiniStat label={t('integrity.source')} value={`${report.source.type} · ${report.source.database}`} />
              <MiniStat label={t('integrity.target')} value={`${report.target.type} · ${report.target.database}`} />
              <MiniStat
                label={t('integrity.matched')}
                value={`${report.summary.matched}/${report.summary.tables}`}
              />
              <MiniStat label={t('integrity.duration')} value={`${report.duration_ms ?? 0} ms`} />
            </div>
            {report.finished_at ? (
              <p className="text-xs text-muted-foreground">
                {t('integrity.finishedAt')}: {fmtTime(report.finished_at)}
              </p>
            ) : null}
            {report.tables.length === 0 ? (
              <EmptyState text={t('integrity.noTables')} />
            ) : (
              <div className="space-y-3">
                {report.tables.map((table) => (
                  <TableReport key={`${table.source_table}:${table.target_table}`} t={t} table={table} />
                ))}
              </div>
            )}
            {report.limits?.length ? (
              <details className="rounded-lg bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
                <summary className="cursor-pointer font-medium">{t('integrity.limits')}</summary>
                <ul className="mt-2 list-disc space-y-1 pl-5">
                  {report.limits.map((limit) => (
                    <li key={limit}>{limit}</li>
                  ))}
                </ul>
              </details>
            ) : null}
          </CardContent>
        </Card>
      ) : null}

      <Card>
        <CardHeader>
          <CardTitle className="text-sm">{t('integrity.resyncTitle')}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <p className="text-xs text-muted-foreground">{t('integrity.resyncHint')}</p>
          {status === 'running' ? (
            <p className="text-xs text-amber-600 dark:text-amber-400">{t('integrity.resyncNeedsStop')}</p>
          ) : null}
          <div className="flex flex-wrap items-end gap-2">
            <label className="flex flex-col gap-1 text-xs text-muted-foreground">
              {t('integrity.tables')}
              <Input
                className="w-64"
                data-testid="integrity-resync-tables"
                placeholder={t('integrity.resyncPlaceholder')}
                value={resyncInput}
                onChange={(event) => setResyncInput(event.target.value)}
              />
            </label>
            <Button
              size="sm"
              variant="outline"
              data-testid="integrity-resync"
              disabled={resyncBusy || resyncTables.length === 0}
              onClick={() => setConfirmResync(true)}
            >
              {resyncBusy ? t('integrity.resyncing') : t('integrity.resync')}
            </Button>
          </div>
          {resyncError ? (
            <div data-testid="integrity-resync-error">
              <ErrorBox message={resyncError} />
            </div>
          ) : null}
          {resyncResult ? (
            <div className="rounded-lg bg-muted/40 px-3 py-2 text-xs" data-testid="integrity-resync-result">
              <div className="font-medium">
                {t('integrity.resyncScheduled')}: {(resyncResult.tables || []).join(', ')}
              </div>
              <div className="mt-1 text-muted-foreground">
                {t('integrity.generation')}: {resyncResult.generation ?? '—'} ·{' '}
                {resyncResult.status || ''}
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>

      <ConfirmDialog
        open={confirmResync}
        onOpenChange={setConfirmResync}
        title={t('integrity.resyncConfirmTitle')}
        description={`${t('integrity.resyncConfirmBody')} ${resyncTables.join(', ')}`}
        confirmLabel={t('integrity.resync')}
        onConfirm={() => void onResync()}
      />
    </div>
  );
}

function TableReport({ t, table }: { t: TFunc; table: ReconcileTableReport }) {
  const [showDiffs, setShowDiffs] = useState(true);
  return (
    <div className="rounded-lg border border-border px-3 py-2" data-testid="integrity-table">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-medium">
          {table.source_table}
          {table.source_table !== table.target_table ? (
            <span className="text-muted-foreground"> → {table.target_table}</span>
          ) : null}
        </div>
        <ToneBadge tone={verdictTone(table.verdict)}>{table.verdict}</ToneBadge>
      </div>
      <div className="mt-2 grid gap-2 text-xs sm:grid-cols-3">
        <div>
          {t('integrity.rows')}: {table.source_rows} / {table.target_rows}
          {table.row_delta !== 0 ? (
            <span className="ml-1 text-amber-600 dark:text-amber-400">
              ({table.row_delta > 0 ? '+' : ''}
              {table.row_delta})
            </span>
          ) : null}
        </div>
        <div>
          {t('integrity.comparedColumns')}: {table.compared_columns?.length ?? 0}
        </div>
        <div>
          {t('integrity.duration')}: {table.duration_ms ?? 0} ms
        </div>
      </div>
      {table.detail ? <p className="mt-2 text-xs text-muted-foreground">{table.detail}</p> : null}
      {table.missing_target_columns?.length ? (
        <p className="mt-2 text-xs text-rose-600 dark:text-rose-400">
          {t('integrity.missingColumns')}: {table.missing_target_columns.join(', ')}
        </p>
      ) : null}
      {table.bucket_mismatches?.length ? (
        <div className="mt-2 text-xs">
          <div className="font-medium">
            {t('integrity.buckets')} ({table.bucket_key || '—'}, {table.buckets ?? 0})
          </div>
          <div className="mt-1 flex flex-wrap gap-1">
            {table.bucket_mismatches.map((bucket) => (
              <ToneBadge key={bucket.bucket} tone="amber">
                #{bucket.bucket} {bucket.source_rows}/{bucket.target_rows}
              </ToneBadge>
            ))}
          </div>
          {table.bucket_scan_truncated ? (
            <p className="mt-1 text-muted-foreground">{t('integrity.truncated')}</p>
          ) : null}
        </div>
      ) : null}
      {table.row_diffs?.length ? (
        <div className="mt-2 text-xs">
          <button
            type="button"
            className="font-medium underline-offset-2 hover:underline"
            onClick={() => setShowDiffs((prev) => !prev)}
          >
            {t('integrity.rowDiffs')} ({table.row_diffs.length}
            {table.row_diff_scope ? ` · ${table.row_diff_scope}` : ''})
          </button>
          {showDiffs ? (
            <ul className="mt-1 space-y-1">
              {table.row_diffs.map((diff) => (
                <RowDiff key={`${diff.kind}:${diff.key}`} t={t} diff={diff} />
              ))}
            </ul>
          ) : null}
          {table.row_diff_truncated ? (
            <p className="mt-1 text-muted-foreground">{t('integrity.truncated')}</p>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function RowDiff({ t, diff }: { t: TFunc; diff: ReconcileRowDiff }) {
  return (
    <li className="flex flex-wrap items-center gap-2">
      <ToneBadge tone={rowDiffTone(diff.kind)}>{diff.kind}</ToneBadge>
      <span className="font-mono">{diff.key}</span>
      {diff.source_row ? (
        <span className="text-muted-foreground">
          {t('integrity.sourceRow')}: {diff.source_row}
        </span>
      ) : null}
      {diff.target_row ? (
        <span className="text-muted-foreground">
          {t('integrity.targetRow')}: {diff.target_row}
        </span>
      ) : null}
    </li>
  );
}

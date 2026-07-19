import { useRef, useState } from 'react';
import { Modal } from '@/components/shared/modal';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { confirmAction } from '@/components/shared/confirm-dialog';
import { ToneBadge } from '@/components/shared/status-badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Textarea } from '@/components/ui/textarea';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { api, getToken, useApi } from '@/lib/api';
import { fmtTime } from '@/lib/format';
import type {
  DAGNode,
  DAGResponse,
  PipelineLogEntry,
  PipelineVersion,
  PreviewResponse,
  TFunc,
} from '@/lib/types';
import { PipelineLogDrawer } from './pipeline-logs';
import { cn } from '@/lib/utils';

export function PipelineLogModal({
  t,
  name,
  refId,
  onClose,
}: {
  t: TFunc;
  name: string;
  refId: string;
  onClose: () => void;
}) {
  return (
    <Modal title={`${t('pipe.logs')} · ${name}`} onClose={onClose} className="sm:max-w-4xl">
      <div style={{ height: '60vh' }}>
        <PipelineLogDrawer t={t} name={refId} />
      </div>
    </Modal>
  );
}

export function PipelinePreviewModal({
  t,
  name,
  refId,
  onClose,
}: {
  t: TFunc;
  name: string;
  refId: string;
  onClose: () => void;
}) {
  const { data, error, loading } = useApi<PreviewResponse>(
    `/api/v2/pipelines/${refId}/preview`,
    0,
  );

  const renderStage = (title: string, entries?: PipelineLogEntry[]) => (
    <Card>
      <CardHeader className="py-2">
        <CardTitle className="text-xs">{title}</CardTitle>
      </CardHeader>
      <CardContent className="max-h-48 overflow-y-auto bg-slate-900 p-0 font-mono text-xs">
        {entries && entries.length > 0 ? (
          entries.map((e, i) => (
            <div
              key={i}
              className="flex gap-2 border-b border-slate-800 px-3 py-1 hover:bg-white/5"
            >
              <span className="shrink-0 text-slate-500">{e.timestamp.slice(11, 23)}</span>
              <span
                className={cn(
                  'w-10 shrink-0',
                  e.level === 'ERROR'
                    ? 'text-rose-400'
                    : e.level === 'WARN'
                      ? 'text-amber-300'
                      : 'text-emerald-400',
                )}
              >
                {e.level}
              </span>
              <span className="truncate text-slate-300">{e.message}</span>
            </div>
          ))
        ) : (
          <div className="py-6 text-center text-slate-600">—</div>
        )}
      </CardContent>
    </Card>
  );

  return (
    <Modal title={`${t('pipe.previewTitle')} · ${name}`} onClose={onClose} className="sm:max-w-4xl">
      {loading ? (
        <EmptyState text={t('common.loading')} />
      ) : error ? (
        <ErrorBox message={error} />
      ) : !data || (data.total_logs === 0 && (data.shard_logs?.length ?? 0) === 0) ? (
        <EmptyState text={t('pipe.noPreview')} />
      ) : (
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            {renderStage(t('pipe.previewSource'), data.stages?.source)}
            {renderStage(t('pipe.previewTransform'), data.stages?.transform)}
            {renderStage(t('pipe.previewSink'), data.stages?.sink)}
          </div>
          <div className="text-xs text-muted-foreground">
            {t('pipe.previewShardLogs')} ({data.shard_logs?.length || 0})
          </div>
          {data.shard_logs?.map((s, i) => (
            <Card key={i}>
              <CardHeader className="py-2">
                <CardTitle className="text-xs">#{s.shard}</CardTitle>
              </CardHeader>
              <CardContent className="max-h-32 overflow-y-auto bg-slate-900 p-0 font-mono text-xs">
                {s.entries?.map((e, j) => (
                  <div key={j} className="flex gap-2 border-b border-slate-800 px-3 py-1">
                    <span className="text-slate-500">{e.timestamp.slice(11, 23)}</span>
                    <span className="text-slate-300">{e.message}</span>
                  </div>
                ))}
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </Modal>
  );
}

export function PipelineDAGModal({
  t,
  name,
  refId,
  onClose,
}: {
  t: TFunc;
  name: string;
  refId: string;
  onClose: () => void;
}) {
  const { data, error, loading } = useApi<DAGResponse>(`/api/v2/pipelines/${refId}/dag`, 0);
  const [selectedNode, setSelectedNode] = useState<DAGNode | null>(null);

  const kindColor: Record<string, string> = {
    source: 'bg-sky-100 text-sky-700 border-sky-300 dark:bg-sky-950/40 dark:text-sky-300 dark:border-sky-800',
    transform:
      'bg-violet-100 text-violet-700 border-violet-300 dark:bg-violet-950/40 dark:text-violet-300 dark:border-violet-800',
    sink: 'bg-emerald-100 text-emerald-700 border-emerald-300 dark:bg-emerald-950/40 dark:text-emerald-300 dark:border-emerald-800',
    fanout:
      'bg-amber-100 text-amber-700 border-amber-300 dark:bg-amber-950/40 dark:text-amber-300 dark:border-amber-800',
    router:
      'bg-rose-100 text-rose-700 border-rose-300 dark:bg-rose-950/40 dark:text-rose-300 dark:border-rose-800',
    tap: 'bg-cyan-100 text-cyan-700 border-cyan-300 dark:bg-cyan-950/40 dark:text-cyan-300 dark:border-cyan-800',
    rate_limiter:
      'bg-lime-100 text-lime-700 border-lime-300 dark:bg-lime-950/40 dark:text-lime-300 dark:border-lime-800',
    enricher:
      'bg-pink-100 text-pink-700 border-pink-300 dark:bg-pink-950/40 dark:text-pink-300 dark:border-pink-800',
    lookup:
      'bg-purple-100 text-purple-700 border-purple-300 dark:bg-purple-950/40 dark:text-purple-300 dark:border-purple-800',
  };

  return (
    <Modal title={`${t('pipe.dagTitle')} · ${name}`} onClose={onClose} className="sm:max-w-5xl">
      <Tabs defaultValue="dag">
        <TabsList className="mb-4">
          <TabsTrigger value="dag">🔀 {t('pipe.dag')}</TabsTrigger>
          <TabsTrigger value="logs">📋 {t('pipe.logs')}</TabsTrigger>
        </TabsList>

        <TabsContent value="dag" className="mt-0">
          {loading ? (
            <EmptyState text={t('common.loading')} />
          ) : error ? (
            <ErrorBox message={error} />
          ) : !data ? (
            <EmptyState text={t('ui.noData')} />
          ) : (
            <div className="grid grid-cols-1 gap-4 lg:grid-cols-[1fr_320px]">
              <div className="space-y-3">
                <div className="flex gap-4 text-xs text-muted-foreground">
                  <span>
                    📊 {data.dag.nodes?.length || 0} {t('pipe.dagNodes')}
                  </span>
                  <span>
                    🔗 {data.dag.edges?.length || 0} {t('pipe.dagEdges')}
                  </span>
                  {data.schedule && (
                    <span>
                      ⏰ {data.schedule.type}
                      {data.schedule.cron ? `: ${data.schedule.cron}` : ''}
                    </span>
                  )}
                </div>
                <div className="flex flex-wrap gap-2">
                  {(data.dag.nodes || []).map((n) => (
                    <div
                      key={n.id}
                      className={cn(
                        'cursor-pointer rounded-lg border-2 px-3 py-2 text-xs font-medium transition-all hover:shadow-md',
                        kindColor[n.kind] || 'border-border bg-muted',
                        selectedNode?.id === n.id && 'ring-2 ring-primary',
                      )}
                      onClick={() => setSelectedNode(n)}
                    >
                      <div className="font-bold">{n.id}</div>
                      <div className="opacity-70">
                        {n.kind} · {n.plugin}
                      </div>
                    </div>
                  ))}
                </div>
                {(data.dag.edges || []).length > 0 && (
                  <Card>
                    <CardHeader className="py-2">
                      <CardTitle className="text-xs">{t('pipe.dagEdges')}</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-1">
                      {(data.dag.edges || []).map((e, i) => (
                        <div key={i} className="text-xs text-muted-foreground">
                          <span className="font-mono">{e.from}</span>
                          <span className="mx-2">→</span>
                          <span className="font-mono">{e.to}</span>
                          {e.condition && (
                            <ToneBadge tone="amber" className="ml-2 text-[10px]">
                              {e.condition.field} {e.condition.operator}{' '}
                              {String(e.condition.value)}
                            </ToneBadge>
                          )}
                        </div>
                      ))}
                    </CardContent>
                  </Card>
                )}
              </div>
              <Card>
                <CardHeader className="py-2">
                  <CardTitle className="text-xs">{t('pipe.dagConfig')}</CardTitle>
                </CardHeader>
                <CardContent>
                  {selectedNode ? (
                    <div className="space-y-2">
                      <div className="text-sm font-bold">{selectedNode.id}</div>
                      <div className="text-xs text-muted-foreground">
                        {selectedNode.kind} · {selectedNode.plugin}
                      </div>
                      <pre className="mt-2 max-h-64 overflow-x-auto rounded-lg bg-slate-900 p-3 text-xs text-slate-300">
                        {JSON.stringify(selectedNode.config || {}, null, 2)}
                      </pre>
                    </div>
                  ) : (
                    <EmptyState text={t('pipe.dagNoConfig')} />
                  )}
                </CardContent>
              </Card>
            </div>
          )}
        </TabsContent>

        <TabsContent value="logs" className="mt-0">
          <div style={{ height: '55vh' }}>
            <PipelineLogDrawer t={t} name={refId} />
          </div>
        </TabsContent>
      </Tabs>
    </Modal>
  );
}

export function PipelineVersionsModal({
  t,
  name,
  refId,
  onClose,
  onAction,
}: {
  t: TFunc;
  name: string;
  refId: string;
  onClose: () => void;
  onAction: (label: string, fn: () => Promise<unknown>) => void;
}) {
  const { data, error, loading } = useApi<{ versions: PipelineVersion[] }>(
    `/api/v2/pipelines/${refId}/versions`,
    0,
  );
  const [diffData, setDiffData] = useState<{
    version: number;
    current: string;
    historical: string;
  } | null>(null);

  const doRollback = async (version: number) => {
    if (!confirmAction(t('pipe.confirmRollback').replace('{version}', String(version)))) return;
    onAction(t('pipe.rolledBack').replace('{version}', String(version)), () =>
      api(`/api/v2/pipelines/${refId}/versions/${version}/rollback`, { method: 'POST' }),
    );
    onClose();
  };

  const doDiff = async (version: number) => {
    try {
      const d = await api<{
        version: { version: number; spec_yaml: string };
        current: string;
        historical: string;
      }>(`/api/v2/pipelines/${refId}/versions/${version}/diff`);
      setDiffData({
        version: d.version.version,
        current: d.current,
        historical: d.version.spec_yaml,
      });
    } catch {
      /* ignore */
    }
  };

  return (
    <Modal
      title={`${t('pipe.versionsTitle')} · ${name}`}
      onClose={onClose}
      className="sm:max-w-4xl"
    >
      {loading ? (
        <EmptyState text={t('common.loading')} />
      ) : error ? (
        <ErrorBox message={error} />
      ) : !data?.versions?.length ? (
        <EmptyState text={t('pipe.noVersions')} />
      ) : (
        <div className="space-y-3">
          {diffData && (
            <Card className="border-primary/30">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 py-2">
                <CardTitle className="text-xs">
                  {t('pipe.versionDiff')} · v{diffData.version}
                </CardTitle>
                <Button variant="ghost" size="sm" onClick={() => setDiffData(null)}>
                  ✕
                </Button>
              </CardHeader>
              <CardContent className="grid grid-cols-2 gap-3">
                <div>
                  <div className="mb-1 text-xs font-semibold text-muted-foreground">
                    {t('pipe.diffHistorical')} (v{diffData.version})
                  </div>
                  <pre className="max-h-64 overflow-x-auto overflow-y-auto rounded-lg bg-rose-50 p-2 text-xs dark:bg-rose-950/30">
                    {diffData.historical || '(empty)'}
                  </pre>
                </div>
                <div>
                  <div className="mb-1 text-xs font-semibold text-muted-foreground">
                    {t('pipe.diffCurrent')}
                  </div>
                  <pre className="max-h-64 overflow-x-auto overflow-y-auto rounded-lg bg-emerald-50 p-2 text-xs dark:bg-emerald-950/30">
                    {diffData.current || '(empty)'}
                  </pre>
                </div>
              </CardContent>
            </Card>
          )}
          {(data.versions || []).map((v) => (
            <div
              key={v.version}
              className="flex items-center justify-between rounded-lg border border-border bg-muted/40 px-4 py-2.5"
            >
              <div>
                <div className="text-sm font-semibold">v{v.version}</div>
                <div className="text-xs text-muted-foreground">{fmtTime(v.created_at)}</div>
              </div>
              <div className="flex gap-2">
                <Button variant="secondary" size="sm" onClick={() => doDiff(v.version)}>
                  {t('pipe.versionDiff')}
                </Button>
                <Button variant="destructive" size="sm" onClick={() => doRollback(v.version)}>
                  {t('pipe.rollback')}
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}
    </Modal>
  );
}

export function SpecImportModal({
  t,
  onClose,
  onImported,
}: {
  t: TFunc;
  onClose: () => void;
  onImported: (name: string) => void;
}) {
  const [yamlText, setYamlText] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState('');
  const fileRef = useRef<HTMLInputElement>(null);

  const doImport = async () => {
    setBusy(true);
    setErr('');
    try {
      const res = await fetch('/api/v2/specs/import', {
        method: 'POST',
        headers: {
          'Content-Type': 'text/plain',
          ...(getToken() ? { 'X-API-Token': getToken() } : {}),
        },
        body: yamlText,
      });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      onImported(data.name || '');
      onClose();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => setYamlText(String(reader.result || ''));
    reader.readAsText(file);
  };

  return (
    <Modal title={t('pipe.importTitle')} onClose={onClose} className="sm:max-w-2xl">
      <div className="space-y-4">
        <div>
          <input
            ref={fileRef}
            type="file"
            accept=".yaml,.yml"
            className="hidden"
            onChange={onFileChange}
          />
          <Button variant="secondary" size="sm" onClick={() => fileRef.current?.click()}>
            📁 {t('pipe.importSelectFile')}
          </Button>
        </div>
        <div>
          <label className="mb-1 block text-xs font-medium text-muted-foreground">
            {t('pipe.importOrPaste')}
          </label>
          <Textarea
            className="w-full font-mono text-xs"
            rows={14}
            value={yamlText}
            onChange={(e) => setYamlText(e.target.value)}
            placeholder={
              'name: my-pipeline\nsource:\n  type: file\n  config:\n    path: ./input.csv\nsink:\n  type: file\n  config:\n    path: ./output.csv'
            }
          />
        </div>
        {err && <ErrorBox message={err} />}
        <Button onClick={doImport} disabled={busy || !yamlText.trim()}>
          {busy ? '...' : t('pipe.importBtn')}
        </Button>
      </div>
    </Modal>
  );
}

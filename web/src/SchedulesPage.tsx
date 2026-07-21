import React, { useEffect, useMemo, useState } from 'react';
import cronstrue from 'cronstrue';
import type { TFunc, Lang } from './types';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { StatusDot, ToneBadge } from '@/components/shared/status-badge';
import { cn } from '@/lib/utils';
import { ScheduleEditorDialog } from '@/components/schedule-editor-dialog';

type Pipeline = { id?: string; name: string; status: string; stats?: Record<string, number> };
type Schedule = { type: string; cron?: string; interval_sec?: number; depends_on?: string[] };
type ScheduleRow = Pipeline & { enabled: boolean; schedule?: Schedule };
type RunHistory = {
  id: number;
  status: string;
  started_at?: string;
  finished_at?: string;
  records_read?: number;
  records_written?: number;
  records_failed?: number;
  records_dlq?: number;
};

const selectClass =
  'flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring';

function getToken() {
  return window.localStorage.getItem('etl_api_token') || '';
}

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  headers.set('Content-Type', 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) throw new Error(data.error || text || `${res.status}`);
  return data as T;
}

function normalizePipelines(data: any): Pipeline[] {
  if (!Array.isArray(data?.pipelines)) return [];
  return data.pipelines
    .filter((p: any) => p && typeof p.name === 'string')
    .map((p: any) => ({
      id: typeof p.id === 'string' && p.id.trim() ? p.id.trim() : undefined,
      name: p.name,
      status: typeof p.status === 'string' ? p.status : 'unknown',
      stats: p.stats || {},
    }));
}

function pipelineKey(p?: Pick<Pipeline, 'id' | 'name'> | null) {
  return (p?.id || p?.name || '').trim();
}

function pipelineRef(p?: Pick<Pipeline, 'id' | 'name'> | null) {
  return encodeURIComponent(pipelineKey(p));
}

function describeSchedule(t: TFunc, lang: Lang, schedule?: Schedule) {
  if (!schedule) return t('sched.manual');
  if (schedule.type === 'cron' && schedule.cron) {
    try {
      return cronstrue.toString(schedule.cron, { locale: lang === 'zh' ? 'zh_CN' : 'en' });
    } catch {
      return t('sched.invalidCron');
    }
  }
  if (schedule.type === 'periodic') return `${schedule.interval_sec || 0}s`;
  if (schedule.type === 'dependency') {
    const deps = Array.isArray(schedule.depends_on) ? schedule.depends_on : [];
    return deps.length > 0 ? deps.join(', ') : t('sched.dependency');
  }
  return t(`sched.${schedule.type}`);
}

const typeTone: Record<string, 'emerald' | 'emerald' | 'blue' | 'emerald' | 'slate' | 'slate'> = {
  streaming: 'emerald',
  once: 'emerald',
  periodic: 'blue',
  cron: 'emerald',
  dependency: 'slate',
  manual: 'slate',
};

export function SchedulesPage({ t, lang, pipelines }: { t: TFunc; lang: Lang; pipelines: any }) {
  const [filter, setFilter] = useState('all');
  const [rows, setRows] = useState<ScheduleRow[]>([]);
  const [selected, setSelected] = useState('');
  const [editorOpen, setEditorOpen] = useState(false);
  const [cronInput, setCronInput] = useState('');
  const [cronDesc, setCronDesc] = useState('');
  const [history, setHistory] = useState<RunHistory[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [refreshKey, setRefreshKey] = useState(0);

  const allPipelines = useMemo(() => normalizePipelines(pipelines.data), [pipelines.data]);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      const loaded = await Promise.all(
        allPipelines.map(async (p) => {
          try {
            const res = await api<{ enabled: boolean; schedule?: Schedule }>(
              `/api/v2/pipelines/${pipelineRef(p)}/schedule`,
            );
            return { ...p, enabled: !!res.enabled, schedule: res.schedule };
          } catch {
            return { ...p, enabled: false };
          }
        }),
      );
      if (!cancelled) {
        setRows(loaded);
        if (!selected && loaded[0]) setSelected(pipelineKey(loaded[0]));
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [allPipelines, refreshKey, selected]);

  const selectedRow = rows.find((r) => pipelineKey(r) === selected || r.name === selected);

  useEffect(() => {
    if (!selected) {
      setHistory([]);
      return;
    }
    let cancelled = false;
    api<{ history: RunHistory[] }>(`/api/v2/pipelines/${encodeURIComponent(selected)}/history`)
      .then((res) => {
        if (!cancelled) setHistory(Array.isArray(res.history) ? res.history : []);
      })
      .catch(() => {
        if (!cancelled) setHistory([]);
      });
    return () => {
      cancelled = true;
    };
  }, [selected, refreshKey]);

  const filtered =
    filter === 'all'
      ? rows
      : rows.filter((p) => (p.enabled ? p.schedule?.type : 'manual') === filter);

  const counts = {
    streaming: rows.filter((p) => p.enabled && p.schedule?.type === 'streaming').length,
    cron: rows.filter((p) => p.enabled && p.schedule?.type === 'cron').length,
    periodic: rows.filter((p) => p.enabled && p.schedule?.type === 'periodic').length,
    manual: rows.filter((p) => !p.enabled).length,
  };

  const parseCron = () => {
    if (!cronInput) {
      setCronDesc('');
      return;
    }
    try {
      const desc = cronstrue.toString(cronInput, { locale: lang === 'zh' ? 'zh_CN' : 'en' });
      setCronDesc(desc);
    } catch {
      setCronDesc(t('sched.invalidCron'));
    }
  };

  const runNow = async () => {
    if (!selected) return;
    setBusy(true);
    setError('');
    setMessage('');
    try {
      await api(`/api/v2/pipelines/${encodeURIComponent(selected)}/start`, { method: 'POST' });
      setMessage(t('sched.runStarted'));
      setRefreshKey((n) => n + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const selectedName = selectedRow?.name || selected;

  return (
    <div className="space-y-6">
      <div className="rounded-xl border border-border bg-muted/30 px-4 py-3 text-sm text-muted-foreground" data-testid="sched-overview-hint">
        {t('sched.overviewHint')}
      </div>
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <Summary label={t('sched.streaming')} value={counts.streaming} tone="text-primary" />
        <Summary label={t('sched.cron')} value={counts.cron} tone="text-primary" />
        <Summary label={t('sched.periodic')} value={counts.periodic} tone="text-blue-600 dark:text-blue-400" />
        <Summary label={t('sched.manual')} value={counts.manual} tone="text-slate-600 dark:text-slate-300" />
      </div>

      {error && <ErrorBox message={error} />}
      {message && (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-300">
          {message}
        </div>
      )}

      <div className="grid gap-6 xl:grid-cols-[380px_minmax(0,1fr)]">
        <div className="space-y-6">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm">{t('sched.editor')}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <p className="text-xs text-muted-foreground">{t('sched.dialogHint')}</p>
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  {t('sched.pipeline')}
                </span>
                <select
                  className={selectClass}
                  value={selected}
                  onChange={(e) => setSelected(e.target.value)}
                >
                  {rows.map((p) => (
                    <option key={pipelineKey(p)} value={pipelineKey(p)}>
                      {p.name}
                    </option>
                  ))}
                </select>
              </label>
              <div className="flex flex-wrap gap-2">
                <Button
                  size="sm"
                  disabled={!selected}
                  onClick={() => setEditorOpen(true)}
                  data-testid="sched-open-editor"
                >
                  {t('sched.editInDialog')}
                </Button>
                <Button
                  size="sm"
                  variant="secondary"
                  disabled={busy || !selected}
                  onClick={runNow}
                >
                  {t('sched.runNow')}
                </Button>
              </div>
              {selectedRow && (
                <div className="rounded-lg bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
                  {describeSchedule(t, lang, selectedRow.enabled ? selectedRow.schedule : undefined)}
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm">{t('common.cron')}</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex gap-2">
                <Input
                  className="flex-1 font-mono"
                  placeholder="*/5 * * * *"
                  value={cronInput}
                  onChange={(e) => {
                    setCronInput(e.target.value);
                    setCronDesc('');
                  }}
                />
                <Button variant="secondary" onClick={parseCron}>
                  {t('common.parse')}
                </Button>
              </div>
              {cronDesc && (
                <div
                  className={cn(
                    'text-sm',
                    cronDesc === t('sched.invalidCron')
                      ? 'text-rose-600'
                      : 'text-emerald-600 dark:text-emerald-400',
                  )}
                >
                  {cronDesc}
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <div className="space-y-6">
          <Card>
            <CardHeader className="flex flex-row flex-wrap items-center justify-between gap-2 space-y-0 pb-3">
              <CardTitle className="text-sm">{t('sched.title')}</CardTitle>
              <div className="flex flex-wrap gap-2">
                {['all', 'cron', 'periodic', 'streaming', 'dependency', 'manual'].map((f) => (
                  <Button
                    key={f}
                    size="sm"
                    variant={filter === f ? 'default' : 'secondary'}
                    onClick={() => setFilter(f)}
                  >
                    {f === 'all' ? t('common.all') : t(`sched.${f}`)}
                  </Button>
                ))}
              </div>
            </CardHeader>
            <CardContent className="p-0">
              {filtered.length === 0 ? (
                <div className="p-6">
                  <EmptyState text={t('sched.noSchedules')} />
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{t('sched.pipeline')}</TableHead>
                      <TableHead>{t('sched.triggerType')}</TableHead>
                      <TableHead>{t('sched.expression')}</TableHead>
                      <TableHead>{t('common.status')}</TableHead>
                      <TableHead>{t('sched.nextRun')}</TableHead>
                      <TableHead className="w-[100px]"></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filtered.map((p) => {
                      const schedType = p.enabled ? p.schedule?.type || 'manual' : 'manual';
                      const key = pipelineKey(p);
                      return (
                        <TableRow
                          key={key}
                          className={cn('cursor-pointer', selected === key && 'bg-muted/50')}
                          onDoubleClick={() => { setSelected(key); setEditorOpen(true); }}
                          onClick={() => setSelected(key)}
                        >
                          <TableCell className="font-medium">{p.name}</TableCell>
                          <TableCell>
                            <ToneBadge tone={typeTone[schedType] || 'slate'}>{schedType}</ToneBadge>
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {p.schedule?.cron ||
                              (p.schedule?.interval_sec ? `${p.schedule.interval_sec}s` : '—')}
                          </TableCell>
                          <TableCell>
                            <StatusDot status={p.status} className="mr-2 inline-block" />
                            <span className="text-sm">{p.status}</span>
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {describeSchedule(t, lang, p.enabled ? p.schedule : undefined)}
                          </TableCell>
                          <TableCell className="text-right">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={(e) => {
                                e.stopPropagation();
                                setSelected(key);
                                setEditorOpen(true);
                              }}
                            >
                              {t('sched.edit')}
                            </Button>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm">{t('sched.history')}</CardTitle>
            </CardHeader>
            <CardContent className="p-0">
              {history.length === 0 ? (
                <div className="p-8 text-center text-sm text-muted-foreground">
                  {t('sched.noHistory')}
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>ID</TableHead>
                      <TableHead>{t('common.status')}</TableHead>
                      <TableHead>{t('sched.started')}</TableHead>
                      <TableHead>{t('sched.finished')}</TableHead>
                      <TableHead>{t('pipe.written')}</TableHead>
                      <TableHead>DLQ</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {history.map((run) => (
                      <TableRow key={run.id}>
                        <TableCell>{run.id}</TableCell>
                        <TableCell>{run.status}</TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {run.started_at || '—'}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {run.finished_at || '—'}
                        </TableCell>
                        <TableCell>{run.records_written || 0}</TableCell>
                        <TableCell>{run.records_dlq || 0}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </div>
      </div>

      <ScheduleEditorDialog
        t={t}
        lang={lang}
        open={editorOpen && !!selected}
        pipelineRef={encodeURIComponent(selected)}
        pipelineName={selectedName}
        onClose={() => setEditorOpen(false)}
        onSaved={() => setRefreshKey((n) => n + 1)}
      />
    </div>
  );
}

function Summary({ label, value, tone }: { label: string; value: number; tone: string }) {
  return (
    <Card>
      <CardContent className="p-5">
        <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          {label}
        </span>
        <div className={`mt-2 text-3xl font-bold ${tone}`}>{value}</div>
      </CardContent>
    </Card>
  );
}

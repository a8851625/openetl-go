import React, { useEffect, useMemo, useState } from 'react';
import cronstrue from 'cronstrue';
import type { TFunc, Lang } from './types';

type Pipeline = { name: string; status: string; stats?: Record<string, number> };
type Schedule = { type: string; cron?: string; interval_sec?: number };
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

function getToken() { return window.localStorage.getItem('etl_api_token') || ''; }

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
      name: p.name,
      status: typeof p.status === 'string' ? p.status : 'unknown',
      stats: p.stats || {},
    }));
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
  return t(`sched.${schedule.type}`);
}

export function SchedulesPage({ t, lang, pipelines }: { t: TFunc; lang: Lang; pipelines: any }) {
  const [filter, setFilter] = useState('all');
  const [rows, setRows] = useState<ScheduleRow[]>([]);
  const [selected, setSelected] = useState('');
  const [type, setType] = useState('cron');
  const [cron, setCron] = useState('*/5 * * * *');
  const [intervalSec, setIntervalSec] = useState(300);
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
      const loaded = await Promise.all(allPipelines.map(async (p) => {
        try {
          const res = await api<{ enabled: boolean; schedule?: Schedule }>(`/api/v2/pipelines/${p.name}/schedule`);
          return { ...p, enabled: !!res.enabled, schedule: res.schedule };
        } catch {
          return { ...p, enabled: false };
        }
      }));
      if (!cancelled) {
        setRows(loaded);
        if (!selected && loaded[0]) setSelected(loaded[0].name);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [allPipelines, refreshKey, selected]);

  const selectedRow = rows.find((r) => r.name === selected);

  useEffect(() => {
    if (!selectedRow) return;
    const sched = selectedRow.schedule;
    if (sched) {
      setType(sched.type || 'cron');
      setCron(sched.cron || '*/5 * * * *');
      setIntervalSec(sched.interval_sec || 300);
    }
  }, [selectedRow?.name, selectedRow?.schedule?.type, selectedRow?.schedule?.cron, selectedRow?.schedule?.interval_sec]);

  useEffect(() => {
    if (!selected) {
      setHistory([]);
      return;
    }
    let cancelled = false;
    api<{ history: RunHistory[] }>(`/api/v2/pipelines/${selected}/history`)
      .then((res) => { if (!cancelled) setHistory(Array.isArray(res.history) ? res.history : []); })
      .catch(() => { if (!cancelled) setHistory([]); });
    return () => { cancelled = true; };
  }, [selected, refreshKey]);

  const filtered = filter === 'all'
    ? rows
    : rows.filter((p) => (p.enabled ? p.schedule?.type : 'manual') === filter);

  const counts = {
    streaming: rows.filter((p) => p.enabled && p.schedule?.type === 'streaming').length,
    cron: rows.filter((p) => p.enabled && p.schedule?.type === 'cron').length,
    periodic: rows.filter((p) => p.enabled && p.schedule?.type === 'periodic').length,
    manual: rows.filter((p) => !p.enabled).length,
  };

  const parseCron = () => {
    if (!cronInput) { setCronDesc(''); return; }
    try {
      const desc = cronstrue.toString(cronInput, { locale: lang === 'zh' ? 'zh_CN' : 'en' });
      setCronDesc(desc);
    } catch {
      setCronDesc(t('sched.invalidCron'));
    }
  };

  const saveSchedule = async () => {
    if (!selected) return;
    setBusy(true);
    setError('');
    setMessage('');
    try {
      const body: Schedule = { type };
      if (type === 'cron') body.cron = cron;
      if (type === 'periodic') body.interval_sec = Number(intervalSec) || 60;
      await api(`/api/v2/pipelines/${selected}/schedule`, { method: 'PUT', body: JSON.stringify(body) });
      setMessage(t('sched.saved'));
      setRefreshKey((n) => n + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const disableSchedule = async () => {
    if (!selected) return;
    setBusy(true);
    setError('');
    setMessage('');
    try {
      await api(`/api/v2/pipelines/${selected}/schedule`, { method: 'DELETE' });
      setMessage(t('sched.disabled'));
      setRefreshKey((n) => n + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const runNow = async () => {
    if (!selected) return;
    setBusy(true);
    setError('');
    setMessage('');
    try {
      await api(`/api/v2/pipelines/${selected}/start`, { method: 'POST' });
      setMessage(t('sched.runStarted'));
      setRefreshKey((n) => n + 1);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const typeBadge: Record<string, string> = {
    streaming: 'badge-cyan',
    once: 'badge-emerald',
    periodic: 'badge-blue',
    cron: 'badge-indigo',
    manual: 'badge-slate',
  };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-4 gap-4">
        <Summary label={t('sched.streaming')} value={counts.streaming} tone="text-cyan-600" />
        <Summary label={t('sched.cron')} value={counts.cron} tone="text-indigo-600" />
        <Summary label={t('sched.periodic')} value={counts.periodic} tone="text-blue-600" />
        <Summary label={t('sched.manual')} value={counts.manual} tone="text-slate-600" />
      </div>

      {error && <div className="rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">{error}</div>}
      {message && <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-700">{message}</div>}

      <div className="grid gap-6 xl:grid-cols-[380px_minmax(0,1fr)]">
        <div className="space-y-6">
          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('sched.editor')}</h2></div>
            <div className="card-body space-y-3">
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('sched.pipeline')}</span>
                <select className="input w-full" value={selected} onChange={(e) => setSelected(e.target.value)}>
                  {rows.map((p) => <option key={p.name} value={p.name}>{p.name}</option>)}
                </select>
              </label>
              <div className="grid grid-cols-2 gap-2">
                {['cron', 'periodic', 'streaming', 'once'].map((tp) => (
                  <button key={tp} className={`btn btn-sm ${type === tp ? 'btn-primary' : 'btn-secondary'}`} onClick={() => setType(tp)}>
                    {t(`sched.${tp}`)}
                  </button>
                ))}
              </div>
              {type === 'cron' && (
                <label className="block">
                  <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('common.cron')}</span>
                  <input className="input w-full font-mono" value={cron} onChange={(e) => setCron(e.target.value)} placeholder="*/5 * * * *" />
                </label>
              )}
              {type === 'periodic' && (
                <label className="block">
                  <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('common.interval')}</span>
                  <input className="input w-full" type="number" min={1} value={intervalSec} onChange={(e) => setIntervalSec(Number(e.target.value))} />
                </label>
              )}
              <div className="grid grid-cols-3 gap-2">
                <button className="btn btn-primary btn-sm" disabled={busy || !selected} onClick={saveSchedule}>{t('sched.save')}</button>
                <button className="btn btn-secondary btn-sm" disabled={busy || !selected} onClick={runNow}>{t('sched.runNow')}</button>
                <button className="btn btn-danger btn-sm" disabled={busy || !selected || !selectedRow?.enabled} onClick={disableSchedule}>{t('sched.disable')}</button>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('common.cron')}</h2></div>
            <div className="card-body space-y-3">
              <div className="flex gap-2">
                <input className="input flex-1 font-mono" placeholder="*/5 * * * *" value={cronInput} onChange={(e) => { setCronInput(e.target.value); setCronDesc(''); }} />
                <button className="btn btn-secondary" onClick={parseCron}>{t('common.parse')}</button>
              </div>
              {cronDesc && <div className={`text-sm ${cronDesc === t('sched.invalidCron') ? 'text-rose-600' : 'text-emerald-600'}`}>{cronDesc}</div>}
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <h2 className="text-sm font-semibold">{t('sched.title')}</h2>
              <div className="flex gap-2">
                {['all', 'cron', 'periodic', 'streaming', 'manual'].map((f) => (
                  <button key={f} className={`btn btn-sm ${filter === f ? 'btn-primary' : 'btn-secondary'}`} onClick={() => setFilter(f)}>
                    {f === 'all' ? t('common.all') : t(`sched.${f}`)}
                  </button>
                ))}
              </div>
            </div>
            <div className="overflow-x-auto">
              {filtered.length === 0 ? (
                <div className="p-8">
                  <div className="rounded-lg border border-dashed border-slate-200 py-10 text-center text-sm text-slate-400">{t('sched.noSchedules')}</div>
                </div>
              ) : (
                <table className="tbl">
                  <thead>
                    <tr>
                      <th>{t('sched.pipeline')}</th>
                      <th>{t('sched.triggerType')}</th>
                      <th>{t('sched.expression')}</th>
                      <th>{t('common.status')}</th>
                      <th>{t('sched.nextRun')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((p) => {
                      const schedType = p.enabled ? p.schedule?.type || 'manual' : 'manual';
                      return (
                        <tr key={p.name} className={selected === p.name ? 'bg-slate-50' : ''} onClick={() => setSelected(p.name)}>
                          <td className="font-medium">{p.name}</td>
                          <td><span className={`badge ${typeBadge[schedType] || 'badge-slate'}`}>{schedType}</span></td>
                          <td className="text-sm text-slate-500">{p.schedule?.cron || (p.schedule?.interval_sec ? `${p.schedule.interval_sec}s` : '—')}</td>
                          <td><span className={`status-dot status-${p.status} mr-2 inline-block`} /><span className="text-sm">{p.status}</span></td>
                          <td className="text-sm text-slate-400">{describeSchedule(t, lang, p.enabled ? p.schedule : undefined)}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('sched.history')}</h2></div>
            <div className="overflow-x-auto">
              {history.length === 0 ? (
                <div className="p-8 text-center text-sm text-slate-400">{t('sched.noHistory')}</div>
              ) : (
                <table className="tbl">
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>{t('common.status')}</th>
                      <th>{t('sched.started')}</th>
                      <th>{t('sched.finished')}</th>
                      <th>{t('pipe.written')}</th>
                      <th>DLQ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.map((run) => (
                      <tr key={run.id}>
                        <td>{run.id}</td>
                        <td>{run.status}</td>
                        <td className="text-xs text-slate-500">{run.started_at || '—'}</td>
                        <td className="text-xs text-slate-500">{run.finished_at || '—'}</td>
                        <td>{run.records_written || 0}</td>
                        <td>{run.records_dlq || 0}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function Summary({ label, value, tone }: { label: string; value: number; tone: string }) {
  return (
    <div className="card card-body">
      <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</span>
      <div className={`mt-2 text-3xl font-bold ${tone}`}>{value}</div>
    </div>
  );
}

import React, { useState, useMemo } from 'react';
import cronstrue from 'cronstrue';
import type { TFunc, Lang } from './types';

type Pipeline = { name: string; status: string; stats: Record<string, number> };

function getToken() { return window.localStorage.getItem('etl_api_token') || ''; }

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  headers.set('Content-Type', 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) throw new Error((await res.text()) || `${res.status}`);
  return res.json();
}

function inferScheduleInfo(status: string, recordsRead: number): { type: string; descKey: string } {
  const isStreaming = status === 'running' && recordsRead > 0;
  if (isStreaming) return { type: 'streaming', descKey: 'sched.descStreaming' };
  if (status === 'completed') return { type: 'once', descKey: 'sched.descOnce' };
  if (status === 'running') return { type: 'streaming', descKey: 'sched.descStreamingIdle' };
  if (status === 'stopped') return { type: 'manual', descKey: 'sched.descManual' };
  return { type: 'unknown', descKey: 'sched.descUnknown' };
}

export function SchedulesPage({ t, lang, pipelines }: { t: TFunc; lang: Lang; pipelines: any }) {
  const [filter, setFilter] = useState('all');
  const [cronInput, setCronInput] = useState('');
  const [cronDesc, setCronDesc] = useState('');

  const allPipelines: Pipeline[] = pipelines.data?.pipelines || [];

  const pipelineSchedules = useMemo(() => {
    return allPipelines.map((p) => {
      const info = inferScheduleInfo(p.status, p.stats.records_read);
      return { ...p, sched: { type: info.type, desc: t(info.descKey) } };
    });
  }, [allPipelines, t]);

  const filtered = filter === 'all'
    ? pipelineSchedules
    : pipelineSchedules.filter((p) => p.sched.type === filter);

  const parseCron = () => {
    if (!cronInput) { setCronDesc(''); return; }
    try {
      const locale = lang === 'zh' ? 'zh_CN' : 'en';
      const desc = cronstrue.toString(cronInput, { locale });
      setCronDesc(desc);
    } catch {
      setCronDesc(t('sched.invalidCron'));
    }
  };

  const counts = {
    streaming: pipelineSchedules.filter((p) => p.sched.type === 'streaming').length,
    once: pipelineSchedules.filter((p) => p.sched.type === 'once').length,
    manual: pipelineSchedules.filter((p) => p.sched.type === 'manual').length,
  };

  const typeBadge: Record<string, string> = {
    streaming: 'badge-cyan',
    once: 'badge-emerald',
    periodic: 'badge-blue',
    cron: 'badge-indigo',
    dependency: 'badge-violet',
    manual: 'badge-slate',
    unknown: 'badge-slate',
  };

  return (
    <div className="space-y-6">
      {/* Summary Cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="card card-body">
          <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{t('sched.streaming')}</span>
          <div className="mt-2 text-3xl font-bold text-cyan-600">{counts.streaming}</div>
        </div>
        <div className="card card-body">
          <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{t('sched.once')}</span>
          <div className="mt-2 text-3xl font-bold text-emerald-600">{counts.once}</div>
        </div>
        <div className="card card-body">
          <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{t('sched.dependency')}</span>
          <div className="mt-2 text-3xl font-bold text-violet-600">0</div>
        </div>
      </div>

      {/* Cron Builder */}
      <div className="card">
        <div className="card-header"><h2 className="text-sm font-semibold">{t('common.cron')}</h2></div>
        <div className="card-body flex items-center gap-4">
          <input
            className="input flex-1 font-mono"
            placeholder="*/5 * * * *"
            value={cronInput}
            onChange={(e) => { setCronInput(e.target.value); setCronDesc(''); }}
          />
          <button className="btn btn-secondary" onClick={parseCron}>{t('common.parse')}</button>
          {cronDesc && (
            <span className={`text-sm ${cronDesc.startsWith('Invalid') ? 'text-rose-600' : 'text-emerald-600'}`}>
              {cronDesc}
            </span>
          )}
        </div>
        <div className="card-body border-t border-slate-100">
          <p className="text-xs text-slate-400">{t('sched.desc')}</p>
          <div className="mt-2 flex flex-wrap gap-2 text-xs">
            <code className="rounded bg-slate-100 px-2 py-1">*/5 * * * *</code>
            <span className="text-slate-400">{t('sched.example5min')}</span>
            <code className="rounded bg-slate-100 px-2 py-1">0 */6 * * *</code>
            <span className="text-slate-400">{t('sched.example6h')}</span>
            <code className="rounded bg-slate-100 px-2 py-1">0 2 * * *</code>
            <span className="text-slate-400">{t('sched.example2am')}</span>
            <code className="rounded bg-slate-100 px-2 py-1">0 0 * * 1</code>
            <span className="text-slate-400">{t('sched.exampleWeekly')}</span>
          </div>
        </div>
      </div>

      {/* Pipeline Schedule List */}
      <div className="card">
        <div className="card-header flex items-center justify-between">
          <h2 className="text-sm font-semibold">{t('sched.title')}</h2>
          <div className="flex gap-2">
            {['all', 'streaming', 'once', 'manual'].map((f) => (
              <button
                key={f}
                className={`btn btn-sm ${filter === f ? 'btn-primary' : 'btn-secondary'}`}
                onClick={() => setFilter(f)}
              >
                {f === 'all' ? t('common.all') : t(`sched.${f === 'manual' ? 'once' : f}`)}
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
                {filtered.map((p) => (
                  <tr key={p.name}>
                    <td className="font-medium">{p.name}</td>
                    <td><span className={`badge ${typeBadge[p.sched.type] || 'badge-slate'}`}>{p.sched.type}</span></td>
                    <td className="text-sm text-slate-500">{p.sched.type === 'streaming' ? '—' : p.sched.type === 'once' ? 'manual' : '—'}</td>
                    <td><span className={`status-dot status-${p.status} inline-block mr-2`} /><span className="text-sm">{p.status}</span></td>
                    <td className="text-sm text-slate-400">{p.sched.desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}

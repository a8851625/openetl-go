import React, { useState } from 'react';
import type { TFunc, Lang } from './types';

type WorkerInfo = {
  id: string;
  host: string;
  port: number;
  slots: number;
  status: string;
  labels?: Record<string, string>;
  last_heartbeat: string;
  registered_at: string;
};

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

export function WorkersPage({ t, lang: _lang }: { t: TFunc; lang: Lang }) {
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const refresh = () => {
    setLoading(true);
    api<{ workers: WorkerInfo[] }>('/api/v2/workers')
      .then((d) => { setWorkers(d.workers || []); setError(''); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  React.useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, []);

  const deregister = (id: string) => {
    api(`/api/v2/workers/${id}/deregister`, { method: 'DELETE' })
      .then(() => { refresh(); })
      .catch((e) => setError(e.message));
  };

  const totalSlots = workers.reduce((a, w) => a + w.slots, 0);
  const onlineCount = workers.filter((w) => w.status === 'online').length;

  const cards = [
    { label: t('worker.registered'), value: workers.length, sub: `${onlineCount} ${t('worker.online')}`, color: 'text-indigo-600' },
    { label: t('worker.totalSlots'), value: totalSlots, sub: `${workers.length} ${t('worker.registered')}`, color: 'text-blue-600' },
    { label: t('worker.online'), value: onlineCount, sub: `${workers.length - onlineCount} ${t('worker.offline')}`, color: 'text-emerald-600' },
  ];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        {cards.map((c) => (
          <div key={c.label} className="card card-body">
            <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{c.label}</span>
            <div className={`mt-2 text-3xl font-bold ${c.color}`}>{c.value}</div>
            <div className="mt-1 text-xs text-slate-400">{c.sub}</div>
          </div>
        ))}
      </div>

      <div className="card">
        <div className="card-header flex items-center justify-between">
          <h2 className="text-sm font-semibold">{t('worker.registered')}</h2>
          <button className="btn btn-secondary btn-sm" onClick={refresh}>{t('common.refresh')}</button>
        </div>
        <div className="overflow-x-auto">
          {error ? (
            <div className="rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">{error}</div>
          ) : loading && workers.length === 0 ? (
            <div className="p-8 text-center text-sm text-slate-400">{t('common.loading')}</div>
          ) : workers.length === 0 ? (
            <div className="p-8">
              <div className="rounded-lg border border-dashed border-slate-200 py-10 text-center text-sm text-slate-400">{t('worker.noWorkers')}</div>
            </div>
          ) : (
            <table className="tbl">
              <thead>
                <tr>
                  <th>{t('worker.id')}</th>
                  <th>{t('common.host')}</th>
                  <th>{t('common.slots')}</th>
                  <th>{t('common.labels')}</th>
                  <th>{t('common.status')}</th>
                  <th>{t('worker.lastHeartbeat')}</th>
                  <th>{t('common.actions')}</th>
                </tr>
              </thead>
              <tbody>
                {workers.map((w) => (
                  <tr key={w.id}>
                    <td className="font-medium">{w.id}</td>
                    <td className="text-sm">{w.host}:{w.port}</td>
                    <td><span className="badge badge-blue">{w.slots}</span></td>
                    <td>
                      {w.labels && Object.keys(w.labels).length > 0 ? (
                        Object.entries(w.labels).map(([k, v]) => (
                          <span key={k} className="badge badge-violet mr-1">{k}={v}</span>
                        ))
                      ) : (
                        <span className="text-xs text-slate-300">—</span>
                      )}
                    </td>
                    <td>
                      <span className={`badge ${w.status === 'online' ? 'badge-emerald' : 'badge-slate'}`}>
                        {w.status === 'online' ? t('worker.online') : t('worker.offline')}
                      </span>
                    </td>
                    <td className="text-xs text-slate-400">{fmtTime(w.last_heartbeat)}</td>
                    <td>
                      <button className="btn btn-danger btn-sm" onClick={() => deregister(w.id)}>{t('worker.deregister')}</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      <div className="card">
        <div className="card-header"><h2 className="text-sm font-semibold">{t('worker.runningTasks')}</h2></div>
        <div className="card-body">
          <div className="rounded-lg border border-dashed border-slate-200 py-8 text-center text-sm text-slate-400">
            {t('worker.noWorkers')}
          </div>
        </div>
      </div>
    </div>
  );
}

function fmtTime(v?: string) {
  if (!v || v.startsWith('0001-') || v.startsWith('1970-')) return t_na();
  try { return new Date(v).toLocaleString(); } catch { return t_na(); }
}
function t_na() { return 'n/a'; }

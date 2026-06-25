import React, { useEffect, useMemo, useState } from 'react';
import type { TFunc, Lang } from './types';

type ConnectorKind = 'source' | 'sink' | 'transform';

type ConnectionEntry = {
  name: string;
  kind: ConnectorKind;
  type: string;
  config?: Record<string, unknown>;
  last_status?: string;
  last_error?: string;
  last_tested_at?: string;
  created_at?: string;
  updated_at?: string;
};

type ConnectorDescriptor = {
  kind: ConnectorKind;
  type: string;
  maturity: string;
  required?: string[];
  capabilities?: string[];
  secret_fields?: string[];
  registered?: boolean;
};

function getToken() { return window.localStorage.getItem('etl_api_token') || ''; }

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  if (!headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) throw new Error((await res.text()) || `${res.status}`);
  return res.json();
}

const starterConfig: Record<ConnectorKind, Record<string, unknown>> = {
  source: { brokers: ['localhost:9092'], topic: 'orders' },
  sink: { host: 'localhost', database: 'default', table: 'orders_wide' },
  transform: {},
};

export function ConnectionsPage({ t, lang: _lang }: { t: TFunc; lang: Lang }) {
  const [connections, setConnections] = useState<ConnectionEntry[]>([]);
  const [descriptors, setDescriptors] = useState<ConnectorDescriptor[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState('');
  const [error, setError] = useState('');
  const [kind, setKind] = useState<ConnectorKind>('source');
  const [type, setType] = useState('kafka');
  const [name, setName] = useState('orders-kafka');
  const [configText, setConfigText] = useState(JSON.stringify(starterConfig.source, null, 2));
  const [openOnTest, setOpenOnTest] = useState(true);

  const refresh = () => {
    setLoading(true);
    Promise.all([
      api<{ connections?: ConnectionEntry[] }>('/api/v2/connections'),
      api<{ descriptors?: ConnectorDescriptor[] }>('/api/v2/connectors/descriptors').catch(() => ({ descriptors: [] })),
    ])
      .then(([c, d]) => {
        setConnections(c.connections || []);
        setDescriptors(d.descriptors || []);
        setError('');
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => { refresh(); }, []);

  const typeOptions = useMemo(() => {
    const list = descriptors.filter((d) => d.kind === kind && d.registered !== false).map((d) => d.type);
    return Array.from(new Set(list)).sort();
  }, [descriptors, kind]);

  const selectedDescriptor = descriptors.find((d) => d.kind === kind && d.type === type);

  const onKindChange = (next: ConnectorKind) => {
    setKind(next);
    const first = descriptors.find((d) => d.kind === next && d.registered !== false)?.type;
    const fallback = next === 'source' ? 'kafka' : next === 'sink' ? 'clickhouse' : 'identity';
    const nextType = first || fallback;
    setType(nextType);
    setName(`${nextType}-connection`);
    setConfigText(JSON.stringify(starterConfig[next], null, 2));
  };

  const save = async () => {
    setSaving(true);
    setError('');
    try {
      const parsed = JSON.parse(configText || '{}');
      await api('/api/v2/connections', {
        method: 'POST',
        body: JSON.stringify({ name, kind, type, config: parsed }),
      });
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const testConnection = async (conn: ConnectionEntry) => {
    setTesting(conn.name);
    setError('');
    try {
      await api(`/api/v2/connections/${encodeURIComponent(conn.name)}/test`, {
        method: 'POST',
        body: JSON.stringify({ open: openOnTest }),
      });
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      refresh();
    } finally {
      setTesting('');
    }
  };

  const deleteConnection = async (conn: ConnectionEntry) => {
    setError('');
    try {
      await api(`/api/v2/connections/${encodeURIComponent(conn.name)}`, { method: 'DELETE' });
      refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const healthClass = (status?: string) => {
    if (status === 'ok') return 'badge-emerald';
    if (status === 'error') return 'badge-rose';
    return 'badge-slate';
  };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        <div className="card card-body">
          <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{t('conn.saved')}</span>
          <div className="mt-2 text-3xl font-bold text-indigo-600">{connections.length}</div>
        </div>
        <div className="card card-body">
          <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{t('conn.healthy')}</span>
          <div className="mt-2 text-3xl font-bold text-emerald-600">{connections.filter((c) => c.last_status === 'ok').length}</div>
        </div>
        <div className="card card-body">
          <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{t('conn.descriptors')}</span>
          <div className="mt-2 text-3xl font-bold text-blue-600">{descriptors.length}</div>
        </div>
      </div>

      {error && <div className="rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">{error}</div>}

      <div className="grid grid-cols-[minmax(0,1fr)_360px] gap-6">
        <div className="card">
          <div className="card-header flex items-center justify-between">
            <h2 className="text-sm font-semibold">{t('conn.catalog')}</h2>
            <div className="flex items-center gap-3">
              <label className="flex items-center gap-2 text-xs text-slate-500">
                <input type="checkbox" checked={openOnTest} onChange={(e) => setOpenOnTest(e.target.checked)} />
                {t('conn.openOnTest')}
              </label>
              <button className="btn btn-secondary btn-sm" onClick={refresh}>{t('common.refresh')}</button>
            </div>
          </div>
          <div className="overflow-x-auto">
            {loading && connections.length === 0 ? (
              <div className="p-8 text-center text-sm text-slate-400">{t('common.loading')}</div>
            ) : connections.length === 0 ? (
              <div className="p-8">
                <div className="rounded-lg border border-dashed border-slate-200 py-10 text-center text-sm text-slate-400">{t('conn.empty')}</div>
              </div>
            ) : (
              <table className="tbl">
                <thead>
                  <tr>
                    <th>{t('common.name')}</th>
                    <th>{t('plugin.kind')}</th>
                    <th>{t('conn.type')}</th>
                    <th>{t('common.status')}</th>
                    <th>{t('conn.lastTested')}</th>
                    <th>{t('common.actions')}</th>
                  </tr>
                </thead>
                <tbody>
                  {connections.map((conn) => (
                    <tr key={conn.name}>
                      <td className="font-medium">{conn.name}</td>
                      <td><span className="badge badge-blue">{conn.kind}</span></td>
                      <td>{conn.type}</td>
                      <td>
                        <span className={`badge ${healthClass(conn.last_status)}`}>{conn.last_status || 'unknown'}</span>
                        {conn.last_error && <div className="mt-1 max-w-xs truncate text-xs text-rose-500">{conn.last_error}</div>}
                      </td>
                      <td className="text-xs text-slate-400">{fmtTime(conn.last_tested_at)}</td>
                      <td>
                        <div className="flex gap-2">
                          <button className="btn btn-secondary btn-sm" disabled={testing === conn.name} onClick={() => testConnection(conn)}>
                            {testing === conn.name ? t('conn.testing') : t('conn.test')}
                          </button>
                          <button className="btn btn-danger btn-sm" onClick={() => deleteConnection(conn)}>{t('pipe.delete')}</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        <div className="card">
          <div className="card-header"><h2 className="text-sm font-semibold">{t('conn.new')}</h2></div>
          <div className="card-body space-y-4">
            <label className="block">
              <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('common.name')}</span>
              <input className="input w-full" value={name} onChange={(e) => setName(e.target.value)} />
            </label>
            <div className="grid grid-cols-2 gap-3">
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('plugin.kind')}</span>
                <select className="input w-full" value={kind} onChange={(e) => onKindChange(e.target.value as ConnectorKind)}>
                  <option value="source">source</option>
                  <option value="sink">sink</option>
                  <option value="transform">transform</option>
                </select>
              </label>
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('conn.type')}</span>
                <select className="input w-full" value={type} onChange={(e) => { setType(e.target.value); setName(`${e.target.value}-connection`); }}>
                  {(typeOptions.length > 0 ? typeOptions : [type]).map((item) => <option key={item} value={item}>{item}</option>)}
                </select>
              </label>
            </div>
            {selectedDescriptor && (
              <div className="space-y-2 rounded-lg bg-slate-50 p-3">
                <div className="flex flex-wrap gap-1">
                  <span className={`badge ${selectedDescriptor.maturity === 'production' ? 'badge-emerald' : selectedDescriptor.maturity === 'beta' ? 'badge-blue' : 'badge-amber'}`}>
                    {selectedDescriptor.maturity}
                  </span>
                  {(selectedDescriptor.capabilities || []).slice(0, 4).map((cap) => <span key={cap} className="badge badge-slate">{cap}</span>)}
                </div>
                <div className="text-xs text-slate-500">
                  {t('conn.required')}: {(selectedDescriptor.required || []).join(', ') || 'n/a'}
                </div>
                <div className="text-xs text-slate-500">
                  {t('conn.secrets')}: {(selectedDescriptor.secret_fields || []).join(', ') || 'n/a'}
                </div>
              </div>
            )}
            <label className="block">
              <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('dag.config')}</span>
              <textarea
                className="input h-52 w-full resize-none font-mono text-xs leading-relaxed"
                value={configText}
                onChange={(e) => setConfigText(e.target.value)}
              />
            </label>
            <button className="btn btn-primary w-full" disabled={saving} onClick={save}>
              {saving ? t('ui.starting') : t('conn.save')}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function fmtTime(v?: string) {
  if (!v || v.startsWith('0001-') || v.startsWith('1970-')) return 'n/a';
  try { return new Date(v).toLocaleString(); } catch { return 'n/a'; }
}

import React, { useMemo, useState } from 'react';
import type { TFunc, Lang } from './types';

type WidePreview = {
  valid: boolean;
  errors?: string[];
  warnings?: string[];
  source?: Record<string, unknown>;
  envelope?: Record<string, unknown>;
  lookups?: Record<string, unknown>[];
  window?: Record<string, unknown>;
  sink?: Record<string, unknown>;
  sample?: Record<string, unknown>[];
  field_types?: Record<string, string>;
  proposed_ddl?: string;
};

function getToken() { return window.localStorage.getItem('etl_api_token') || ''; }

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  if (!headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok && !data.valid) return data as T;
  if (!res.ok) throw new Error(text || `${res.status}`);
  return data as T;
}

const sampleDebezium = {
  payload: {
    op: 'c',
    ts_ms: 1710000000123,
    source: { table: 'orders' },
    after: { id: 10001, user_id: 1001, amount: 128.5, region: 'east' },
  },
};

export function WideTablePage({ t, lang: _lang }: { t: TFunc; lang: Lang }) {
  const [pipelineName, setPipelineName] = useState('orders-wide-aggregate');
  const [brokers, setBrokers] = useState('redpanda:9092');
  const [topic, setTopic] = useState('orders.cdc');
  const [groupID, setGroupID] = useState('openetl-wide-orders');
  const [lookupDSN, setLookupDSN] = useState('root:root@tcp(mysql:3306)/app');
  const [lookupQuery, setLookupQuery] = useState('SELECT id, tier, region FROM dim_users');
  const [joinKey, setJoinKey] = useState('user_id');
  const [dimKey, setDimKey] = useState('id');
  const [lookupFields, setLookupFields] = useState('tier,region');
  const [groupBy, setGroupBy] = useState('region,tier');
  const [sumField, setSumField] = useState('amount');
  const [windowSize, setWindowSize] = useState(60);
  const [clickhouseDB, setClickhouseDB] = useState('wide');
  const [clickhouseTable, setClickhouseTable] = useState('order_minute_aggregate');
  const [sampleText, setSampleText] = useState(JSON.stringify(sampleDebezium, null, 2));
  const [preview, setPreview] = useState<WidePreview | null>(null);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [loading, setLoading] = useState(false);
  const [deploying, setDeploying] = useState(false);
  const [previewSpecJSON, setPreviewSpecJSON] = useState('');

  const spec = useMemo(() => {
    const groupFields = csv(groupBy);
    return {
      name: pipelineName,
      source: {
        type: 'kafka',
        config: {
          brokers: csv(brokers),
          topic,
          group_id: groupID,
          format: 'json',
          initial_offset: 'oldest',
        },
      },
      transforms: [
        { type: 'normalize_envelope', config: { keep_metadata: true } },
        {
          type: 'lookup',
          config: {
            dsn: lookupDSN,
            query: lookupQuery,
            join_key: joinKey,
            dim_key: dimKey,
            fields: csv(lookupFields),
            refresh_interval_sec: 300,
            max_cache_entries: 100000,
            on_miss: 'null',
            on_refresh_error: 'error',
          },
        },
        {
          type: 'window',
          config: {
            window_type: 'tumbling',
            window_size_seconds: Number(windowSize) || 60,
            allowed_lateness_seconds: 10,
            group_by: groupFields,
            aggregates: {
              order_count: { func: 'count' },
              total_amount: { func: 'sum', field: sumField },
            },
          },
        },
      ],
      sink: {
        type: 'clickhouse',
        config: {
          database: clickhouseDB,
          table: clickhouseTable,
          pk_columns: ['window_start', ...groupFields],
          version_column: '_version',
          auto_create: true,
          schema_drift: 'add_columns',
        },
      },
      batch_size: 1000,
      checkpoint_interval_sec: 30,
      tags: ['wide-table', 'kafka', 'clickhouse'],
    };
  }, [pipelineName, brokers, topic, groupID, lookupDSN, lookupQuery, joinKey, dimKey, lookupFields, groupBy, sumField, windowSize, clickhouseDB, clickhouseTable]);

  const runPreview = async () => {
    setLoading(true);
    setError('');
    setSuccess('');
    try {
      const sample = JSON.parse(sampleText || '{}');
      const result = await api<WidePreview>('/api/v2/wide-table/preview', {
        method: 'POST',
        body: JSON.stringify({ spec, samples: [sample], run_preflight: false }),
      });
      setPreview(result);
      setPreviewSpecJSON(JSON.stringify(spec));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const deployPipeline = async () => {
    setError('');
    setSuccess('');
    if (!preview) {
      setError(t('wide.previewRequired'));
      return;
    }
    if (!preview.valid) {
      setError(t('wide.previewInvalid'));
      return;
    }
    if (previewSpecJSON !== JSON.stringify(spec)) {
      setError(t('wide.previewStale'));
      return;
    }
    setDeploying(true);
    try {
      const created = await api<{ name?: string; status?: string; error?: string; preflight_warnings?: string[] }>('/api/v2/pipelines', {
        method: 'POST',
        body: JSON.stringify({ spec }),
      });
      if (created.error) {
        throw new Error(created.error);
      }
      setSuccess(`${t('wide.deployed')}: ${created.name || pipelineName}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setDeploying(false);
    }
  };

  const fields = preview?.field_types ? Object.entries(preview.field_types).sort(([a], [b]) => a.localeCompare(b)) : [];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-4 gap-4">
        <Summary label={t('wide.source')} value={topic || 'n/a'} tone="text-blue-600" />
        <Summary label={t('wide.lookup')} value={joinKey && dimKey ? `${joinKey}=${dimKey}` : 'n/a'} tone="text-violet-600" />
        <Summary label={t('wide.window')} value={`${windowSize}s`} tone="text-cyan-600" />
        <Summary label={t('wide.sink')} value={`${clickhouseDB}.${clickhouseTable}`} tone="text-emerald-600" />
      </div>

      {error && <div className="rounded-lg border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">{error}</div>}
      {success && <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-700">{success}</div>}

      <div className="grid grid-cols-[420px_minmax(0,1fr)] gap-6">
        <div className="space-y-6">
          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('wide.factSource')}</h2></div>
            <div className="card-body space-y-3">
              <TextField label={t('wide.pipelineName')} value={pipelineName} onChange={setPipelineName} />
              <TextField label={t('wide.brokers')} value={brokers} onChange={setBrokers} />
              <TextField label={t('wide.topic')} value={topic} onChange={setTopic} />
              <TextField label={t('wide.groupId')} value={groupID} onChange={setGroupID} />
            </div>
          </div>

          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('wide.dimensionJoin')}</h2></div>
            <div className="card-body space-y-3">
              <TextField label={t('wide.lookupDsn')} value={lookupDSN} onChange={setLookupDSN} />
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('wide.lookupQuery')}</span>
                <textarea className="input h-20 w-full resize-none font-mono text-xs leading-relaxed" value={lookupQuery} onChange={(e) => setLookupQuery(e.target.value)} />
              </label>
              <div className="grid grid-cols-2 gap-3">
                <TextField label={t('wide.joinKey')} value={joinKey} onChange={setJoinKey} />
                <TextField label={t('wide.dimKey')} value={dimKey} onChange={setDimKey} />
              </div>
              <TextField label={t('wide.lookupFields')} value={lookupFields} onChange={setLookupFields} />
            </div>
          </div>

          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('wide.aggregateSink')}</h2></div>
            <div className="card-body space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <TextField label={t('wide.groupBy')} value={groupBy} onChange={setGroupBy} />
                <TextField label={t('wide.sumField')} value={sumField} onChange={setSumField} />
              </div>
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{t('wide.windowSize')}</span>
                <input className="input w-full" type="number" min={1} value={windowSize} onChange={(e) => setWindowSize(Number(e.target.value))} />
              </label>
              <div className="grid grid-cols-2 gap-3">
                <TextField label={t('wide.database')} value={clickhouseDB} onChange={setClickhouseDB} />
                <TextField label={t('wide.table')} value={clickhouseTable} onChange={setClickhouseTable} />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <button className="btn btn-secondary w-full" disabled={loading || deploying} onClick={runPreview}>{loading ? t('wide.previewing') : t('wide.preview')}</button>
                <button className="btn btn-primary w-full" disabled={loading || deploying || !preview?.valid} onClick={deployPipeline}>{deploying ? t('wide.deploying') : t('wide.deploy')}</button>
              </div>
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="card">
            <div className="card-header flex items-center justify-between">
              <h2 className="text-sm font-semibold">{t('wide.sample')}</h2>
              <button className="btn btn-secondary btn-sm" onClick={() => setSampleText(JSON.stringify(sampleDebezium, null, 2))}>{t('ui.loadExample')}</button>
            </div>
            <div className="card-body">
              <textarea className="input h-44 w-full resize-none font-mono text-xs leading-relaxed" value={sampleText} onChange={(e) => setSampleText(e.target.value)} />
            </div>
          </div>

          <div className="card">
            <div className="card-header"><h2 className="text-sm font-semibold">{t('wide.previewResult')}</h2></div>
            <div className="card-body space-y-4">
              {!preview ? (
                <div className="rounded-lg border border-dashed border-slate-200 py-10 text-center text-sm text-slate-400">{t('wide.noPreview')}</div>
              ) : (
                <>
                  <div className="flex flex-wrap gap-2">
                    <span className={`badge ${preview.valid ? 'badge-emerald' : 'badge-rose'}`}>{preview.valid ? 'valid' : 'invalid'}</span>
                    <span className="badge badge-blue">{String(preview.envelope?.type || 'normalize_envelope')}</span>
                    <span className="badge badge-violet">{`${preview.lookups?.length || 0} lookup`}</span>
                    <span className="badge badge-cyan">{String(preview.window?.type || 'no window')}</span>
                  </div>
                  {(preview.errors || []).map((msg) => <div key={msg} className="rounded-lg bg-rose-50 p-3 text-sm text-rose-700">{msg}</div>)}
                  {(preview.warnings || []).map((msg) => <div key={msg} className="rounded-lg bg-amber-50 p-3 text-sm text-amber-700">{msg}</div>)}
                  <div>
                    <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">{t('wide.fieldTypes')}</h3>
                    <div className="grid grid-cols-2 gap-2">
                      {fields.map(([name, typ]) => (
                        <div key={name} className="flex items-center justify-between rounded-lg bg-slate-50 px-3 py-2 text-xs">
                          <span className="font-medium text-slate-700">{name}</span>
                          <span className="text-slate-500">{typ}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">{t('wide.ddl')}</h3>
                    <pre className="code-block overflow-x-auto whitespace-pre-wrap">{preview.proposed_ddl || 'n/a'}</pre>
                  </div>
                  <div>
                    <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">{t('wide.normalizedSample')}</h3>
                    <pre className="code-block max-h-56 overflow-auto whitespace-pre-wrap">{JSON.stringify(preview.sample || [], null, 2)}</pre>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function Summary({ label, value, tone }: { label: string; value: string; tone: string }) {
  return (
    <div className="card card-body">
      <span className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</span>
      <div className={`mt-2 truncate text-lg font-bold ${tone}`}>{value}</div>
    </div>
  );
}

function TextField({ label, value, onChange }: { label: string; value: string; onChange: (v: string) => void }) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-slate-500">{label}</span>
      <input className="input w-full" value={value} onChange={(e) => onChange(e.target.value)} />
    </label>
  );
}

function csv(v: string): string[] {
  return v.split(',').map((s) => s.trim()).filter(Boolean);
}

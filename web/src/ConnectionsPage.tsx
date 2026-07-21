import React, { useEffect, useMemo, useState } from 'react';
import type { TFunc, Lang } from './types';
import {
  ConfigForm,
  buildDefaultConfig,
  filterFieldsByScope,
  missingRequiredFields,
  type PluginSchemaField,
} from './configFields';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { ToneBadge } from '@/components/shared/status-badge';
import { cn } from '@/lib/utils';

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
  readiness?: {
    status: string;
    summary?: string;
    gates?: { code: string; label: string; status: string; evidence?: string; remediation?: string }[];
  };
  required?: string[];
  capabilities?: string[];
  fields?: PluginSchemaField[];
  secret_fields?: string[];
  registered?: boolean;
};

const selectClass =
  'flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring';

function getToken() {
  return window.localStorage.getItem('etl_api_token') || '';
}

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
  const [config, setConfig] = useState<Record<string, unknown>>(starterConfig.source);
  const [configText, setConfigText] = useState(JSON.stringify(starterConfig.source, null, 2));
  const [jsonOpen, setJsonOpen] = useState(false);
  const [jsonError, setJsonError] = useState('');
  const [openOnTest, setOpenOnTest] = useState(true);
  const [drawerOpen, setDrawerOpen] = useState(false);

  const refresh = () => {
    setLoading(true);
    Promise.all([
      api<{ connections?: ConnectionEntry[] }>('/api/v2/connections'),
      api<{ descriptors?: ConnectorDescriptor[] }>('/api/v2/connectors/descriptors').catch(() => ({
        descriptors: [],
      })),
    ])
      .then(([c, d]) => {
        setConnections(c.connections || []);
        setDescriptors(d.descriptors || []);
        setError('');
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    refresh();
  }, []);

  const typeOptions = useMemo(() => {
    const list = descriptors
      .filter((d) => d.kind === kind && d.registered !== false)
      .map((d) => d.type);
    return Array.from(new Set(list)).sort();
  }, [descriptors, kind]);

  const selectedDescriptor = descriptors.find((d) => d.kind === kind && d.type === type);
  const selectedFields = filterFieldsByScope(
    selectedDescriptor?.fields || [],
    kind === 'transform' ? 'all' : 'connection',
  );
  const missingFields = missingRequiredFields(selectedFields, config);

  const nextConfigFor = (nextKind: ConnectorKind, nextType: string) => {
    const descriptor = descriptors.find((d) => d.kind === nextKind && d.type === nextType);
    if (descriptor?.fields?.length) {
      const scoped = filterFieldsByScope(
        descriptor.fields,
        nextKind === 'transform' ? 'all' : 'connection',
      );
      return buildDefaultConfig(scoped);
    }
    return starterConfig[nextKind] || {};
  };

  const applyConfig = (next: Record<string, unknown>) => {
    setConfig(next);
    setConfigText(JSON.stringify(next, null, 2));
    setJsonError('');
  };

  const applyType = (nextType: string, nextKind = kind) => {
    setType(nextType);
    setName(`${nextType}-connection`);
    applyConfig(nextConfigFor(nextKind, nextType));
  };

  useEffect(() => {
    if (!descriptors.length) return;
    if (selectedDescriptor) return;
    const first = descriptors.find((d) => d.kind === kind && d.registered !== false)?.type;
    if (first) applyType(first);
  }, [descriptors, kind, selectedDescriptor]);

  const onKindChange = (next: ConnectorKind) => {
    setKind(next);
    const first = descriptors.find((d) => d.kind === next && d.registered !== false)?.type;
    const fallback = next === 'source' ? 'kafka' : next === 'sink' ? 'clickhouse' : 'identity';
    const nextType = first || fallback;
    applyType(nextType, next);
  };

  const updateConfigFromJson = (text: string) => {
    setConfigText(text);
    try {
      const parsed = JSON.parse(text || '{}');
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error(t('conn.jsonObjectRequired'));
      }
      setConfig(parsed as Record<string, unknown>);
      setJsonError('');
    } catch (e) {
      setJsonError(e instanceof Error ? e.message : String(e));
    }
  };

  const save = async () => {
    setSaving(true);
    setError('');
    try {
      const parsed = JSON.parse(configText || '{}');
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error(t('conn.jsonObjectRequired'));
      }
      const missing = missingRequiredFields(selectedFields, parsed as Record<string, unknown>);
      if (missing.length) {
        throw new Error(`${t('conn.missingRequired')}: ${missing.join(', ')}`);
      }
      await api('/api/v2/connections', {
        method: 'POST',
        body: JSON.stringify({ name, kind, type, config: parsed }),
      });
      setDrawerOpen(false);
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

  const loadConnection = (conn: ConnectionEntry) => {
    setKind(conn.kind);
    setType(conn.type);
    setName(conn.name);
    applyConfig(conn.config || {});
    setDrawerOpen(true);
  };

  const healthTone = (status?: string): 'emerald' | 'rose' | 'slate' => {
    if (status === 'ok') return 'emerald';
    if (status === 'error') return 'rose';
    return 'slate';
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-primary">Resources</div>
          <h2 className="mt-1 text-2xl font-semibold tracking-tight">{t('nav.connections')}</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {connections.length} instances · {connections.filter((c) => c.last_status === 'ok').length} healthy
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            size="sm"
            onClick={() => {
              setDrawerOpen(true);
              setName('');
              applyConfig(nextConfigFor(kind, type));
            }}
            data-testid="connection-new"
          >
            {t('conn.new')}
          </Button>
          <Button variant="outline" size="sm" onClick={() => { window.location.hash = '#/connectors'; }}>
            {t('nav.connectors')}
          </Button>
        </div>
      </div>

      {error && <ErrorBox message={error} />}

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(320px,420px)]">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
            <CardTitle className="text-sm">{t('nav.connections')}</CardTitle>
            <div className="flex items-center gap-3">
              <label className="flex items-center gap-2 text-xs text-muted-foreground">
                <input
                  type="checkbox"
                  checked={openOnTest}
                  onChange={(e) => setOpenOnTest(e.target.checked)}
                  className="h-4 w-4 rounded border-input"
                />
                {t('conn.openOnTest')}
              </label>
              <Button variant="secondary" size="sm" onClick={refresh}>
                {t('common.refresh')}
              </Button>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            {loading && connections.length === 0 ? (
              <div className="p-8 text-center text-sm text-muted-foreground">{t('common.loading')}</div>
            ) : connections.length === 0 ? (
              <div className="p-6">
                <EmptyState text={t('conn.empty')} hint={t('conn.emptyHint')} />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>{t('common.name')}</TableHead>
                    <TableHead>{t('plugin.kind')}</TableHead>
                    <TableHead>{t('conn.type')}</TableHead>
                    <TableHead>{t('common.status')}</TableHead>
                    <TableHead>{t('conn.lastTested')}</TableHead>
                    <TableHead>{t('common.actions')}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {connections.map((conn) => (
                    <TableRow key={conn.name}>
                      <TableCell className="font-medium">{conn.name}</TableCell>
                      <TableCell>
                        <ToneBadge tone="slate">{conn.kind}</ToneBadge>
                      </TableCell>
                      <TableCell>{conn.type}</TableCell>
                      <TableCell>
                        <ToneBadge tone={healthTone(conn.last_status)}>
                          {conn.last_status || 'unknown'}
                        </ToneBadge>
                        {conn.last_error && (
                          <div className="mt-1 max-w-xs truncate text-xs text-rose-500">
                            {conn.last_error}
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {fmtTime(conn.last_tested_at)}
                      </TableCell>
                      <TableCell>
                        <div className="flex gap-2">
                          <Button variant="secondary" size="sm" onClick={() => loadConnection(conn)}>
                            {t('conn.load')}
                          </Button>
                          <Button
                            variant="secondary"
                            size="sm"
                            disabled={testing === conn.name}
                            onClick={() => testConnection(conn)}
                          >
                            {testing === conn.name ? t('conn.testing') : t('conn.test')}
                          </Button>
                          <Button
                            variant="destructive"
                            size="sm"
                            onClick={() => deleteConnection(conn)}
                          >
                            {t('pipe.delete')}
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        {drawerOpen && (
        <Card data-testid="connection-editor-drawer">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
            <CardTitle className="text-sm">{t('conn.new')}</CardTitle>
            <Button variant="ghost" size="sm" onClick={() => setDrawerOpen(false)}>{t('common.cancel')}</Button>
          </CardHeader>
          <CardContent className="space-y-4">
            <label className="block">
              <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-muted-foreground">
                {t('common.name')}
              </span>
              <Input value={name} onChange={(e) => setName(e.target.value)} />
            </label>
            <div className="grid grid-cols-2 gap-3">
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  {t('plugin.kind')}
                </span>
                <select
                  className={selectClass}
                  value={kind}
                  onChange={(e) => onKindChange(e.target.value as ConnectorKind)}
                >
                  <option value="source">source</option>
                  <option value="sink">sink</option>
                  <option value="transform">transform</option>
                </select>
              </label>
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  {t('conn.type')}
                </span>
                <select
                  className={selectClass}
                  value={type}
                  onChange={(e) => applyType(e.target.value)}
                >
                  {(typeOptions.length > 0 ? typeOptions : [type]).map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            {selectedDescriptor && (
              <div className="space-y-3 rounded-lg border border-border bg-muted/40 p-3">
                <div className="flex flex-wrap gap-1.5">
                  <ToneBadge
                    tone={
                      selectedDescriptor.maturity === 'production'
                        ? 'emerald'
                        : selectedDescriptor.maturity === 'beta'
                          ? 'blue'
                          : 'amber'
                    }
                  >
                    {selectedDescriptor.maturity}
                  </ToneBadge>
                  {selectedDescriptor.readiness?.status && (
                    <ToneBadge tone="blue">{selectedDescriptor.readiness.status}</ToneBadge>
                  )}
                  {(selectedDescriptor.capabilities || []).slice(0, 6).map((cap) => (
                    <ToneBadge key={cap} tone="slate">
                      {cap}
                    </ToneBadge>
                  ))}
                </div>
                {selectedDescriptor.readiness && (
                  <div className="rounded border border-border bg-card p-2 text-xs text-muted-foreground">
                    <div className="mb-1 font-medium text-foreground">Readiness</div>
                    {selectedDescriptor.readiness.summary && (
                      <div className="mb-1">{selectedDescriptor.readiness.summary}</div>
                    )}
                    <div className="flex flex-wrap gap-1">
                      {(selectedDescriptor.readiness.gates || []).slice(0, 5).map((gate) => (
                        <ToneBadge
                          key={gate.code}
                          tone={
                            gate.status === 'pass'
                              ? 'emerald'
                              : gate.status === 'partial'
                                ? 'blue'
                                : gate.status === 'missing'
                                  ? 'rose'
                                  : 'slate'
                          }
                        >
                          {gate.label}: {gate.status}
                        </ToneBadge>
                      ))}
                    </div>
                  </div>
                )}
                <div className="grid gap-2 text-xs text-muted-foreground sm:grid-cols-2">
                  <div>
                    <div className="font-medium text-foreground">{t('conn.required')}</div>
                    <div>{(selectedDescriptor.required || []).join(', ') || 'n/a'}</div>
                  </div>
                  <div>
                    <div className="font-medium text-foreground">{t('conn.secrets')}</div>
                    <div>{(selectedDescriptor.secret_fields || []).join(', ') || 'n/a'}</div>
                  </div>
                </div>
              </div>
            )}
            <div data-testid="connection-catalog-config-form">
              <div className="mb-2 flex items-center justify-between">
                <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  {t('conn.configTitle')}
                </span>
                <button
                  type="button"
                  className="text-xs font-medium text-primary hover:underline"
                  onClick={() => applyConfig(nextConfigFor(kind, type))}
                >
                  {t('conn.loadDefaults')}
                </button>
              </div>
              <ConfigForm fields={selectedFields} config={config} onChange={applyConfig} t={t} />
              <div className="mt-2 text-[11px] text-muted-foreground">{t('conn.scopeNote')}</div>
              {missingFields.length > 0 && (
                <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300">
                  {t('conn.missingRequired')}: {missingFields.join(', ')}
                </div>
              )}
            </div>
            <div className="rounded-lg border border-border">
              <button
                type="button"
                className="flex w-full items-center justify-between px-3 py-2 text-left text-xs font-medium text-muted-foreground"
                onClick={() => setJsonOpen((v) => !v)}
              >
                <span>{t('conn.advancedJson')}</span>
                <span>{jsonOpen ? '-' : '+'}</span>
              </button>
              {jsonOpen && (
                <div className="border-t border-border p-3">
                  <Textarea
                    className={cn('h-52 font-mono text-xs leading-relaxed')}
                    value={configText}
                    onChange={(e) => updateConfigFromJson(e.target.value)}
                  />
                  {jsonError && <div className="mt-2 text-xs text-rose-600">{jsonError}</div>}
                </div>
              )}
            </div>
            <Button
              className="w-full"
              disabled={saving || !!jsonError || missingFields.length > 0}
              onClick={save}
            >
              {saving ? t('ui.starting') : t('conn.save')}
            </Button>
          </CardContent>
        </Card>
        )}
      </div>
    </div>
  );
}

function fmtTime(v?: string) {
  if (!v || v.startsWith('0001-') || v.startsWith('1970-')) return 'n/a';
  try {
    return new Date(v).toLocaleString();
  } catch {
    return 'n/a';
  }
}

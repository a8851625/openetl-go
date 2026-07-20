import { useCallback, useEffect, useState } from 'react';
import YAML from 'yaml';
import { Modal } from '@/components/shared/modal';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { ToneBadge } from '@/components/shared/status-badge';
import { cn } from '@/lib/utils';
import { ErrorBox } from '@/components/shared/empty-state';
import {
  ConfigForm,
  buildDefaultConfig,
  filterFieldsByScope,
  missingRequiredFields,
  type PluginSchemaField,
} from '@/configFields';
import { api, getToken, normalizeConnectionEntry } from '@/lib/api';
import { parseJSONObject, parseJSONText, prettyJSON } from '@/lib/format';
import type {
  ConnectionContext,
  ConnectionEntry,
  ConnectionRecommendation,
  TFunc,
} from '@/lib/types';

const wizardSelectClass =
  'flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring';

// First Task Wizard
// ════════════════════════════════════════════════
type WizardTemplate = {
  id: string;
  title: string;
  descKey?: string;
  recommended?: boolean;
  sourceTypes: string[];
  sinkTypes: string[];
  transforms: { type: string; config: Record<string, unknown> }[];
  sample: Record<string, unknown>;
  tableMapping?: { template?: string; rules?: Record<string, string> };
  hideSinkTable?: boolean;
};

type ValidateResult = {
  valid?: boolean;
  warnings?: string[];
  preflight?: {
    passed?: boolean;
    summary?: string;
    issues?: { level: string; check: string; message: string; remediation?: string }[];
    field_issues?: { level: string; field: string; check: string; message: string; remediation?: string }[];
    ddl_preview?: { dialect: string; table: string; statements?: string[]; warnings?: string[] };
    guidance?: { level: string; category: string; code: string; message: string; action?: string }[];
    readiness?: {
      kind: string;
      type: string;
      maturity: string;
      status: string;
      summary?: string;
      gates?: { code: string; label: string; status: string; evidence?: string; remediation?: string }[];
    }[];
    recommendations?: { path: string; value: unknown; reason: string; safety?: string }[];
  };
  errors?: string[];
};

const WIZARD_TEMPLATES: WizardTemplate[] = [
  {
    id: 'multi-table-sync',
    title: 'multi-table-sync',
    descKey: 'wizard.multiTableSyncDesc',
    recommended: true,
    sourceTypes: ['mysql_snapshot_cdc', 'mysql_cdc'],
    sinkTypes: ['mysql', 'postgresql', 'clickhouse', 'doris'],
    transforms: [{ type: 'identity', config: {} }],
    sample: { operation: 'INSERT', data: { id: 1, name: 'Alice', updated_at: '2026-06-27T10:00:00Z' }, metadata: { source: 'wizard', table: 'customers' } },
    tableMapping: { template: 'ods_{source_table}' },
    hideSinkTable: true,
  },
  {
    id: 'cdc-wide-table',
    title: 'cdc-wide-table',
    descKey: 'wizard.cdcWideTableDesc',
    recommended: true,
    sourceTypes: ['mysql_cdc'],
    sinkTypes: ['clickhouse', 'mysql', 'postgresql'],
    transforms: [
      { type: 'cdc_policy', config: { include_tables: ['orders'], skip_tombstone: true } },
      { type: 'lookup', config: { join_key: 'customer_id', dim_key: 'id', fields: ['name', 'tier', 'region'], on_miss: 'null', on_refresh_error: 'pass', refresh_interval_sec: 60 } },
      { type: 'rename', config: { mappings: { name: 'user_name', tier: 'user_tier', region: 'user_region' } } },
      { type: 'add_field', config: { field: '_version', value: '1' } },
    ],
    sample: { operation: 'INSERT', data: { id: 1001, customer_id: 42, amount: 19.5 }, metadata: { source: 'wizard', table: 'orders' } },
    tableMapping: { rules: { orders: 'order_detail_wide' } },
  },
  { id: 'database-sync', title: 'database-sync', descKey: 'wizard.databaseSync', sourceTypes: ['mysql_batch', 'mysql_cdc', 'mysql_snapshot_cdc'], sinkTypes: ['mysql', 'postgresql', 'clickhouse', 'doris'], transforms: [{ type: 'identity', config: {} }], sample: { operation: 'INSERT', data: { id: 1, name: 'Alice', updated_at: '2026-06-27T10:00:00Z' }, metadata: { source: 'wizard', table: 'customers' } } },
  { id: 'kafka-detail', title: 'kafka-detail', descKey: 'wizard.kafkaDetail', sourceTypes: ['kafka'], sinkTypes: ['clickhouse', 'mysql', 'postgresql'], transforms: [{ type: 'project', config: { fields: ['id', 'user_id', 'amount', 'dt'] } }, { type: 'deduplicate', config: { key_fields: ['id'] } }], sample: { operation: 'INSERT', data: { id: 1001, user_id: 42, amount: 19.5, dt: '20260627' }, metadata: { source: 'kafka', table: 'orders' } } },
  { id: 'debezium-cdc', title: 'debezium-cdc', descKey: 'wizard.debeziumCdc', sourceTypes: ['kafka'], sinkTypes: ['mysql', 'postgresql', 'clickhouse', 'doris'], transforms: [{ type: 'debezium_cdc', config: { skip_snapshot: true } }, { type: 'cdc_policy', config: { skip_delete: false, dangerous_ddl: 'reject' } }], sample: { operation: 'INSERT', data: { payload: { op: 'c', source: { db: 'app', table: 'orders' }, after: { id: 1, amount: 29.9 } } }, metadata: { source: 'debezium', table: 'orders' } } },
  { id: 'kafka-parser', title: 'kafka-parser', descKey: 'wizard.kafkaParser', sourceTypes: ['kafka'], sinkTypes: ['kafka', 'clickhouse', 'file_sink'], transforms: [{ type: 'flat_map', config: { script: 'return { { data = { id = record.data.id, value = record.data.value } } }' } }, { type: 'project', config: { fields: ['id', 'value'] } }], sample: { operation: 'INSERT', data: { id: 'raw-1', value: 7, payload: '010203' }, metadata: { source: 'kafka', table: 'raw' } } },
  { id: 'file-http-landing', title: 'file-http-landing', descKey: 'wizard.fileHttp', sourceTypes: ['file', 'http'], sinkTypes: ['file_sink', 's3', 'maxcompute'], transforms: [{ type: 'identity', config: {} }], sample: { operation: 'INSERT', data: { id: 1, name: 'UI Wizard', dt: '20260627' }, metadata: { source: 'wizard', table: 'landing' } } },
];

function defaultSourceConfig(type: string): Record<string, unknown> {
  switch (type) {
    case 'mysql_batch':
    case 'mysql_cdc':
    case 'mysql_snapshot_cdc':
      return { host: 'host.docker.internal', port: 13306, user: 'sync_user', password: 'sync_password_123', database: 'dzh3136_go', table: 'customers', tables: ['customers'], pk_column: 'id', server_id: 12001 };
    case 'kafka':
      return { brokers: ['host.docker.internal:19092'], topic: 'orders', group_id: 'openetl-ui-wizard', format: 'json' };
    case 'http':
      return { url: 'http://host.docker.internal:18080/customers', method: 'GET', format: 'json' };
    case 'file':
    default:
      return { path: '/app/testdata/files/customers.jsonl', format: 'json' };
  }
}

function defaultSinkConfig(type: string): Record<string, unknown> {
  switch (type) {
    case 'mysql':
    case 'postgresql':
      return { host: 'host.docker.internal', port: type === 'mysql' ? 13306 : 15432, user: 'sync_user', password: 'sync_password_123', database: 'dzh3136_go', table: 'wizard_output', batch_mode: 'upsert', pk_columns: ['id'], auto_create: true };
    case 'clickhouse':
      return { host: 'host.docker.internal', port: 9000, database: 'default', table: 'wizard_output', username: 'default', password: 'dzh123456', batch_mode: 'upsert', pk_columns: ['id'], auto_create: true };
    case 'doris':
      return { host: 'host.docker.internal', port: 9030, http_port: 8030, user: 'root', database: 'dzh3136_go', table: 'wizard_output', batch_mode: 'upsert', pk_columns: ['id'], auto_create: true };
    case 'kafka':
      return { brokers: ['host.docker.internal:19092'], topic: 'ods.orders', format: 'json' };
    case 's3':
      return { endpoint: 'http://host.docker.internal:9001', bucket: 'openetl', prefix: 'wizard/', access_key: 'minioadmin', secret_key: 'minioadmin', format: 'jsonl' };
    case 'maxcompute':
      return { endpoint: 'http://127.0.0.1:1/api', project: 'demo_project', table: 'wizard_output', access_key_id: 'replace-me', access_key_secret: 'replace-me', columns: { id: 'BIGINT', name: 'STRING', dt: 'STRING' }, partition_fields: ['dt'] };
    case 'file_sink':
    default:
      return { output_dir: '/app/data/output/ui-wizard', format: 'jsonl', prefix: 'wizard_' };
  }
}


function sourceSupportsSampleSchemaHint(type: string): boolean {
  return ['file', 'http', 'kafka'].includes(type);
}


function parseTransformList(text: string): { type: string; config: Record<string, unknown> }[] {
  const parsed = parseJSONText(text, []);
  if (!Array.isArray(parsed)) return [];
  return parsed
    .filter((item) => item && typeof item === 'object' && typeof (item as any).type === 'string')
    .map((item) => ({ type: (item as any).type, config: parseJSONObject(prettyJSON((item as any).config || {})) }));
}


export function FirstTaskWizard({ t, plugins, schema, onClose, onCreated }: { t: TFunc; plugins: any; schema: any; onClose: () => void; onCreated: (name: string) => void }) {
  const [templateId, setTemplateId] = useState('multi-table-sync');
  const template = WIZARD_TEMPLATES.find((tpl) => tpl.id === templateId) || WIZARD_TEMPLATES[0];
  const [name, setName] = useState('ui-wizard-multi-table');
  const [sourceType, setSourceType] = useState(template.sourceTypes[0]);
  const [sinkType, setSinkType] = useState(template.sinkTypes[0]);
  const [sourceConfigText, setSourceConfigText] = useState(prettyJSON(defaultSourceConfig(sourceType)));
  const [sinkConfigText, setSinkConfigText] = useState(prettyJSON(defaultSinkConfig(sinkType)));
  const [transformsText, setTransformsText] = useState(prettyJSON(template.transforms));
  const [sampleText, setSampleText] = useState(prettyJSON(template.sample));
  const [yamlText, setYamlText] = useState('');
  const [sourceJsonOpen, setSourceJsonOpen] = useState(false);
  const [sinkJsonOpen, setSinkJsonOpen] = useState(false);
  const [transformJsonOpen, setTransformJsonOpen] = useState(false);
  const [tableMappingOpen, setTableMappingOpen] = useState(false);
  const [tableMappingText, setTableMappingText] = useState(template.tableMapping ? prettyJSON(template.tableMapping) : '');
  const [result, setResult] = useState<ValidateResult | null>(null);
  const [dryRunResult, setDryRunResult] = useState<unknown>(null);
  const [stageDryRunResult, setStageDryRunResult] = useState<{ index: number; result?: unknown; error?: string } | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState('');
  const [connections, setConnections] = useState<ConnectionEntry[]>([]);
  const [sourceConnection, setSourceConnection] = useState('');
  const [sinkConnection, setSinkConnection] = useState('');
  const [sourceContext, setSourceContext] = useState<ConnectionContext | null>(null);
  const [sinkContext, setSinkContext] = useState<ConnectionContext | null>(null);
  const [batchSize, setBatchSize] = useState(100);
  const [checkpointIntervalSec, setCheckpointIntervalSec] = useState(1);
  const [dlqEnabled, setDlqEnabled] = useState(true);

  const allSourceFields = (schema?.data?.sources?.[sourceType] || []) as PluginSchemaField[];
  const allSinkFields = (schema?.data?.sinks?.[sinkType] || []) as PluginSchemaField[];
  const sourceFields = filterFieldsByScope(allSourceFields, sourceConnection ? 'behavior' : 'all');
  const sinkFields = filterFieldsByScope(allSinkFields, sinkConnection ? 'behavior' : 'all');
  const sourceConfig = parseJSONObject(sourceConfigText);
  const sinkConfig = parseJSONObject(sinkConfigText);
  const transformConfigs = parseTransformList(transformsText);
  const sourceMissing = sourceConnection
    ? missingRequiredFields(sourceFields, sourceConfig)
    : missingRequiredFields(allSourceFields, sourceConfig);
  const sinkMissing = sinkConnection
    ? missingRequiredFields(sinkFields, sinkConfig)
    : missingRequiredFields(allSinkFields, sinkConfig);
  const metadata = plugins?.data?.metadata || {};
  const transformTypes = Object.keys(schema?.data?.transforms || {}).sort();
  const sourceMaturity = metadata.sources?.[sourceType]?.maturity || 'unknown';
  const sinkMaturity = metadata.sinks?.[sinkType]?.maturity || 'unknown';
  const sourceCapabilities = metadata.sources?.[sourceType]?.capabilities || [];
  const sinkCapabilities = metadata.sinks?.[sinkType]?.capabilities || [];
  const sourceConnections = connections.filter((conn) => conn.kind === 'source');
  const sinkConnections = connections.filter((conn) => conn.kind === 'sink');
  const recommendationValue = (field: string, fallback: number) => {
    const rec = sourceContext?.recommendations?.find((item) => item.field === field);
    return typeof rec?.value === 'number' ? rec.value : fallback;
  };
  const recommendationNumber = (recommendations: ConnectionRecommendation[] | undefined, field: string, fallback: number) => {
    const rec = recommendations?.find((item) => item.field === field);
    return typeof rec?.value === 'number' ? rec.value : fallback;
  };
  const positiveIntValue = (value: string, fallback: number) => {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  };
  const refreshConnections = useCallback(() => {
    return api<{ connections?: ConnectionEntry[] }>('/api/v2/connections')
      .then((data) => setConnections((data.connections || []).map(normalizeConnectionEntry).filter((conn): conn is ConnectionEntry => conn !== null)))
      .catch(() => setConnections([]));
  }, []);

  const seedSourceConfig = useCallback((type: string) => {
    const fields = filterFieldsByScope((schema?.data?.sources?.[type] || []) as PluginSchemaField[], 'all');
    return { ...buildDefaultConfig(fields), ...defaultSourceConfig(type) };
  }, [schema?.data]);

  const seedSinkConfig = useCallback((type: string) => {
    const fields = filterFieldsByScope((schema?.data?.sinks?.[type] || []) as PluginSchemaField[], 'all');
    return { ...buildDefaultConfig(fields), ...defaultSinkConfig(type) };
  }, [schema?.data]);

  const seedBehaviorConfig = useCallback((kind: 'source' | 'sink', type: string) => {
    const group = kind === 'source' ? schema?.data?.sources : schema?.data?.sinks;
    const fields = filterFieldsByScope((group?.[type] || []) as PluginSchemaField[], 'behavior');
    return buildDefaultConfig(fields);
  }, [schema?.data]);

  const buildSpec = useCallback(() => {
    const sourceConfigForSpec = parseJSONText(sourceConfigText, {}) as Record<string, unknown>;
    if (sourceSupportsSampleSchemaHint(sourceType) && !sourceConfigForSpec.schema && !sourceConfigForSpec.sample) {
      sourceConfigForSpec.sample = parseJSONText(sampleText, template.sample);
    }
    const source: Record<string, unknown> = { type: sourceType, config: sourceConfigForSpec };
    const sinkConfigForSpec = parseJSONText(sinkConfigText, {}) as Record<string, unknown>;
    if (template.hideSinkTable) {
      delete sinkConfigForSpec.table;
    }
    const sink: Record<string, unknown> = { type: sinkType, config: sinkConfigForSpec };
    if (sourceConnection) source.connection = sourceConnection;
    if (sinkConnection) sink.connection = sinkConnection;
    const spec: Record<string, unknown> = {
      name: name.trim(),
      source,
      transforms: parseJSONText(transformsText, []),
      sink,
      batch_size: batchSize,
      checkpoint_interval_sec: checkpointIntervalSec,
      backpressure_buffer: 100,
      retry: { max_attempts: 3, initial_interval_ms: 100, max_interval_ms: 1000 },
      dlq: { enable: dlqEnabled },
      tags: ['ui-wizard', template.id],
    };
    const tm = parseJSONText(tableMappingText, null);
    if (tm && typeof tm === 'object' && !Array.isArray(tm) && Object.keys(tm).length > 0) {
      spec.table_mapping = tm;
    }
    return spec;
  }, [name, sourceType, sourceConfigText, sampleText, transformsText, sinkType, sinkConfigText, sourceConnection, sinkConnection, batchSize, checkpointIntervalSec, dlqEnabled, template.id, template.sample, template.hideSinkTable, tableMappingText]);

  useEffect(() => {
    refreshConnections();
    const timer = window.setInterval(refreshConnections, 3000);
    return () => window.clearInterval(timer);
  }, [refreshConnections]);

  useEffect(() => {
    const nextTemplate = WIZARD_TEMPLATES.find((tpl) => tpl.id === templateId) || WIZARD_TEMPLATES[0];
    const nextSource = nextTemplate.sourceTypes[0];
    const nextSink = nextTemplate.sinkTypes[0];
    setSourceType(nextSource);
    setSinkType(nextSink);
    setSourceConfigText(prettyJSON(seedSourceConfig(nextSource)));
    setSinkConfigText(prettyJSON(seedSinkConfig(nextSink)));
    setSourceConnection('');
    setSinkConnection('');
    setSourceContext(null);
    setSinkContext(null);
    setBatchSize(100);
    setCheckpointIntervalSec(1);
    setDlqEnabled(true);
    setTransformsText(prettyJSON(nextTemplate.transforms));
    setSampleText(prettyJSON(nextTemplate.sample));
    setTableMappingText(nextTemplate.tableMapping ? prettyJSON(nextTemplate.tableMapping) : '');
    setName(`ui-wizard-${nextTemplate.id}`);
    setSourceJsonOpen(false);
    setSinkJsonOpen(false);
    setTransformJsonOpen(false);
    setResult(null);
    setDryRunResult(null);
    setStageDryRunResult(null);
  }, [templateId]);

  useEffect(() => {
    if (!sourceContext) return;
    setBatchSize(recommendationValue('batch_size', batchSize));
    setCheckpointIntervalSec(recommendationValue('checkpoint_interval_sec', checkpointIntervalSec));
  }, [sourceContext]);

  useEffect(() => {
    setYamlText(YAML.stringify(buildSpec()));
  }, [buildSpec]);

  const loadConnectionContext = useCallback(async (name: string, target: 'source' | 'sink') => {
    if (!name) {
      if (target === 'source') setSourceContext(null);
      if (target === 'sink') setSinkContext(null);
      return;
    }
    const data = await api<ConnectionContext>(`/api/v2/connections/${encodeURIComponent(name)}/context`);
    if (target === 'source') {
      if (data.connection?.type) setSourceType(data.connection.type);
      setSourceConfigText(prettyJSON(seedBehaviorConfig('source', data.connection?.type || sourceType)));
      const firstSample = data.introspection?.sample?.[0];
      if (firstSample) setSampleText(prettyJSON(firstSample));
      setBatchSize(recommendationNumber(data.recommendations, 'batch_size', batchSize));
      setCheckpointIntervalSec(recommendationNumber(data.recommendations, 'checkpoint_interval_sec', checkpointIntervalSec));
      setSourceContext(data);
    } else {
      if (data.connection?.type) setSinkType(data.connection.type);
      setSinkConfigText(prettyJSON(seedBehaviorConfig('sink', data.connection?.type || sinkType)));
      setSinkContext(data);
    }
  }, [batchSize, checkpointIntervalSec, seedBehaviorConfig, sourceType, sinkType]);

  const selectSourceConnection = async (connName: string) => {
    setSourceConnection(connName);
    setResult(null);
    setError('');
    try {
      await loadConnectionContext(connName, 'source');
    } catch (e) {
      setSourceContext({ introspection: { ok: false, error: e instanceof Error ? e.message : String(e) } });
    }
  };

  const selectSinkConnection = async (connName: string) => {
    setSinkConnection(connName);
    setResult(null);
    setError('');
    try {
      await loadConnectionContext(connName, 'sink');
    } catch (e) {
      setSinkContext({ introspection: { ok: false, error: e instanceof Error ? e.message : String(e) } });
    }
  };

  const validate = async (throwOnInvalid = false) => {
    setBusy('validate'); setError(''); setResult(null);
    try {
      const spec = YAML.parse(yamlText);
      const res = await fetch('/api/v2/specs/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(getToken() ? { 'X-API-Token': getToken() } : {}) },
        body: JSON.stringify({ spec }),
      });
      const data = await res.json();
      setResult(data);
      if (!res.ok) throw new Error((data.errors || data.warnings || ['validation failed']).join('\n'));
      if (data.valid === false) {
        const message = (data.errors || data.warnings || ['preflight failed']).join('\n');
        setError(message);
        if (throwOnInvalid) throw new Error(message);
      }
      return data as ValidateResult;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      throw e;
    } finally {
      setBusy('');
    }
  };

  const dryRun = async () => {
    setBusy('dry-run'); setError(''); setDryRunResult(null); setStageDryRunResult(null);
    try {
      const data = await api('/api/v2/transforms/dry-run', {
        method: 'POST',
        body: JSON.stringify({ transforms: parseJSONText(transformsText, []), record: parseJSONText(sampleText, template.sample) }),
      });
      setDryRunResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy('');
    }
  };

  const createAndStart = async () => {
    setBusy('create'); setError('');
    try {
      const checked = await validate(true);
      if (checked.valid === false) throw new Error((checked.errors || checked.warnings || ['preflight failed']).join('\n'));
      const spec = YAML.parse(yamlText);
      const created = await api<{ id?: string; name: string }>('/api/v2/pipelines', { method: 'POST', body: JSON.stringify({ spec }) });
      await api(`/api/v2/pipelines/${encodeURIComponent(created.id || created.name || spec.name)}/start`, { method: 'POST' });
      onCreated(created.name || spec.name);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy('');
    }
  };

  const syncFromYaml = () => {
    try {
      const spec = YAML.parse(yamlText);
      setName(spec.name || name);
      if (spec.source?.type) {
        setSourceType(spec.source.type);
        setSourceConfigText(prettyJSON(spec.source.config || {}));
      }
      if (spec.sink?.type) {
        setSinkType(spec.sink.type);
        setSinkConfigText(prettyJSON(spec.sink.config || {}));
      }
      if (typeof spec.batch_size === 'number') setBatchSize(spec.batch_size);
      if (typeof spec.checkpoint_interval_sec === 'number') setCheckpointIntervalSec(spec.checkpoint_interval_sec);
      if (typeof spec.dlq?.enable === 'boolean') setDlqEnabled(spec.dlq.enable);
      setTransformsText(prettyJSON(spec.transforms || []));
      setError('');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const setValueAtPath = (target: Record<string, unknown>, path: string, value: unknown) => {
    const parts = path.split('.').filter(Boolean);
    let cursor: Record<string, unknown> = target;
    parts.forEach((part, index) => {
      if (index === parts.length - 1) {
        cursor[part] = value;
        return;
      }
      const next = cursor[part];
      if (!next || typeof next !== 'object' || Array.isArray(next)) cursor[part] = {};
      cursor = cursor[part] as Record<string, unknown>;
    });
  };

  const applyPreflightRecommendation = (rec: { path: string; value: unknown; reason: string; safety?: string }) => {
    try {
      const spec = (YAML.parse(yamlText) || buildSpec()) as Record<string, unknown>;
      setValueAtPath(spec, rec.path, rec.value);
      if (rec.path.startsWith('source.config.')) {
        const next = parseJSONObject(sourceConfigText);
        setValueAtPath(next, rec.path.replace('source.config.', ''), rec.value);
        setSourceConfigText(prettyJSON(next));
      }
      if (rec.path.startsWith('sink.config.')) {
        const next = parseJSONObject(sinkConfigText);
        setValueAtPath(next, rec.path.replace('sink.config.', ''), rec.value);
        setSinkConfigText(prettyJSON(next));
      }
      if (rec.path === 'transforms') {
        setTransformsText(prettyJSON(Array.isArray(rec.value) ? rec.value : []));
      }
      if (rec.path === 'batch_size' && typeof rec.value === 'number') {
        setBatchSize(rec.value);
      }
      if (rec.path === 'checkpoint_interval_sec' && typeof rec.value === 'number') {
        setCheckpointIntervalSec(rec.value);
      }
      if (rec.path === 'dlq.enable' && typeof rec.value === 'boolean') {
        setDlqEnabled(rec.value);
      }
      setYamlText(YAML.stringify(spec));
      setResult(null);
      setError('');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const configPathForConnectionRecommendation = (title: string, rec: ConnectionRecommendation): string | null => {
    if (rec.value === 'review' || rec.value === '') return null;
    const scope = title.toLowerCase() === 'sink' ? 'sink' : 'source';
    const scopedPrefix = `${scope}.config.`;
    if (rec.field.startsWith(scopedPrefix)) return rec.field.slice(scopedPrefix.length);
    if (scope === 'source' && !rec.field.includes('.') && !['batch_size', 'checkpoint_interval_sec'].includes(rec.field)) {
      return rec.field;
    }
    return null;
  };

  const canApplyConnectionRecommendation = (title: string, rec: ConnectionRecommendation): boolean => {
    if (configPathForConnectionRecommendation(title, rec)) return true;
    return title.toLowerCase() === 'source' && ['batch_size', 'checkpoint_interval_sec'].includes(rec.field) && typeof rec.value === 'number';
  };

  const applyConnectionRecommendation = (title: string, rec: ConnectionRecommendation) => {
    const configPath = configPathForConnectionRecommendation(title, rec);
    const scope = title.toLowerCase() === 'sink' ? 'sink' : 'source';
    if (!configPath) {
      if (scope === 'source' && rec.field === 'batch_size' && typeof rec.value === 'number') {
        setBatchSize(rec.value);
        setResult(null);
        setError('');
      }
      if (scope === 'source' && rec.field === 'checkpoint_interval_sec' && typeof rec.value === 'number') {
        setCheckpointIntervalSec(rec.value);
        setResult(null);
        setError('');
      }
      return;
    }
    const next = title.toLowerCase() === 'sink' ? parseJSONObject(sinkConfigText) : parseJSONObject(sourceConfigText);
    setValueAtPath(next, configPath, rec.value);
    try {
      const spec = (YAML.parse(yamlText) || buildSpec()) as Record<string, unknown>;
      const endpoint = (spec[scope] && typeof spec[scope] === 'object' ? spec[scope] : {}) as Record<string, unknown>;
      const endpointConfig = (endpoint.config && typeof endpoint.config === 'object' && !Array.isArray(endpoint.config) ? endpoint.config : {}) as Record<string, unknown>;
      setValueAtPath(endpointConfig, configPath, rec.value);
      endpoint.config = endpointConfig;
      spec[scope] = endpoint;
      setYamlText(YAML.stringify(spec));
    } catch {
      // The form state below remains authoritative if the YAML editor is temporarily invalid.
    }
    if (scope === 'sink') {
      setSinkConfigText(prettyJSON(next));
    } else {
      setSourceConfigText(prettyJSON(next));
    }
    setResult(null);
    setError('');
  };

  const renderSchemaSummary = (title: string, fields: PluginSchemaField[], maturity: string, capabilities: string[], missing: string[]) => (
    <div className="rounded-lg border border-border bg-muted/40 p-3">
      <div className="mb-2 flex items-center justify-between gap-2">
        <div className="text-xs font-semibold text-slate-600">{title}</div>
        <ToneBadge tone="slate">{maturity}</ToneBadge>
      </div>
      <div className="mb-2 flex flex-wrap gap-1">
        {capabilities.slice(0, 5).map((cap: string) => <ToneBadge key={cap} tone="blue" className="text-[10px]">{cap}</ToneBadge>)}
        {missing.map((field) => <ToneBadge key={field} tone="rose" className="text-[10px]">missing {field}</ToneBadge>)}
      </div>
      <div className="grid gap-1">
        {fields.slice(0, 8).map((field) => (
          <div key={field.name} className="flex items-center gap-2 text-xs text-slate-500">
            <span className="font-mono text-slate-700">{field.name}</span>
            <span>{field.type}</span>
            {field.required && <span className="text-rose-500">required</span>}
            {field.secret && <span className="text-amber-600">secret</span>}
          </div>
        ))}
        {!fields.length && <div className="text-xs text-slate-400">No schema fields returned.</div>}
      </div>
    </div>
  );

  const renderConfigEditor = (
    title: string,
    fields: PluginSchemaField[],
    config: Record<string, unknown>,
    configText: string,
    setConfigText: (text: string) => void,
    jsonOpen: boolean,
    setJsonOpen: (open: boolean) => void,
    testId: string,
  ) => (
    <div className="rounded-lg border border-border bg-card p-3" data-testid={testId}>
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="text-xs font-semibold text-slate-600">{title}</div>
        <Button variant="secondary" size="sm" className="text-[11px]" onClick={() => setJsonOpen(!jsonOpen)}>{jsonOpen ? 'Hide JSON' : 'Advanced JSON'}</Button>
      </div>
      {title.toLowerCase().includes('source') || title.toLowerCase().includes('sink') ? (
        <div className="mb-2 text-[11px] text-slate-400" data-testid={`${testId}-scope-hint`}>
          {fields.every((f) => f.scope === 'behavior')
            ? t('field.behaviorOnlyHint')
            : t('field.fullConfigHint')}
        </div>
      ) : null}
      <ConfigForm fields={fields} config={config} onChange={(next) => setConfigText(prettyJSON(next))} t={t} />
      {jsonOpen && (
        <Textarea className="mt-3 min-h-36 font-mono text-xs" value={configText} onChange={(e) => setConfigText(e.target.value)} />
      )}
    </div>
  );

  const renderConnectionContext = (title: string, ctx: ConnectionContext | null) => {
    if (!ctx) {
      return <div className="rounded-lg border border-dashed border-border bg-muted/40 p-3 text-xs text-slate-400">{title}: select a saved connection to load health, schema, sample, and recommendations.</div>;
    }
    const intro = ctx.introspection;
    const schemaRows = intro?.schema || intro?.tables?.find((table) => table.columns?.length)?.columns || [];
    return (
      <div className={`rounded-lg border p-3 ${intro?.ok === false ? 'border-rose-200 bg-rose-50' : 'border-cyan-200 bg-cyan-50'}`} data-testid={`${title.toLowerCase().replace(/\s+/g, '-')}-context`}>
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-xs font-semibold text-slate-700">{title} context</div>
          <ToneBadge tone={intro?.ok === false ? 'rose' : 'blue'}>{intro?.status || 'ready'}</ToneBadge>
        </div>
        {ctx.connection && (
          <div className="mb-2 text-xs text-slate-600">
            {ctx.connection.name} · {ctx.connection.type}
            {ctx.connection.last_status ? ` · last ${ctx.connection.last_status}` : ''}
            {ctx.connection.last_error ? ` · ${ctx.connection.last_error}` : ''}
          </div>
        )}
        {intro?.error && <div className="mb-2 text-xs text-rose-700">{intro.error}</div>}
        {ctx.recommendations?.length ? (
          <div className="mb-2 flex flex-wrap gap-1">
            {ctx.recommendations.map((rec) => {
              const canApply = canApplyConnectionRecommendation(title, rec);
              return (
                <span key={rec.field} className="inline-flex items-center gap-1 rounded-full border border-cyan-200 bg-white/80 px-2 py-0.5 text-[10px] text-slate-600">
                  <span>{rec.field}: {String(rec.value || 'review')}</span>
                  {canApply && (
                    <button
                      type="button"
                      data-testid="connection-recommendation-apply"
                      className="font-semibold text-indigo-600 hover:text-indigo-800"
                      onClick={() => applyConnectionRecommendation(title, rec)}
                    >
                      Apply
                    </button>
                  )}
                </span>
              );
            })}
          </div>
        ) : null}
        {intro?.tables?.length ? (
          <div className="mb-2 text-xs text-slate-600">
            Tables: {intro.tables.slice(0, 6).map((table) => table.schema ? `${table.schema}.${table.name}` : table.name).join(', ')}
            {intro.tables.length > 6 ? ` +${intro.tables.length - 6}` : ''}
          </div>
        ) : null}
        {intro?.topics?.length ? (
          <div className="mb-2 text-xs text-slate-600">
            Topics: {intro.topics.slice(0, 6).map((topic) => `${topic.name}${topic.partitions?.length ? `(${topic.partitions.length})` : ''}`).join(', ')}
            {intro.topics.length > 6 ? ` +${intro.topics.length - 6}` : ''}
          </div>
        ) : null}
        {intro?.targets?.length ? (
          <div className="mb-2 text-xs text-slate-600">
            Targets: {intro.targets.slice(0, 4).map((target) => `${target.kind}:${target.location}${target.prefix ? `/${target.prefix}` : ''}${target.writable === false ? ' (not writable)' : ''}`).join(', ')}
            {intro.targets.length > 4 ? ` +${intro.targets.length - 4}` : ''}
          </div>
        ) : null}
        {schemaRows.length ? (
          <div className="mb-2 grid grid-cols-2 gap-1 text-xs sm:grid-cols-3">
            {schemaRows.slice(0, 9).map((col) => <div key={col.name} className="truncate rounded bg-white/80 px-2 py-1 font-mono text-slate-600">{col.name}<span className="ml-1 text-slate-400">{col.data_type}</span></div>)}
          </div>
        ) : null}
        {intro?.sample?.length ? (
          <pre className="max-h-32 overflow-auto rounded bg-white/80 p-2 text-xs">{prettyJSON(intro.sample[0])}</pre>
        ) : null}
        {intro?.warnings?.map((warning, i) => <div key={i} className="mt-1 text-xs text-amber-700">{warning}</div>)}
      </div>
    );
  };

  const updateTransformConfig = (index: number, nextConfig: Record<string, unknown>) => {
    const next = transformConfigs.map((item, i) => i === index ? { ...item, config: nextConfig } : item);
    setTransformsText(prettyJSON(next));
  };

  const updateTransformType = (index: number, type: string) => {
    const fields = (schema?.data?.transforms?.[type] || []) as PluginSchemaField[];
    const next = transformConfigs.map((item, i) => i === index ? { type, config: buildDefaultConfig(fields) } : item);
    setTransformsText(prettyJSON(next));
    setStageDryRunResult(null);
  };

  const addTransform = () => {
    const type = transformTypes.includes('project') ? 'project' : transformTypes[0] || 'identity';
    const fields = (schema?.data?.transforms?.[type] || []) as PluginSchemaField[];
    setTransformsText(prettyJSON([...transformConfigs, { type, config: buildDefaultConfig(fields) }]));
    setStageDryRunResult(null);
  };

  const removeTransform = (index: number) => {
    setTransformsText(prettyJSON(transformConfigs.filter((_, i) => i !== index)));
    setStageDryRunResult(null);
  };

  const moveTransform = (index: number, direction: -1 | 1) => {
    const target = index + direction;
    if (target < 0 || target >= transformConfigs.length) return;
    const next = [...transformConfigs];
    [next[index], next[target]] = [next[target], next[index]];
    setTransformsText(prettyJSON(next));
    setStageDryRunResult(null);
  };

  const dryRunThroughStage = async (index: number) => {
    setBusy(`stage-${index}`); setError(''); setStageDryRunResult(null);
    try {
      const data = await api('/api/v2/transforms/dry-run', {
        method: 'POST',
        body: JSON.stringify({ transforms: transformConfigs.slice(0, index + 1), record: parseJSONText(sampleText, template.sample) }),
      });
      if ((data as any)?.partial_error) {
        const message = prettyJSON((data as any).errors || data);
        setStageDryRunResult({ index, error: message });
        setError(`Stage ${index + 1} failed: ${message}`);
        return;
      }
      setStageDryRunResult({ index, result: data });
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      setStageDryRunResult({ index, error: message });
      setError(`Stage ${index + 1} failed: ${message}`);
    } finally {
      setBusy('');
    }
  };

  return (
    <Modal title={t('wizard.title')} onClose={onClose} className="sm:max-w-6xl">
      <div className="grid gap-5 xl:grid-cols-[280px_1fr]">
        <div className="space-y-3">
          <div className="px-1 pb-1 text-xs font-medium text-slate-400">{t('wizard.emptyStart')}</div>
          {WIZARD_TEMPLATES.map((tpl) => (
            <button key={tpl.id} className={`relative w-full rounded-lg border p-3 text-left text-sm transition ${templateId === tpl.id ? 'border-primary bg-accent text-primary' : 'border-border bg-card text-muted-foreground hover:border-primary/40'}`} onClick={() => setTemplateId(tpl.id)}>
              <div className="flex items-center gap-1.5">
                <span className="font-semibold">{tpl.title}</span>
                {tpl.recommended && <ToneBadge tone="indigo" className="px-1.5 py-0 text-[10px]">{t('wizard.recommended')}</ToneBadge>}
              </div>
              <div className="mt-1 text-xs text-slate-400">{tpl.descKey ? t(tpl.descKey) : `${tpl.sourceTypes.join(' / ')} → ${tpl.sinkTypes.join(' / ')}`}</div>
            </button>
          ))}
        </div>
        <div className="space-y-4">
          <div className="grid gap-3 md:grid-cols-3">
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-500">Pipeline name</label>
              <Input data-testid="wizard-pipeline-name" value={name} onChange={(e) => setName(e.target.value)} />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-500">Source</label>
              <select data-testid="wizard-source-type" className={wizardSelectClass} value={sourceType} onChange={(e) => { setSourceType(e.target.value); setSourceConnection(''); setSourceContext(null); setSourceConfigText(prettyJSON(seedSourceConfig(e.target.value))); }}>
                {template.sourceTypes.map((tp) => <option key={tp} value={tp}>{tp}</option>)}
              </select>
              <select data-testid="wizard-source-connection" className={cn(wizardSelectClass, "mt-2 text-sm")} value={sourceConnection} onFocus={() => refreshConnections()} onChange={(e) => selectSourceConnection(e.target.value)}>
                <option value="">Inline source config</option>
                {sourceConnections.map((conn) => <option key={conn.name} value={conn.name}>{conn.name} · {conn.type} · {conn.last_status || 'untested'}</option>)}
              </select>
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-500">Sink</label>
              <select data-testid="wizard-sink-type" className={wizardSelectClass} value={sinkType} onChange={(e) => { setSinkType(e.target.value); setSinkConnection(''); setSinkContext(null); setSinkConfigText(prettyJSON(seedSinkConfig(e.target.value))); }}>
                {template.sinkTypes.map((tp) => <option key={tp} value={tp}>{tp}</option>)}
              </select>
              <select data-testid="wizard-sink-connection" className={cn(wizardSelectClass, "mt-2 text-sm")} value={sinkConnection} onFocus={() => refreshConnections()} onChange={(e) => selectSinkConnection(e.target.value)}>
                <option value="">Inline sink config</option>
                {sinkConnections.map((conn) => <option key={conn.name} value={conn.name}>{conn.name} · {conn.type} · {conn.last_status || 'untested'}</option>)}
              </select>
              {/* E2E-only remediation shortcuts (P4.2: hidden in production UI). */}
              {typeof window !== 'undefined' &&
                (window.location.search.includes('e2e=') ||
                  window.localStorage.getItem('etl_e2e') === '1') && (
                <div className="mt-1 flex flex-wrap gap-1">
                  {template.sinkTypes.includes('maxcompute') && (
                    <Button
                      variant="secondary"
                      size="sm"
                      className="text-[11px]"
                      onClick={() => {
                        setSinkType('maxcompute');
                        setSinkConnection('');
                        setSinkContext(null);
                        setSinkConfigText(prettyJSON(seedSinkConfig('maxcompute')));
                        setResult(null);
                      }}
                    >
                      Failure demo
                    </Button>
                  )}
                  {template.sinkTypes.includes('file_sink') && (
                    <Button
                      variant="secondary"
                      size="sm"
                      className="text-[11px]"
                      onClick={() => {
                        setSinkType('file_sink');
                        setSinkConnection('');
                        setSinkContext(null);
                        setSinkConfigText(prettyJSON(seedSinkConfig('file_sink')));
                        setResult(null);
                      }}
                    >
                      Repair to file_sink
                    </Button>
                  )}
                </div>
              )}
            </div>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            {renderSchemaSummary(`Descriptor schema: ${sourceType}`, sourceFields, sourceMaturity, sourceCapabilities, sourceMissing)}
            {renderSchemaSummary(`Descriptor schema: ${sinkType}`, sinkFields, sinkMaturity, sinkCapabilities, sinkMissing)}
          </div>
          {(template.tableMapping || tableMappingOpen) && (
            <div className="rounded-lg border border-indigo-200 bg-indigo-50/40 p-3" data-testid="wizard-table-mapping">
              <div className="mb-2 flex items-center justify-between">
                <div>
                  <div className="text-xs font-semibold text-slate-700">{t('wizard.tableMapping')}</div>
                  <div className="text-[11px] text-slate-500">{t('wizard.tableMappingHint')}</div>
                </div>
                <Button variant="ghost" size="sm" className="text-[11px]" onClick={() => setTableMappingOpen(!tableMappingOpen)}>{tableMappingOpen ? '−' : '+'}</Button>
              </div>
              {tableMappingOpen ? (
                <Textarea
                  data-testid="wizard-table-mapping-json"
                  className="min-h-20 w-full font-mono text-xs"
                  value={tableMappingText}
                  onChange={(e) => setTableMappingText(e.target.value)}
                  placeholder={'{\n  "template": "ods_{source_table}"\n}'}
                />
              ) : (
                <pre className="overflow-auto rounded bg-white/70 p-2 text-xs text-slate-600">{tableMappingText || '—'}</pre>
              )}
            </div>
          )}
          <div className="grid gap-3 lg:grid-cols-2">
            {renderConnectionContext('Source', sourceContext)}
            {renderConnectionContext('Sink', sinkContext)}
          </div>
          <div className="rounded-lg border border-border bg-card p-3" data-testid="wizard-runtime-safety">
            <div className="mb-3 text-xs font-semibold text-slate-600">Runtime safety</div>
            <div className="grid gap-3 sm:grid-cols-3">
              <label className="block text-xs text-slate-500">
                <span className="mb-1 block font-medium">Batch size</span>
                <Input
                  data-testid="wizard-batch-size"
                  
                  type="number"
                  min={1}
                  value={batchSize}
                  onChange={(e) => setBatchSize(positiveIntValue(e.target.value, batchSize))}
                />
              </label>
              <label className="block text-xs text-slate-500">
                <span className="mb-1 block font-medium">Checkpoint sec</span>
                <Input
                  data-testid="wizard-checkpoint-sec"
                  
                  type="number"
                  min={1}
                  value={checkpointIntervalSec}
                  onChange={(e) => setCheckpointIntervalSec(positiveIntValue(e.target.value, checkpointIntervalSec))}
                />
              </label>
              <label className="flex items-center gap-2 rounded border border-slate-100 bg-slate-50 px-3 py-2 text-xs font-medium text-slate-600">
                <input data-testid="wizard-dlq-enabled" type="checkbox" checked={dlqEnabled} onChange={(e) => setDlqEnabled(e.target.checked)} />
                DLQ enabled
              </label>
            </div>
          </div>
          <div className="grid gap-3 lg:grid-cols-2">
            {renderConfigEditor('Source config', sourceFields, sourceConfig, sourceConfigText, setSourceConfigText, sourceJsonOpen, setSourceJsonOpen, 'wizard-source-config-form')}
            {renderConfigEditor('Sink config', sinkFields, sinkConfig, sinkConfigText, setSinkConfigText, sinkJsonOpen, setSinkJsonOpen, 'wizard-sink-config-form')}
          </div>
          <div className="grid gap-3 lg:grid-cols-2">
            <div>
              <div className="mb-1 flex items-center justify-between gap-2">
                <label className="block text-xs font-medium text-slate-500">Transform chain</label>
                <Button data-testid="wizard-add-transform" variant="secondary" size="sm" className="text-[11px]" onClick={addTransform}>Add transform</Button>
              </div>
              <div className="mb-3 rounded-lg border border-border bg-card p-3" data-testid="wizard-transform-config-form">
                {transformConfigs.length ? (
                  <div className="space-y-4">
                    {transformConfigs.map((item, index) => {
                      const fields = (schema?.data?.transforms?.[item.type] || []) as PluginSchemaField[];
                      return (
                        <div key={`${item.type}-${index}`} className="rounded border border-slate-100 bg-slate-50 p-3" data-testid={`wizard-transform-stage-${index}`}>
                          <div className="mb-2 flex items-center justify-between gap-2">
                            <div className="flex min-w-0 flex-1 items-center gap-2">
                              <span className="shrink-0 text-xs font-semibold text-slate-600">{index + 1}.</span>
                              <select data-testid={`wizard-transform-type-${index}`} className={cn(wizardSelectClass, "h-8 min-w-0 flex-1 text-xs")} value={item.type} onChange={(e) => updateTransformType(index, e.target.value)}>
                                {transformTypes.map((type) => <option key={type} value={type}>{type}</option>)}
                              </select>
                              <ToneBadge tone="slate" className="text-[10px]">{metadata.transforms?.[item.type]?.maturity || 'unknown'}</ToneBadge>
                            </div>
                            <div className="flex shrink-0 gap-1">
                              <Button data-testid={`wizard-transform-move-up-${index}`} variant="ghost" size="sm" className="px-2" onClick={() => moveTransform(index, -1)} disabled={index === 0} title="Move up">↑</Button>
                              <Button data-testid={`wizard-transform-move-down-${index}`} variant="ghost" size="sm" className="px-2" onClick={() => moveTransform(index, 1)} disabled={index === transformConfigs.length - 1} title="Move down">↓</Button>
                              <Button data-testid={`wizard-transform-dry-run-${index}`} variant="secondary" size="sm" className="px-2" onClick={() => dryRunThroughStage(index)} disabled={busy === `stage-${index}`} title="Dry-run through this stage">▶</Button>
                              <Button data-testid={`wizard-transform-remove-${index}`} variant="destructive" size="sm" className="px-2" onClick={() => removeTransform(index)} title="Remove">×</Button>
                            </div>
                          </div>
                          <ConfigForm fields={fields} config={item.config || {}} onChange={(next) => updateTransformConfig(index, next)} t={t} emptyText="No config fields for this transform." />
                          {stageDryRunResult?.index === index && stageDryRunResult.result !== undefined && (
                            <div data-testid={`wizard-transform-stage-result-${index}`} className="mt-2 rounded border border-emerald-100 bg-white p-2">
                              <div className="mb-1 text-[11px] font-semibold text-emerald-700">Stage {index + 1} output</div>
                              <pre className="max-h-36 overflow-auto text-xs text-slate-700">{prettyJSON(stageDryRunResult.result)}</pre>
                            </div>
                          )}
                          {stageDryRunResult?.index === index && stageDryRunResult.error && (
                            <div data-testid={`wizard-transform-stage-error-${index}`} className="mt-2 rounded border border-rose-100 bg-white p-2 text-xs text-rose-700">
                              <div className="mb-1 font-semibold">Stage {index + 1} failed</div>
                              <pre className="max-h-36 overflow-auto whitespace-pre-wrap">{stageDryRunResult.error}</pre>
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="space-y-2">
                    <div className="text-xs text-slate-400">No valid transforms in the chain.</div>
                    <Button data-testid="wizard-add-transform-empty" variant="secondary" size="sm" onClick={addTransform}>Add transform</Button>
                  </div>
                )}
              </div>
              <Button variant="secondary" size="sm" className="text-[11px]" onClick={() => setTransformJsonOpen(!transformJsonOpen)}>
                {transformJsonOpen ? 'Hide chain JSON' : 'Advanced chain JSON'}
              </Button>
              {transformJsonOpen && (
                <Textarea data-testid="wizard-transform-json" className="mt-2 min-h-32 w-full font-mono text-xs" value={transformsText} onChange={(e) => setTransformsText(e.target.value)} />
              )}
            </div>
            <div>
              <div className="mb-1 flex items-center justify-between gap-2">
                <label className="block text-xs font-medium text-slate-500">Sample record</label>
                <a className="text-xs text-indigo-600 hover:underline" href="/api/v2/docs" target="_blank" rel="noreferrer">API docs</a>
              </div>
              <Textarea className="min-h-32 font-mono text-xs" value={sampleText} onChange={(e) => setSampleText(e.target.value)} />
            </div>
          </div>
          <div>
            <div className="mb-1 flex items-center justify-between gap-2">
              <label className="block text-xs font-medium text-slate-500">Generated YAML</label>
              <Button variant="secondary" size="sm" onClick={syncFromYaml}>Sync YAML to form</Button>
            </div>
            <Textarea data-testid="wizard-yaml" className="min-h-56 w-full font-mono text-xs" value={yamlText} onChange={(e) => setYamlText(e.target.value)} />
          </div>
          <div className="flex flex-wrap gap-2">
            <Button data-testid="wizard-dry-run" variant="secondary" disabled={busy === 'dry-run'} onClick={dryRun}>Transform dry-run</Button>
            <Button data-testid="wizard-validate" variant="secondary" disabled={busy === 'validate'} onClick={() => validate().catch(() => {})}>Validate + preflight</Button>
            <Button data-testid="wizard-create-start" disabled={busy === 'create'} onClick={createAndStart}>Create and start</Button>
          </div>
          {error && <ErrorBox message={error} />}
          {dryRunResult !== null && (
            <div className="rounded-lg border border-cyan-200 bg-cyan-50 p-3">
              <div className="mb-2 text-xs font-semibold text-cyan-700">Dry-run output</div>
              <pre className="max-h-56 overflow-auto text-xs text-cyan-950">{prettyJSON(dryRunResult)}</pre>
            </div>
          )}
          {result && (
            <div data-testid="wizard-preflight-result" className={`rounded-lg border p-3 ${result.valid === false ? 'border-rose-200 bg-rose-50' : 'border-emerald-200 bg-emerald-50'}`}>
              <div className="mb-2 text-sm font-semibold">{result.valid === false ? 'Preflight failed' : 'Preflight passed'} · {result.preflight?.summary || 'validation complete'}</div>
              {(result.warnings || result.errors || []).map((msg, i) => <div key={i} className="text-xs text-slate-700">{msg}</div>)}
              {(result.preflight?.recommendations || []).map((rec, i) => (
                <div key={`recommendation-${rec.path}-${i}`} className="mt-2 rounded border border-white/70 bg-white/70 p-2 text-xs">
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <div className="font-semibold">{rec.safety || 'review'} · {rec.path}</div>
                      <div>{rec.reason}</div>
                      <div className="mt-1 font-mono text-slate-500">{prettyJSON(rec.value)}</div>
                    </div>
                    <Button variant="secondary" size="sm" className="shrink-0 text-[11px]" onClick={() => applyPreflightRecommendation(rec)}>Apply</Button>
                  </div>
                </div>
              ))}
              {(result.preflight?.issues || []).map((issue, i) => (
                <div key={`issue-${i}`} className="mt-2 rounded border border-white/70 bg-white/70 p-2 text-xs">
                  <div className="font-semibold">{issue.level} · {issue.check}</div>
                  <div>{issue.message}</div>
                  {issue.remediation && <div className="mt-1 text-slate-500">Fix: {issue.remediation}</div>}
                </div>
              ))}
              {(result.preflight?.field_issues || []).map((issue, i) => (
                <div key={`field-${i}`} className="mt-2 rounded border border-white/70 bg-white/70 p-2 text-xs">
                  <div className="font-semibold">{issue.field} · {issue.check}</div>
                  <div>{issue.message}</div>
                  {issue.remediation && <div className="mt-1 text-slate-500">Fix: {issue.remediation}</div>}
                </div>
              ))}
              {(result.preflight?.guidance || []).map((item, i) => (
                <div key={`guidance-${i}`} className="mt-2 rounded border border-white/70 bg-white/70 p-2 text-xs">
                  <div className="font-semibold">{item.level} · {item.category} · {item.code}</div>
                  <div>{item.message}</div>
                  {item.action && <div className="mt-1 text-slate-500">Action: {item.action}</div>}
                </div>
              ))}
              {(result.preflight?.readiness || []).map((connector, i) => (
                <div key={`readiness-${connector.kind}-${connector.type}-${i}`} className="mt-2 rounded border border-white/70 bg-white/70 p-2 text-xs">
                  <div className="font-semibold">{connector.kind} · {connector.type} · {connector.maturity} · {connector.status}</div>
                  {connector.summary && <div>{connector.summary}</div>}
                  {(connector.gates || []).filter((gate) => gate.status === 'missing' || gate.status === 'partial').slice(0, 3).map((gate) => (
                    <div key={gate.code} className="mt-1 text-slate-600">
                      {gate.status} · {gate.label}{gate.remediation ? ` · ${gate.remediation}` : ''}
                    </div>
                  ))}
                </div>
              ))}
              {result.preflight?.ddl_preview?.statements?.length ? (
                <pre className="mt-2 max-h-40 overflow-auto rounded bg-white/80 p-2 text-xs">{result.preflight.ddl_preview.statements.join('\n')}</pre>
              ) : null}
            </div>
          )}
        </div>
      </div>
    </Modal>
  );
}

// ════════════════════════════════════════════════

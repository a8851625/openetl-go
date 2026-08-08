import { useEffect, useRef, useState } from 'react';
import type {
  ApiState,
  ConnectionEntry,
  Pipeline,
  PipelineStats,
} from './types';

export type ApiIssue = {
  level?: string;
  scope?: string;
  field?: string;
  node?: string;
  node_id?: string;
  node_kind?: string;
  plugin?: string;
  check?: string;
  category?: string;
  code?: string;
  message: string;
  remediation?: string;
  action?: string;
};

export type ApiErrorPayload = {
  error?: string;
  message?: string;
  code?: string;
  operation?: string;
  valid?: boolean;
  preflight_valid?: boolean;
  errors?: unknown[];
  warnings?: unknown[];
  issues?: unknown[];
  field_issues?: unknown[];
  preflight?: {
    issues?: unknown[];
    field_issues?: unknown[];
    summary?: string;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

export type ApiErrorDetails = {
  status?: number;
  summary: string;
  issues: ApiIssue[];
  raw?: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function isApiErrorDetails(value: unknown): value is ApiErrorDetails {
  return isRecord(value) && typeof value.summary === 'string' && Array.isArray(value.issues);
}

function asText(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function parseResponseBody(text: string): unknown {
  const trimmed = text.trim();
  if (!trimmed) return undefined;
  try {
    return JSON.parse(trimmed);
  } catch {
    return text;
  }
}

function issueKey(issue: ApiIssue): string {
  return [
    issue.level,
    canonicalIssueField(issue.field),
    issue.node_id || issue.node,
    issue.check || issue.code,
    issue.message,
  ].join('|');
}

function canonicalIssueField(field?: string): string {
  if (!field) return '';
  const bracketed = field.match(/^dag\.nodes\[([^\]]+)\](.*)$/);
  if (bracketed) return `dag.nodes.${bracketed[1]}${bracketed[2]}`;
  return field;
}

function appendIssue(issues: ApiIssue[], issue: ApiIssue | null | undefined) {
  if (!issue || !issue.message.trim()) return;
  const duplicate = issues.findIndex((item) => issueKey(item) === issueKey(issue) || item.message === issue.message);
  if (duplicate < 0) {
    issues.push(issue);
    return;
  }
  // Keep the structured variant when a legacy errors[] string and an issue
  // envelope carry the same message.
  const score = (item: ApiIssue) => [item.field, item.node_id || item.node, item.code, item.check, item.remediation, item.action].filter(Boolean).length;
  if (score(issue) > score(issues[duplicate])) issues[duplicate] = issue;
}

function appendStringIssues(issues: ApiIssue[], value: unknown, level: string) {
  if (!Array.isArray(value)) return;
  value.forEach((message) => {
    if (typeof message === 'string' && message.trim()) appendIssue(issues, { level, message: message.trim() });
  });
}

function appendStructuredIssues(issues: ApiIssue[], value: unknown, fallbackLevel: string) {
  if (!Array.isArray(value)) return;
  value.forEach((raw) => {
    if (typeof raw === 'string') {
      appendIssue(issues, { level: fallbackLevel, message: raw });
      return;
    }
    if (!isRecord(raw)) return;
    const message = asText(raw.message) || asText(raw.reason) || asText(raw.label);
    if (!message) return;
    const nodeID = asText(raw.node_id) || asText(raw.node);
    appendIssue(issues, {
      level: asText(raw.level) || fallbackLevel,
      scope: asText(raw.scope) || undefined,
      field: asText(raw.field) || undefined,
      node: asText(raw.node) || nodeID || undefined,
      node_id: nodeID || undefined,
      node_kind: asText(raw.node_kind) || undefined,
      plugin: asText(raw.plugin) || undefined,
      check: asText(raw.check) || undefined,
      category: asText(raw.category) || undefined,
      code: asText(raw.code) || undefined,
      message,
      remediation: asText(raw.remediation) || undefined,
      action: asText(raw.action) || undefined,
    });
  });
}

function isSensitiveKey(key: string): boolean {
  const normalized = key.replace(/[^a-z0-9]/gi, '').toLowerCase();
  return /(password|passwd|secret|token|apikey|accesskey|privatekey|credential|authorization|cookie|dsn)/.test(normalized);
}

function redactText(value: string): string {
  return value
    .replace(/\b(authorization\s*[:=]\s*bearer\s+)[^\s,;}]+/gi, '$1[REDACTED]')
    .replace(
      /\b(password|passwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential|authorization|cookie|dsn)\s*[:=]\s*("[^"]*"|'[^']*'|[^\s,;}]+)/gi,
      '$1=[REDACTED]',
    )
    .trim();
}

function safeRaw(value: unknown): string | undefined {
  if (typeof value === 'string') return redactText(value) || undefined;
  if (!value || typeof value !== 'object') return undefined;
  try {
    return JSON.stringify(
      value,
      (key, item) => (key && isSensitiveKey(key) ? '[REDACTED]' : item),
      2,
    );
  } catch {
    return undefined;
  }
}

export function toApiErrorDetails(value: unknown, status = 0, fallback = 'Request failed'): ApiErrorDetails {
  if (value instanceof ApiError) return value.details;
  if (isApiErrorDetails(value)) {
    return { ...value, status: value.status || status || undefined };
  }
  if (value instanceof Error) {
    const attached = (value as Error & { payload?: unknown }).payload;
    if (attached !== undefined) {
      return toApiErrorDetails(attached, status, value.message || fallback);
    }
    return { status: status || undefined, summary: value.message || fallback, issues: [] };
  }

  let payload = value;
  let raw: string | undefined;
  if (typeof payload === 'string') {
    const parsed = parseResponseBody(payload);
    if (parsed === payload) raw = safeRaw(payload);
    else payload = parsed;
  }

  const issues: ApiIssue[] = [];
  let summary = '';
  if (isRecord(payload)) {
    summary = asText(payload.error) || asText(payload.message) || asText(payload.title);
    if (payload.valid === false && !summary) summary = 'Validation failed';
    if (payload.preflight_valid === false && !summary) summary = 'Preflight failed';
    appendStringIssues(issues, payload.errors, 'error');
    appendStringIssues(issues, payload.warnings, 'warning');
    appendStringIssues(issues, payload.preflight_warnings, 'warning');
    appendStructuredIssues(issues, payload.issues, 'error');
    appendStructuredIssues(issues, payload.field_issues, 'error');

    const preflight = isRecord(payload.preflight) ? payload.preflight : undefined;
    if (preflight) {
      appendStructuredIssues(issues, preflight.issues, 'error');
      appendStructuredIssues(issues, preflight.field_issues, 'error');
      appendStructuredIssues(issues, preflight.guidance, 'warning');
      if (Array.isArray(preflight.recommendations)) {
        preflight.recommendations.forEach((rawRecommendation) => {
          if (!isRecord(rawRecommendation)) return;
          const path = asText(rawRecommendation.path);
          const reason = asText(rawRecommendation.reason);
          if (!reason) return;
          appendIssue(issues, {
            level: asText(rawRecommendation.safety) || 'warning',
            field: path || undefined,
            code: 'recommendation',
            message: reason,
            action: path ? `Apply recommendation for ${path}` : undefined,
          });
        });
      }
      if (Array.isArray(preflight.readiness)) {
        preflight.readiness.forEach((rawConnector) => {
          if (!isRecord(rawConnector)) return;
          const connector = `${asText(rawConnector.kind)}/${asText(rawConnector.type)}`.replace(/^\//, '');
          const summaryText = asText(rawConnector.summary);
          if (summaryText) {
            appendIssue(issues, { level: 'info', code: 'readiness', message: `${connector}: ${summaryText}` });
          }
          if (!Array.isArray(rawConnector.gates)) return;
          rawConnector.gates.forEach((rawGate) => {
            if (!isRecord(rawGate) || !['missing', 'partial'].includes(asText(rawGate.status))) return;
            const label = asText(rawGate.label) || asText(rawGate.code) || 'readiness gate';
            appendIssue(issues, {
              level: 'warning',
              code: asText(rawGate.code) || 'readiness',
              message: `${connector}: ${asText(rawGate.status)} · ${label}`,
              remediation: asText(rawGate.remediation) || undefined,
            });
          });
        });
      }
      if (!summary) summary = asText(preflight.summary);
    }
  } else if (typeof payload === 'string') {
    summary = payload.trim();
  }

  if (!summary && issues.length > 0) summary = issues[0].message;
  if (!summary) summary = fallback;
  if (!raw && isRecord(payload)) raw = safeRaw(payload);
  return { status: status || undefined, summary, issues, raw };
}

export function apiErrorSummary(value: unknown, fallback = 'Request failed'): string {
  return toApiErrorDetails(value, 0, fallback).summary;
}

export function apiErrorPayload(value: unknown): ApiErrorPayload | null {
  if (value instanceof ApiError) {
    return isRecord(value.payload) ? value.payload as ApiErrorPayload : null;
  }
  if (value instanceof Error) {
    const attached = (value as Error & { payload?: unknown }).payload;
    if (isRecord(attached)) return attached as ApiErrorPayload;
    const parsed = parseResponseBody(value.message);
    return isRecord(parsed) ? parsed as ApiErrorPayload : (value.message.trim() ? { error: value.message.trim() } : null);
  }
  if (isRecord(value)) {
    const attached = value.payload;
    if (isRecord(attached)) return attached as ApiErrorPayload;
    return value as ApiErrorPayload;
  }
  if (typeof value === 'string') {
    const parsed = parseResponseBody(value);
    return isRecord(parsed) ? parsed as ApiErrorPayload : (value.trim() ? { error: value.trim() } : null);
  }
  return null;
}

export function apiErrorIssues(value: unknown): ApiIssue[] {
  return toApiErrorDetails(value).issues;
}

export class ApiError extends Error {
  readonly status: number;
  readonly payload: unknown;
  readonly details: ApiErrorDetails;

  constructor(status: number, payload: unknown, rawText = '', fallback = 'Request failed') {
    const details = toApiErrorDetails(payload ?? rawText, status, fallback);
    super(details.summary);
    this.name = 'ApiError';
    this.status = status;
    this.payload = payload;
    this.details = details;
  }
}

function isErrorEnvelope(payload: unknown): boolean {
  if (!isRecord(payload)) return false;
  if (payload.valid === false || payload.preflight_valid === false) return true;
  if (typeof payload.error === 'string' && payload.error.trim()) return true;
  if (Array.isArray(payload.errors) && payload.errors.length > 0) return true;
  return false;
}
// API credentials intentionally live only for the lifetime of this page.  A
// persistent localStorage token is readable by every script running in the UI
// origin and survives browser restarts, so it is not an acceptable default for
// a self-hosted production console.  The settings dialog can repopulate this
// value after an explicit user action.
let memoryToken = '';

export function getToken(): string {
  return memoryToken;
}

export function setToken(value: string): void {
  memoryToken = value.trim();
}

export function clearToken(): void {
  memoryToken = '';
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  headers.set('Content-Type', headers.get('Content-Type') || 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  const text = await res.text();
  const payload = parseResponseBody(text);
  if (!res.ok || isErrorEnvelope(payload)) {
    throw new ApiError(res.status, payload, text, `${res.status} ${res.statusText}`);
  }
  return (payload === undefined ? undefined : payload) as T;
}

export function useApi<T>(path: string, refreshKey: number): ApiState<T> {
  const [state, setState] = useState<ApiState<T>>({ loading: true });
  const firstRender = useRef(true);
  useEffect(() => {
    let cancelled = false;
    // 仅首次请求显示 loading，后续刷新保留旧数据避免闪烁
    if (firstRender.current) {
      setState((p) => ({ ...p, loading: true }));
    }
    api<T>(path)
      .then((d) => {
        if (!cancelled) {
          firstRender.current = false;
          setState({ data: d, loading: false });
        }
      })
      .catch((e) => {
        if (!cancelled) {
          firstRender.current = false;
          setState((p) => ({ ...p, error: apiErrorSummary(e), loading: false }));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [path, refreshKey]);
  return state;
}

export function zeroPipelineStats(raw: any = {}): PipelineStats {
  return {
    records_read: Number(raw.records_read) || 0,
    records_written: Number(raw.records_written) || 0,
    records_failed: Number(raw.records_failed) || 0,
    records_dlq: Number(raw.records_dlq) || 0,
    last_error: typeof raw.last_error === 'string' ? raw.last_error : undefined,
    last_checkpoint: typeof raw.last_checkpoint === 'string' ? raw.last_checkpoint : undefined,
    started_at: typeof raw.started_at === 'string' ? raw.started_at : undefined,
    uptime: typeof raw.uptime === 'string' ? raw.uptime : undefined,
    bytes_read: Number(raw.bytes_read) || 0,
    bytes_written: Number(raw.bytes_written) || 0,
    dlq_replay_count: Number(raw.dlq_replay_count) || 0,
    dlq_delete_count: Number(raw.dlq_delete_count) || 0,
  };
}

export function normalizePipeline(raw: unknown): Pipeline | null {
  if (!raw || typeof raw !== 'object') return null;
  const p = raw as any;
  const name = typeof p.name === 'string' ? p.name.trim() : '';
  if (!name) return null;
  const id = typeof p.id === 'string' ? p.id.trim() : '';
  return {
    ...p,
    id: id || undefined,
    name,
    status: typeof p.status === 'string' && p.status ? p.status : 'unknown',
    stats: zeroPipelineStats(p.stats),
    tags: Array.isArray(p.tags)
      ? p.tags.filter((tag: unknown): tag is string => typeof tag === 'string' && tag.trim() !== '')
      : [],
    shards: Array.isArray(p.shards)
      ? p.shards.map((s: any) => ({ ...s, stats: zeroPipelineStats(s?.stats) }))
      : undefined,
  };
}

export function pipelineRef(p?: Pick<Pipeline, 'id' | 'name'> | null): string {
  return encodeURIComponent((p?.id || p?.name || '').trim());
}

export function pipelineKey(p?: Pick<Pipeline, 'id' | 'name'> | null): string {
  return (p?.id || p?.name || '').trim();
}

export function normalizePipelines(data?: { pipelines?: Pipeline[] | null }): Pipeline[] {
  if (!Array.isArray(data?.pipelines)) return [];
  return data.pipelines.map(normalizePipeline).filter((p): p is Pipeline => p !== null);
}

export function normalizeConnectionEntry(raw: any): ConnectionEntry | null {
  if (!raw || typeof raw !== 'object') return null;
  const name = String(raw.name || raw.Name || '').trim();
  const kind = String(raw.kind || raw.Kind || '').trim();
  const type = String(raw.type || raw.Type || '').trim();
  if (!name || !kind || !type) return null;
  if (kind !== 'source' && kind !== 'sink' && kind !== 'transform') return null;
  return {
    name,
    kind,
    type,
    last_status: raw.last_status || raw.LastStatus,
    last_error: raw.last_error || raw.LastError,
    last_tested_at: raw.last_tested_at || raw.LastTestedAt,
    config: raw.config || raw.Config,
  };
}

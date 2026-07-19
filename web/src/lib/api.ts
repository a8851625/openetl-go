import { useEffect, useRef, useState } from 'react';
import type {
  ApiState,
  ConnectionEntry,
  Pipeline,
  PipelineStats,
} from './types';

export function getToken() {
  return window.localStorage.getItem('etl_api_token') || '';
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  headers.set('Content-Type', headers.get('Content-Type') || 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) throw new Error((await res.text()) || `${res.status} ${res.statusText}`);
  return res.json();
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
          setState((p) => ({ ...p, error: e.message, loading: false }));
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

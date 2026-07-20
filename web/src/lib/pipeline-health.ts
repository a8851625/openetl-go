import type { MetricsPipeline, Pipeline } from './types';

/** Derived pipeline health used across overview / list / detail / issues. */
export type PipelineHealth =
  | 'healthy'
  | 'degraded'
  | 'failed'
  | 'paused'
  | 'scheduled'
  | 'completed'
  | 'stopped'
  | 'starting'
  | 'unknown';

export type IssueSeverity = 'blocking' | 'warning' | 'info';

export type DerivedIssue = {
  id: string;
  severity: IssueSeverity;
  health: PipelineHealth;
  pipelineKey: string;
  pipelineName: string;
  title: string;
  summary: string;
  node?: string;
  field?: string;
  metricLabel?: string;
  metricValue?: string | number;
  action: 'issues' | 'dlq' | 'detail' | 'connections' | 'restart';
  startedHint?: string;
};

export type HealthCounts = {
  healthy: number;
  degraded: number;
  failed: number;
  paused: number;
  scheduled: number;
  completed: number;
  stopped: number;
  starting: number;
  unknown: number;
  total: number;
};

const STALE_CHECKPOINT_SEC = 300;
const HIGH_CDC_LAG_MS = 60_000;

export function derivePipelineHealth(p: Pipeline, m?: MetricsPipeline): PipelineHealth {
  const s = (p.status || '').toLowerCase();
  if (s === 'failed' || s === 'error') return 'failed';
  if (s === 'paused') return 'paused';
  if (s === 'scheduled') return 'scheduled';
  if (s === 'completed') return 'completed';
  if (s === 'starting') return 'starting';
  if (s === 'stopped') return 'stopped';
  if (s === 'running') {
    if ((p.stats?.records_failed || 0) > 0) return 'degraded';
    if ((p.stats?.records_dlq || 0) > 0) return 'degraded';
    if (p.stats?.last_error) return 'degraded';
    if (m && m.cdc_lag_ms > HIGH_CDC_LAG_MS) return 'degraded';
    if (m && m.checkpoint_age_seconds > STALE_CHECKPOINT_SEC) return 'degraded';
    return 'healthy';
  }
  return 'unknown';
}

export function countHealth(
  pipelines: Pipeline[],
  metrics: MetricsPipeline[] = [],
): HealthCounts {
  const counts: HealthCounts = {
    healthy: 0,
    degraded: 0,
    failed: 0,
    paused: 0,
    scheduled: 0,
    completed: 0,
    stopped: 0,
    starting: 0,
    unknown: 0,
    total: pipelines.length,
  };
  for (const p of pipelines) {
    const m = metrics.find((x) => (x.id && x.id === p.id) || x.name === p.name);
    const h = derivePipelineHealth(p, m);
    counts[h] = (counts[h] || 0) + 1;
  }
  return counts;
}

export function findMetric(
  p: Pipeline,
  metrics: MetricsPipeline[] = [],
): MetricsPipeline | undefined {
  return metrics.find((x) => (x.id && x.id === p.id) || x.name === p.name);
}

/** Infer a short Source → Transform → Sink path from available runtime fields. */
export function derivePipelinePath(p: Pipeline): { source: string; transform: string; sink: string } {
  const tags = p.tags || [];
  const tagHint = (prefix: string) => {
    const hit = tags.find((t) => t.toLowerCase().startsWith(prefix));
    return hit ? hit.slice(prefix.length) : '';
  };
  const source = tagHint('src:') || tagHint('source:') || (p.dag ? 'DAG source' : 'Source');
  const sink = tagHint('sink:') || tagHint('dst:') || (p.dag ? 'DAG sink' : 'Sink');
  const transform = tagHint('tf:') || tagHint('transform:') || (p.dag ? 'DAG' : '—');
  return { source, transform, sink };
}

export function deriveModeLabel(p: Pipeline, m?: MetricsPipeline): string {
  if (m && m.cdc_lag_ms > 0) return 'CDC';
  if (p.dag) return 'DAG';
  const tags = (p.tags || []).map((t) => t.toLowerCase());
  if (tags.some((t) => t.includes('cdc'))) return 'CDC';
  if (tags.some((t) => t.includes('batch'))) return 'batch';
  if (tags.some((t) => t.includes('stream'))) return 'streaming';
  if (p.status === 'scheduled' || p.status === 'completed') return 'scheduled';
  return p.status === 'running' ? 'streaming' : 'batch';
}

export function deriveIssues(
  pipelines: Pipeline[],
  metrics: MetricsPipeline[] = [],
): DerivedIssue[] {
  const issues: DerivedIssue[] = [];
  for (const p of pipelines) {
    const m = findMetric(p, metrics);
    const health = derivePipelineHealth(p, m);
    const key = (p.id || p.name || '').trim();
    if (!key) continue;

    if (health === 'failed' || p.status === 'failed' || p.status === 'error') {
      issues.push({
        id: `${key}:failed`,
        severity: 'blocking',
        health: 'failed',
        pipelineKey: key,
        pipelineName: p.name,
        title: `${p.name} · ${p.stats?.last_error ? '运行失败' : '管道失败'}`,
        summary: p.stats?.last_error || `status=${p.status}`,
        metricLabel: 'failed records',
        metricValue: p.stats?.records_failed || 0,
        action: 'detail',
      });
    } else if (health === 'degraded') {
      if ((p.stats?.records_dlq || 0) > 0) {
        issues.push({
          id: `${key}:dlq`,
          severity: 'blocking',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · DLQ backlog`,
          summary: `${p.stats.records_dlq.toLocaleString()} records in DLQ`,
          metricLabel: 'DLQ backlog',
          metricValue: p.stats.records_dlq,
          action: 'dlq',
          node: 'sink',
        });
      }
      if ((p.stats?.records_failed || 0) > 0 && (p.stats?.records_dlq || 0) === 0) {
        issues.push({
          id: `${key}:rec-failed`,
          severity: 'warning',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · 失败记录`,
          summary: `${p.stats.records_failed.toLocaleString()} failed records`,
          metricLabel: 'failed records',
          metricValue: p.stats.records_failed,
          action: 'issues',
        });
      }
      if (m && m.cdc_lag_ms > HIGH_CDC_LAG_MS) {
        issues.push({
          id: `${key}:lag`,
          severity: 'warning',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · CDC lag`,
          summary: `lag ${formatLag(m.cdc_lag_ms)} · checkpoint age ${m.checkpoint_age_seconds}s`,
          metricLabel: 'CDC lag',
          metricValue: formatLag(m.cdc_lag_ms),
          action: 'detail',
        });
      } else if (m && m.checkpoint_age_seconds > STALE_CHECKPOINT_SEC) {
        issues.push({
          id: `${key}:cp`,
          severity: 'warning',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · checkpoint 停滞`,
          summary: `checkpoint age ${m.checkpoint_age_seconds}s`,
          metricLabel: 'checkpoint age',
          metricValue: `${m.checkpoint_age_seconds}s`,
          action: 'detail',
        });
      } else if (p.stats?.last_error) {
        issues.push({
          id: `${key}:lasterr`,
          severity: 'warning',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · 最近错误`,
          summary: p.stats.last_error,
          action: 'detail',
        });
      }
    }
  }

  const rank: Record<IssueSeverity, number> = { blocking: 0, warning: 1, info: 2 };
  return issues.sort((a, b) => rank[a.severity] - rank[b.severity] || a.pipelineName.localeCompare(b.pipelineName));
}

export function formatLag(ms: number): string {
  if (!ms || ms < 0) return '—';
  if (ms < 1000) return `${ms}ms`;
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m ${s}s`;
}

export function healthTone(
  health: PipelineHealth,
): 'emerald' | 'amber' | 'rose' | 'blue' | 'slate' | 'violet' {
  switch (health) {
    case 'healthy':
      return 'emerald';
    case 'degraded':
    case 'starting':
      return 'amber';
    case 'failed':
      return 'rose';
    case 'completed':
      return 'blue';
    case 'scheduled':
      return 'violet';
    default:
      return 'slate';
  }
}

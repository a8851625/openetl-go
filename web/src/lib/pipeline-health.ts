import { pipelineDLQBacklog } from './dlq';
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
  /** i18n key suffix for the issue kind; display layers translate it. */
  titleKey?: string;
  summary: string;
  summaryKey?: string;
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

function healthThresholds(m?: MetricsPipeline) {
  return {
    checkpoint: m?.health_thresholds?.checkpoint_stale_seconds ?? STALE_CHECKPOINT_SEC,
    lag: m?.health_thresholds?.cdc_lag_ms ?? HIGH_CDC_LAG_MS,
  };
}

export function derivePipelineHealth(p: Pipeline, m?: MetricsPipeline): PipelineHealth {
  const s = (p.status || '').trim().toLowerCase();
  if (s === 'failed' || s === 'error' || s === 'restore_failed') return 'failed';
  if (s === 'paused') return 'paused';
  if (s === 'scheduled') return 'scheduled';
  if (s === 'completed') return 'completed';
  if (s === 'starting') return 'starting';
  if (s === 'stopped') return 'stopped';
  if (s === 'running') {
    if (
      m &&
      ((p.id && m.id ? p.id !== m.id : p.name !== m.name) ||
        (m.status && m.status.trim().toLowerCase() !== s))
    ) {
      m = undefined;
    }
    // Only CURRENT signals degrade a running pipeline. Cumulative counters
    // (records_failed / records_dlq) and the sticky last_error string used to be
    // included, which pinned a recovered pipeline to "degraded" forever and made
    // a restarted pipeline with real DLQ rows look healthy. Backlog comes from
    // the server's current row count.
    if (pipelineDLQBacklog(p) > 0 || m?.circuit_breaker_state === 1 || m?.health === 'degraded') {
      return 'degraded';
    }
    if (p.dlq_backlog_known === false || (m && m.dlq_file_count < 0)) return 'unknown';
    // The server applies configured thresholds. Older payloads use the
    // documented defaults after current metrics have been matched by identity.
    if (m?.health === 'healthy') return 'healthy';
    if (m?.health || !m) return 'unknown';
    const thresholds = healthThresholds(m);
    if (m.cdc_lag_ms > thresholds.lag) return 'degraded';
    if (m.checkpoint_age_seconds > thresholds.checkpoint) return 'degraded';
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
    const m = findMetric(p, metrics);
    const h = derivePipelineHealth(p, m);
    counts[h] = (counts[h] || 0) + 1;
  }
  return counts;
}

/**
 * healthBuckets returns the buckets a health summary must render so that every
 * pipeline is accounted for. Healthy/Degraded/Failed are always shown (they are
 * the states an operator must act on); every other bucket is shown once it has
 * a non-zero count. Because all non-zero buckets are included, the rendered
 * values always sum to `total` — a summary that silently drops Completed /
 * Scheduled / Starting / Unknown reads as "0 of N healthy" with no way to
 * explain where the remaining pipelines went.
 */
export function healthBuckets(
  counts: HealthCounts,
): Array<{ key: keyof Omit<HealthCounts, 'total'>; value: number }> {
  const always: Array<keyof Omit<HealthCounts, 'total'>> = ['healthy', 'degraded', 'failed'];
  const optional: Array<keyof Omit<HealthCounts, 'total'>> = [
    'paused',
    'stopped',
    'completed',
    'scheduled',
    'starting',
    'unknown',
  ];
  return [
    ...always.map((key) => ({ key, value: counts[key] })),
    ...optional.filter((key) => counts[key] > 0).map((key) => ({ key, value: counts[key] })),
  ];
}

export function findMetric(
  p: Pipeline,
  metrics: MetricsPipeline[] = [],
): MetricsPipeline | undefined {
  const exact = p.id ? metrics.find((x) => x.id === p.id) : undefined;
  if (exact) return exact;
  const legacy = metrics.filter((x) => (!p.id || !x.id) && x.name === p.name);
  return legacy.length === 1 ? legacy[0] : undefined;
}

/** Derive a short Source → Transform → Sink path from spec_summary (UI-B.1), falling back to tags. */
export function derivePipelinePath(p: Pipeline): { source: string; transform: string; sink: string } {
  const sum = p.spec_summary;
  if (sum && (sum.source || sum.sink || (sum.dag_sources && sum.dag_sources.length))) {
    const source = sum.source || (sum.dag_sources && sum.dag_sources.length ? sum.dag_sources.join(' + ') : 'Source');
    const sink = sum.sink || (sum.dag_sinks && sum.dag_sinks.length ? sum.dag_sinks.join(' + ') : 'Sink');
    const tfCount = sum.transforms?.length || sum.dag_transforms?.length || 0;
    const transform = tfCount
      ? (sum.transforms || sum.dag_transforms || []).join(' + ')
      : '—';
    return { source, transform, sink };
  }
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
  // UI-B.1: authoritative mode from the backend spec summary.
  const mode = p.spec_summary?.source_mode;
  if (mode) return mode;
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
    const status = (p.status || '').trim().toLowerCase();
    const thresholds = healthThresholds(m);
    const key = (p.id || p.name || '').trim();
    if (!key) continue;

    if (health === 'failed' || status === 'failed' || status === 'error') {
      issues.push({
        id: `${key}:failed`,
        severity: 'blocking',
        health: 'failed',
        pipelineKey: key,
        pipelineName: p.name,
        title: `${p.name} · ${p.stats?.last_error ? 'run failed' : 'pipeline failed'}`,
        titleKey: status === 'restore_failed'
          ? 'issue.restoreFailed'
          : p.stats?.last_error ? 'issue.runFailed' : 'issue.pipelineFailed',
        summary: p.stats?.last_error || `status=${p.status}`,
        summaryKey: p.stats?.last_error
          ? undefined
          : status === 'restore_failed' ? 'issue.restoreFailedSummary' : 'pipe.startupFailureHint',
        metricLabel: 'failed records',
        metricValue: p.stats?.records_failed || 0,
        action: 'detail',
      });
    } else if (health === 'degraded') {
      if (m?.circuit_breaker_state === 1) {
        issues.push({
          id: `${key}:breaker`,
          severity: 'blocking',
          health,
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · write circuit open`,
          titleKey: 'issue.circuitOpen',
          summary: p.stats?.last_error || '',
          summaryKey: p.stats?.last_error ? undefined : 'issue.circuitOpenSummary',
          action: 'detail',
        });
      }
      const backlog = pipelineDLQBacklog(p);
      if (backlog > 0) {
        issues.push({
          id: `${key}:dlq`,
          severity: 'blocking',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · DLQ backlog`,
          summary: `${backlog.toLocaleString()} records in DLQ`,
          metricLabel: 'DLQ backlog',
          metricValue: backlog,
          action: 'dlq',
          node: 'sink',
        });
      }
      if ((p.stats?.records_failed || 0) > 0 && backlog === 0) {
        issues.push({
          id: `${key}:rec-failed`,
          severity: 'warning',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · failed records`,
          titleKey: 'issue.failedRecords',
          summary: `${p.stats.records_failed.toLocaleString()} failed records`,
          metricLabel: 'failed records',
          metricValue: p.stats.records_failed,
          action: 'issues',
        });
      }
      if (m && m.cdc_lag_ms > thresholds.lag) {
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
      } else if (m && m.checkpoint_age_seconds > thresholds.checkpoint) {
        issues.push({
          id: `${key}:cp`,
          severity: 'warning',
          health: 'degraded',
          pipelineKey: key,
          pipelineName: p.name,
          title: `${p.name} · checkpoint stalled`,
          titleKey: 'issue.checkpointStalled',
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
          title: `${p.name} · last error`,
          titleKey: 'issue.lastError',
          summary: p.stats.last_error,
          action: 'detail',
        });
      }
    } else if (health === 'unknown' && status === 'running') {
      const dlqUnknown = p.dlq_backlog_known === false || Boolean(m && m.dlq_file_count < 0);
      issues.push({
        id: `${key}:health-unknown`,
        severity: 'warning',
        health,
        pipelineKey: key,
        pipelineName: p.name,
        title: `${p.name} · health unavailable`,
        titleKey: 'issue.healthUnknown',
        summary: '',
        summaryKey: dlqUnknown ? 'issue.dlqUnknownSummary' : 'issue.metricsUnknownSummary',
        action: dlqUnknown ? 'dlq' : 'detail',
      });
    }
  }

  // Fixed order: failed → degraded → DLQ → lag/checkpoint → connection/worker
  const kindRank = (issue: DerivedIssue): number => {
    if (issue.health === 'failed' || issue.id.endsWith(':failed')) return 0;
    if (issue.id.endsWith(':dlq') || issue.action === 'dlq') return 2;
    if (issue.id.endsWith(':lag') || issue.id.endsWith(':cp')) return 3;
    if (issue.action === 'connections') return 4;
    if (issue.health === 'degraded') return 1;
    return 5;
  };
  const rank: Record<IssueSeverity, number> = { blocking: 0, warning: 1, info: 2 };
  return issues.sort(
    (a, b) =>
      kindRank(a) - kindRank(b) ||
      rank[a.severity] - rank[b.severity] ||
      a.pipelineName.localeCompare(b.pipelineName),
  );
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

// issueTitle renders a localized issue title: pipeline name + translated kind.
// Falls back to the stored English title when no key is present.
export function issueTitle(issue: DerivedIssue, t: (key: string) => string): string {
  if (!issue.titleKey) return issue.title;
  const translated = t(issue.titleKey);
  if (!translated || translated === issue.titleKey) return issue.title;
  const name = issue.pipelineName || '';
  return name ? `${name} · ${translated}` : translated;
}

export function issueSummary(issue: DerivedIssue, t: (key: string) => string): string {
  return issue.summaryKey ? t(issue.summaryKey) : issue.summary;
}

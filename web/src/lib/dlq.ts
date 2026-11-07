import type { DLQItem, Pipeline } from './types';

/**
 * pipelineDLQBacklog returns the CURRENT dead-letter backlog for a pipeline.
 *
 * `stats.records_dlq` is a cumulative per-process counter: it resets to 0 when
 * the process restarts and never decreases after a replay or delete, so using it
 * as a backlog made recovered pipelines look permanently degraded and restarted
 * pipelines with real DLQ rows look healthy. The server reports the real row
 * count as `dlq_backlog`.
 */
export function pipelineDLQBacklog(p?: Pipeline | null): number {
  if (!p) return 0;
  const value = p.dlq_backlog;
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) return value;
  return 0;
}

/** True when the server could not read the backlog count (unknown, not zero). */
export function pipelineDLQBacklogUnknown(p?: Pipeline | null): boolean {
  return Boolean(p && p.dlq_backlog_known === false);
}

/** replayBlockedReason explains why a DLQ row must not be replayed yet. */
export function replayBlockedReason(item: DLQItem): string | undefined {
  const state = item.identity_context?.replay_state;
  if (state === 'quarantined' || state === 'repair_required') {
    return item.identity_context?.replay_error || `replay_state=${state}`;
  }
  return undefined;
}

import { useEffect, useState } from 'react';
import { formatDuration, parseStartedAt } from './format';
import type { TFunc } from './types';

/** 根据 started_at 实时计算运行时长（每秒刷新） */
export function useLiveUptime(startedAt: string | undefined): string {
  const [, force] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => force((n) => n + 1), 1000);
    return () => clearInterval(timer);
  }, []);
  const startMs = parseStartedAt(startedAt);
  if (startMs === null) return 'N/A';
  return formatDuration((Date.now() - startMs) / 1000);
}

export function PipelineUptime({
  label,
  startedAt,
  fallback,
}: {
  label: string;
  startedAt?: string;
  fallback: string;
}) {
  const uptime = useLiveUptime(startedAt);
  return (
    <div className="text-xs text-muted-foreground">
      {label} {startedAt ? uptime : fallback}
    </div>
  );
}

export function PipelineRowMeta({
  t,
  startedAt,
  uptimeFallback,
  written,
  cdcLagMs,
}: {
  t: TFunc;
  startedAt?: string;
  uptimeFallback: string;
  written: number;
  cdcLagMs?: number;
}) {
  const uptime = useLiveUptime(startedAt);
  return (
    <div className="mt-0.5 text-xs text-muted-foreground">
      {t('dash.uptime')} {startedAt ? uptime : uptimeFallback} · {written} {t('pipe.written')}
      {cdcLagMs && cdcLagMs > 0 ? ` · lag ${cdcLagMs}ms` : ''}
    </div>
  );
}

export function LiveUptimeInline({
  startedAt,
  fallback,
}: {
  startedAt?: string;
  fallback: string;
}) {
  const uptime = useLiveUptime(startedAt);
  return <>{startedAt ? uptime : fallback}</>;
}

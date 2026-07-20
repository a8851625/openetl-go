import type { ComponentType } from 'react';
import { AlertTriangle, CheckCircle2, Clock3, PauseCircle, PlayCircle, XCircle } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import type { PipelineHealth } from '@/lib/pipeline-health';
import type { TFunc } from '@/lib/types';

const STYLES: Record<PipelineHealth, string> = {
  healthy:
    'border-transparent bg-emerald-50 text-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-300',
  degraded:
    'border-transparent bg-amber-50 text-amber-800 dark:bg-amber-950/50 dark:text-amber-300',
  failed: 'border-transparent bg-rose-50 text-rose-800 dark:bg-rose-950/50 dark:text-rose-300',
  paused: 'border-transparent bg-amber-50 text-amber-800 dark:bg-amber-950/50 dark:text-amber-300',
  scheduled:
    'border-transparent bg-violet-50 text-violet-800 dark:bg-violet-950/50 dark:text-violet-300',
  completed: 'border-transparent bg-blue-50 text-blue-800 dark:bg-blue-950/50 dark:text-blue-300',
  stopped: 'border-transparent bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300',
  starting:
    'border-transparent bg-amber-50 text-amber-800 dark:bg-amber-950/50 dark:text-amber-300',
  unknown: 'border-transparent bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300',
};

const ICONS: Record<PipelineHealth, ComponentType<{ className?: string }>> = {
  healthy: CheckCircle2,
  degraded: AlertTriangle,
  failed: XCircle,
  paused: PauseCircle,
  scheduled: Clock3,
  completed: CheckCircle2,
  stopped: PauseCircle,
  starting: PlayCircle,
  unknown: AlertTriangle,
};

export function healthLabel(t: TFunc | undefined, health: PipelineHealth): string {
  const key = `health.${health}`;
  if (t) {
    const v = t(key);
    if (v !== key) return v;
  }
  const fallback: Record<PipelineHealth, string> = {
    healthy: 'Healthy',
    degraded: 'Degraded',
    failed: 'Failed',
    paused: 'Paused',
    scheduled: 'Scheduled',
    completed: 'Completed',
    stopped: 'Stopped',
    starting: 'Starting',
    unknown: 'Unknown',
  };
  return fallback[health];
}

export function PipelineHealthBadge({
  health,
  t,
  className,
  showIcon = true,
}: {
  health: PipelineHealth;
  t?: TFunc;
  className?: string;
  showIcon?: boolean;
}) {
  const Icon = ICONS[health] || AlertTriangle;
  return (
    <Badge
      variant="outline"
      className={cn('gap-1 text-[10px] font-medium px-1.5', STYLES[health], className)}
      title={healthLabel(t, health)}
    >
      {showIcon && <Icon className="h-3 w-3" aria-hidden />}
      <span>{healthLabel(t, health)}</span>
    </Badge>
  );
}

export function HealthDot({ health, className }: { health: PipelineHealth; className?: string }) {
  const color: Record<PipelineHealth, string> = {
    healthy: 'bg-emerald-500',
    degraded: 'bg-amber-500',
    failed: 'bg-rose-500',
    paused: 'bg-amber-500',
    scheduled: 'bg-violet-500',
    completed: 'bg-blue-500',
    stopped: 'bg-slate-400',
    starting: 'bg-amber-400 animate-pulse',
    unknown: 'bg-slate-400',
  };
  return (
    <span
      className={cn('inline-block h-2 w-2 rounded-full', color[health], className)}
      aria-label={health}
    />
  );
}

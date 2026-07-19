import type { HTMLAttributes } from 'react';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import type { TFunc } from '@/lib/types';

const STATUS_VARIANT: Record<string, string> = {
  running: 'border-transparent bg-emerald-50 text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300',
  completed: 'border-transparent bg-blue-50 text-blue-700 dark:bg-blue-950/50 dark:text-blue-300',
  failed: 'border-transparent bg-rose-50 text-rose-700 dark:bg-rose-950/50 dark:text-rose-300',
  error: 'border-transparent bg-rose-50 text-rose-700 dark:bg-rose-950/50 dark:text-rose-300',
  stopped: 'border-transparent bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300',
  starting: 'border-transparent bg-amber-50 text-amber-700 dark:bg-amber-950/50 dark:text-amber-300',
  paused: 'border-transparent bg-amber-50 text-amber-700 dark:bg-amber-950/50 dark:text-amber-300',
};

const STATUS_LABEL: Record<string, string> = {
  running: 'Running',
  completed: 'Completed',
  failed: 'Failed',
  stopped: 'Stopped',
  starting: 'Starting...',
  paused: 'Paused',
  error: 'Error',
};

export function statusLabel(t: TFunc, status: string): string {
  const key = `status.${status}`;
  const translated = t(key);
  return translated !== key ? translated : STATUS_LABEL[status] || status;
}

export function StatusBadge({
  status,
  t,
  className,
}: {
  status: string;
  t?: TFunc;
  className?: string;
}) {
  const label = t ? statusLabel(t, status) : STATUS_LABEL[status] || status;
  return (
    <Badge
      variant="outline"
      className={cn(
        'text-[10px] px-1.5 font-medium',
        STATUS_VARIANT[status] || STATUS_VARIANT.stopped,
        className,
      )}
    >
      {label}
    </Badge>
  );
}

export function StatusDot({ status, className }: { status: string; className?: string }) {
  return <span className={cn(`status-dot status-${status}`, className)} aria-hidden />;
}

/** 通用语义色 Badge（替代旧 badge-* class） */
export function ToneBadge({
  tone = 'slate',
  className,
  children,
  ...props
}: HTMLAttributes<HTMLDivElement> & {
  tone?:
    | 'emerald'
    | 'blue'
    | 'amber'
    | 'rose'
    | 'slate'
    | 'violet'
    | 'cyan'
    | 'indigo'
    | 'purple';
}) {
  const tones: Record<string, string> = {
    emerald: 'border-transparent bg-emerald-50 text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300',
    blue: 'border-transparent bg-blue-50 text-blue-700 dark:bg-blue-950/50 dark:text-blue-300',
    amber: 'border-transparent bg-amber-50 text-amber-700 dark:bg-amber-950/50 dark:text-amber-300',
    rose: 'border-transparent bg-rose-50 text-rose-700 dark:bg-rose-950/50 dark:text-rose-300',
    slate: 'border-transparent bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300',
    violet: 'border-transparent bg-violet-50 text-violet-700 dark:bg-violet-950/50 dark:text-violet-300',
    cyan: 'border-transparent bg-cyan-50 text-cyan-700 dark:bg-cyan-950/50 dark:text-cyan-300',
    indigo: 'border-transparent bg-indigo-50 text-indigo-700 dark:bg-indigo-950/50 dark:text-indigo-300',
    purple: 'border-transparent bg-purple-50 text-purple-700 dark:bg-purple-950/50 dark:text-purple-300',
  };
  return (
    <Badge variant="outline" className={cn(tones[tone], className)} {...props}>
      {children}
    </Badge>
  );
}

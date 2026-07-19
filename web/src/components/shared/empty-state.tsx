import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';

export function EmptyState({
  text,
  hint,
  className,
  children,
}: {
  text: string;
  hint?: string;
  className?: string;
  children?: ReactNode;
}) {
  return (
    <div
      className={cn(
        'rounded-lg border border-dashed border-border bg-muted/30 px-4 py-10 text-center',
        className,
      )}
    >
      <div className="text-sm text-muted-foreground">{text}</div>
      {hint ? <div className="mt-1 text-xs text-muted-foreground/80">{hint}</div> : null}
      {children ? <div className="mt-3">{children}</div> : null}
    </div>
  );
}

export function ErrorBox({ message, className }: { message: string; className?: string }) {
  return (
    <div
      className={cn(
        'rounded-lg border border-rose-200 bg-rose-50 px-3 py-2.5 text-sm text-rose-700 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-300',
        className,
      )}
    >
      {message}
    </div>
  );
}

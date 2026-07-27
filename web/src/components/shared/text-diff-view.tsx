import { useMemo } from 'react';
import { cn } from '@/lib/utils';
import { diffLines, toSideBySide, type DiffOp } from '@/lib/text-diff';

type Props = {
  historical: string;
  current: string;
  leftTitle: string;
  rightTitle: string;
  className?: string;
  /** Max height for the scrollable body (Tailwind class). */
  maxHeightClass?: string;
  /** data-testid for the outer container. */
  testId?: string;
};

function cellBg(op: DiffOp | undefined, side: 'left' | 'right'): string {
  if (!op || op === 'equal') return '';
  if (side === 'left' && op === 'remove') {
    return 'bg-rose-100/90 dark:bg-rose-950/50';
  }
  if (side === 'right' && op === 'add') {
    return 'bg-emerald-100/90 dark:bg-emerald-950/50';
  }
  // Opposite side of a change stays blank (no full-block tint).
  return '';
}

function gutterText(op: DiffOp | undefined): string {
  if (op === 'remove') return '−';
  if (op === 'add') return '+';
  return ' ';
}

/**
 * Side-by-side line diff. Only changed lines are tinted; equal lines stay neutral.
 */
export function TextDiffView({
  historical,
  current,
  leftTitle,
  rightTitle,
  className,
  maxHeightClass = 'max-h-72',
  testId,
}: Props) {
  const rows = useMemo(
    () => toSideBySide(diffLines(historical || '', current || '')),
    [historical, current],
  );

  const changeCount = useMemo(
    () => rows.filter((r) => r.left?.op === 'remove' || r.right?.op === 'add').length,
    [rows],
  );

  if (!historical && !current) {
    return (
      <div className={cn('rounded-lg border border-border p-3 text-xs text-muted-foreground', className)} data-testid={testId}>
        (empty)
      </div>
    );
  }

  if (historical === current) {
    return (
      <div className={cn('rounded-lg border border-border p-3 text-xs text-muted-foreground', className)} data-testid={testId}>
        No differences
      </div>
    );
  }

  return (
    <div
      className={cn('overflow-hidden rounded-lg border border-border bg-card', className)}
      data-testid={testId}
    >
      <div className="grid grid-cols-2 border-b border-border bg-muted/40 text-[11px] font-semibold text-muted-foreground">
        <div className="border-r border-border px-2 py-1.5">{leftTitle}</div>
        <div className="px-2 py-1.5">{rightTitle}</div>
      </div>
      <div className={cn('overflow-auto text-[11px] leading-5', maxHeightClass)}>
        <div className="grid min-w-[640px] grid-cols-2">
          {/* Left pane */}
          <div className="border-r border-border font-mono">
            {rows.map((row, idx) => {
              const cell = row.left;
              const op = cell?.op;
              return (
                <div
                  key={`L-${idx}`}
                  className={cn(
                    'flex min-h-5 whitespace-pre-wrap break-all border-b border-border/40',
                    cellBg(op, 'left'),
                    !cell && 'bg-muted/20',
                  )}
                >
                  <span className="w-8 shrink-0 select-none px-1 text-right text-muted-foreground/70">
                    {cell?.no ?? ''}
                  </span>
                  <span
                    className={cn(
                      'w-4 shrink-0 select-none text-center font-semibold',
                      op === 'remove' && 'text-rose-600 dark:text-rose-400',
                    )}
                  >
                    {gutterText(op)}
                  </span>
                  <span className="flex-1 px-1">{cell?.text ?? ''}</span>
                </div>
              );
            })}
          </div>
          {/* Right pane */}
          <div className="font-mono">
            {rows.map((row, idx) => {
              const cell = row.right;
              const op = cell?.op;
              return (
                <div
                  key={`R-${idx}`}
                  className={cn(
                    'flex min-h-5 whitespace-pre-wrap break-all border-b border-border/40',
                    cellBg(op, 'right'),
                    !cell && 'bg-muted/20',
                  )}
                >
                  <span className="w-8 shrink-0 select-none px-1 text-right text-muted-foreground/70">
                    {cell?.no ?? ''}
                  </span>
                  <span
                    className={cn(
                      'w-4 shrink-0 select-none text-center font-semibold',
                      op === 'add' && 'text-emerald-600 dark:text-emerald-400',
                    )}
                  >
                    {gutterText(op)}
                  </span>
                  <span className="flex-1 px-1">{cell?.text ?? ''}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
      <div className="border-t border-border bg-muted/30 px-2 py-1 text-[10px] text-muted-foreground">
        {changeCount} changed line{changeCount === 1 ? '' : 's'}
      </div>
    </div>
  );
}

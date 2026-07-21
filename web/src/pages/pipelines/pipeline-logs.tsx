import { useEffect, useRef, useState } from 'react';
import { Button } from '@/components/ui/button';
import { api } from '@/lib/api';
import type { PipelineLogEntry, TFunc } from '@/lib/types';
import { cn } from '@/lib/utils';
import { Pause, Play, Trash2, ScrollText, CircleDot } from 'lucide-react';

/** Live pipeline log stream — design-token surface (not terminal chrome). */
export function PipelineLogDrawer({
  t,
  name,
  className,
  heightClass = 'h-full min-h-[280px]',
}: {
  t: TFunc;
  name: string;
  className?: string;
  /** Override height when embedded in a page tab */
  heightClass?: string;
}) {
  const [entries, setEntries] = useState<PipelineLogEntry[]>([]);
  const lastSeq = useRef(0);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [paused, setPaused] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;
    const fetchLogs = async () => {
      try {
        const data = await api<{ entries: PipelineLogEntry[]; last_seq: number }>(
          `/api/v2/pipelines/${name}/log?since=${lastSeq.current}`,
        );
        if (!active) return;
        setError('');
        if (data.entries?.length) {
          setEntries((prev) => [...prev.slice(-2000), ...data.entries]);
        }
        lastSeq.current = data.last_seq;
      } catch (e) {
        if (active) setError(String(e));
      }
    };
    setEntries([]);
    lastSeq.current = 0;
    setError('');
    fetchLogs();
    const timer = setInterval(() => {
      if (!paused) fetchLogs();
    }, 1000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [name, paused]);

  useEffect(() => {
    if (!paused && autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries, paused, autoScroll]);

  const levelClass = (lvl: string) => {
    switch ((lvl || '').toUpperCase()) {
      case 'ERROR':
        return 'bg-rose-50 text-rose-700 dark:bg-rose-950/40 dark:text-rose-300';
      case 'WARN':
      case 'WARNING':
        return 'bg-amber-50 text-amber-800 dark:bg-amber-950/40 dark:text-amber-300';
      case 'DEBUG':
        return 'bg-muted text-muted-foreground';
      default:
        return 'bg-primary/10 text-primary';
    }
  };

  const levelText = (lvl: string) => {
    switch ((lvl || '').toUpperCase()) {
      case 'ERROR':
        return 'text-rose-700 dark:text-rose-300';
      case 'WARN':
      case 'WARNING':
        return 'text-amber-800 dark:text-amber-300';
      case 'DEBUG':
        return 'text-muted-foreground';
      default:
        return 'text-foreground';
    }
  };

  return (
    <div
      className={cn(
        'flex flex-col overflow-hidden rounded-xl border border-border bg-card',
        heightClass,
        className,
      )}
      data-testid="pipeline-log-panel"
    >
      <div className="flex flex-wrap items-center gap-2 border-b border-border bg-muted/30 px-3 py-2">
        <div className="flex min-w-0 items-center gap-2">
          <ScrollText className="h-4 w-4 shrink-0 text-muted-foreground" />
          <span className="truncate text-sm font-medium">{t('pipe.logs')}</span>
          <span
            className={cn(
              'inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium',
              paused
                ? 'bg-muted text-muted-foreground'
                : 'bg-primary/10 text-primary',
            )}
          >
            <CircleDot className={cn('h-3 w-3', !paused && 'animate-pulse')} />
            {paused ? t('log.paused') : t('log.live')}
          </span>
        </div>
        <div className="ml-auto flex flex-wrap items-center gap-1.5">
          <Button
            size="sm"
            variant="outline"
            className="h-7 gap-1 px-2 text-xs"
            onClick={() => setPaused((p) => !p)}
            data-testid="log-toggle-pause"
          >
            {paused ? (
              <>
                <Play className="h-3.5 w-3.5" /> {t('log.resume')}
              </>
            ) : (
              <>
                <Pause className="h-3.5 w-3.5" /> {t('log.pause')}
              </>
            )}
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="h-7 gap-1 px-2 text-xs"
            onClick={() => setAutoScroll((v) => !v)}
            data-testid="log-toggle-autoscroll"
          >
            {autoScroll ? t('log.autoScrollOn') : t('log.autoScrollOff')}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            className="h-7 gap-1 px-2 text-xs text-muted-foreground"
            onClick={() => {
              setEntries([]);
              lastSeq.current = 0;
            }}
            data-testid="log-clear"
          >
            <Trash2 className="h-3.5 w-3.5" /> {t('log.clear')}
          </Button>
          <span className="tabular text-xs text-muted-foreground">
            {entries.length} {t('log.lines')}
          </span>
        </div>
      </div>

      {error ? (
        <div className="border-b border-border bg-rose-50 px-3 py-1.5 text-xs text-rose-700 dark:bg-rose-950/30 dark:text-rose-300">
          {error}
        </div>
      ) : null}

      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto bg-background/60 p-2 font-mono text-xs leading-relaxed"
        onScroll={(e) => {
          const el = e.currentTarget;
          const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
          if (!nearBottom && autoScroll) setAutoScroll(false);
        }}
      >
        {entries.length === 0 ? (
          <div className="flex h-full min-h-[160px] flex-col items-center justify-center gap-2 px-4 text-center">
            <ScrollText className="h-8 w-8 text-muted-foreground/50" />
            <p className="text-sm text-muted-foreground">{t('pipe.noLogs')}</p>
            <p className="max-w-sm text-[11px] text-muted-foreground/80">{t('log.emptyHint')}</p>
          </div>
        ) : (
          <ul className="space-y-0.5">
            {entries.map((e, i) => (
              <li
                key={`${e.timestamp}-${i}`}
                className="flex gap-2 rounded-md px-2 py-1 hover:bg-muted/50"
              >
                <span className="w-[5.5rem] shrink-0 tabular text-muted-foreground">
                  {(e.timestamp || '').slice(11, 23) || '—'}
                </span>
                <span
                  className={cn(
                    'h-5 w-12 shrink-0 rounded px-1 text-center text-[10px] font-semibold uppercase leading-5',
                    levelClass(e.level),
                  )}
                >
                  {(e.level || 'INFO').slice(0, 5)}
                </span>
                <span className={cn('min-w-0 flex-1 break-words whitespace-pre-wrap', levelText(e.level))}>
                  {e.message}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

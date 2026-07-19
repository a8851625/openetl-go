import { useEffect, useRef, useState } from 'react';
import { Button } from '@/components/ui/button';
import { api } from '@/lib/api';
import type { PipelineLogEntry, TFunc } from '@/lib/types';

/** 全高度实时日志查看器（用于 Modal / Drawer） */
export function PipelineLogDrawer({ t, name }: { t: TFunc; name: string }) {
  const [entries, setEntries] = useState<PipelineLogEntry[]>([]);
  const lastSeq = useRef(0);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [paused, setPaused] = useState(false);

  useEffect(() => {
    let active = true;
    const fetchLogs = async () => {
      try {
        const data = await api<{ entries: PipelineLogEntry[]; last_seq: number }>(
          `/api/v2/pipelines/${name}/log?since=${lastSeq.current}`,
        );
        if (!active) return;
        if (data.entries?.length) setEntries((prev) => [...prev.slice(-2000), ...data.entries]);
        lastSeq.current = data.last_seq;
      } catch {
        /* retry */
      }
    };
    setEntries([]);
    lastSeq.current = 0;
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
    if (!paused && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries, paused]);

  const levelColor = (lvl: string) => {
    switch (lvl) {
      case 'ERROR':
        return 'text-rose-400';
      case 'WARN':
        return 'text-amber-300';
      case 'DEBUG':
        return 'text-slate-500';
      default:
        return 'text-emerald-400';
    }
  };

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-2 border-b border-border bg-slate-900 px-4 py-1.5">
        <Button
          size="sm"
          variant={paused ? 'default' : 'secondary'}
          className="h-6 px-2 text-xs"
          onClick={() => setPaused(!paused)}
        >
          {paused ? '▶ ' + t('log.resume') : '⏸ ' + t('log.pause')}
        </Button>
        <Button
          size="sm"
          variant="secondary"
          className="h-6 px-2 text-xs"
          onClick={() => {
            setEntries([]);
            lastSeq.current = 0;
          }}
        >
          {'✕ ' + t('log.clear')}
        </Button>
        <span className="ml-auto text-xs text-slate-500">
          {entries.length} {t('log.lines')}
        </span>
      </div>
      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto bg-slate-900 p-3 font-mono text-xs leading-relaxed"
      >
        {entries.length === 0 ? (
          <div className="py-10 text-center text-slate-500">{t('pipe.noLogs')}</div>
        ) : (
          entries.map((e, i) => (
            <div key={i} className="flex gap-2 hover:bg-white/5">
              <span className="shrink-0 text-slate-600">{e.timestamp.slice(11, 23)}</span>
              <span className={`w-12 shrink-0 ${levelColor(e.level)}`}>{e.level.padEnd(5)}</span>
              <span className={levelColor(e.level)}>{e.message}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

import { useApi } from '@/lib/api';
import { StatusDot, ToneBadge } from '@/components/shared/status-badge';
import type { ShardInfo, TFunc } from '@/lib/types';

export function ShardsInline({
  t,
  name,
  refreshKey,
}: {
  t: TFunc;
  name: string;
  refreshKey: number;
}) {
  const shards = useApi<{ shards: ShardInfo[] }>(
    `/api/v2/pipelines/${name}/shards`,
    refreshKey,
  );
  return (
    <div className="space-y-2">
      {(shards.data?.shards || []).map((s) => (
        <div
          key={s.index}
          className="flex items-center justify-between rounded-lg border border-border bg-muted/40 px-3 py-2"
        >
          <div className="flex items-center gap-2">
            <span className="text-xs font-semibold">#{s.index}</span>
            <StatusDot status={s.status} />
            <span className="text-xs text-muted-foreground">{s.status}</span>
          </div>
          <div className="flex gap-1.5">
            <ToneBadge tone="blue" className="text-[10px]">
              {s.stats.records_written} w
            </ToneBadge>
            <ToneBadge tone="slate" className="text-[10px]">
              {s.stats.records_read} r
            </ToneBadge>
          </div>
        </div>
      ))}
    </div>
  );
}

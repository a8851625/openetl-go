import { cn } from '@/lib/utils';
import { derivePipelinePath } from '@/lib/pipeline-health';
import type { Pipeline } from '@/lib/types';

export function PipelinePath({
  pipeline,
  source,
  transform,
  sink,
  className,
}: {
  pipeline?: Pipeline;
  source?: string;
  transform?: string;
  sink?: string;
  className?: string;
}) {
  const derived = pipeline ? derivePipelinePath(pipeline) : null;
  const s = source || derived?.source || 'Source';
  const t = transform || derived?.transform || '—';
  const k = sink || derived?.sink || 'Sink';
  return (
    <div className={cn('flex flex-wrap items-center gap-1.5 text-xs', className)}>
      <span className="rounded-md bg-muted px-2 py-1 text-foreground/90">{s}</span>
      <span className="text-muted-foreground">→</span>
      <span className="rounded-md bg-muted px-2 py-1 text-muted-foreground">{t}</span>
      <span className="text-muted-foreground">→</span>
      <span className="rounded-md bg-muted px-2 py-1 text-foreground/90">{k}</span>
    </div>
  );
}

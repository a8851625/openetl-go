import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { EmptyState } from '@/components/shared/empty-state';
import { ToneBadge } from '@/components/shared/status-badge';
import type { ApiState, PluginResponse, TFunc } from '@/lib/types';
import type { Lang } from '@/i18n';
import { useMemo, useState } from 'react';

type Props = {
  t: TFunc;
  lang: Lang;
  plugins: ApiState<PluginResponse>;
  schema: ApiState<any>;
};

type CatalogItem = {
  name: string;
  kind: 'source' | 'sink' | 'transform';
  maturity: string;
  capabilities: string[];
};

export function ConnectorsPage({ t, plugins }: Props) {
  const [q, setQ] = useState('');
  const items = useMemo(() => {
    const meta = plugins.data?.metadata || {};
    const list: CatalogItem[] = [];
    const push = (names: string[] | undefined, kind: CatalogItem['kind']) => {
      for (const name of names || []) {
        const m = meta[name] || {};
        list.push({
          name,
          kind,
          maturity: typeof m.maturity === 'string' ? m.maturity : 'ga',
          capabilities: Array.isArray(m.capabilities)
            ? m.capabilities.filter((c): c is string => typeof c === 'string')
            : [],
        });
      }
    };
    push(plugins.data?.sources, 'source');
    push(plugins.data?.sinks, 'sink');
    push(plugins.data?.transforms, 'transform');
    return list;
  }, [plugins.data]);

  const filtered = items.filter((i) => {
    if (!q) return true;
    const s = q.toLowerCase();
    return (
      i.name.toLowerCase().includes(s) ||
      i.kind.includes(s) ||
      i.maturity.toLowerCase().includes(s)
    );
  });

  const tone = (m: string) => {
    const x = m.toLowerCase();
    if (x.includes('exp')) return 'slate' as const;
    if (x.includes('beta')) return 'amber' as const;
    return 'emerald' as const;
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-primary">
            {t('nav.groupResources')} · /connectors
          </div>
          <h2 className="mt-1 text-2xl font-semibold tracking-tight">{t('nav.connectors')}</h2>
          <p className="mt-1 text-sm text-muted-foreground">{t('connectors.subtitle')}</p>
        </div>
        <Input
          className="max-w-xs"
          placeholder={t('connectors.search')}
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
      </div>

      {!filtered.length ? (
        <EmptyState text={plugins.loading ? '…' : t('connectors.empty')} />
      ) : (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          {filtered.map((item) => (
            <Card key={`${item.kind}:${item.name}`}>
              <CardContent className="space-y-3 p-4">
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <div className="font-semibold">{item.name}</div>
                    <div className="mt-0.5 text-xs capitalize text-muted-foreground">
                      {item.kind}
                    </div>
                  </div>
                  <ToneBadge tone={tone(item.maturity)} className="text-[10px] uppercase">
                    {item.maturity}
                  </ToneBadge>
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {(item.capabilities.length ? item.capabilities : [item.kind]).slice(0, 6).map((c) => (
                    <span
                      key={c}
                      className="rounded-md bg-muted px-2 py-1 text-[11px] text-muted-foreground"
                    >
                      {c}
                    </span>
                  ))}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}

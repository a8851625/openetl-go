import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { EmptyState } from '@/components/shared/empty-state';
import { ToneBadge } from '@/components/shared/status-badge';
import type { ApiState, PluginInfo, PluginResponse, TFunc } from '@/lib/types';
import type { Lang } from '@/i18n';
import { useMemo, useState } from 'react';

type Props = {
  t: TFunc;
  lang: Lang;
  plugins: ApiState<PluginResponse>;
  schema: ApiState<any>;
  /** Prefer matrix when opened from legacy Built-in deep link / e2e. */
  initialView?: 'cards' | 'matrix';
};

type CatalogItem = {
  name: string;
  kind: 'source' | 'sink' | 'transform';
  maturity: string;
  capabilities: string[];
};

const kindTone: Record<string, 'emerald' | 'slate' | 'violet'> = {
  source: 'emerald',
  sink: 'emerald',
  transform: 'violet',
};

function maturityTone(m?: string): 'emerald' | 'blue' | 'amber' | 'slate' {
  if (m === 'production' || m === 'ga') return 'emerald';
  if (m === 'beta') return 'blue';
  if (m === 'experimental' || (m || '').includes('exp')) return 'amber';
  return 'slate';
}

export function ConnectorsPage({ t, plugins, initialView = 'cards' }: Props) {
  const [q, setQ] = useState('');
  const [view, setView] = useState<'cards' | 'matrix'>(initialView);

  const nestedMeta = plugins.data?.metadata as
    | {
        sources?: Record<string, PluginInfo>;
        sinks?: Record<string, PluginInfo>;
        transforms?: Record<string, PluginInfo>;
      }
    | undefined;

  const items = useMemo(() => {
    const list: CatalogItem[] = [];
    const push = (
      names: string[] | undefined,
      kind: CatalogItem['kind'],
      bucket?: Record<string, PluginInfo>,
    ) => {
      for (const name of names || []) {
        const info = bucket?.[name] || {};
        list.push({
          name,
          kind,
          maturity: typeof info.maturity === 'string' ? info.maturity : 'ga',
          capabilities: Array.isArray(info.capabilities)
            ? info.capabilities.filter((c: unknown): c is string => typeof c === 'string')
            : [],
        });
      }
    };
    push(plugins.data?.sources, 'source', nestedMeta?.sources);
    push(plugins.data?.sinks, 'sink', nestedMeta?.sinks);
    push(plugins.data?.transforms, 'transform', nestedMeta?.transforms);
    return list;
  }, [plugins.data, nestedMeta]);

  const matrixRows = useMemo(() => {
    if (nestedMeta?.sources || nestedMeta?.sinks || nestedMeta?.transforms) {
      return [
        ...Object.entries(nestedMeta.sources || {}).map(([n, i]) => ({
          kind: 'source',
          name: n,
          info: i,
        })),
        ...Object.entries(nestedMeta.sinks || {}).map(([n, i]) => ({
          kind: 'sink',
          name: n,
          info: i,
        })),
        ...Object.entries(nestedMeta.transforms || {}).map(([n, i]) => ({
          kind: 'transform',
          name: n,
          info: i,
        })),
      ];
    }
    return items.map((i) => ({
      kind: i.kind,
      name: i.name,
      info: {
        maturity: i.maturity,
        capabilities: i.capabilities,
        required: [] as string[],
      } as PluginInfo,
    }));
  }, [nestedMeta, items]);

  const filtered = items.filter((i) => {
    if (!q) return true;
    const s = q.toLowerCase();
    return (
      i.name.toLowerCase().includes(s) ||
      i.kind.includes(s) ||
      i.maturity.toLowerCase().includes(s) ||
      i.capabilities.some((c) => c.toLowerCase().includes(s))
    );
  });

  const filteredMatrix = matrixRows.filter((r) => {
    if (!q) return true;
    const s = q.toLowerCase();
    return (
      r.name.toLowerCase().includes(s) ||
      r.kind.includes(s) ||
      (r.info.maturity || '').toLowerCase().includes(s) ||
      (r.info.capabilities || []).some((c) => c.toLowerCase().includes(s)) ||
      (r.info.required || []).some((f) => f.toLowerCase().includes(s))
    );
  });

  const tone = (m: string) => {
    const x = m.toLowerCase();
    if (x.includes('exp')) return 'slate' as const;
    if (x.includes('beta')) return 'amber' as const;
    return 'emerald' as const;
  };

  return (
    <div className="space-y-6" data-testid="connectors-catalog">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <div className="text-xs font-bold uppercase tracking-[0.08em] text-primary">
            {t('nav.groupResources')} · /connectors
          </div>
          <h2 className="mt-1 text-2xl font-semibold tracking-tight">{t('nav.connectors')}</h2>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{t('connectors.subtitle')}</p>
          <p className="mt-1 max-w-2xl text-xs text-muted-foreground">{t('connectors.vsConnections')}</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex rounded-lg border border-border p-0.5">
            <button
              type="button"
              className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
                view === 'cards' ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-muted'
              }`}
              onClick={() => setView('cards')}
            >
              {t('connectors.viewCards')}
            </button>
            <button
              type="button"
              className={`rounded-md px-3 py-1.5 text-xs font-medium transition ${
                view === 'matrix' ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:bg-muted'
              }`}
              onClick={() => setView('matrix')}
              data-testid="connectors-view-matrix"
            >
              {t('connectors.viewMatrix')}
            </button>
          </div>
          <Input
            className="max-w-xs"
            placeholder={t('connectors.search')}
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
        </div>
      </div>

      {view === 'cards' ? (
        !filtered.length ? (
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
                    {(item.capabilities.length ? item.capabilities : [item.kind])
                      .slice(0, 6)
                      .map((c) => (
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
        )
      ) : (
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
            <CardTitle className="text-sm">{t('plugin.matrix')}</CardTitle>
            <div className="flex gap-3 text-xs text-muted-foreground">
              <span>
                {plugins.data?.sources.length || 0} {t('plugin.sources')}
              </span>
              <span>
                {plugins.data?.sinks.length || 0} {t('plugin.sinks')}
              </span>
              <span>
                {plugins.data?.transforms.length || 0} {t('plugin.transforms')}
              </span>
            </div>
          </CardHeader>
          <CardContent className="p-0">
            {!filteredMatrix.length ? (
              <div className="p-6">
                <EmptyState text={plugins.loading ? t('plugin.loading') : t('connectors.empty')} />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>{t('plugin.kind')}</TableHead>
                    <TableHead>{t('plugin.plugin')}</TableHead>
                    <TableHead>{t('plugin.maturity')}</TableHead>
                    <TableHead>{t('plugin.requiredFields')}</TableHead>
                    <TableHead>{t('plugin.capabilities')}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredMatrix.map((r) => (
                    <TableRow key={`${r.kind}-${r.name}`}>
                      <TableCell>
                        <ToneBadge tone={kindTone[r.kind] || 'slate'}>{r.kind}</ToneBadge>
                      </TableCell>
                      <TableCell className="font-medium">{r.name}</TableCell>
                      <TableCell>
                        <ToneBadge tone={maturityTone(r.info.maturity)}>
                          {r.info.maturity || 'unknown'}
                        </ToneBadge>
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1">
                          {(r.info.required || []).map((f) => (
                            <ToneBadge key={f} tone="rose">
                              {f}
                            </ToneBadge>
                          ))}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1">
                          {(r.info.capabilities || []).map((c) => (
                            <ToneBadge key={c} tone="slate">
                              {c}
                            </ToneBadge>
                          ))}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}

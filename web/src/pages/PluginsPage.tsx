import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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

type Props = {
  t: TFunc;
  lang: Lang;
  plugins: ApiState<PluginResponse>;
};

const kindTone: Record<string, 'emerald' | 'slate' | 'violet'> = {
  source: 'emerald',
  sink: 'emerald',
  transform: 'violet',
};

function maturityTone(m?: string): 'emerald' | 'blue' | 'amber' | 'slate' {
  if (m === 'production') return 'emerald';
  if (m === 'beta') return 'blue';
  if (m === 'experimental') return 'amber';
  return 'slate';
}

export function PluginsPage({ t, plugins }: Props) {
  const meta = plugins.data?.metadata;
  if (!meta) {
    return (
      <Card>
        <CardContent className="p-6">
          <EmptyState text={t('plugin.loading')} />
        </CardContent>
      </Card>
    );
  }

  const rows: { kind: string; name: string; info: PluginInfo }[] = [
    ...Object.entries(meta.sources || {}).map(([n, i]) => ({ kind: 'source', name: n, info: i })),
    ...Object.entries(meta.sinks || {}).map(([n, i]) => ({ kind: 'sink', name: n, info: i })),
    ...Object.entries(meta.transforms || {}).map(([n, i]) => ({ kind: 'transform', name: n, info: i })),
  ];

  return (
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
            {rows.map((r) => (
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
      </CardContent>
    </Card>
  );
}

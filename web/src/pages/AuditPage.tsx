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
import { fmtTime } from '@/lib/format';
import type { ApiState, AuditEvent, TFunc } from '@/lib/types';
import type { Lang } from '@/i18n';

type Props = {
  t: TFunc;
  lang: Lang;
  audit: ApiState<{ events: AuditEvent[] }>;
};

export function AuditPage({ t, audit }: Props) {
  const events = audit.data?.events || [];

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm">{t('audit.trail')}</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t('audit.action')}</TableHead>
              <TableHead>{t('audit.method')}</TableHead>
              <TableHead>{t('audit.path')}</TableHead>
              <TableHead>{t('audit.time')}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {events.map((e, i) => (
              <TableRow key={i}>
                <TableCell>
                  <ToneBadge tone="indigo">{e.action || 'event'}</ToneBadge>
                </TableCell>
                <TableCell>
                  <ToneBadge tone="slate">{e.method || t('common.na')}</ToneBadge>
                </TableCell>
                <TableCell className="text-xs">{e.target || e.path || t('common.na')}</TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {fmtTime(e.timestamp)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        {!events.length && (
          <div className="p-8">
            <EmptyState text={t('audit.noEvents')} hint={t('audit.emptyHint')} />
          </div>
        )}
      </CardContent>
    </Card>
  );
}

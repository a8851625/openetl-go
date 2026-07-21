import React, { useState } from 'react';
import type { TFunc, Lang } from './types';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { EmptyState, ErrorBox } from '@/components/shared/empty-state';
import { ToneBadge } from '@/components/shared/status-badge';
import { cn } from '@/lib/utils';

type WorkerInfo = {
  id: string;
  host: string;
  port: number;
  slots: number;
  status: string;
  labels?: Record<string, string>;
  last_heartbeat: string;
  registered_at: string;
};

function getToken() {
  return window.localStorage.getItem('etl_api_token') || '';
}

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken();
  const headers = new Headers(init.headers);
  headers.set('Content-Type', 'application/json');
  if (token) headers.set('X-API-Token', token);
  const res = await fetch(path, { ...init, headers });
  if (!res.ok) throw new Error((await res.text()) || `${res.status}`);
  return res.json();
}

export function WorkersPage({ t, lang: _lang }: { t: TFunc; lang: Lang }) {
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const refresh = () => {
    setLoading(true);
    api<{ workers: WorkerInfo[] }>('/api/v2/workers')
      .then((d) => {
        setWorkers(d.workers || []);
        setError('');
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };

  React.useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, []);

  const deregister = (id: string) => {
    api(`/api/v2/workers/${id}/deregister`, { method: 'DELETE' })
      .then(() => {
        refresh();
      })
      .catch((e) => setError(e.message));
  };

  const totalSlots = workers.reduce((a, w) => a + w.slots, 0);
  const onlineCount = workers.filter((w) => w.status === 'online').length;

  const cards = [
    {
      label: t('worker.registered'),
      value: workers.length,
      sub: `${onlineCount} ${t('worker.online')}`,
      color: 'text-primary dark:text-indigo-400',
    },
    {
      label: t('worker.totalSlots'),
      value: totalSlots,
      sub: `${workers.length} ${t('worker.registered')}`,
      color: 'text-blue-600 dark:text-blue-400',
    },
    {
      label: t('worker.online'),
      value: onlineCount,
      sub: `${workers.length - onlineCount} ${t('worker.offline')}`,
      color: 'text-emerald-600 dark:text-emerald-400',
    },
  ];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        {cards.map((c) => (
          <Card key={c.label} className="transition-shadow hover:shadow-md">
            <CardContent className="p-5">
              <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                {c.label}
              </span>
              <div className={cn('mt-2 text-3xl font-bold', c.color)}>{c.value}</div>
              <div className="mt-1 text-xs text-muted-foreground">{c.sub}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
          <CardTitle className="text-sm">{t('worker.registered')}</CardTitle>
          <Button variant="secondary" size="sm" onClick={refresh}>
            {t('common.refresh')}
          </Button>
        </CardHeader>
        <CardContent className="p-0">
          {error ? (
            <div className="p-4">
              <ErrorBox message={error} />
            </div>
          ) : loading && workers.length === 0 ? (
            <div className="p-8 text-center text-sm text-muted-foreground">{t('common.loading')}</div>
          ) : workers.length === 0 ? (
            <div className="p-8">
              <EmptyState text={t('worker.noWorkers')} />
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>{t('worker.id')}</TableHead>
                  <TableHead>{t('common.host')}</TableHead>
                  <TableHead>{t('common.slots')}</TableHead>
                  <TableHead>{t('common.labels')}</TableHead>
                  <TableHead>{t('common.status')}</TableHead>
                  <TableHead>{t('worker.lastHeartbeat')}</TableHead>
                  <TableHead>{t('common.actions')}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {workers.map((w) => (
                  <TableRow key={w.id}>
                    <TableCell className="font-medium">{w.id}</TableCell>
                    <TableCell className="text-sm">
                      {w.host}:{w.port}
                    </TableCell>
                    <TableCell>
                      <ToneBadge tone="blue">{w.slots}</ToneBadge>
                    </TableCell>
                    <TableCell>
                      {w.labels && Object.keys(w.labels).length > 0 ? (
                        <div className="flex flex-wrap gap-1">
                          {Object.entries(w.labels).map(([k, v]) => (
                            <ToneBadge key={k} tone="violet">
                              {k}={v}
                            </ToneBadge>
                          ))}
                        </div>
                      ) : (
                        <span className="text-xs text-muted-foreground">—</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <ToneBadge tone={w.status === 'online' ? 'emerald' : 'slate'}>
                        {w.status === 'online' ? t('worker.online') : t('worker.offline')}
                      </ToneBadge>
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {fmtTime(w.last_heartbeat)}
                    </TableCell>
                    <TableCell>
                      <Button variant="destructive" size="sm" onClick={() => deregister(w.id)}>
                        {t('worker.deregister')}
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-sm">{t('worker.runningTasks')}</CardTitle>
        </CardHeader>
        <CardContent>
          <EmptyState text={t('worker.noWorkers')} />
        </CardContent>
      </Card>
    </div>
  );
}

function fmtTime(v?: string) {
  if (!v || v.startsWith('0001-') || v.startsWith('1970-')) return 'n/a';
  try {
    return new Date(v).toLocaleString();
  } catch {
    return 'n/a';
  }
}

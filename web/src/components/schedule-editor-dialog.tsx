import { useEffect, useState } from 'react';
import cronstrue from 'cronstrue';
import { Modal } from '@/components/shared/modal';
import { ErrorBox } from '@/components/shared/empty-state';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { api } from '@/lib/api';
import type { TFunc } from '@/lib/types';
import type { Lang } from '@/i18n';

export type ScheduleConfig = {
  type: string;
  cron?: string;
  interval_sec?: number;
  depends_on?: string[];
};

export type ScheduleState = {
  enabled: boolean;
  schedule?: ScheduleConfig;
};

export function describeSchedule(
  t: TFunc,
  lang: Lang,
  schedule?: ScheduleConfig,
  enabled = true,
) {
  if (!enabled || !schedule) return t('sched.manual');
  if (schedule.type === 'cron' && schedule.cron) {
    try {
      return cronstrue.toString(schedule.cron, { locale: lang === 'zh' ? 'zh_CN' : 'en' });
    } catch {
      return t('sched.invalidCron');
    }
  }
  if (schedule.type === 'periodic') return `${schedule.interval_sec || 0}s`;
  if (schedule.type === 'dependency') {
    const deps = Array.isArray(schedule.depends_on) ? schedule.depends_on : [];
    return deps.length ? deps.join(', ') : t('sched.dependency');
  }
  return t(`sched.${schedule.type}` as 'sched.cron');
}

type Props = {
  t: TFunc;
  lang: Lang;
  open: boolean;
  pipelineRef: string;
  pipelineName: string;
  onClose: () => void;
  onSaved?: () => void;
};

/** Per-pipeline schedule editor (dialog). Shared by detail page and schedules overview. */
export function ScheduleEditorDialog({
  t,
  lang,
  open,
  pipelineRef,
  pipelineName,
  onClose,
  onSaved,
}: Props) {
  const [type, setType] = useState('cron');
  const [cron, setCron] = useState('*/5 * * * *');
  const [intervalSec, setIntervalSec] = useState(300);
  const [dependsOn, setDependsOn] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [enabled, setEnabled] = useState(false);
  const [cronHint, setCronHint] = useState('');

  useEffect(() => {
    if (!open || !pipelineRef) return;
    let cancelled = false;
    setError('');
    setMessage('');
    api<ScheduleState>(`/api/v2/pipelines/${pipelineRef}/schedule`)
      .then((res) => {
        if (cancelled) return;
        setEnabled(!!res.enabled);
        const sched = res.schedule;
        if (sched) {
          setType(sched.type || 'cron');
          setCron(sched.cron || '*/5 * * * *');
          setIntervalSec(sched.interval_sec || 300);
          setDependsOn(Array.isArray(sched.depends_on) ? sched.depends_on.join(', ') : '');
        } else {
          setType('cron');
          setCron('*/5 * * * *');
          setIntervalSec(300);
          setDependsOn('');
        }
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [open, pipelineRef]);

  useEffect(() => {
    if (type !== 'cron' || !cron) {
      setCronHint('');
      return;
    }
    try {
      setCronHint(cronstrue.toString(cron, { locale: lang === 'zh' ? 'zh_CN' : 'en' }));
    } catch {
      setCronHint(t('sched.invalidCron'));
    }
  }, [cron, type, lang, t]);

  const save = async () => {
    setBusy(true);
    setError('');
    setMessage('');
    try {
      const body: ScheduleConfig = { type };
      if (type === 'cron') body.cron = cron;
      if (type === 'periodic') body.interval_sec = Number(intervalSec) || 60;
      if (type === 'dependency') {
        body.depends_on = dependsOn
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean);
      }
      await api(`/api/v2/pipelines/${pipelineRef}/schedule`, {
        method: 'PUT',
        body: JSON.stringify(body),
      });
      setEnabled(true);
      setMessage(t('sched.saved'));
      onSaved?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const disable = async () => {
    setBusy(true);
    setError('');
    setMessage('');
    try {
      await api(`/api/v2/pipelines/${pipelineRef}/schedule`, { method: 'DELETE' });
      setEnabled(false);
      setMessage(t('sched.disabled'));
      onSaved?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const runNow = async () => {
    setBusy(true);
    setError('');
    setMessage('');
    try {
      await api(`/api/v2/pipelines/${pipelineRef}/start`, { method: 'POST' });
      setMessage(t('sched.runStarted'));
      onSaved?.();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  if (!open) return null;

  return (
    <Modal
      title={`${t('sched.editor')} · ${pipelineName}`}
      onClose={onClose}
      className="sm:max-w-lg"
    >
      <div className="space-y-4" data-testid="schedule-editor-dialog">
        <p className="text-xs text-muted-foreground">{t('sched.dialogHint')}</p>
        {error && <ErrorBox message={error} />}
        {message && (
          <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200">
            {message}
          </div>
        )}

        <div className="flex flex-wrap gap-2">
          {['cron', 'periodic', 'streaming', 'once', 'dependency'].map((tp) => (
            <Button
              key={tp}
              size="sm"
              variant={type === tp ? 'default' : 'secondary'}
              onClick={() => setType(tp)}
            >
              {t(`sched.${tp}` as 'sched.cron')}
            </Button>
          ))}
        </div>

        {type === 'cron' && (
          <label className="block space-y-1">
            <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              {t('common.cron')}
            </span>
            <Input
              className="font-mono"
              value={cron}
              onChange={(e) => setCron(e.target.value)}
              placeholder="*/5 * * * *"
            />
            {cronHint ? <span className="text-xs text-muted-foreground">{cronHint}</span> : null}
          </label>
        )}
        {type === 'periodic' && (
          <label className="block space-y-1">
            <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              {t('common.interval')}
            </span>
            <Input
              type="number"
              min={1}
              value={intervalSec}
              onChange={(e) => setIntervalSec(Number(e.target.value))}
            />
          </label>
        )}
        {type === 'dependency' && (
          <label className="block space-y-1">
            <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              {t('sched.dependsOn')}
            </span>
            <Input
              className="font-mono"
              value={dependsOn}
              onChange={(e) => setDependsOn(e.target.value)}
              placeholder="pipeline-a, pipeline-b"
            />
          </label>
        )}
        {(type === 'streaming' || type === 'once') && (
          <p className="text-xs text-muted-foreground">{t('sched.sourceHint')}</p>
        )}

        <div className="flex flex-wrap gap-2 border-t border-border pt-3">
          <Button size="sm" disabled={busy} onClick={save} data-testid="schedule-save">
            {t('sched.save')}
          </Button>
          <Button
            size="sm"
            variant="outline"
            disabled={busy || !enabled}
            onClick={disable}
            data-testid="schedule-disable"
          >
            {t('sched.disable')}
          </Button>
          <Button size="sm" variant="secondary" disabled={busy} onClick={runNow}>
            {t('sched.runNow')}
          </Button>
          <Button size="sm" variant="ghost" className="ml-auto" onClick={onClose}>
            {t('common.cancel')}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

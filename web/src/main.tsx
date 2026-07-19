import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './styles.css';
import { getLang, setLang, translate, type Lang } from './i18n';
import { DagEditorPage } from './DagEditorPage';
import { WorkersPage } from './WorkersPage';
import { MyPluginsPage } from './MyPluginsPage';
import { SchedulesPage } from './SchedulesPage';
import { ConnectionsPage } from './ConnectionsPage';
import { ThemeProvider } from '@/components/theme-provider';
import { Toaster } from '@/components/ui/sonner';
import { AppShell, type AppPage } from '@/components/layout/app-shell';
import { api, getToken, normalizePipelines, pipelineKey, useApi } from '@/lib/api';
import { showToast, type ToastFn } from '@/lib/toast';
import type {
  AuditEvent,
  Checkpoint,
  MetricsPipeline,
  Pipeline,
  PluginResponse,
} from '@/lib/types';
import { DashboardPage } from '@/pages/DashboardPage';
import { PipelinesPage } from '@/pages/pipelines/PipelinesPage';
import { DLQPage } from '@/pages/DLQPage';
import { PluginsPage } from '@/pages/PluginsPage';
import { AuditPage } from '@/pages/AuditPage';
import { SettingsModal } from '@/pages/SettingsPage';

type Page = AppPage;

function App() {
  const [lang, setLangState] = useState<Lang>(getLang());
  const [page, setPage] = useState<Page>('dashboard');
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedPipeline, setSelectedPipeline] = useState('');
  const [editTarget, setEditTarget] = useState('');
  const [token, setToken] = useState(getToken());
  const [showSettings, setShowSettings] = useState(false);
  const [llmConfig, setLLMConfig] = useState({ base_url: '', model: '', api_key: '' });
  const autoRefresh = useRef(setInterval(() => {}, 99999));

  const t = useCallback((key: string) => translate(key, lang), [lang]);

  const pipelines = useApi<{ pipelines: Pipeline[] }>('/api/v2/pipelines', refreshKey);
  const metrics = useApi<{ pipelines: MetricsPipeline[] }>('/api/v2/metrics', refreshKey);
  const plugins = useApi<PluginResponse>('/api/v2/plugins', refreshKey);
  const pluginSchema = useApi<any>('/api/v2/plugins/schema', refreshKey);
  const checkpoints = useApi<{ checkpoints: Checkpoint[] }>('/api/v2/checkpoints', refreshKey);
  const audit = useApi<{ events: AuditEvent[] }>('/api/v2/audit?limit=50', refreshKey);

  const pipelinesList = normalizePipelines(pipelines.data);
  const metricsList = metrics.data?.pipelines || [];
  const selected =
    pipelinesList.find(
      (p) => pipelineKey(p) === selectedPipeline || p.name === selectedPipeline,
    ) || pipelinesList[0];
  const selectedMetric = metricsList.find(
    (p) => (p.id && p.id === selected?.id) || p.name === selected?.name,
  );

  const totals = useMemo(() => {
    const list = normalizePipelines(pipelines.data);
    return list.reduce(
      (a, p) => ({
        read: a.read + p.stats.records_read,
        written: a.written + p.stats.records_written,
        failed: a.failed + p.stats.records_failed,
        dlq: a.dlq + p.stats.records_dlq,
        running: a.running + (p.status === 'running' ? 1 : 0),
      }),
      { read: 0, written: 0, failed: 0, dlq: 0, running: 0 },
    );
  }, [pipelines.data]);

  useEffect(() => {
    clearInterval(autoRefresh.current);
    autoRefresh.current = setInterval(() => setRefreshKey((n) => n + 1), 5000);
    return () => clearInterval(autoRefresh.current);
  }, []);

  const toast: ToastFn = useCallback((type, msg) => {
    showToast(type, msg);
  }, []);

  const runAction = useCallback(
    async (label: string, fn: () => Promise<unknown>) => {
      try {
        const result = await fn();
        const toastMessage =
          result &&
          typeof result === 'object' &&
          'toastMessage' in result &&
          typeof (result as any).toastMessage === 'string'
            ? (result as any).toastMessage
            : label;
        toast('success', toastMessage);
        setRefreshKey((n) => n + 1);
      } catch (e) {
        toast('error', `${label}: ${e instanceof Error ? e.message : String(e)}`);
      }
    },
    [toast],
  );

  const editPipeline = useCallback((ref: string) => {
    setEditTarget(ref);
    setPage('designer');
  }, []);

  const loadLLMConfig = useCallback(() => {
    api<{ llm_base_url?: string; llm_model?: string; llm_api_key?: string }>('/api/v2/settings')
      .then((d) =>
        setLLMConfig({
          base_url: d.llm_base_url || '',
          model: d.llm_model || '',
          api_key: d.llm_api_key || '',
        }),
      )
      .catch(() => {});
  }, []);

  const switchLang = (l: Lang) => {
    setLangState(l);
    setLang(l);
  };

  const navItems: { id: Page; key: string; badge?: number }[] = [
    { id: 'dashboard', key: 'nav.dashboard' },
    { id: 'pipelines', key: 'nav.pipelines', badge: totals.running },
    { id: 'connections', key: 'nav.connections' },
    { id: 'designer', key: 'nav.designer' },
    { id: 'dlq', key: 'nav.dlq' },
    { id: 'plugins', key: 'nav.plugins' },
    { id: 'myPlugins', key: 'nav.myPlugins' },
    { id: 'workers', key: 'nav.workers' },
    { id: 'schedules', key: 'nav.schedules' },
    { id: 'audit', key: 'nav.audit' },
  ];

  const pageTitle = page === 'dlq' ? t('top.dlqWorkbench') : t(`nav.${page}`);

  return (
    <>
      <AppShell
        title={t('app.title')}
        subtitle={t('app.subtitle')}
        page={page}
        pageTitle={pageTitle}
        navItems={navItems}
        t={t}
        onNavigate={setPage}
        onOpenSettings={() => {
          setShowSettings(true);
          loadLLMConfig();
        }}
        onToggleLang={() => switchLang(lang === 'en' ? 'zh' : 'en')}
        langLabel={lang === 'en' ? '中文' : 'EN'}
        onReloadSpecs={() =>
          runAction(t('toast.reloadSpecs'), () => api('/api/v2/specs/reload', { method: 'POST' }))
        }
        reloadLabel={t('top.reloadSpecs')}
        autoRefreshLabel={t('top.autorefresh')}
        hasRunning={pipelinesList.some((p) => p.status === 'running')}
      >
        {page === 'dashboard' && (
          <DashboardPage
            t={t}
            lang={lang}
            totals={totals}
            pipelines={pipelines}
            metrics={metrics}
            selected={selected}
            selectedMetric={selectedMetric}
            onSelect={setSelectedPipeline}
          />
        )}
        {page === 'pipelines' && (
          <PipelinesPage
            t={t}
            lang={lang}
            pipelines={pipelines}
            metrics={metrics}
            selected={selected}
            selectedMetric={selectedMetric}
            onSelect={setSelectedPipeline}
            onAction={runAction}
            checkpoints={checkpoints}
            onResetCheckpoint={(ref: string, label?: string) =>
              runAction(`${t('toast.resetCheckpoint')}: ${label || ref}`, () =>
                api(`/api/v2/pipelines/${encodeURIComponent(ref)}/checkpoint/reset`, {
                  method: 'POST',
                }),
              )
            }
            onEdit={editPipeline}
            refreshKey={refreshKey}
            onShowToast={toast}
            plugins={plugins}
            pluginSchema={pluginSchema}
          />
        )}
        {page === 'connections' && <ConnectionsPage t={t} lang={lang} />}
        {page === 'designer' && (
          <DagEditorPage
            t={t}
            lang={lang}
            plugins={plugins}
            schema={pluginSchema}
            onAction={runAction}
            editTarget={editTarget}
          />
        )}
        {page === 'dlq' && (
          <DLQPage
            t={t}
            lang={lang}
            pipelines={pipelines}
            selected={selected}
            onSelect={setSelectedPipeline}
            onAction={runAction}
          />
        )}
        {page === 'plugins' && <PluginsPage t={t} lang={lang} plugins={plugins} />}
        {page === 'myPlugins' && <MyPluginsPage t={t} lang={lang} />}
        {page === 'workers' && <WorkersPage t={t} lang={lang} />}
        {page === 'schedules' && <SchedulesPage t={t} lang={lang} pipelines={pipelines} />}
        {page === 'audit' && <AuditPage t={t} lang={lang} audit={audit} />}
      </AppShell>

      <SettingsModal
        t={t}
        lang={lang}
        token={token}
        setToken={setToken}
        switchLang={switchLang}
        llmConfig={llmConfig}
        setLLMConfig={setLLMConfig}
        open={showSettings}
        onClose={() => setShowSettings(false)}
        onSaveToken={() => {
          window.localStorage.setItem('etl_api_token', token);
          setRefreshKey((n) => n + 1);
          toast('success', t('settings.tokenSaved'));
        }}
        onSaveLLM={() => {
          api('/api/v2/settings', {
            method: 'POST',
            body: JSON.stringify({
              llm_base_url: llmConfig.base_url,
              llm_model: llmConfig.model,
              llm_api_key: llmConfig.api_key,
            }),
          })
            .then(() => toast('success', t('settings.llmSaved')))
            .catch((e) => toast('error', e.message));
        }}
      />
    </>
  );
}

createRoot(document.getElementById('root')!).render(
  <ThemeProvider defaultTheme="light" storageKey="etl_theme">
    <App />
    <Toaster richColors position="top-right" />
  </ThemeProvider>,
);

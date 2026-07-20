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
import { AppShell, type AppPage, type NavGroup } from '@/components/layout/app-shell';
import { api, getToken, normalizePipelines, pipelineKey, useApi } from '@/lib/api';
import { showToast, type ToastFn } from '@/lib/toast';
import {
  navigate,
  parseHash,
  routeToNavPage,
  type AppRoute,
  type DetailTab,
} from '@/lib/routing';
import { deriveIssues } from '@/lib/pipeline-health';
import type {
  AuditEvent,
  Checkpoint,
  MetricsPipeline,
  Pipeline,
  PluginResponse,
} from '@/lib/types';
import { DashboardPage } from '@/pages/DashboardPage';
import { PipelinesPage } from '@/pages/pipelines/PipelinesPage';
import { PipelineDetailPage } from '@/pages/pipelines/PipelineDetailPage';
import { IssuesPage } from '@/pages/IssuesPage';
import { DLQPage } from '@/pages/DLQPage';
import { PluginsPage } from '@/pages/PluginsPage';
import { AuditPage } from '@/pages/AuditPage';
import { SettingsModal } from '@/pages/SettingsPage';
import { ConnectorsPage } from '@/pages/ConnectorsPage';

function App() {
  const [lang, setLangState] = useState<Lang>(getLang());
  const [route, setRoute] = useState<AppRoute>(() => parseHash());
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedPipeline, setSelectedPipeline] = useState('');
  const [editTarget, setEditTarget] = useState('');
  const [token, setToken] = useState(getToken());
  const [showSettings, setShowSettings] = useState(false);
  const [llmConfig, setLLMConfig] = useState({ base_url: '', model: '', api_key: '' });
  const [distributedHint, setDistributedHint] = useState(false);
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

  const issueCount = useMemo(
    () => deriveIssues(pipelinesList, metricsList).length,
    [pipelinesList, metricsList],
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

  // Hash routing
  useEffect(() => {
    const onHash = () => {
      const next = parseHash();
      setRoute(next);
      if (next.page === 'pipeline-detail') {
        setSelectedPipeline(next.id);
      }
      if (next.page === 'designer' && next.editTarget) {
        setEditTarget(next.editTarget);
      }
      if (next.page === 'settings') {
        setShowSettings(true);
        loadLLMConfig();
      }
    };
    window.addEventListener('hashchange', onHash);
    onHash();
    return () => window.removeEventListener('hashchange', onHash);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Detect multi-worker / distributed for Cluster nav
  useEffect(() => {
    api<{ workers?: { id: string }[] }>('/api/v2/workers')
      .then((d) => {
        const n = d.workers?.length || 0;
        setDistributedHint(n > 1);
      })
      .catch(() => setDistributedHint(false));
  }, [refreshKey]);

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
    navigate({ page: 'designer', editTarget: ref });
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

  const go = useCallback((page: AppPage) => {
    if (page === 'settings') {
      setShowSettings(true);
      loadLLMConfig();
      navigate({ page: 'settings' });
      return;
    }
    navigate({ page } as AppRoute);
  }, [loadLLMConfig]);

  const openPipelineDetail = useCallback((key: string, tab: DetailTab | string = 'overview') => {
    if (!key) {
      navigate({ page: 'pipelines' });
      return;
    }
    setSelectedPipeline(key);
    const safeTab = (
      ['overview', 'runs', 'issues', 'checkpoints', 'spec'].includes(tab) ? tab : 'overview'
    ) as DetailTab;
    navigate({ page: 'pipeline-detail', id: key, tab: safeTab });
  }, []);

  const openWizard = useCallback(() => {
    navigate({ page: 'pipeline-new' });
  }, []);

  const openDLQ = useCallback((key: string) => {
    if (key) setSelectedPipeline(key);
    navigate({ page: 'dlq' });
  }, []);

  const navPage = routeToNavPage(route);

  const navGroups: NavGroup[] = useMemo(() => {
    return [
      {
        id: 'primary',
        items: [{ id: 'dashboard', key: 'nav.dashboard', dataNav: 'dashboard' }],
      },
      {
        id: 'run',
        labelKey: 'nav.groupRun',
        items: [
          { id: 'pipelines', key: 'nav.pipelines', dataNav: 'pipelines' },
          {
            id: 'issues',
            key: 'nav.issues',
            dataNav: 'issues',
            badge: issueCount,
          },
          { id: 'dlq', key: 'nav.dlq', dataNav: 'dlq' },
        ],
      },
      {
        id: 'resources',
        labelKey: 'nav.groupResources',
        items: [
          { id: 'connections', key: 'nav.connections', dataNav: 'connections' },
          { id: 'connectors', key: 'nav.connectors', dataNav: 'connectors' },
        ],
      },
      {
        id: 'system',
        labelKey: 'nav.groupSystem',
        items: [
          { id: 'audit', key: 'nav.audit', dataNav: 'audit' },
          { id: 'schedules', key: 'nav.schedules', dataNav: 'schedules' },
          {
            id: 'workers',
            key: 'nav.workers',
            dataNav: 'workers',
            // Prefer progressive disclosure: hide when clearly standalone (0–1 workers)
            hidden: !distributedHint,
          },
          { id: 'plugins', key: 'nav.plugins', dataNav: 'plugins' },
          { id: 'myPlugins', key: 'nav.myPlugins', dataNav: 'myPlugins' },
        ],
      },
    ];
  }, [issueCount, distributedHint]);

  const pageTitle = (() => {
    if (route.page === 'pipeline-detail') return selected?.name || t('nav.pipelines');
    if (route.page === 'pipeline-new') return t('nav.createPipeline');
    if (route.page === 'dlq') return t('top.dlqWorkbench');
    if (route.page === 'designer') return t('nav.dagEditor');
    return t(`nav.${navPage}`);
  })();

  const crumb =
    route.page === 'pipeline-detail'
      ? `pipelines / ${selected?.name || route.id}`
      : route.page === 'pipeline-new'
        ? 'pipelines / new'
        : route.page === 'designer'
          ? 'pipelines / advanced DAG'
          : undefined;

  return (
    <>
      <AppShell
        title={t('app.title')}
        subtitle={t('app.subtitle')}
        page={navPage}
        pageTitle={pageTitle}
        crumb={crumb}
        navGroups={navGroups}
        t={t}
        onNavigate={go}
        onCreatePipeline={openWizard}
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
        issueCount={issueCount}
      >
        {(route.page === 'dashboard') && (
          <DashboardPage
            t={t}
            lang={lang}
            totals={totals}
            pipelines={pipelines}
            metrics={metrics}
            selected={selected}
            selectedMetric={selectedMetric}
            onSelect={setSelectedPipeline}
            onOpenPipeline={openPipelineDetail}
            onOpenIssues={() => navigate({ page: 'issues' })}
            onOpenDLQ={openDLQ}
            onCreatePipeline={openWizard}
            onOpenConnections={() => navigate({ page: 'connections' })}
          />
        )}

        {(route.page === 'pipelines' || route.page === 'pipeline-new') && (
          <PipelinesPage
            t={t}
            lang={lang}
            pipelines={pipelines}
            metrics={metrics}
            selected={selected}
            selectedMetric={selectedMetric}
            onSelect={setSelectedPipeline}
            onOpenDetail={(key) => openPipelineDetail(key, 'overview')}
            onOpenWizard={openWizard}
            forceWizard={route.page === 'pipeline-new'}
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
            onWizardClose={() => {
              if (route.page === 'pipeline-new') navigate({ page: 'pipelines' });
            }}
          />
        )}

        {route.page === 'pipeline-detail' && (
          <PipelineDetailPage
            t={t}
            pipeline={
              pipelinesList.find((p) => pipelineKey(p) === route.id || p.name === route.id) ||
              selected
            }
            metric={
              metricsList.find(
                (m) =>
                  m.name === route.id ||
                  m.id === route.id ||
                  (selected && ((m.id && m.id === selected.id) || m.name === selected.name)),
              ) || selectedMetric
            }
            checkpoints={checkpoints.data?.checkpoints || []}
            tab={route.tab}
            onTabChange={(tab) => openPipelineDetail(route.id, tab)}
            onBack={() => navigate({ page: 'pipelines' })}
            onAction={runAction}
            onResetCheckpoint={(ref: string, label?: string) =>
              runAction(`${t('toast.resetCheckpoint')}: ${label || ref}`, () =>
                api(`/api/v2/pipelines/${encodeURIComponent(ref)}/checkpoint/reset`, {
                  method: 'POST',
                }),
              )
            }
            onEdit={editPipeline}
            onOpenDLQ={openDLQ}
            onOpenDesigner={editPipeline}
          />
        )}

        {route.page === 'issues' && (
          <IssuesPage
            t={t}
            pipelines={pipelines}
            metrics={metrics}
            onSelect={setSelectedPipeline}
            onOpenPipeline={openPipelineDetail}
            onOpenDLQ={openDLQ}
            onOpenConnections={() => navigate({ page: 'connections' })}
          />
        )}

        {route.page === 'connections' && <ConnectionsPage t={t} lang={lang} />}
        {route.page === 'connectors' && (
          <ConnectorsPage t={t} lang={lang} plugins={plugins} schema={pluginSchema} />
        )}
        {route.page === 'designer' && (
          <DagEditorPage
            t={t}
            lang={lang}
            plugins={plugins}
            schema={pluginSchema}
            onAction={runAction}
            editTarget={editTarget}
          />
        )}
        {route.page === 'dlq' && (
          <DLQPage
            t={t}
            lang={lang}
            pipelines={pipelines}
            selected={selected}
            onSelect={setSelectedPipeline}
            onAction={runAction}
          />
        )}
        {route.page === 'plugins' && <PluginsPage t={t} lang={lang} plugins={plugins} />}
        {route.page === 'myPlugins' && <MyPluginsPage t={t} lang={lang} />}
        {route.page === 'workers' && <WorkersPage t={t} lang={lang} />}
        {route.page === 'schedules' && (
          <SchedulesPage t={t} lang={lang} pipelines={pipelines} />
        )}
        {route.page === 'audit' && <AuditPage t={t} lang={lang} audit={audit} />}
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
        onClose={() => {
          setShowSettings(false);
          if (route.page === 'settings') navigate({ page: 'dashboard' });
        }}
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

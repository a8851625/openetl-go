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
import { Button } from '@/components/ui/button';
import { AppShell, type AppPage, type NavGroup } from '@/components/layout/app-shell';
import {
  api,
  getToken,
  normalizePipelines,
  pipelineKey,
  resolvePipelineRoute,
  setToken as setApiToken,
  restorePersistedToken,
  memorySeedToken,
  clearToken as clearApiToken,
  toApiErrorDetails,
  useApi,
} from '@/lib/api';
import { pipelineDLQBacklog } from '@/lib/dlq';
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
import { FirstTaskWizard } from '@/pages/pipelines/first-task-wizard';
import { IssuesPage } from '@/pages/IssuesPage';
import { DLQPage } from '@/pages/DLQPage';
import { AuditPage } from '@/pages/AuditPage';
import { SettingsModal } from '@/pages/SettingsPage';
import { ConnectorsPage } from '@/pages/ConnectorsPage';

function App() {
  const [lang, setLangState] = useState<Lang>(getLang());
  const [route, setRoute] = useState<AppRoute>(() => parseHash());
  const [refreshKey, setRefreshKey] = useState(0);
  const [selectedPipeline, setSelectedPipeline] = useState('');
  const [editTarget, setEditTarget] = useState('');
  const [token, setToken] = useState(() => {
    // PR-0 keeps tokens memory-only by default; restore only what the user
    // explicitly persisted via "remember token". Restore must NOT re-write
    // storage: setToken without { persist } removes the stored item, which
    // would silently undo the user's opt-in on every reload.
    const persisted = restorePersistedToken();
    if (persisted) memorySeedToken(persisted);
    return persisted || getToken();
  });
  // Settings is route-owned. Keeping a separate open state made a stale
  // dialog survive hash navigation and intercept the next page's controls.
  const [llmConfig, setLLMConfig] = useState({ base_url: '', model: '', api_key: '' });
  const [distributedHint, setDistributedHint] = useState(false);
  // UI-B.3: user-controllable auto-refresh with last-success/failure feedback.
  const [autoPaused, setAutoPaused] = useState(false);
  const [lastRefreshAt, setLastRefreshAt] = useState<string | null>(null);
  const [lastRefreshFailed, setLastRefreshFailed] = useState(false);
  const autoRefresh = useRef(setInterval(() => {}, 99999));

  const t = useCallback((key: string) => translate(key, lang), [lang]);

  const pipelines = useApi<{ pipelines: Pipeline[] }>('/api/v2/pipelines', refreshKey);
  const metrics = useApi<{ pipelines: MetricsPipeline[] }>('/api/v2/metrics', refreshKey);
  const plugins = useApi<PluginResponse>('/api/v2/plugins', refreshKey);
  const pluginSchema = useApi<any>('/api/v2/plugins/schema', refreshKey);
  const checkpoints = useApi<{ checkpoints: Checkpoint[] }>('/api/v2/checkpoints', refreshKey);
  const audit = useApi<{ events: AuditEvent[] }>('/api/v2/audit?limit=50', refreshKey);

  // UI-D.5: refresh status must reflect a completed request, not merely a
  // refresh-key bump. `useApi.refreshing` remains true during background and
  // manual fetches while old data stays rendered.
  useEffect(() => {
    if (pipelines.refreshing) return;
    if (pipelines.error) {
      setLastRefreshFailed(true);
      return;
    }
    setLastRefreshFailed(false);
    setLastRefreshAt(new Date().toLocaleTimeString());
  }, [pipelines.refreshing, pipelines.error]);

  const pipelinesList = normalizePipelines(pipelines.data);
  const metricsList = metrics.data?.pipelines || [];
  const selected =
    pipelinesList.find(
      (p) => pipelineKey(p) === selectedPipeline || p.name === selectedPipeline,
    ) || pipelinesList[0];
  const selectedMetric = metricsList.find(
    (p) => (p.id && p.id === selected?.id) || p.name === selected?.name,
  );

  // Detail routes have a separate, fail-closed identity resolution path.
  // `selected` remains list/dashboard state only and must never become a
  // fallback action target for an unmatched hash URL.
  const detailPipeline =
    route.page === 'pipeline-detail' ? resolvePipelineRoute(pipelinesList, route.id) : undefined;
  const detailMetric = detailPipeline
    ? metricsList.find(
        (metric) =>
          (Boolean(detailPipeline.id) && metric.id === detailPipeline.id) ||
          metric.name === detailPipeline.name,
      )
    : undefined;
  const detailLoading = route.page === 'pipeline-detail' && pipelines.loading && !pipelines.data;
  const detailLoadError =
    route.page === 'pipeline-detail' && !pipelines.data ? pipelines.error : undefined;

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
        dlq: a.dlq + pipelineDLQBacklog(p),
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
    if (autoPaused) return;
    autoRefresh.current = setInterval(() => setRefreshKey((n) => n + 1), 5000);
    return () => clearInterval(autoRefresh.current);
  }, [autoPaused]);

  const toast: ToastFn = useCallback((type, msg) => {
    showToast(type, msg);
  }, []);

  const runAction = useCallback(
    async (
      label: string,
      fn: () => Promise<unknown>,
      onError?: (details: ReturnType<typeof toApiErrorDetails>) => void,
    ) => {
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
        const details = toApiErrorDetails(e);
        toast('error', `${label}: ${details.summary}`);
        onError?.(details);
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
      ['overview', 'runs', 'issues', 'checkpoints', 'logs', 'topology', 'spec'].includes(tab)
        ? tab
        : 'overview'
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
          // Built-in matrix merged into connector catalog (single capability surface).
          { id: 'connectors', key: 'nav.connectors', dataNav: 'connectors' },
        ],
      },
      {
        id: 'system',
        labelKey: 'nav.groupSystem',
        items: [
          { id: 'audit', key: 'nav.audit', dataNav: 'audit' },
          // Schedule *editing* lives on pipeline detail dialog; this page is fleet overview.
          {
            id: 'schedules',
            key: 'nav.schedules',
            dataNav: 'schedules',
          },
          {
            id: 'workers',
            key: 'nav.workers',
            dataNav: 'workers',
            // Prefer progressive disclosure: hide when clearly standalone (0–1 workers)
            hidden: !distributedHint,
          },
        ],
      },
      {
        id: 'extensions',
        labelKey: 'nav.groupExtensions',
        items: [
          // WASM / custom only — built-in matrix is under Connector catalog.
          { id: 'myPlugins', key: 'nav.myPlugins', dataNav: 'myPlugins' },
        ],
      },
    ];
  }, [issueCount, distributedHint]);

  const pageTitle = (() => {
    if (route.page === 'pipeline-detail') {
      if (detailLoading) return t('common.loading');
      return detailPipeline?.name || t('pipe.notFound');
    }
    if (route.page === 'pipeline-new') return t('nav.createPipeline');
    if (route.page === 'not-found') return t('nav.notFound');
    if (route.page === 'dlq') return t('top.dlqWorkbench');
    if (route.page === 'designer') return t('nav.dagEditor');
    return t(`nav.${navPage}`);
  })();

  const crumb =
    route.page === 'pipeline-detail'
      ? `pipelines / ${detailPipeline?.name || route.id}`
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
        onOpenSettings={() => go('settings')}
        onToggleLang={() => switchLang(lang === 'en' ? 'zh' : 'en')}
        langLabel={lang === 'en' ? '中文' : 'EN'}
        onReloadSpecs={() =>
          runAction(t('toast.reloadSpecs'), () => api('/api/v2/specs/reload', { method: 'POST' }))
        }
        reloadLabel={t('top.reloadSpecs')}
        autoRefreshLabel={t('top.autorefresh')}
        autoRefreshTitle={t('top.autorefreshTitle')}
        autoRefreshPausedLabel={t('top.autorefreshPaused')}
        autoRefreshPausedTitle={t('top.autorefreshPausedTitle')}
        autoRefreshFailedLabel={t('top.autorefreshFailed')}
        lastRefreshLabel={t('top.lastRefresh')}
        manualRefreshLabel={t('top.manualRefresh')}
        manualRefreshBusyLabel={t('top.manualRefreshBusy')}
        manualRefreshBusy={Boolean(pipelines.refreshing)}
        onManualRefresh={() => setRefreshKey((n) => n + 1)}
        autoPaused={autoPaused}
        onToggleAutoRefresh={() => setAutoPaused((v) => !v)}
        lastRefreshAt={lastRefreshAt}
        lastRefreshFailed={lastRefreshFailed}
        issueCount={issueCount}
      >
        {route.page === 'not-found' && (
          <div className="space-y-4 p-8" data-testid="not-found-page">
            <h2 className="text-2xl font-semibold">{t('nav.notFound')}</h2>
            <p className="text-sm text-muted-foreground">
              {t('nav.notFoundHint')} <code className="rounded bg-muted px-1">{(route as { path?: string }).path || ''}</code>
            </p>
            <Button onClick={() => navigate({ page: 'dashboard' })}>{t('nav.backHome')}</Button>
          </div>
        )}
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

        {route.page === 'pipelines' && (
          <PipelinesPage
            t={t}
            lang={lang}
            pipelines={pipelines}
            metrics={metrics}
            selected={selected}
            selectedMetric={selectedMetric}
            onSelect={setSelectedPipeline}
            onOpenDetail={(key, tab) => openPipelineDetail(key, tab || 'overview')}
            onOpenWizard={openWizard}
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

        {route.page === 'pipeline-new' && (
          <FirstTaskWizard
            t={t}
            schema={pluginSchema}
            initialStep={route.step}
            onClose={() => navigate({ page: 'pipelines' })}
            onCreated={(name) => {
              toast('success', `Pipeline created: ${name}`);
              setRefreshKey((n) => n + 1);
              openPipelineDetail(name, 'overview');
            }}
            onShowToast={toast}
          />
        )}

        {route.page === 'pipeline-detail' && (
          <PipelineDetailPage
            t={t}
            lang={lang}
            pipeline={detailPipeline}
            metric={detailMetric}
            loading={detailLoading}
            loadError={detailLoadError}
            routeID={route.id}
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
            onOpenDesigner={editPipeline}
            onOpenDLQ={openDLQ}
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
          <ConnectorsPage t={t} lang={lang} plugins={plugins} schema={pluginSchema} onRetry={() => setRefreshKey((n) => n + 1)} />
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
        {/* Built-in matrix merged into Connector catalog; keep route for deep links / e2e. */}
        {route.page === 'plugins' && (
          <ConnectorsPage
            t={t}
            lang={lang}
            plugins={plugins}
            schema={pluginSchema}
            initialView="matrix"
            onRetry={() => setRefreshKey((n) => n + 1)}
          />
        )}
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
        open={route.page === 'settings'}
        onClose={() => {
          // Dialog close/Esc/overlay returns to the preceding normal UI route
          // without toggling a second local state or creating a history loop.
          if (route.page === 'settings') navigate({ page: 'dashboard' });
        }}
        onSaveToken={(persist) => {
          setApiToken(token, { persist });
          setRefreshKey((n) => n + 1);
          toast('success', persist ? t('settings.tokenSavedRemembered') : t('settings.tokenSaved'));
        }}
        onClearPersistedToken={() => {
          clearApiToken();
          setToken('');
          setRefreshKey((n) => n + 1);
          toast('success', t('settings.tokenCleared'));
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

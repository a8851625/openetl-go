/** Lightweight hash router for shareable UI contexts (no react-router dependency). */

export type TaskContext = {
  pipelineId?: string;
  recoveryId?: string;
  rowId?: string;
  nodeId?: string;
  connectionName?: string;
  state?: 'open' | 'replaying' | 'resolved' | 'abandoned' | 'unknown';
};

export type AppRoute =
  | { page: 'dashboard' }
  | { page: 'not-found'; path?: string; invalidContext?: boolean }
  | { page: 'pipelines' }
  | { page: 'pipeline-detail'; id: string; tab: DetailTab; context?: TaskContext }
  | { page: 'pipeline-new'; step?: string }
  | { page: 'issues'; context?: TaskContext }
  | { page: 'dlq'; context?: TaskContext }
  | { page: 'connections'; context?: TaskContext }
  | { page: 'connectors' }
  | { page: 'designer'; editTarget?: string }
  | { page: 'plugins' }
  | { page: 'myPlugins' }
  | { page: 'workers' }
  | { page: 'schedules' }
  | { page: 'audit' }
  | { page: 'settings' };

export type DetailTab =
  | 'overview'
  | 'runs'
  | 'issues'
  | 'checkpoints'
  | 'logs'
  | 'topology'
  | 'integrity'
  | 'spec';

export type NavPage =
  | 'dashboard'
  | 'pipelines'
  | 'issues'
  | 'dlq'
  | 'connections'
  | 'connectors'
  | 'designer'
  | 'plugins'
  | 'myPlugins'
  | 'workers'
  | 'schedules'
  | 'audit'
  | 'settings';

const DETAIL_TABS: DetailTab[] = [
  'overview',
  'runs',
  'issues',
  'checkpoints',
  'logs',
  'topology',
  'integrity',
  'spec',
];

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const CONNECTION = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const CONTEXT_FIELDS = {
  pipeline_id: 'pipelineId', recovery_id: 'recoveryId', row_id: 'rowId',
  node_id: 'nodeId', connection_name: 'connectionName', state: 'state',
} as const;
const RECOVERY_STATES = ['open', 'replaying', 'resolved', 'abandoned', 'unknown'];

export function validRowId(value: string): boolean {
  return /^[1-9]\d{0,18}$/.test(value) && (value.length < 19 || value <= '9223372036854775807');
}

function validEntityKey(value: string, max = 255): boolean {
  return value.length > 0 && value.length <= max && !/[\\/?#&=]/.test(value)
    && !Array.from(value).some((character) => character.charCodeAt(0) < 32);
}

function readTaskContext(qs: URLSearchParams): TaskContext | null {
  const context: TaskContext = {};
  for (const [parameter, property] of Object.entries(CONTEXT_FIELDS)) {
    if (!qs.has(parameter)) continue;
    const value = qs.get(parameter) || '';
    if (qs.getAll(parameter).length !== 1) return null;
    if ((property === 'pipelineId' || property === 'recoveryId') && !UUID.test(value)) return null;
    if (property === 'rowId' && !validRowId(value)) return null;
    if (property === 'nodeId' && !validEntityKey(value)) return null;
    if (property === 'connectionName' && !CONNECTION.test(value)) return null;
    if (property === 'state' && !RECOVERY_STATES.includes(value)) return null;
    (context as Record<string, string>)[property] = property === 'pipelineId' || property === 'recoveryId' ? value.toLowerCase() : value;
  }
  return context;
}

function taskQuery(context?: TaskContext): string {
  if (!context) return '';
  const params = new URLSearchParams();
  for (const [parameter, property] of Object.entries(CONTEXT_FIELDS)) {
    const value = context[property as keyof TaskContext];
    if (value !== undefined) params.set(parameter, value);
  }
  if (!readTaskContext(params)) return '?invalid_context=1';
  const query = params.toString();
  return query ? `?${query}` : '';
}

export function parseHash(hash = window.location.hash): AppRoute {
  const raw = (hash || '').replace(/^#\/?/, '').trim();
  if (!raw) return { page: 'dashboard' };
  const queryIndex = raw.indexOf('?');
  const pathPart = queryIndex < 0 ? raw : raw.slice(0, queryIndex);
  const queryPart = queryIndex < 0 ? '' : raw.slice(queryIndex + 1);
  const parts = pathPart.split('/').filter(Boolean);
  const qs = new URLSearchParams(queryPart || '');

  const invalid: AppRoute = { page: 'not-found', path: 'invalid-context', invalidContext: true };
  const isWizard = parts[0] === 'pipeline-new' || (parts[0] === 'pipelines' && parts[1] === 'new');
  const isDesigner = parts[0] === 'designer' || parts[0] === 'editor';
  const contextual = ['issues', 'dlq', 'connections'].includes(parts[0]) || (parts[0] === 'pipelines' && !!parts[1] && !isWizard);
  const allowed = contextual ? Object.keys(CONTEXT_FIELDS) : isWizard ? ['step'] : isDesigner ? ['edit'] : [];
  for (const parameter of qs.keys()) {
    if (!allowed.includes(parameter) || qs.getAll(parameter).length !== 1) return invalid;
  }
  const context = readTaskContext(qs);
  if (!context) return invalid;
  const task = Object.keys(context).length ? context : undefined;
  if (parts[0] === 'dlq' && !context.pipelineId && (context.recoveryId || context.rowId || context.nodeId || context.connectionName)) return invalid;
  if (isWizard && qs.has('step') && !/^[A-Za-z0-9_-]{1,64}$/.test(qs.get('step') || '')) return invalid;
  if (isDesigner && qs.has('edit') && !validEntityKey(qs.get('edit') || '', 128)) return invalid;
  if (parts[0] === 'invalid-context') return invalid;

  if (parts[0] === 'overview' || parts[0] === 'dashboard') return { page: 'dashboard' };
  if (parts[0] === 'pipelines') {
    if (parts[1] === 'new') return { page: 'pipeline-new', step: qs.get('step') || undefined };
    if (parts[1]) {
      const tab = (DETAIL_TABS.includes(parts[2] as DetailTab) ? parts[2] : 'overview') as DetailTab;
      let id: string;
      try { id = decodeURIComponent(parts[1]); } catch { return invalid; }
      if (!validEntityKey(id, 128) || (context.pipelineId && context.pipelineId !== id.toLowerCase())) return invalid;
      return { page: 'pipeline-detail', id, tab, ...(task ? { context: task } : {}) };
    }
    return { page: 'pipelines' };
  }
  if (parts[0] === 'issues') return { page: 'issues', ...(task ? { context: task } : {}) };
  if (parts[0] === 'dlq') return { page: 'dlq', ...(task ? { context: task } : {}) };
  if (parts[0] === 'connections') return { page: 'connections', ...(task ? { context: task } : {}) };
  if (parts[0] === 'connectors') return { page: 'connectors' };
  if (parts[0] === 'designer' || parts[0] === 'editor') {
    return { page: 'designer', editTarget: qs.get('edit') || undefined };
  }
  if (parts[0] === 'plugins') return { page: 'plugins' };
  if (parts[0] === 'extensions' || parts[0] === 'my-plugins' || parts[0] === 'myPlugins') {
    return { page: 'myPlugins' };
  }
  if (parts[0] === 'workers' || parts[0] === 'cluster') return { page: 'workers' };
  if (parts[0] === 'schedules') return { page: 'schedules' };
  if (parts[0] === 'audit') return { page: 'audit' };
  if (parts[0] === 'settings') return { page: 'settings' };

  // legacy flat ids
  const legacy = parts[0] as NavPage;
  if (
    [
      'dashboard',
      'pipelines',
      'issues',
      'dlq',
      'connections',
      'connectors',
      'designer',
      'plugins',
      'myPlugins',
      'workers',
      'schedules',
      'audit',
      'settings',
    ].includes(legacy)
  ) {
    return { page: legacy } as AppRoute;
  }
  // UI-A.4 (P2-7): legacy wizard alias and explicit not-found instead of a
  // silent dashboard fallback.
  if (parts[0] === 'pipeline-new') return { page: 'pipeline-new', step: qs.get('step') || undefined };
  return { page: 'not-found', path: parts[0] };
}

export function routeToHash(route: AppRoute): string {
  switch (route.page) {
    case 'dashboard':
      return '#/overview';
    case 'pipelines':
      return '#/pipelines';
    case 'pipeline-detail':
      return `#/pipelines/${encodeURIComponent(route.id)}/${route.tab}${taskQuery(route.context)}`;
    case 'pipeline-new':
      return route.step ? `#/pipelines/new?step=${encodeURIComponent(route.step)}` : '#/pipelines/new';
    case 'issues':
      return '#/issues' + taskQuery(route.context);
    case 'dlq':
      return '#/dlq' + taskQuery(route.context);
    case 'connections':
      return '#/connections' + taskQuery(route.context);
    case 'not-found':
      return '#/invalid-context';
    case 'connectors':
      return '#/connectors';
    case 'designer':
      return route.editTarget
        ? `#/designer?edit=${encodeURIComponent(route.editTarget)}`
        : '#/designer';
    case 'plugins':
      return '#/plugins';
    case 'myPlugins':
      return '#/extensions';
    case 'workers':
      return '#/cluster';
    case 'schedules':
      return '#/schedules';
    case 'audit':
      return '#/audit';
    case 'settings':
      return '#/settings';
    default:
      return '#/overview';
  }
}

export function navigate(route: AppRoute) {
  const next = routeToHash(route);
  if (window.location.hash !== next) {
    window.location.hash = next;
  } else {
    // force listeners when same hash (e.g. re-open same page)
    window.dispatchEvent(new HashChangeEvent('hashchange'));
  }
}

export function routeToNavPage(route: AppRoute): NavPage {
  if (route.page === 'pipeline-detail' || route.page === 'pipeline-new') return 'pipelines';
  return route.page as NavPage;
}

export function useHashRoute(): [AppRoute, (r: AppRoute) => void] {
  // lazy import pattern avoided; consumer wires useState/useEffect
  throw new Error('use useHashRouteState from routing-hooks or wire parseHash/navigate manually');
}

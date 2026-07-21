/** Lightweight hash router for shareable UI contexts (no react-router dependency). */

export type AppRoute =
  | { page: 'dashboard' }
  | { page: 'pipelines' }
  | { page: 'pipeline-detail'; id: string; tab: DetailTab }
  | { page: 'pipeline-new'; step?: string }
  | { page: 'issues' }
  | { page: 'dlq' }
  | { page: 'connections' }
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
  'spec',
];

export function parseHash(hash = window.location.hash): AppRoute {
  const raw = (hash || '').replace(/^#\/?/, '').trim();
  if (!raw) return { page: 'dashboard' };
  const [pathPart, queryPart] = raw.split('?');
  const parts = pathPart.split('/').filter(Boolean);
  const qs = new URLSearchParams(queryPart || '');

  if (parts[0] === 'overview' || parts[0] === 'dashboard') return { page: 'dashboard' };
  if (parts[0] === 'pipelines') {
    if (parts[1] === 'new') return { page: 'pipeline-new', step: qs.get('step') || undefined };
    if (parts[1]) {
      const tab = (DETAIL_TABS.includes(parts[2] as DetailTab) ? parts[2] : 'overview') as DetailTab;
      return { page: 'pipeline-detail', id: decodeURIComponent(parts[1]), tab };
    }
    return { page: 'pipelines' };
  }
  if (parts[0] === 'issues') return { page: 'issues' };
  if (parts[0] === 'dlq') return { page: 'dlq' };
  if (parts[0] === 'connections') return { page: 'connections' };
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
  return { page: 'dashboard' };
}

export function routeToHash(route: AppRoute): string {
  switch (route.page) {
    case 'dashboard':
      return '#/overview';
    case 'pipelines':
      return '#/pipelines';
    case 'pipeline-detail':
      return `#/pipelines/${encodeURIComponent(route.id)}/${route.tab}`;
    case 'pipeline-new':
      return route.step ? `#/pipelines/new?step=${encodeURIComponent(route.step)}` : '#/pipelines/new';
    case 'issues':
      return '#/issues';
    case 'dlq':
      return '#/dlq';
    case 'connections':
      return '#/connections';
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

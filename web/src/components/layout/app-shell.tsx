import React, { useState } from 'react';
import {
  Activity,
  AlertTriangle,
  Boxes,
  CalendarClock,
  ClipboardList,
  Database,
  GitBranch,
  LayoutDashboard,
  Menu,
  Moon,
  Package,
  Plus,
  RefreshCw,
  Search,
  Server,
  Settings,
  Sun,
  Trash2,
  User,
  Workflow,
  BookOpen,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Separator } from '@/components/ui/separator';
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { useTheme } from '@/components/theme-provider';
import { cn } from '@/lib/utils';
import type { NavPage } from '@/lib/routing';

export type AppPage = NavPage;

export type NavItem = {
  id: AppPage;
  key: string;
  badge?: number;
  /** Keep data-nav for e2e; may differ from id for aliases */
  dataNav?: string;
  hidden?: boolean;
};

export type NavGroup = {
  id: string;
  labelKey?: string;
  label?: string;
  items: NavItem[];
};

const PAGE_ICONS: Record<string, React.ComponentType<{ className?: string }>> = {
  dashboard: LayoutDashboard,
  pipelines: Activity,
  issues: AlertTriangle,
  connections: Database,
  connectors: BookOpen,
  designer: GitBranch,
  dlq: Trash2,
  plugins: Boxes,
  myPlugins: Package,
  workers: Server,
  schedules: CalendarClock,
  audit: ClipboardList,
  settings: Settings,
};

type AppShellProps = {
  title: string;
  subtitle: string;
  page: AppPage;
  pageTitle: string;
  /** Grouped nav (preferred). Falls back to flat navItems. */
  navGroups?: NavGroup[];
  navItems?: NavItem[];
  t: (key: string) => string;
  onNavigate: (page: AppPage) => void;
  onCreatePipeline?: () => void;
  onOpenSettings: () => void;
  onToggleLang: () => void;
  langLabel: string;
  onReloadSpecs: () => void;
  reloadLabel: string;
  autoRefreshLabel: string;
  autoRefreshTitle: string;
  autoRefreshPausedLabel: string;
  autoRefreshPausedTitle: string;
  autoRefreshFailedLabel: string;
  lastRefreshLabel: string;
  manualRefreshLabel: string;
  manualRefreshBusyLabel: string;
  manualRefreshBusy?: boolean;
  onManualRefresh?: () => void;
  autoPaused?: boolean;
  onToggleAutoRefresh?: () => void;
  lastRefreshAt?: string | null;
  lastRefreshFailed?: boolean;
  issueCount?: number;
  crumb?: string;
  topbarExtra?: React.ReactNode;
  children: React.ReactNode;
};

function flattenItems(groups?: NavGroup[], items?: NavItem[]): NavItem[] {
  if (groups?.length) return groups.flatMap((g) => g.items).filter((i) => !i.hidden);
  return (items || []).filter((i) => !i.hidden);
}

function NavList({
  navGroups,
  navItems,
  page,
  t,
  onNavigate,
  onOpenSettings,
  onCreatePipeline,
  onItemClick,
}: {
  navGroups?: NavGroup[];
  navItems?: NavItem[];
  page: AppPage;
  t: (key: string) => string;
  onNavigate: (page: AppPage) => void;
  onOpenSettings: () => void;
  onCreatePipeline?: () => void;
  onItemClick?: () => void;
}) {
  const groups: NavGroup[] =
    navGroups && navGroups.length
      ? navGroups
      : [{ id: 'main', items: navItems || [] }];

  return (
    <div className="flex h-full flex-col">
      <div className="px-3 pt-3">
        {onCreatePipeline && (
          <button
            type="button"
            data-nav="pipeline-new"
            data-testid="nav-create-pipeline"
            className="mb-2 flex w-full items-center justify-center gap-2 rounded-lg bg-primary px-3 py-2.5 text-sm font-semibold text-primary-foreground transition hover:opacity-90"
            onClick={() => {
              onCreatePipeline();
              onItemClick?.();
            }}
          >
            <Plus className="h-4 w-4" />
            <span>{t('nav.createPipeline')}</span>
          </button>
        )}
      </div>
      <ScrollArea className="flex-1 px-3 py-1">
        <nav className="space-y-0.5" aria-label={t('nav.mainNavigation')}>
          {groups.map((group) => (
            <div key={group.id}>
              {(group.labelKey || group.label) && (
                <div className="sidebar-group">
                  {group.labelKey ? t(group.labelKey) : group.label}
                </div>
              )}
              {group.items
                .filter((item) => !item.hidden)
                .map((item) => {
                  const IconComp = PAGE_ICONS[item.id] || Workflow;
                  const active = page === item.id;
                  const navId = item.dataNav || item.id;
                  return (
                    <button
                      key={item.id}
                      type="button"
                      data-nav={navId}
                      className={cn('sidebar-item w-full text-left', active && 'active')}
                      onClick={() => {
                        onNavigate(item.id);
                        onItemClick?.();
                      }}
                    >
                      <IconComp className="sidebar-icon h-4 w-4 shrink-0" />
                      <span className="truncate">{t(item.key)}</span>
                      {item.badge != null && item.badge > 0 && (
                        <span className="ml-auto min-w-[1.25rem] rounded-full bg-rose-600 px-1.5 py-0.5 text-center text-[11px] font-bold text-white tabular">
                          {item.badge > 99 ? '99+' : item.badge}
                        </span>
                      )}
                    </button>
                  );
                })}
            </div>
          ))}
          {/* Off-screen compatibility anchors for e2e data-nav lookups (designer/workers/…). */}
          <div className="pointer-events-none fixed -left-[9999px] top-0 h-px w-px overflow-hidden opacity-0" aria-hidden>
            {(['designer', 'workers', 'schedules', 'plugins', 'myPlugins'] as AppPage[]).map(
              (id) => (
                <button
                  key={`compat-${id}`}
                  type="button"
                  data-nav={id}
                  tabIndex={-1}
                  className="pointer-events-auto"
                  onClick={() => {
                    onNavigate(id);
                    onItemClick?.();
                  }}
                />
              ),
            )}
          </div>
        </nav>
      </ScrollArea>
      <div className="border-t border-white/10 p-3">
        <button
          type="button"
          data-nav="settings"
          className="sidebar-item w-full cursor-pointer text-left"
          onClick={() => {
            onOpenSettings();
            onItemClick?.();
          }}
        >
          <Settings className="h-4 w-4 shrink-0" strokeWidth={1.75} />
          <span>{t('nav.settings')}</span>
        </button>
      </div>
    </div>
  );
}

function BrandHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div className="flex h-16 items-center gap-2.5 border-b border-white/10 px-5">
      <div className="flex h-8 w-8 items-center justify-center rounded-[10px] bg-primary text-primary-foreground">
        <span className="text-sm font-black">O</span>
      </div>
      <div className="min-w-0">
        <div className="truncate text-sm font-bold text-white">{title}</div>
        <div className="truncate text-xs text-white/50">{subtitle}</div>
      </div>
    </div>
  );
}

export function AppShell({
  title,
  subtitle,
  page,
  pageTitle,
  navGroups,
  navItems,
  t,
  onNavigate,
  onCreatePipeline,
  onOpenSettings,
  onToggleLang,
  langLabel,
  onReloadSpecs,
  reloadLabel,
  autoRefreshLabel,
  autoRefreshTitle,
  autoRefreshPausedLabel,
  autoRefreshPausedTitle,
  autoRefreshFailedLabel,
  lastRefreshLabel,
  manualRefreshLabel,
  manualRefreshBusyLabel,
  manualRefreshBusy = false,
  onManualRefresh,
  autoPaused = false,
  onToggleAutoRefresh,
  lastRefreshAt = null,
  lastRefreshFailed = false,
  issueCount = 0,
  crumb,
  topbarExtra,
  children,
}: AppShellProps) {
  const { resolvedTheme, toggleTheme } = useTheme();
  const [mobileOpen, setMobileOpen] = useState(false);
  const items = flattenItems(navGroups, navItems);

  const mobilePrimary: AppPage[] = ['dashboard', 'pipelines', 'issues', 'settings']; // more via settings entry
  // UI-D.5: keep state, secondary recency, and the action to refresh distinct.
  // A successful refresh time must never masquerade as the control's primary
  // state label.
  const refreshStatusLabel = autoPaused
    ? autoRefreshPausedLabel
    : lastRefreshFailed
      ? autoRefreshFailedLabel
      : autoRefreshLabel;
  const refreshToggleTitle = autoPaused ? autoRefreshPausedTitle : autoRefreshTitle;
  const lastRefreshText = lastRefreshAt
    ? lastRefreshLabel.replace('{time}', lastRefreshAt)
    : '';

  return (
    <TooltipProvider delayDuration={300}>
      <div className="flex min-h-[100dvh] bg-background text-foreground">
        {/* Desktop sidebar */}
        <aside className="fixed inset-y-0 left-0 z-30 hidden w-[248px] flex-col bg-[hsl(var(--nav))] text-[hsl(var(--nav-foreground))] md:flex">
          <BrandHeader title={title} subtitle={subtitle} />
          <NavList
            navGroups={navGroups}
            navItems={navItems}
            page={page}
            t={t}
            onNavigate={onNavigate}
            onOpenSettings={onOpenSettings}
            onCreatePipeline={onCreatePipeline}
          />
        </aside>

        {/* Mobile sidebar */}
        <Sheet open={mobileOpen} onOpenChange={setMobileOpen}>
          <SheetContent side="left" className="w-72 bg-[hsl(var(--nav))] p-0 text-[hsl(var(--nav-foreground))]">
            <SheetHeader className="sr-only">
              <SheetTitle>{title}</SheetTitle>
            </SheetHeader>
            <BrandHeader title={title} subtitle={subtitle} />
            <NavList
              navGroups={navGroups}
              navItems={navItems}
              page={page}
              t={t}
              onNavigate={onNavigate}
              onOpenSettings={onOpenSettings}
              onCreatePipeline={onCreatePipeline}
              onItemClick={() => setMobileOpen(false)}
            />
          </SheetContent>
        </Sheet>

        {/* Main */}
        <div className="flex min-h-[100dvh] min-w-0 flex-1 flex-col md:ml-[248px]">
          {/* UI-D.3: header must wrap instead of forcing the page wide on
              small screens (390px) — the action group loses shrink-0 and the
              auto-refresh label truncates rather than expanding the document. */}
          <header className="sticky top-0 z-20 flex min-h-16 flex-wrap items-center justify-between gap-x-3 gap-y-1 border-b border-border bg-background/85 px-4 py-2 backdrop-blur md:h-16 md:flex-nowrap md:px-8 md:py-0">
            <div className="flex min-w-0 items-center gap-2">
              <Button
                variant="ghost"
                size="icon"
                className="md:hidden"
                onClick={() => setMobileOpen(true)}
                aria-label={t('top.openNavigation')}
              >
                <Menu className="h-5 w-5" />
              </Button>
              <div className="min-w-0">
                {crumb ? (
                  <div className="truncate text-xs text-muted-foreground">{crumb}</div>
                ) : null}
                <h1 className="truncate text-lg font-semibold tracking-tight text-foreground">
                  {pageTitle}
                </h1>
              </div>
            </div>

            <div className="flex min-w-0 flex-wrap items-center gap-2 sm:gap-3">
              {/* UI small-pool: global search has a visible affordance — Enter
              filters the pipelines list; Escape clears. */}
              <div className="relative hidden md:block">
                <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
                <Input
                  className="h-9 w-56 pl-8 text-xs lg:w-64"
                  placeholder={t('top.searchPlaceholder')}
                  aria-label={t('top.searchPlaceholder')}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      const q = (e.target as HTMLInputElement).value.trim();
                      if (q) window.location.hash = `#/pipelines?q=${encodeURIComponent(q)}`;
                      else window.location.hash = '#/pipelines';
                    } else if (e.key === 'Escape') {
                      (e.target as HTMLInputElement).value = '';
                    }
                  }}
                />
              </div>

              {issueCount > 0 && (
                <Button
                  variant="outline"
                  size="sm"
                  className="hidden text-rose-700 sm:inline-flex"
                  onClick={() => onNavigate('issues')}
                  aria-label={t('nav.issues')}
                >
                  <AlertTriangle className="h-3.5 w-3.5" />
                  {issueCount}
                </Button>
              )}

              {/* UI-A.4 (P2-1): dead notifications button removed until a real notification center exists. */}

              {/* UI-D.5: state toggle, last successful refresh, and manual
                  refresh are separate controls. The primary text is always a
                  state; recency is supporting information only. */}
              <button
                type="button"
                data-testid="auto-refresh-toggle"
                aria-pressed={autoPaused}
                aria-label={refreshToggleTitle}
                title={refreshToggleTitle}
                onClick={() => onToggleAutoRefresh?.()}
                className="flex items-center gap-1.5 rounded px-1.5 py-1 text-xs text-muted-foreground hover:bg-muted"
              >
                <span
                  className={cn(
                    'status-dot',
                    lastRefreshFailed ? 'status-error' : autoPaused ? 'status-paused' : 'status-running',
                  )}
                  aria-hidden
                />
                <span data-testid="auto-refresh-label" className="max-w-[16vw] truncate sm:max-w-none">
                  {refreshStatusLabel}
                </span>
              </button>
              {lastRefreshText && (
                <span
                  data-testid="last-refresh-at"
                  className="hidden max-w-40 truncate text-[11px] text-muted-foreground lg:inline"
                  aria-live="polite"
                >
                  {lastRefreshText}
                </span>
              )}
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                data-testid="manual-refresh"
                aria-label={manualRefreshBusy ? manualRefreshBusyLabel : manualRefreshLabel}
                title={manualRefreshBusy ? manualRefreshBusyLabel : manualRefreshLabel}
                aria-busy={manualRefreshBusy}
                disabled={manualRefreshBusy}
                onClick={() => onManualRefresh?.()}
              >
                <RefreshCw className={cn('h-3.5 w-3.5', manualRefreshBusy && 'animate-spin')} />
              </Button>

              {/* Compact language toggle has a current-language accessible name. */}
              <Button
                variant="ghost"
                size="sm"
                onClick={onToggleLang}
                title={t('top.switchLanguage')}
                aria-label={t('top.switchLanguage')}
                data-testid="toggle-lang"
              >
                {langLabel}
              </Button>

              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="outline" size="sm" aria-label={t('top.userMenu')}>
                    <User className="h-3.5 w-3.5" />
                    <span className="hidden sm:inline">{t('top.userMenu')}</span>
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-52">
                  <DropdownMenuLabel>{t('top.userMenu')}</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={toggleTheme}>
                    {resolvedTheme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
                    {t('top.theme')}
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={onToggleLang}>
                    {t('top.language')}: {langLabel}
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={onReloadSpecs} data-testid="reload-specs">
                    <RefreshCw className="h-4 w-4" />
                    {reloadLabel}
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={onOpenSettings}>
                    <Settings className="h-4 w-4" />
                    {t('nav.settings')}
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>

              {/* Off-screen but clickable e2e anchors (menu items not always open) */}
              <button
                type="button"
                className="pointer-events-auto fixed -left-[9999px] top-0 h-px w-px opacity-0"
                onClick={onReloadSpecs}
                data-testid="reload-specs-anchor"
                aria-hidden
                tabIndex={-1}
              >
                {reloadLabel}
              </button>

              {topbarExtra}
            </div>
          </header>

          <Separator className="md:hidden" />

          <main className="flex-1 p-4 pb-24 md:p-8 md:pb-8">
            <div className="page-container">{children}</div>
          </main>

          {/* Mobile bottom nav */}
          <nav
            className="fixed inset-x-0 bottom-0 z-30 grid grid-cols-4 border-t border-border bg-card md:hidden"
            aria-label={t('nav.mobileNavigation')}
          >
            {mobilePrimary.map((id) => {
              const item = items.find((i) => i.id === id) || {
                id,
                key: `nav.${id}`,
              };
              const Icon = PAGE_ICONS[id] || Workflow;
              const active = page === id || (id === 'pipelines' && page === 'designer');
              return (
                <button
                  key={id}
                  type="button"
                  className={cn(
                    'flex flex-col items-center gap-0.5 py-2 text-[11px]',
                    active ? 'font-semibold text-primary' : 'text-muted-foreground',
                  )}
                  onClick={() => {
                    if (id === 'settings') onOpenSettings();
                    else onNavigate(id);
                  }}
                >
                  <Icon className="h-4 w-4" />
                  {t(item.key)}
                  {id === 'issues' && issueCount > 0 && (
                    <span className="sr-only">{issueCount} open</span>
                  )}
                </button>
              );
            })}
          </nav>
        </div>
      </div>
    </TooltipProvider>
  );
}

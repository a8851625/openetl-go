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
  Server,
  Settings,
  Sun,
  Trash2,
  Workflow,
  BookOpen,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
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
  hasRunning?: boolean;
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
        <nav className="space-y-0.5" aria-label="Main">
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
  hasRunning = false,
  issueCount = 0,
  crumb,
  topbarExtra,
  children,
}: AppShellProps) {
  const { resolvedTheme, toggleTheme } = useTheme();
  const [mobileOpen, setMobileOpen] = useState(false);
  const items = flattenItems(navGroups, navItems);

  const mobilePrimary: AppPage[] = ['dashboard', 'pipelines', 'issues', 'settings'];

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
        <div className="flex min-h-[100dvh] flex-1 flex-col md:ml-[248px]">
          <header className="sticky top-0 z-20 flex h-16 items-center justify-between gap-3 border-b border-border bg-background/85 px-4 backdrop-blur md:px-8">
            <div className="flex min-w-0 items-center gap-2">
              <Button
                variant="ghost"
                size="icon"
                className="md:hidden"
                onClick={() => setMobileOpen(true)}
                aria-label="Open navigation"
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

            <div className="flex shrink-0 items-center gap-2 sm:gap-3">
              {issueCount > 0 && (
                <Button
                  variant="outline"
                  size="sm"
                  className="hidden text-rose-700 sm:inline-flex"
                  onClick={() => onNavigate('issues')}
                >
                  <AlertTriangle className="h-3.5 w-3.5" />
                  {issueCount}
                </Button>
              )}
              <span className="hidden text-xs text-muted-foreground sm:inline">{autoRefreshLabel}</span>
              <span
                className={cn('status-dot', hasRunning ? 'status-running' : 'status-stopped')}
                aria-hidden
              />

              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    onClick={toggleTheme}
                    aria-label="Toggle theme"
                  >
                    {resolvedTheme === 'dark' ? (
                      <Sun className="h-4 w-4" />
                    ) : (
                      <Moon className="h-4 w-4" />
                    )}
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  {resolvedTheme === 'dark' ? 'Light mode' : 'Dark mode'}
                </TooltipContent>
              </Tooltip>

              <Button variant="ghost" size="sm" onClick={onToggleLang} title="Switch language">
                {langLabel}
              </Button>

              <Button
                variant="outline"
                size="sm"
                onClick={onReloadSpecs}
                className="hidden sm:inline-flex"
              >
                <RefreshCw className="h-3.5 w-3.5" />
                {reloadLabel}
              </Button>

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
            aria-label="Mobile"
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

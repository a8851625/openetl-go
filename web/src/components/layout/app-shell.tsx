import React, { useState } from 'react';
import {
  Activity,
  Boxes,
  CalendarClock,
  ClipboardList,
  Database,
  GitBranch,
  LayoutDashboard,
  Menu,
  Moon,
  Package,
  RefreshCw,
  Server,
  Settings,
  Sun,
  Trash2,
  Workflow,
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

export type AppPage =
  | 'dashboard'
  | 'pipelines'
  | 'designer'
  | 'dlq'
  | 'plugins'
  | 'audit'
  | 'workers'
  | 'myPlugins'
  | 'schedules'
  | 'connections';

export type NavItem = {
  id: AppPage;
  key: string;
  badge?: number;
};

const PAGE_ICONS: Record<AppPage, React.ComponentType<{ className?: string }>> = {
  dashboard: LayoutDashboard,
  pipelines: Activity,
  connections: Database,
  designer: GitBranch,
  dlq: Trash2,
  plugins: Boxes,
  myPlugins: Package,
  workers: Server,
  schedules: CalendarClock,
  audit: ClipboardList,
};

type AppShellProps = {
  title: string;
  subtitle: string;
  page: AppPage;
  pageTitle: string;
  navItems: NavItem[];
  t: (key: string) => string;
  onNavigate: (page: AppPage) => void;
  onOpenSettings: () => void;
  onToggleLang: () => void;
  langLabel: string;
  onReloadSpecs: () => void;
  reloadLabel: string;
  autoRefreshLabel: string;
  hasRunning?: boolean;
  topbarExtra?: React.ReactNode;
  children: React.ReactNode;
};

function NavList({
  navItems,
  page,
  t,
  onNavigate,
  onOpenSettings,
  onItemClick,
}: {
  navItems: NavItem[];
  page: AppPage;
  t: (key: string) => string;
  onNavigate: (page: AppPage) => void;
  onOpenSettings: () => void;
  onItemClick?: () => void;
}) {
  return (
    <div className="flex h-full flex-col">
      <ScrollArea className="flex-1 px-3 py-3">
        <nav className="space-y-1">
          {navItems.map((item) => {
            const IconComp = PAGE_ICONS[item.id] || Workflow;
            const active = page === item.id;
            return (
              <button
                key={item.id}
                type="button"
                data-nav={item.id}
                className={cn(
                  'sidebar-item w-full text-left',
                  active && 'active',
                )}
                onClick={() => {
                  onNavigate(item.id);
                  onItemClick?.();
                }}
              >
                <IconComp className="sidebar-icon h-4 w-4 shrink-0" />
                <span className="truncate">{t(item.key)}</span>
                {item.badge != null && item.badge > 0 && (
                  <span className="ml-auto rounded-full bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300">
                    {item.badge}
                  </span>
                )}
              </button>
            );
          })}
        </nav>
      </ScrollArea>
      <div className="border-t border-border p-3">
        <button
          type="button"
          data-nav="settings"
          className="sidebar-item w-full cursor-pointer text-left"
          onClick={() => {
            onOpenSettings();
            onItemClick?.();
          }}
        >
          <Settings className="h-4 w-4 shrink-0" />
          <span>{t('nav.settings')}</span>
        </button>
      </div>
    </div>
  );
}

function BrandHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div className="flex h-16 items-center gap-2.5 border-b border-border px-5">
      <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
        <Workflow className="h-4 w-4" />
      </div>
      <div className="min-w-0">
        <div className="truncate text-sm font-bold text-foreground">{title}</div>
        <div className="truncate text-xs text-muted-foreground">{subtitle}</div>
      </div>
    </div>
  );
}

export function AppShell({
  title,
  subtitle,
  page,
  pageTitle,
  navItems,
  t,
  onNavigate,
  onOpenSettings,
  onToggleLang,
  langLabel,
  onReloadSpecs,
  reloadLabel,
  autoRefreshLabel,
  hasRunning = false,
  topbarExtra,
  children,
}: AppShellProps) {
  const { resolvedTheme, toggleTheme } = useTheme();
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <TooltipProvider delayDuration={300}>
      <div className="flex min-h-screen bg-background text-foreground">
        {/* Desktop sidebar */}
        <aside className="fixed inset-y-0 left-0 z-30 hidden w-56 flex-col border-r border-border bg-card md:flex">
          <BrandHeader title={title} subtitle={subtitle} />
          <NavList
            navItems={navItems}
            page={page}
            t={t}
            onNavigate={onNavigate}
            onOpenSettings={onOpenSettings}
          />
        </aside>

        {/* Mobile sidebar (Sheet drawer) */}
        <Sheet open={mobileOpen} onOpenChange={setMobileOpen}>
          <SheetContent side="left" className="w-72 p-0">
            <SheetHeader className="sr-only">
              <SheetTitle>{title}</SheetTitle>
            </SheetHeader>
            <BrandHeader title={title} subtitle={subtitle} />
            <NavList
              navItems={navItems}
              page={page}
              t={t}
              onNavigate={onNavigate}
              onOpenSettings={onOpenSettings}
              onItemClick={() => setMobileOpen(false)}
            />
          </SheetContent>
        </Sheet>

        {/* Main column */}
        <div className="flex min-h-screen flex-1 flex-col md:ml-56">
          <header className="sticky top-0 z-20 flex h-16 items-center justify-between gap-3 border-b border-border bg-background/80 px-4 backdrop-blur md:px-8">
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
              <h1 className="truncate text-lg font-semibold text-foreground">{pageTitle}</h1>
            </div>

            <div className="flex shrink-0 items-center gap-2 sm:gap-3">
              <span className="hidden text-xs text-muted-foreground sm:inline">{autoRefreshLabel}</span>
              <span
                className={cn(
                  'status-dot',
                  hasRunning ? 'status-running' : 'status-stopped',
                )}
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

              <Button variant="outline" size="sm" onClick={onReloadSpecs} className="hidden sm:inline-flex">
                <RefreshCw className="h-3.5 w-3.5" />
                {reloadLabel}
              </Button>

              {topbarExtra}
            </div>
          </header>

          <Separator className="md:hidden" />

          <main className="flex-1 p-4 md:p-8">{children}</main>
        </div>
      </div>
    </TooltipProvider>
  );
}

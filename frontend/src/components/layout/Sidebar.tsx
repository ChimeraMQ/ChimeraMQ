import { NavLink } from 'react-router';
import { Menu, X, Server, Layers, Users, BookOpen, AlertTriangle, Zap, Cpu, Workflow, PanelLeftClose, PanelLeftOpen } from 'lucide-react';
import { cn } from '@/lib/utils';
import { ThemeToggle } from '@/components/shared/ThemeToggle';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Separator } from '@/components/ui/separator';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { Button } from '@/components/ui/button';
import { useRoutePrefetch } from '@/hooks/use-route-prefetch';
import { useAppState } from '@/stores/app-store';

const navItems = [
  { to: '/', label: 'Overview', icon: Zap },
  { to: '/topics', label: 'Topics', icon: Layers },
  { to: '/consumers', label: 'Consumers', icon: Users },
  { to: '/cluster', label: 'Cluster', icon: Server },
  { to: '/schemas', label: 'Schemas', icon: BookOpen },
  { to: '/dlq', label: 'DLQ', icon: AlertTriangle },
  { to: '/wasm', label: 'WASM', icon: Cpu },
  { to: '/processors', label: 'Processors', icon: Workflow },
];

function NavIcon({ icon: Icon, label, collapsed }: { icon: typeof Zap; label: string; collapsed: boolean }) {
  if (!collapsed) {
    return <Icon className="h-5 w-5 shrink-0" aria-hidden="true" />;
  }
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Icon className="h-5 w-5 shrink-0" aria-hidden="true" />
      </TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}

export function Sidebar({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { sidebarCollapsed, setSidebarCollapsed } = useAppState();

  return (
    <>
      {/* Mobile overlay */}
      {open && (
        <div
          className="fixed inset-0 z-40 bg-background/80 backdrop-blur-sm md:hidden"
          onClick={onClose}
          aria-hidden="true"
        />
      )}

      <aside
        className={cn(
          'fixed inset-y-0 left-0 z-40 border-r border-border bg-background transform -translate-x-full transition-all',
          sidebarCollapsed ? 'w-16 md:w-16' : 'w-64',
          !sidebarCollapsed && 'md:w-64',
          !open && 'md:translate-x-0',
          open && 'translate-x-0 md:w-64',
        )}
        aria-label="Sidebar"
      >
        <div className="flex h-16 items-center gap-3 border-b border-border px-3">
          <Zap className="h-6 w-6 text-accent shrink-0" aria-hidden="true" />
          {!sidebarCollapsed && (
            <div className="flex flex-col">
              <span className="text-base font-semibold tracking-tight text-foreground">ChimeraMQ</span>
              <span className="text-xs text-text-muted">Three Heads. One Binary.</span>
            </div>
          )}
          <div className="ml-auto flex items-center gap-1">
            {/* Collapse toggle — visible on md+ */}
            <Button
              variant="ghost"
              size="sm"
              className="hidden md:inline-flex h-8 w-8 p-0"
              onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
              aria-label={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            >
              {sidebarCollapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
            </Button>
            {/* Close button — visible on mobile */}
            <button
              className="md:hidden rounded-md p-2 hover:bg-background-muted"
              onClick={onClose}
              aria-label="Close sidebar"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        <ScrollArea className="h-[calc(100vh-4rem)]">
          <nav className="flex flex-col gap-1 p-2" aria-label="Main navigation">
            {navItems.map(({ to, label, icon: Icon }) => {
              const prefetch = useRoutePrefetch(to);
              return (
                <NavLink
                  key={to}
                  to={to}
                  end={to === '/'}
                  className={({ isActive }) =>
                    cn(
                      'flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                      'lg:px-3',
                      isActive
                        ? 'bg-accent/10 text-accent'
                        : 'text-text-secondary hover:bg-background-muted hover:text-foreground',
                    )
                  }
                  onClick={() => onClose()}
                  {...prefetch}
                >
                  <NavIcon icon={Icon} label={label} collapsed={sidebarCollapsed} />
                  <span className={cn('transition-opacity', sidebarCollapsed ? 'sr-only' : 'inline')}>{label}</span>
                </NavLink>
              );
            })}
          </nav>

          <Separator className="mx-2" />

          <div className="p-2">
            <div className={cn('rounded-md bg-background-muted px-3 py-2', sidebarCollapsed && 'hidden')}>
              <p className="text-xs font-medium text-text-secondary">ChimeraMQ</p>
              <p className="text-xs text-text-muted">v1.0.0-draft</p>
            </div>
          </div>
        </ScrollArea>
      </aside>
    </>
  );
}

export function Header({ onMenuClick }: { onMenuClick: () => void }) {
  return (
    <header className="sticky top-0 z-30 h-16 border-b border-border bg-background/80 backdrop-blur-sm flex items-center gap-4 px-4 sm:px-6 lg:px-8">
      <button
        className="md:hidden rounded-md p-2 hover:bg-background-muted"
        onClick={onMenuClick}
        aria-label="Open sidebar"
      >
        <Menu className="h-5 w-5" />
      </button>

      <div className="flex-1" />

      <ThemeToggle />
    </header>
  );
}

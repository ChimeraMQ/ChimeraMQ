import { useState } from 'react';
import { Outlet } from 'react-router';
import { Sidebar, Header } from '@/components/layout/Sidebar';
import { cn } from '@/lib/utils';
import { useAppState } from '@/stores/app-store';

export function AppLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const { sidebarCollapsed } = useAppState();

  return (
    <div className="min-h-screen bg-background text-foreground">
      <Sidebar open={sidebarOpen} onClose={() => setSidebarOpen(false)} />

      <div className={cn(sidebarCollapsed ? 'md:pl-16' : 'md:pl-64')}>
        <Header onMenuClick={() => setSidebarOpen(true)} />

        <main id="main-content" className="px-4 sm:px-6 lg:px-8 py-4 sm:py-6 lg:py-8" tabIndex={-1}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}

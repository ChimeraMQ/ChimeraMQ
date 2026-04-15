import { Routes, Route } from 'react-router';
import { Suspense, lazy } from 'react';
import { TooltipProvider } from '@/components/ui/tooltip';
import { Toaster } from 'sonner';
import { AppLayout } from '@/components/layout/AppLayout';
import { Skeleton } from '@/components/ui/skeleton';
import { ErrorBoundary } from '@/components/shared/ErrorBoundary';
import { SkipToContent } from '@/components/shared/SkipToContent';

const OverviewPage = lazy(() => import('@/pages/OverviewPage').then(m => ({ default: m.OverviewPage })));
const TopicsPage = lazy(() => import('@/pages/TopicsPage').then(m => ({ default: m.TopicsPage })));
const ConsumersPage = lazy(() => import('@/pages/ConsumersPage').then(m => ({ default: m.ConsumersPage })));
const ClusterPage = lazy(() => import('@/pages/ClusterPage').then(m => ({ default: m.ClusterPage })));
const SchemasPage = lazy(() => import('@/pages/SchemasPage').then(m => ({ default: m.SchemasPage })));
const DLQPage = lazy(() => import('@/pages/DLQPage').then(m => ({ default: m.DLQPage })));
const WASMPage = lazy(() => import('@/pages/WASMPage').then(m => ({ default: m.WASMPage })));
const ProcessorsPage = lazy(() => import('@/pages/ProcessorsPage').then(m => ({ default: m.ProcessorsPage })));

function PageFallback() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-8 w-48" />
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        {[1, 2, 3, 4].map((i) => (
          <Skeleton key={i} className="h-28 w-full" />
        ))}
      </div>
    </div>
  );
}

export function App() {
  return (
    <ErrorBoundary>
      <SkipToContent />
      <TooltipProvider delayDuration={150}>
        <Routes>
          <Route path="/" element={<AppLayout />}>
            <Route index element={<Suspense fallback={<PageFallback />}><OverviewPage /></Suspense>} />
            <Route path="topics" element={<Suspense fallback={<PageFallback />}><TopicsPage /></Suspense>} />
            <Route path="consumers" element={<Suspense fallback={<PageFallback />}><ConsumersPage /></Suspense>} />
            <Route path="cluster" element={<Suspense fallback={<PageFallback />}><ClusterPage /></Suspense>} />
            <Route path="schemas" element={<Suspense fallback={<PageFallback />}><SchemasPage /></Suspense>} />
            <Route path="dlq" element={<Suspense fallback={<PageFallback />}><DLQPage /></Suspense>} />
            <Route path="wasm" element={<Suspense fallback={<PageFallback />}><WASMPage /></Suspense>} />
            <Route path="processors" element={<Suspense fallback={<PageFallback />}><ProcessorsPage /></Suspense>} />
          </Route>
        </Routes>
        <Toaster position="bottom-right" expand={false} richColors closeButton visibleToasts={3} />
      </TooltipProvider>
    </ErrorBoundary>
  );
}

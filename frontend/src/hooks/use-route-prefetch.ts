import { useState } from 'react';

const routeImporters: Record<string, () => Promise<unknown>> = {
  '/': () => import('@/pages/OverviewPage'),
  '/topics': () => import('@/pages/TopicsPage'),
  '/consumers': () => import('@/pages/ConsumersPage'),
  '/cluster': () => import('@/pages/ClusterPage'),
  '/schemas': () => import('@/pages/SchemasPage'),
  '/dlq': () => import('@/pages/DLQPage'),
  '/wasm': () => import('@/pages/WASMPage'),
  '/processors': () => import('@/pages/ProcessorsPage'),
};

const loaded = new Set<string>();

/**
 * Prefetch a route's chunk on hover. Triggers a dynamic import to
 * download the code-split JS bundle before the user clicks.
 */
export function prefetchRoute(path: string) {
  if (loaded.has(path)) return;
  loaded.add(path);
  const importer = routeImporters[path];
  if (importer) importer();
}

/**
 * Hook that returns onMouseEnter/onMouseLeave handlers for prefetching.
 * Use on <a> or <NavLink> elements to preload route chunks on hover.
 */
export function useRoutePrefetch(path: string, delayMs = 80) {
  const [timer, setTimer] = useState<ReturnType<typeof setTimeout> | null>(null);

  return {
    onMouseEnter: () => {
      const t = setTimeout(() => prefetchRoute(path), delayMs);
      setTimer(t);
    },
    onMouseLeave: () => {
      if (timer) clearTimeout(timer);
      setTimer(null);
    },
  };
}

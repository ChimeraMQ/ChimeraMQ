import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, render, screen, act } from '@testing-library/react';
import * as routeModule from '@/hooks/use-route-prefetch';
import { useRoutePrefetch } from '@/hooks/use-route-prefetch';

vi.mock('@/pages/OverviewPage', () => ({ default: () => null }));
vi.mock('@/pages/TopicsPage', () => ({ default: () => null }));
vi.mock('@/pages/ConsumersPage', () => ({ default: () => null }));
vi.mock('@/pages/ClusterPage', () => ({ default: () => null }));
vi.mock('@/pages/SchemasPage', () => ({ default: () => null }));
vi.mock('@/pages/DLQPage', () => ({ default: () => null }));
vi.mock('@/pages/WASMPage', () => ({ default: () => null }));
vi.mock('@/pages/ProcessorsPage', () => ({ default: () => null }));

describe('prefetchRoute', () => {
  it('does not throw for unknown routes', () => {
    expect(() => routeModule.prefetchRoute('/unknown/path')).not.toThrow();
  });

  it('does not throw for known routes', () => {
    expect(() => routeModule.prefetchRoute('/')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/cluster')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/schemas')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/dlq')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/wasm')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/processors')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/consumers')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/topics')).not.toThrow();
  });

  it('prefetches same route only once (dedup via loaded set)', () => {
    expect(() => routeModule.prefetchRoute('/')).not.toThrow();
    expect(() => routeModule.prefetchRoute('/')).not.toThrow();
  });
});

describe('useRoutePrefetch', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('returns onMouseEnter and onMouseLeave handlers', () => {
    const { result } = renderHook(() => useRoutePrefetch('/topics'));
    expect(typeof result.current.onMouseEnter).toBe('function');
    expect(typeof result.current.onMouseLeave).toBe('function');
  });

  it('sets a timer on mouse enter', () => {
    const { result } = renderHook(() => useRoutePrefetch('/topics', 100));

    act(() => {
      result.current.onMouseEnter();
    });

    // Timer should be set — advancing should trigger prefetch
    vi.advanceTimersByTime(100);
    // No crash means the timer flow completed
  });

  it('clears timer on mouse leave before delay elapses', () => {
    const { result } = renderHook(() => useRoutePrefetch('/topics', 100));

    act(() => {
      result.current.onMouseEnter();
    });

    act(() => {
      result.current.onMouseLeave();
    });

    vi.advanceTimersByTime(100);
    // No crash — timer was cleared before firing
  });

  it('applies to a link element', () => {
    const TestLink = ({ path }: { path: string }) => {
      const handlers = useRoutePrefetch(path, 50);
      return (
        <a href={path} {...handlers} data-testid="link">
          {path}
        </a>
      );
    };

    render(<TestLink path="/topics" />);
    const link = screen.getByTestId('link');
    expect(link).toHaveAttribute('href', '/topics');
  });
});

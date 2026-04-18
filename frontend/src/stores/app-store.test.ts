import { describe, it, expect, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useAppState } from '@/stores/app-store';

describe('useAppState', () => {
  beforeEach(() => {
    // Reset state by getting fresh store
    const { result } = renderHook(() => useAppState());
    act(() => {
      result.current.setSidebarCollapsed(false);
    });
  });

  it('has default sidebarCollapsed state of false', () => {
    const { result } = renderHook(() => useAppState());
    expect(result.current.sidebarCollapsed).toBe(false);
  });

  it('sets sidebarCollapsed to true', () => {
    const { result } = renderHook(() => useAppState());

    act(() => {
      result.current.setSidebarCollapsed(true);
    });

    expect(result.current.sidebarCollapsed).toBe(true);
  });

  it('toggles sidebarCollapsed state', () => {
    const { result } = renderHook(() => useAppState());

    act(() => {
      result.current.setSidebarCollapsed(true);
    });
    expect(result.current.sidebarCollapsed).toBe(true);

    act(() => {
      result.current.setSidebarCollapsed(false);
    });
    expect(result.current.sidebarCollapsed).toBe(false);
  });
});

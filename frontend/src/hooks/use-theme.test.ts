import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { useTheme } from '@/hooks/use-theme';

const setupMatchMedia = (matches: boolean) => {
  vi.stubGlobal('matchMedia', vi.fn((query) => ({
    matches,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })));
};

describe('useTheme', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove('light', 'dark');
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('defaults to system theme', () => {
    setupMatchMedia(false);
    const { result } = renderHook(() => useTheme());
    expect(result.current.theme).toBe('system');
  });

  it('applies light class when system preference is light', () => {
    setupMatchMedia(false);
    renderHook(() => useTheme());
    expect(document.documentElement.classList.contains('light')).toBe(true);
  });

  it('applies dark class when system preference is dark', () => {
    setupMatchMedia(true);
    renderHook(() => useTheme());
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('applies explicit dark theme', async () => {
    setupMatchMedia(false);
    const { result } = renderHook(() => useTheme());
    result.current.setTheme('dark');
    await waitFor(() => {
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });
  });

  it('applies explicit light theme', async () => {
    setupMatchMedia(true);
    const { result } = renderHook(() => useTheme());
    result.current.setTheme('light');
    await waitFor(() => {
      expect(document.documentElement.classList.contains('light')).toBe(true);
    });
  });
});

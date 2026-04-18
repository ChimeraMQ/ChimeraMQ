import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useLocalStorage } from '@/hooks/use-local-storage';

describe('useLocalStorage', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it('returns initial value when localStorage is empty', () => {
    const { result } = renderHook(() => useLocalStorage('theme', 'light'));
    expect(result.current[0]).toBe('light');
  });

  it('reads existing value from localStorage', () => {
    localStorage.setItem('theme', '"dark"');
    const { result } = renderHook(() => useLocalStorage('theme', 'light'));
    expect(result.current[0]).toBe('dark');
  });

  it('writes value to localStorage', () => {
    const { result } = renderHook(() => useLocalStorage('theme', 'light'));

    act(() => {
      result.current[1]('dark');
    });

    expect(localStorage.getItem('theme')).toBe('"dark"');
    expect(result.current[0]).toBe('dark');
  });

  it('supports function updater', () => {
    const { result } = renderHook(() => useLocalStorage('count', 0));

    act(() => {
      result.current[1]((prev) => prev + 1);
    });

    expect(result.current[0]).toBe(1);
    expect(localStorage.getItem('count')).toBe('1');
  });

  it('handles invalid JSON in localStorage gracefully', () => {
    localStorage.setItem('config', 'not-json');
    const { result } = renderHook(() => useLocalStorage('config', { key: 'default' }));
    expect(result.current[0]).toEqual({ key: 'default' });
  });

  it('handles localStorage being unavailable', () => {
    const getItem = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('unavailable');
    });

    const { result } = renderHook(() => useLocalStorage('test', 'fallback'));
    expect(result.current[0]).toBe('fallback');

    getItem.mockRestore();
  });

  it('works with object values', () => {
    const { result } = renderHook(() => useLocalStorage('user', { name: '', age: 0 }));

    act(() => {
      result.current[1]({ name: 'Alice', age: 30 });
    });

    expect(result.current[0]).toEqual({ name: 'Alice', age: 30 });
  });
});

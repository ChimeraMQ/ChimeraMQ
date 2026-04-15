import { useCallback, useEffect } from 'react';
import { useLocalStorage } from './use-local-storage';

export type Theme = 'light' | 'dark' | 'system';

const STORAGE_KEY = 'chimera-theme';

function applyTheme(theme: Theme) {
  const html = document.documentElement;
  html.classList.remove('light', 'dark');

  if (theme === 'system') {
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    html.classList.add(prefersDark ? 'dark' : 'light');
  } else {
    html.classList.add(theme);
  }
}

export function useTheme() {
  const [theme, setTheme] = useLocalStorage<Theme>(STORAGE_KEY, 'system');

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  useEffect(() => {
    if (theme !== 'system') return;

    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = () => applyTheme('system');
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, [theme]);

  const setThemeCallback = useCallback((t: Theme) => setTheme(t), [setTheme]);

  return { theme, setTheme: setThemeCallback };
}

import { create } from 'zustand';

interface AppState {
  sidebarCollapsed: boolean;
  setSidebarCollapsed: (collapsed: boolean) => void;
}

/**
 * Global UI state managed with Zustand.
 * Stores preferences that persist across route navigation.
 */
export const useAppState = create<AppState>((set) => ({
  sidebarCollapsed: false,
  setSidebarCollapsed: (collapsed) => set({ sidebarCollapsed: collapsed }),
}));

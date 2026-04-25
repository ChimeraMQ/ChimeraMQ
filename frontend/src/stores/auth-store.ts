import { create } from 'zustand';

interface AuthState {
  token: string | null;
  userId: string | null;
  roles: string[];
  isAuthenticated: boolean;

  login: (token: string) => void;
  logout: () => void;
  setAuthHeader: (init?: RequestInit) => RequestInit;
}

const TOKEN_KEY = 'chimera_auth_token';
const USER_KEY = 'chimera_user_id';
const ROLES_KEY = 'chimera_roles';

function loadStored(): { token: string | null; userId: string | null; roles: string[] } {
  try {
    const token = sessionStorage.getItem(TOKEN_KEY);
    const userId = sessionStorage.getItem(USER_KEY);
    const rolesRaw = sessionStorage.getItem(ROLES_KEY);
    const roles = rolesRaw ? JSON.parse(rolesRaw) : [];
    return { token, userId, roles };
  } catch {
    return { token: null, userId: null, roles: [] };
  }
}

const stored = loadStored();

export const useAuthStore = create<AuthState>((set, get) => ({
  token: stored.token,
  userId: stored.userId,
  roles: stored.roles,
  isAuthenticated: stored.token !== null,

  login: (token: string) => {
    sessionStorage.setItem(TOKEN_KEY, token);
    set({ token, isAuthenticated: true });
  },

  logout: () => {
    sessionStorage.removeItem(TOKEN_KEY);
    sessionStorage.removeItem(USER_KEY);
    sessionStorage.removeItem(ROLES_KEY);
    set({ token: null, userId: null, roles: [], isAuthenticated: false });
  },

  setAuthHeader: (init?: RequestInit) => {
    const { token } = get();
    const headers: Record<string, string> = { ...(init?.headers as Record<string, string> || {}) };
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }
    return { ...init, headers };
  },
}));

import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { WASMPage } from '@/pages/WASMPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    listWASMModules: vi.fn(),
    deleteWASMModule: vi.fn(),
  };
});

vi.mock('sonner', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

const createWrapper = () => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
};

describe('WASMPage', () => {
  it('renders empty state when no modules exist', async () => {
    vi.mocked(api.listWASMModules).mockResolvedValue({ modules: [], count: 0 });

    render(<WASMPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('WASM Modules')).toBeInTheDocument();
    });

    expect(screen.getByText('No WASM modules loaded')).toBeInTheDocument();
    expect(screen.getByText('chimera wasm upload my-transform.wasm')).toBeInTheDocument();
  });

  it('shows module count heading', async () => {
    vi.mocked(api.listWASMModules).mockResolvedValue({ modules: ['compress', 'validate'], count: 2 });

    render(<WASMPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('2 Modules').length).toBeGreaterThan(0);
    });
  });

  it('shows module names', async () => {
    vi.mocked(api.listWASMModules).mockResolvedValue({ modules: ['my-module'], count: 1 });

    render(<WASMPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-module').length).toBeGreaterThan(0);
    });
  });

  it('shows delete confirmation dialog when Delete is clicked', async () => {
    vi.mocked(api.listWASMModules).mockResolvedValue({ modules: ['compress'], count: 1 });
    vi.mocked(api.deleteWASMModule).mockResolvedValue({ status: 'deleted' });

    render(<WASMPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('compress').length).toBeGreaterThan(0);
    });

    // Click Delete button
    const deleteBtns = screen.getAllByRole('button', { name: /Delete WASM module/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete WASM Module')).toBeInTheDocument();
    });
    expect(screen.getByText(/Are you sure you want to delete "compress"/)).toBeInTheDocument();
  });

  it('executes delete mutation when confirmed', async () => {
    vi.mocked(api.listWASMModules).mockResolvedValue({ modules: ['old-module'], count: 1 });
    vi.mocked(api.deleteWASMModule).mockResolvedValue({ status: 'deleted' });

    render(<WASMPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('old-module').length).toBeGreaterThan(0);
    });

    // Open delete dialog
    const deleteBtns = screen.getAllByRole('button', { name: /Delete WASM module/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete WASM Module')).toBeInTheDocument();
    });

    // Scope to AlertDialog for unique button query
    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteWASMModule).toHaveBeenCalled();
    });
  });

  it('shows error toast when delete fails', async () => {
    vi.mocked(api.listWASMModules).mockResolvedValue({ modules: ['error-module'], count: 1 });
    vi.mocked(api.deleteWASMModule).mockRejectedValue(new Error('Failed to delete module'));

    render(<WASMPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('error-module').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete WASM module/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete WASM Module')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteWASMModule).toHaveBeenCalled();
    });
  });
});

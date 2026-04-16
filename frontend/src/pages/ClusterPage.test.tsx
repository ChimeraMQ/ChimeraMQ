import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ClusterPage } from '@/pages/ClusterPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    getCluster: vi.fn(),
    getHealth: vi.fn(),
  };
});

const createWrapper = () => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
};

describe('ClusterPage', () => {
  it('renders single-node mode message', async () => {
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });
    vi.mocked(api.getHealth).mockResolvedValue({ status: 'healthy', node_id: 1, name: 'node-1', version: '0.1.0', uptime: '1h' });

    render(<ClusterPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Single-Node Mode')).toBeInTheDocument();
    });
    expect(screen.getByText(/This broker is running standalone without clustering/)).toBeInTheDocument();
  });

  it('shows cluster mode heading', async () => {
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'cluster', alive_count: 2, members: [] });
    vi.mocked(api.getHealth).mockResolvedValue({ status: 'healthy', node_id: 1, name: 'node-1', version: '0.1.0', uptime: '3h' });

    render(<ClusterPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Clustered mode')).toBeInTheDocument();
    });
  });

  it('shows member count in cluster mode', async () => {
    vi.mocked(api.getCluster).mockResolvedValue({
      mode: 'cluster',
      leader_id: 'leader-1',
      alive_count: 5,
      members: [{ id: 'node-1', addr: '10.0.0.1', port: 5672, state: 'Alive', incarnation: 1 }],
    });
    vi.mocked(api.getHealth).mockResolvedValue({ status: 'healthy', node_id: 1, name: 'node-1', version: '0.1.0', uptime: '2h' });

    render(<ClusterPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('5')).toBeInTheDocument();
    });
    expect(screen.getAllByText('leader-1').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Alive').length).toBeGreaterThan(0);
  });
});

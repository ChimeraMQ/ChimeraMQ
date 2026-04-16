import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { OverviewPage } from '@/pages/OverviewPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    getHealth: vi.fn(),
    getTopics: vi.fn(),
    getConsumers: vi.fn(),
    getCluster: vi.fn(),
  };
});

const createWrapper = () => {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
};

describe('OverviewPage', () => {
  it('renders stats cards after data loads', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'chimera-01',
      version: '0.1.0',
      uptime: '1h 23m',
      mode: 'single-node',
      storage: { hot_size_bytes: 50 * 1024 * 1024, partitions: 8 },
    });
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16T10:00:00Z' },
      { name: 'events', mode: 'stream', partitions: 8, created_at: '2026-04-16T11:00:00Z' },
    ]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['order-processor'], count: 1 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Overview')).toBeInTheDocument();
    });

    expect(screen.getByText('Topics')).toBeInTheDocument();
    expect(screen.getByText('Consumer Groups')).toBeInTheDocument();
  });

  it('shows node name and version from health response', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'chimera-01',
      version: '0.1.0',
      uptime: '1h 23m',
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText(/chimera-01/)).toBeInTheDocument();
    });
  });

  it('shows healthy status badge', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('healthy')).toBeInTheDocument();
    });
  });
});

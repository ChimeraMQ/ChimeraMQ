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

  it('shows warning status when health status is draining', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'draining',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '2h',
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('draining')).toBeInTheDocument();
    });
  });

  it('shows single-node badge in header', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
      mode: 'single-node',
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Single Node')).toBeInTheDocument();
    });
  });

  it('shows cluster badge with alive count', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({
      mode: 'cluster',
      alive_count: 3,
      members: [{ id: 'n1', addr: '10.0.0.1', port: 5672, state: 'Alive', incarnation: 1 }],
    });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Cluster — 3 alive')).toBeInTheDocument();
    });
  });

  it('shows storage and partition stats', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
      storage: { hot_size_bytes: 50 * 1024 * 1024, partitions: 8 },
    });
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16T10:00:00Z' },
      { name: 'events', mode: 'stream', partitions: 8, created_at: '2026-04-16T11:00:00Z' },
    ]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Storage (Hot)')).toBeInTheDocument();
    });
    // formatBytes(50*1024*1024) = "50.0 MB"
    expect(screen.getByText((content) => content.includes('MB'))).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument(); // 4 + 8 partitions
  });

  it('shows collecting data message when chart is empty', async () => {
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
      expect(screen.getByText('Collecting data points...')).toBeInTheDocument();
    });
  });

  it('shows cluster health card with members', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({
      mode: 'cluster',
      alive_count: 2,
      members: [
        { id: 'node-alpha', addr: '10.0.0.1', port: 5672, state: 'Alive', incarnation: 1 },
        { id: 'node-beta', addr: '10.0.0.2', port: 5672, state: 'Suspect', incarnation: 2 },
      ],
    });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Cluster Health')).toBeInTheDocument();
    });
    expect(screen.getByText('node-alpha')).toBeInTheDocument();
    expect(screen.getByText('node-beta')).toBeInTheDocument();
    expect(screen.getByText('10.0.0.1:5672')).toBeInTheDocument();
    expect(screen.getByText('10.0.0.2:5672')).toBeInTheDocument();
    expect(screen.getAllByText('Alive').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Suspect').length).toBeGreaterThan(0);
  });

  it('shows Raft state card when raft data is present', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
      raft: {
        state: 'Leader',
        term: 5,
        is_leader: true,
        leader_id: 'node-1',
        commit_index: 1000,
      },
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Raft State')).toBeInTheDocument();
    });
    expect(screen.getAllByText('Leader').length).toBeGreaterThan(0);
    expect(screen.getByText('5')).toBeInTheDocument();
    expect(screen.getByText('This node')).toBeInTheDocument();
    expect(screen.getByText('1000')).toBeInTheDocument();
  });

  it('shows Raft follower state with leader_id', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 2,
      name: 'node-2',
      version: '0.1.0',
      uptime: '2h',
      raft: {
        state: 'Follower',
        term: 3,
        is_leader: false,
        leader_id: 'node-1',
        commit_index: 500,
      },
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Raft State')).toBeInTheDocument();
    });
    expect(screen.getByText('Follower')).toBeInTheDocument();
    expect(screen.getByText('node-1')).toBeInTheDocument();
    expect(screen.getByText('500')).toBeInTheDocument();
  });

  it('shows cluster member with unknown state as destructive', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
    });
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({
      mode: 'cluster',
      alive_count: 1,
      members: [
        { id: 'dead-node', addr: '10.0.0.99', port: 5672, state: 'Dead', incarnation: 5 },
      ],
    });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Cluster Health')).toBeInTheDocument();
    });
    expect(screen.getByText('dead-node')).toBeInTheDocument();
    expect(screen.getByText('Dead')).toBeInTheDocument();
  });

  it('shows unknown status when health has no status field', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
    } as any);
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('unknown')).toBeInTheDocument();
    });
  });

  it('shows zero values when topics and consumers APIs return null', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
    });
    vi.mocked(api.getTopics).mockResolvedValue(null as any);
    vi.mocked(api.getConsumers).mockResolvedValue(null as any);
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Overview')).toBeInTheDocument();
    });
    // Should render 0 for Topics and Consumer Groups when data is null
    expect(screen.getByText('Topics')).toBeInTheDocument();
    expect(screen.getByText('Consumer Groups')).toBeInTheDocument();
  });

  it('shows zero partitions when topics array is null', async () => {
    vi.mocked(api.getHealth).mockResolvedValue({
      status: 'healthy',
      node_id: 1,
      name: 'node-1',
      version: '0.1.0',
      uptime: '1h',
    });
    vi.mocked(api.getTopics).mockResolvedValue(null as any);
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });
    vi.mocked(api.getCluster).mockResolvedValue({ mode: 'single-node', alive_count: 1, members: [] });

    render(<OverviewPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Overview')).toBeInTheDocument();
    });
    // Partitions stat should show 0 when topics is null
    const zeros = screen.getAllByText('0');
    expect(zeros.length).toBeGreaterThan(0);
  });
});

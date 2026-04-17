import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor, within, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConsumersPage } from '@/pages/ConsumersPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    getConsumers: vi.fn(),
    getConsumerGroupDetail: vi.fn(),
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

describe('ConsumersPage', () => {
  it('renders empty state when no consumer groups', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: [], count: 0 });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Consumers')).toBeInTheDocument();
    });

    expect(screen.getByText('No consumer groups')).toBeInTheDocument();
  });

  it('shows consumer group count', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['group-a', 'group-b'], count: 2 });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Consumer Groups')).toBeInTheDocument();
    });
  });

  it('shows group names in the list', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['my-consumer-group'], count: 1 });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-consumer-group').length).toBeGreaterThan(0);
    });
  });

  it('shows consumer group detail dialog when View is clicked', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['order-processor'], count: 1 });
    vi.mocked(api.getConsumerGroupDetail).mockResolvedValue({
      group: 'order-processor',
      state: 'Stable',
      members: [
        { id: 'consumer-1', partitions: [0, 1] },
        { id: 'consumer-2', partitions: [2, 3] },
      ],
      assignments: {
        'consumer-1': [0, 1],
        'consumer-2': [2, 3],
      },
    });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('order-processor').length).toBeGreaterThan(0);
    });

    // Click View button
    const viewBtns = screen.getAllByRole('button', { name: /View consumer group/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('2 member(s), 2 assignment(s)')).toBeInTheDocument();
    });
    expect(screen.getAllByText('consumer-1').length).toBeGreaterThan(0);
    expect(screen.getByText('Partition Assignments')).toBeInTheDocument();
  });

  it('shows no results when filtering produces no matches', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['orders-group'], count: 1 });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-group').length).toBeGreaterThan(0);
    });

    // Type a filter that won't match
    const searchInput = screen.getByPlaceholderText('Filter groups...');
    await userEvent.type(searchInput, 'nonexistent');

    await waitFor(() => {
      expect(screen.getByText(/No groups matching/)).toBeInTheDocument();
    });
  });

  it('closes consumer group detail dialog when Close is clicked', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['unique-cg'], count: 1 });
    vi.mocked(api.getConsumerGroupDetail).mockResolvedValue({
      group: 'unique-cg',
      state: 'Stable',
      members: [],
      assignments: {},
    });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('unique-cg').length).toBeGreaterThan(0);
    });

    // Open detail dialog
    const viewBtns = screen.getAllByRole('button', { name: /View consumer group/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('0 member(s), 0 assignment(s)')).toBeInTheDocument();
    });

    // Close the dialog - there are two Close buttons (X icon and footer button), pick one
    const closeButtons = screen.getAllByRole('button', { name: /Close/i });
    await userEvent.click(closeButtons[0]);

    // Dialog should close - detail text should disappear
    await waitFor(() => {
      expect(screen.queryByText('0 member(s), 0 assignment(s)')).not.toBeInTheDocument();
    });
  });

  it('shows no active members when consumer group has zero members', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['empty-group'], count: 1 });
    vi.mocked(api.getConsumerGroupDetail).mockResolvedValue({
      group: 'empty-group',
      state: 'Empty',
      members: [],
      assignments: {},
    });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('empty-group').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View consumer group/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('No active members')).toBeInTheDocument();
    });
  });

  it('shows no groups matching filters when search returns no results', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['my-group'], count: 1 });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-group').length).toBeGreaterThan(0);
    });

    const searchInput = screen.getByPlaceholderText('Filter groups...');
    await userEvent.type(searchInput, 'zzzznotexist');

    await waitFor(() => {
      expect(screen.getByText(/No groups matching/)).toBeInTheDocument();
    });
  });

  it('handles API failure when loading consumer groups', async () => {
    vi.mocked(api.getConsumers).mockRejectedValue(new Error('Network error'));

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Consumers')).toBeInTheDocument();
    });
    // Page renders gracefully - counts are visible
    expect(document.body).toHaveTextContent('0');
  });

  it('shows group detail with member assignments in dialog', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['assign-group'], count: 1 });
    vi.mocked(api.getConsumerGroupDetail).mockResolvedValue({
      group: 'assign-group',
      state: 'Stable',
      members: [
        { id: 'member-1', partitions: [0, 2] },
        { id: 'member-2', partitions: [1, 3] },
      ],
      assignments: {
        'member-1': [0, 2],
        'member-2': [1, 3],
      },
    });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('assign-group').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View consumer group assign-group/ });
    expect(viewBtns.length).toBeGreaterThan(0);
    await userEvent.click(viewBtns[1]); // desktop button

    await waitFor(() => {
      expect(screen.getByText('2 member(s), 2 assignment(s)')).toBeInTheDocument();
    });
    expect(screen.getAllByText('member-1').length).toBeGreaterThan(0);
    expect(screen.getAllByText('member-2').length).toBeGreaterThan(0);
  });

  it('closes group detail dialog via Escape key (onOpenChange path)', async () => {
    vi.mocked(api.getConsumers).mockResolvedValue({ groups: ['escape-group'], count: 1 });
    vi.mocked(api.getConsumerGroupDetail).mockResolvedValue({
      group: 'escape-group',
      state: 'Empty',
      members: [{ id: 'member-1', partitions: [0, 1] }],
      assignments: { 'member-1': [0, 1] },
    });

    render(<ConsumersPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('escape-group').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View consumer group escape-group/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('1 member(s), 1 assignment(s)')).toBeInTheDocument();
    });

    // Press Escape to trigger onOpenChange(false)
    fireEvent.keyDown(document, { key: 'Escape' });

    await waitFor(() => {
      expect(screen.queryByText('1 member(s), 1 assignment(s)')).not.toBeInTheDocument();
    });
  });
});

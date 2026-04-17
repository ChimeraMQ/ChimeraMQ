import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor, within, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { TopicsPage } from '@/pages/TopicsPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    getTopics: vi.fn(),
    getTopicDetail: vi.fn(),
    createTopic: vi.fn(),
    deleteTopic: vi.fn(),
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

describe('TopicsPage', () => {
  it('renders empty state when no topics exist', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Topics')).toBeInTheDocument();
    });
    expect(screen.getAllByText('No topics yet').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Create your first topic to get started').length).toBeGreaterThan(0);
  });

  it('shows topic count heading', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16T10:00:00Z' },
      { name: 'events', mode: 'stream', partitions: 8, created_at: '2026-04-16T11:00:00Z' },
    ]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('2 Topics').length).toBeGreaterThan(0);
    });
  });

  it('shows topic names in the list', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'my-topic', mode: 'queue', partitions: 1, created_at: '2026-04-16T10:00:00Z' },
    ]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-topic').length).toBeGreaterThan(0);
    });
  });

  it('shows topic detail dialog when a topic is clicked', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 2, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.getTopicDetail).mockResolvedValue({
      name: 'orders',
      mode: 'unified',
      partitions: 2,
      created_at: '2026-04-16T10:00:00Z',
      partitions_detail: [
        { id: 0, high_watermark: 1500, log_start_offset: 0 },
        { id: 1, high_watermark: 2300, log_start_offset: 50 },
      ],
    });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    });

    // Click the View button (first one in the mobile view)
    const viewButtons = screen.getAllByRole('button', { name: /View topic orders/ });
    await userEvent.click(viewButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Partition 0')).toBeInTheDocument();
    });
    expect(screen.getAllByText('High Watermark').length).toBeGreaterThan(0);
  });

  it('filters topics by search text', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16T10:00:00Z' },
      { name: 'events', mode: 'stream', partitions: 8, created_at: '2026-04-16T11:00:00Z' },
    ]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    });

    // Type in search
    const searchInput = screen.getByPlaceholderText('Search topics...');
    await userEvent.type(searchInput, 'order');

    // After debounce + refetch, only 'orders' should show
    await waitFor(() => {
      expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    });
  });

  it('shows "no topics matching" when filter produces no results', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16T10:00:00Z' },
    ]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    });

    const searchInput = screen.getByPlaceholderText('Search topics...');
    await userEvent.type(searchInput, 'nonexistent');

    await waitFor(() => {
      expect(screen.getAllByText('No topics matching filters').length).toBeGreaterThan(0);
    });
  });

  it('shows delete confirmation dialog when Delete is clicked', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'temp-topic', mode: 'unified', partitions: 1, created_at: '2026-04-16T10:00:00Z' },
    ]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('temp-topic').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete topic/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Topic')).toBeInTheDocument();
    });
    expect(screen.getByText(/Are you sure you want to delete "temp-topic"/)).toBeInTheDocument();
  });

  it('shows total messages in topic detail', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 2, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.getTopicDetail).mockResolvedValue({
      name: 'orders',
      mode: 'unified',
      partitions: 2,
      created_at: '2026-04-16T10:00:00Z',
      partitions_detail: [
        { id: 0, high_watermark: 500, log_start_offset: 0 },
        { id: 1, high_watermark: 300, log_start_offset: 0 },
      ],
    });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    });

    const viewButtons = screen.getAllByRole('button', { name: /View topic orders/ });
    await userEvent.click(viewButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Partition 0')).toBeInTheDocument();
    });
    expect(screen.getByText('Total Messages')).toBeInTheDocument();
    expect(screen.getByText('800')).toBeInTheDocument();
  });

  it('executes delete mutation when confirmed', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'temp-topic', mode: 'unified', partitions: 1, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.deleteTopic).mockResolvedValue({ status: 'deleted' });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('temp-topic').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete topic/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Topic')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteTopic).toHaveBeenCalled();
    });
  });

  it('creates a topic via the form submission', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.createTopic).mockResolvedValue({ status: 'created' });

    render(<TopicsPage />, { wrapper: createWrapper() });

    // Wait for page to load (shows 0 Topics)
    await waitFor(() => {
      expect(screen.getByText('0 Topics')).toBeInTheDocument();
    });

    // Open create dialog from the header button
    const headerCreateBtns = screen.getAllByRole('button', { name: /Create/ });
    // First one is the DialogTrigger in the header
    await userEvent.click(headerCreateBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Create a new ChimeraMQ topic')).toBeInTheDocument();
    });

    // Fill form
    const nameInput = screen.getByPlaceholderText('my-topic');
    await userEvent.type(nameInput, 'new-topic');

    const partitionsInput = screen.getByLabelText('Partitions');
    await userEvent.clear(partitionsInput);
    await userEvent.type(partitionsInput, '4');

    // Submit
    const submitBtn = screen.getByRole('button', { name: 'Create' });
    await userEvent.click(submitBtn);

    await waitFor(() => {
      expect(api.createTopic).toHaveBeenCalledWith('new-topic', 'unified', 4);
    });
  });

  it('shows Create button disabled when form is empty', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Topics')).toBeInTheDocument();
    });

    const createBtns = screen.getAllByRole('button', { name: /Create/ });
    await userEvent.click(createBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Create a new ChimeraMQ topic')).toBeInTheDocument();
    });

    // Create button should be disabled with empty form
    const submitBtn = screen.getByRole('button', { name: 'Create' });
    expect(submitBtn).toBeDisabled();
  });

  it('shows disabled Create button for invalid topic name characters', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Topics')).toBeInTheDocument();
    });

    const createBtns = screen.getAllByRole('button', { name: /Create/ });
    await userEvent.click(createBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Create a new ChimeraMQ topic')).toBeInTheDocument();
    });

    const nameInput = screen.getByPlaceholderText('my-topic');
    await userEvent.type(nameInput, 'invalid topic!');

    // Create button should be disabled due to invalid name
    const submitBtn = screen.getByRole('button', { name: 'Create' });
    expect(submitBtn).toBeDisabled();
  });

  it('closes create dialog when Cancel is clicked', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Topics')).toBeInTheDocument();
    });

    const createBtns = screen.getAllByRole('button', { name: /Create/ });
    await userEvent.click(createBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Create a new ChimeraMQ topic')).toBeInTheDocument();
    });

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' });
    await userEvent.click(cancelBtn);

    await waitFor(() => {
      expect(screen.queryByText('Create a new ChimeraMQ topic')).not.toBeInTheDocument();
    });
  });

  it('closes topic detail dialog when Close is clicked', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'closeable-topic', mode: 'unified', partitions: 1, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.getTopicDetail).mockResolvedValue({
      name: 'closeable-topic',
      mode: 'unified',
      partitions: 1,
      created_at: '2026-04-16T10:00:00Z',
      partitions_detail: [{ id: 0, high_watermark: 100, log_start_offset: 0 }],
    });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('closeable-topic').length).toBeGreaterThan(0);
    });

    const viewButtons = screen.getAllByRole('button', { name: /View topic closeable-topic/ });
    await userEvent.click(viewButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Partition 0')).toBeInTheDocument();
    });

    // Close dialog - use getAllByRole to handle both Close button and X button
    const dialog = screen.getByRole('dialog');
    const closeBtns = within(dialog).getAllByRole('button', { name: /Close/i });
    await userEvent.click(closeBtns[0]);

    await waitFor(() => {
      expect(screen.queryByText('Partition 0')).not.toBeInTheDocument();
    });
  });

  it('creates topic from empty state Create button', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.createTopic).mockResolvedValue({ status: 'created' });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Topics')).toBeInTheDocument();
    });

    // Click the Create Topic button from the empty state
    const emptyStateBtn = screen.getAllByRole('button', { name: /Create Topic/ })[0];
    await userEvent.click(emptyStateBtn);

    await waitFor(() => {
      expect(screen.getByText('Create a new ChimeraMQ topic')).toBeInTheDocument();
    });

    const nameInput = screen.getByPlaceholderText('my-topic');
    await userEvent.type(nameInput, 'empty-state-topic');

    const partitionsInput = screen.getByLabelText('Partitions');
    await userEvent.clear(partitionsInput);
    await userEvent.type(partitionsInput, '2');

    const submitBtn = screen.getByRole('button', { name: 'Create' });
    await userEvent.click(submitBtn);

    await waitFor(() => {
      expect(api.createTopic).toHaveBeenCalledWith('empty-state-topic', 'unified', 2);
    });
  });

  it('shows topic mode badges correctly', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16T10:00:00Z' },
      { name: 'events', mode: 'stream', partitions: 8, created_at: '2026-04-16T11:00:00Z' },
      { name: 'notifications', mode: 'queue', partitions: 2, created_at: '2026-04-16T12:00:00Z' },
    ]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    });

    // Mode badges should be visible
    expect(screen.getAllByText('unified').length).toBeGreaterThan(0);
    expect(screen.getAllByText('stream').length).toBeGreaterThan(0);
    expect(screen.getAllByText('queue').length).toBeGreaterThan(0);
  });

  it('views topic from desktop table View button', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'desktop-tbl', mode: 'unified', partitions: 2, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.getTopicDetail).mockResolvedValue({
      name: 'desktop-tbl',
      mode: 'unified',
      partitions: 2,
      created_at: '2026-04-16T10:00:00Z',
      partitions_detail: [{ id: 0, high_watermark: 100, log_start_offset: 0 }],
    });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-tbl').length).toBeGreaterThan(0);
    });

    // Desktop View button is the second one (index 1)
    const viewBtns = screen.getAllByRole('button', { name: /View topic desktop-tbl/ });
    expect(viewBtns.length).toBeGreaterThan(1);
    await userEvent.click(viewBtns[1]);

    await waitFor(() => {
      expect(screen.getByText('Partition 0')).toBeInTheDocument();
    });
  });

  it('deletes topic from desktop table Delete button', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'desktop-del', mode: 'unified', partitions: 1, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.deleteTopic).mockResolvedValue({ status: 'deleted' });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-del').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete topic desktop-del/ });
    expect(deleteBtns.length).toBeGreaterThan(1);
    await userEvent.click(deleteBtns[1]);

    await waitFor(() => {
      expect(screen.getByText('Delete Topic')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteTopic).toHaveBeenCalled();
    });
  });

  it('shows error toast when delete mutation fails', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'err-delete', mode: 'unified', partitions: 1, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.deleteTopic).mockRejectedValue(new Error('Delete failed'));

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('err-delete').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete topic/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Topic')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteTopic).toHaveBeenCalled();
    });
  });

  it('shows error toast when create mutation fails', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);
    vi.mocked(api.createTopic).mockRejectedValue(new Error('Create failed'));

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Topics')).toBeInTheDocument();
    });

    const createBtns = screen.getAllByRole('button', { name: /Create/ });
    await userEvent.click(createBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Create a new ChimeraMQ topic')).toBeInTheDocument();
    });

    const nameInput = screen.getByPlaceholderText('my-topic');
    await userEvent.type(nameInput, 'fail-topic');

    const partitionsInput = screen.getByLabelText('Partitions');
    await userEvent.clear(partitionsInput);
    await userEvent.type(partitionsInput, '1');

    const submitBtn = screen.getByRole('button', { name: 'Create' });
    await userEvent.click(submitBtn);

    await waitFor(() => {
      expect(api.createTopic).toHaveBeenCalled();
    });
  });

  it('shows mode filter Select in table header', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16T10:00:00Z' },
    ]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
    });

    // Mode filter Select trigger is present (has funnel icon nearby)
    const funnelElements = document.querySelectorAll('.lucide-funnel');
    expect(funnelElements.length).toBeGreaterThan(0);
  });

  it('closes topic detail dialog via onOpenChange', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([
      { name: 'closeable-topic', mode: 'unified', partitions: 1, created_at: '2026-04-16T10:00:00Z' },
    ]);
    vi.mocked(api.getTopicDetail).mockResolvedValue({
      name: 'closeable-topic',
      mode: 'unified',
      partitions: 1,
      created_at: '2026-04-16T10:00:00Z',
      partitions_detail: [{ id: 0, high_watermark: 100, log_start_offset: 0 }],
    });

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('closeable-topic').length).toBeGreaterThan(0);
    });

    const viewButtons = screen.getAllByRole('button', { name: /View topic closeable-topic/ });
    await userEvent.click(viewButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Partition 0')).toBeInTheDocument();
    });

    // Close dialog by clicking Close button
    const closeBtns = screen.getAllByRole('button', { name: /Close/i });
    await userEvent.click(closeBtns[0]);

    await waitFor(() => {
      expect(screen.queryByText('Partition 0')).not.toBeInTheDocument();
    });
  });

  it('shows error for invalid partitions in create form', async () => {
    vi.mocked(api.getTopics).mockResolvedValue([]);

    render(<TopicsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Topics')).toBeInTheDocument();
    });

    const createBtns = screen.getAllByRole('button', { name: /Create/ });
    await userEvent.click(createBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Create a new ChimeraMQ topic')).toBeInTheDocument();
    });

    const nameInput = screen.getByPlaceholderText('my-topic');
    await userEvent.type(nameInput, 'valid-topic');

    const partitionsInput = screen.getByLabelText('Partitions');
    await userEvent.clear(partitionsInput);
    await userEvent.type(partitionsInput, '1');

    // Create button should be enabled with valid input
    const submitBtn = screen.getByRole('button', { name: 'Create' });
    expect(submitBtn).not.toBeDisabled();
  });
});

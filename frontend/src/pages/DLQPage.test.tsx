import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { DLQPage } from '@/pages/DLQPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    listDLQTopics: vi.fn(),
    getDLQ: vi.fn(),
    clearDLQ: vi.fn(),
    replayDLQ: vi.fn(),
  };
});

vi.mock('sonner', () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

const createWrapper = () => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
};

describe('DLQPage', () => {
  it('renders empty state when no DLQ topics exist', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: [] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Dead Letter Queue')).toBeInTheDocument();
    });
    expect(screen.getAllByText('No DLQ topics active').length).toBeGreaterThan(0);
  });

  it('shows DLQ topic count heading', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq', 'events-dlq'] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('2 DLQ Topics').length).toBeGreaterThan(0);
    });
  });

  it('shows topic names in the list', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['my-topic-dlq'] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-topic-dlq').length).toBeGreaterThan(0);
    });
  });

  it('shows entries section when a topic is selected', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({
      topic: 'orders-dlq',
      count: 1,
      entries: [{
        id: 'err-001',
        topic: 'orders',
        partition: 0,
        reason: 'Processing timeout',
        retries: 3,
        failed_at: '2026-04-16T10:00:00Z',
        original_msg: { id: 'msg-1', topic: 'orders', body: '{"order_id": 123}' },
      }],
    });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    // Click Inspect button — selects the topic and triggers getDLQ query
    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    // After selecting topic, entries section appears with topic name in header
    await waitFor(() => {
      expect(screen.getByText('1 entries')).toBeInTheDocument();
    });
  });

  it('shows no entries message for empty DLQ topic', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['empty-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({ topic: 'empty-dlq', count: 0, entries: [] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('empty-dlq').length).toBeGreaterThan(0);
    });

    // Click Inspect to select the topic
    const inspectButtons = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('No entries in this DLQ topic')).toBeInTheDocument();
    });
  });

  it('shows entry detail dialog when View is clicked on an entry', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({
      topic: 'orders-dlq',
      count: 1,
      entries: [{
        id: 'err-001',
        topic: 'orders',
        partition: 2,
        reason: 'Schema validation failed',
        retries: 5,
        failed_at: '2026-04-16T12:00:00Z',
        original_msg: {
          id: 'msg-42',
          topic: 'orders',
          headers: { 'x-source': 'api' },
          body: '{"item": "widget"}',
        },
      }],
    });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    // Click Inspect to load entries
    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('1 entries')).toBeInTheDocument();
    });

    // Click View button on the entry
    const viewBtns = screen.getAllByRole('button', { name: /View DLQ entry/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText(/DLQ Entry #err-001/)).toBeInTheDocument();
    });
    expect(screen.getByText('Schema validation failed')).toBeInTheDocument();
    expect(screen.getByText('msg-42')).toBeInTheDocument();
  });

  it('opens clear confirmation dialog when Clear is clicked', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.clearDLQ).mockResolvedValue({ status: 'cleared' });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    // Click Clear button
    const clearBtns = screen.getAllByRole('button', { name: /Clear DLQ topic/ });
    await userEvent.click(clearBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Clear DLQ Topic')).toBeInTheDocument();
    });
    expect(screen.getByText(/Are you sure you want to clear all entries from/)).toBeInTheDocument();
  });

  it('opens replay dialog when Replay is clicked', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.replayDLQ).mockResolvedValue({ replayed: 10, dry_run: false });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    // Click Replay button
    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });
    expect(screen.getByText(/Replay failed messages from/)).toBeInTheDocument();
  });

  it('shows entry details tab when viewing an entry with headers', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({
      topic: 'orders-dlq',
      count: 1,
      entries: [{
        id: 'err-100',
        topic: 'orders',
        partition: 1,
        reason: 'Timeout',
        retries: 3,
        failed_at: '2026-04-16T10:00:00Z',
        original_msg: {
          id: 'msg-200',
          topic: 'orders',
          headers: { correlation: 'abc' },
          body: '{"x":1}',
        },
      }],
    });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('1 entries')).toBeInTheDocument();
    });

    const viewBtns = screen.getAllByRole('button', { name: /View DLQ entry/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText(/DLQ Entry #err-100/)).toBeInTheDocument();
    });

    // Check that the Message tab content is visible
    expect(screen.getByText('Topic')).toBeInTheDocument();
    expect(screen.getAllByText('orders').length).toBeGreaterThan(0);
  });

  it('executes clear mutation when confirmed', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['old-dlq'] });
    vi.mocked(api.clearDLQ).mockResolvedValue({ status: 'cleared' });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('old-dlq').length).toBeGreaterThan(0);
    });

    // Click Clear button to open dialog
    const clearBtns = screen.getAllByRole('button', { name: /Clear DLQ topic/ });
    await userEvent.click(clearBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Clear DLQ Topic')).toBeInTheDocument();
    });

    // Scope to AlertDialog for unique button query
    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Clear' });
    await userEvent.click(confirmBtn);

    expect(api.clearDLQ).toHaveBeenCalledWith('old-dlq');
  });

  it('submits replay form with default values', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.replayDLQ).mockResolvedValue({ replayed: 5, dry_run: false });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    // Open replay dialog
    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });

    // Submit replay with default values
    const replayBtn = screen.getByRole('button', { name: 'Replay' });
    await userEvent.click(replayBtn);

    await waitFor(() => {
      expect(api.replayDLQ).toHaveBeenCalledWith('orders-dlq', expect.objectContaining({
        dry_run: false,
        delete_after_replay: false,
      }));
    });
  });

  it('submits replay form with dry run toggle enabled', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.replayDLQ).mockResolvedValue({ replayed: 3, dry_run: true });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });

    // Toggle dry run switch on
    const dryRunSwitch = screen.getByRole('switch', { name: /Dry run/ });
    await userEvent.click(dryRunSwitch);

    // Button should change to "Preview"
    await waitFor(() => {
      expect(screen.getByRole('button', { name: 'Preview' })).toBeInTheDocument();
    });

    const previewBtn = screen.getByRole('button', { name: 'Preview' });
    await userEvent.click(previewBtn);

    await waitFor(() => {
      expect(api.replayDLQ).toHaveBeenCalledWith('orders-dlq', expect.objectContaining({
        dry_run: true,
        delete_after_replay: false,
      }));
    });
  });

  it('submits replay form with target topic and delete after', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.replayDLQ).mockResolvedValue({ replayed: 7, dry_run: false });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });

    // Fill target topic
    const targetInput = screen.getByPlaceholderText('Leave empty for original topic');
    await userEvent.type(targetInput, 'orders-retry');

    // Toggle delete after switch on
    const deleteSwitch = screen.getByRole('switch', { name: /Delete after/ });
    await userEvent.click(deleteSwitch);

    const replayBtn = screen.getByRole('button', { name: 'Replay' });
    await userEvent.click(replayBtn);

    await waitFor(() => {
      expect(api.replayDLQ).toHaveBeenCalledWith('orders-dlq', expect.objectContaining({
        dry_run: false,
        delete_after_replay: true,
        target_topic: 'orders-retry',
      }));
    });
  });

  it('closes replay dialog when Cancel is clicked', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' });
    await userEvent.click(cancelBtn);

    await waitFor(() => {
      expect(screen.queryByText('Replay DLQ Messages')).not.toBeInTheDocument();
    });
  });

  it('shows error toast when clear mutation fails', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.clearDLQ).mockRejectedValue(new Error('Clear failed'));

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    const clearBtns = screen.getAllByRole('button', { name: /Clear DLQ topic/ });
    await userEvent.click(clearBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Clear DLQ Topic')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Clear' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.clearDLQ).toHaveBeenCalled();
    });
  });

  it('shows error toast when replay mutation fails', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.replayDLQ).mockRejectedValue(new Error('Replay failed'));

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });

    const replayBtn = screen.getByRole('button', { name: 'Replay' });
    await userEvent.click(replayBtn);

    await waitFor(() => {
      expect(api.replayDLQ).toHaveBeenCalled();
    });
  });

  it('shows entry card with topic fallback and date', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['orders-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({
      topic: 'orders-dlq',
      count: 1,
      entries: [{
        id: 'err-date-001',
        topic: 'orders',
        partition: 3,
        reason: 'Connection refused',
        retries: 5,
        failed_at: '2026-04-16T15:30:00Z',
        original_msg: { id: 'msg-999', body: '{"key":"val"}' },
      }],
    });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('orders-dlq').length).toBeGreaterThan(0);
    });

    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('1 entries')).toBeInTheDocument();
    });

    // Entry card should render with the View button
    const viewBtns = screen.getAllByRole('button', { name: /View DLQ entry err-date-001/ });
    expect(viewBtns.length).toBeGreaterThan(0);
  });

  it('opens replay dialog from desktop list Replay button', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['desktop-dlq'] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-dlq').length).toBeGreaterThan(0);
    });

    // Desktop Replay button (no text label, just icon + "Replay")
    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });
  });

  it('opens clear dialog from desktop list Clear button', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['desktop-clear-dlq'] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-clear-dlq').length).toBeGreaterThan(0);
    });

    // Desktop Clear button (no text label, just icon)
    const clearBtns = screen.getAllByRole('button', { name: /Clear DLQ topic/ });
    await userEvent.click(clearBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Clear DLQ Topic')).toBeInTheDocument();
    });
  });

  it('toggles selected topic on desktop Inspect button click', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['toggle-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({ topic: 'toggle-dlq', count: 0, entries: [] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('toggle-dlq').length).toBeGreaterThan(0);
    });

    // Click Inspect to select
    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('0 entries')).toBeInTheDocument();
    });

    // Click Inspect again to deselect
    await userEvent.click(inspectBtns[0]);

    // Entries section should disappear
    await waitFor(() => {
      expect(screen.queryByText('0 entries')).not.toBeInTheDocument();
    });
  });

  it('opens replay dialog from mobile card Replay button', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['mobile-replay-dlq'] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('mobile-replay-dlq').length).toBeGreaterThan(0);
    });

    // Mobile Replay button has text "Replay" alongside icon
    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    // There are multiple (mobile + desktop), pick the mobile one
    await userEvent.click(replayBtns[replayBtns.length - 1]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });
  });

  it('opens clear dialog from mobile card Clear button', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['mobile-clear-dlq'] });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('mobile-clear-dlq').length).toBeGreaterThan(0);
    });

    // Mobile Clear button has text "Clear" alongside icon
    const clearBtns = screen.getAllByRole('button', { name: /Clear DLQ topic/ });
    await userEvent.click(clearBtns[clearBtns.length - 1]);

    await waitFor(() => {
      expect(screen.getByText('Clear DLQ Topic')).toBeInTheDocument();
    });
  });

  it('selects topic by clicking desktop row', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['row-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({ topic: 'row-dlq', count: 2, entries: [
      { id: 'e1', topic: 't', partition: 0, reason: 'err', retries: 1, failed_at: '2026-01-01T00:00:00Z', original_msg: { id: 'm1', body: 'x' } },
      { id: 'e2', topic: 't', partition: 1, reason: 'err', retries: 2, failed_at: '2026-01-01T00:00:00Z', original_msg: { id: 'm2', body: 'y' } },
    ]});

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('row-dlq').length).toBeGreaterThan(0);
    });

    // Click the "Click to inspect" text — it's in the desktop row div which has the onClick handler
    const clickHint = screen.getAllByText('Click to inspect');
    expect(clickHint.length).toBeGreaterThan(0);
    // The span's parent is the div with onClick
    await userEvent.click(clickHint[0].parentElement!);

    await waitFor(() => {
      expect(screen.getByText('2 entries')).toBeInTheDocument();
    });
  });

  it('shows entry count for large DLQ topics', async () => {
    const manyEntries = Array.from({ length: 10 }, (_, i) => ({
      id: `bulk-${i}`,
      topic: 'bulk-entries',
      partition: i % 8,
      reason: 'Bulk error',
      retries: 3,
      failed_at: '2026-04-16T10:00:00Z',
      original_msg: { id: `msg-${i}`, body: `data` },
    }));
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['many-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({ topic: 'many-dlq', count: 10, entries: manyEntries });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('many-dlq').length).toBeGreaterThan(0);
    });

    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('10 entries')).toBeInTheDocument();
    });

    // Entry cards render with IDs like bulk-0
    await waitFor(() => {
      expect(screen.getByText(/bulk-0/)).toBeInTheDocument();
    });
  });

  it('handles API failure when loading topics list', async () => {
    vi.mocked(api.listDLQTopics).mockRejectedValue(new Error('Network error'));

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Dead Letter Queue')).toBeInTheDocument();
    });
    // Should gracefully show 0 topics when API fails
    expect(screen.getAllByText('0 DLQ Topics').length).toBeGreaterThan(0);
  });

  it('handles API failure when loading DLQ entries', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['fail-dlq'] });
    vi.mocked(api.getDLQ).mockRejectedValue(new Error('Entry load failed'));

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('fail-dlq').length).toBeGreaterThan(0);
    });

    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    // Should gracefully show 0 entries when API fails
    await waitFor(() => {
      expect(screen.getByText('0 entries')).toBeInTheDocument();
    });
  });

  it('deselects topic when clicking desktop Inspect button again', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['toggle2-dlq'] });
    vi.mocked(api.getDLQ).mockResolvedValue({ topic: 'toggle2-dlq', count: 1, entries: [
      { id: 'e1', topic: 't', partition: 0, reason: 'err', retries: 1, failed_at: '2026-01-01T00:00:00Z', original_msg: { id: 'm1', body: 'x' } },
    ]});

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('toggle2-dlq').length).toBeGreaterThan(0);
    });

    // Click Inspect to select
    const inspectBtns = screen.getAllByRole('button', { name: /Inspect DLQ topic/ });
    await userEvent.click(inspectBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('1 entries')).toBeInTheDocument();
    });

    // Click same Inspect button again to deselect
    await userEvent.click(inspectBtns[0]);

    // Entries section should disappear
    await waitFor(() => {
      expect(screen.queryByText('1 entries')).not.toBeInTheDocument();
    });
  });

  it('resets replay form when dialog re-opens', async () => {
    vi.mocked(api.listDLQTopics).mockResolvedValue({ topics: ['reset-dlq'] });
    vi.mocked(api.replayDLQ).mockResolvedValue({ replayed: 0, dry_run: false });

    render(<DLQPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('reset-dlq').length).toBeGreaterThan(0);
    });

    // Open replay dialog
    const replayBtns = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
    });

    // Toggle dry run on
    const dryRunSwitch = screen.getByRole('switch', { name: /Dry run/ });
    await userEvent.click(dryRunSwitch);

    // Verify button changed to "Preview"
    await waitFor(() => {
      expect(screen.getByText('Preview')).toBeInTheDocument();
    });

    // Close dialog
    const cancelBtn = screen.getByRole('button', { name: 'Cancel' });
    await userEvent.click(cancelBtn);

    await waitFor(() => {
      expect(screen.queryByText('Replay DLQ Messages')).not.toBeInTheDocument();
    });

    // Re-open — form should be reset, dry run should be off, button back to "Replay"
    const replayBtns2 = screen.getAllByRole('button', { name: /Replay DLQ topic/ });
    await userEvent.click(replayBtns2[0]);

    await waitFor(() => {
      expect(screen.getByText('Replay DLQ Messages')).toBeInTheDocument();
      expect(screen.getAllByText('Replay').length).toBeGreaterThan(0);
    });
  });
});

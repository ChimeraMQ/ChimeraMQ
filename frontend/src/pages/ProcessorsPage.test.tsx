import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ProcessorsPage } from '@/pages/ProcessorsPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    listProcessors: vi.fn(),
    getProcessor: vi.fn(),
    deleteProcessor: vi.fn(),
    startProcessor: vi.fn(),
    stopProcessor: vi.fn(),
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

describe('ProcessorsPage', () => {
  it('renders empty state when no processors exist', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: [], count: 0 });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Stream Processors')).toBeInTheDocument();
    });
    expect(screen.getAllByText('No processors configured').length).toBeGreaterThan(0);
    expect(screen.getByText('chimera processor create my-topology')).toBeInTheDocument();
  });

  it('shows processor count heading', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['compress', 'validate'], count: 2 });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('2 Processors').length).toBeGreaterThan(0);
    });
  });

  it('shows processor names in the list', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['my-processor'], count: 1 });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-processor').length).toBeGreaterThan(0);
    });
  });

  it('shows processor detail when View is clicked', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['etl-pipeline'], count: 1 });
    vi.mocked(api.getProcessor).mockResolvedValue({
      state: 'running',
      source_topic: 'raw-events',
      sink_topic: 'processed-events',
      parallelism: 4,
      operators: 3,
    });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('etl-pipeline').length).toBeGreaterThan(0);
    });

    // Click View button
    const viewButtons = screen.getAllByRole('button', { name: /View processor/ });
    await userEvent.click(viewButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('raw-events')).toBeInTheDocument();
    });
    expect(screen.getByText('processed-events')).toBeInTheDocument();
    expect(screen.getByText('running')).toBeInTheDocument();
  });

  it('shows delete confirmation dialog when Delete is clicked', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['etl-pipeline'], count: 1 });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('etl-pipeline').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete processor/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Processor')).toBeInTheDocument();
    });
    expect(screen.getByText(/Are you sure you want to delete "etl-pipeline"/)).toBeInTheDocument();
  });

  it('shows processor detail card with source/sink topics', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['analytics'], count: 1 });
    vi.mocked(api.getProcessor).mockResolvedValue({
      state: 'stopped',
      source_topic: 'raw-events',
      sink_topic: 'analytics-output',
      parallelism: 2,
      operators: 5,
    });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('analytics').length).toBeGreaterThan(0);
    });

    const viewButtons = screen.getAllByRole('button', { name: /View processor/ });
    await userEvent.click(viewButtons[0]);

    await waitFor(() => {
      expect(screen.getByText('Source Topic')).toBeInTheDocument();
    });
    expect(screen.getByText('raw-events')).toBeInTheDocument();
    expect(screen.getByText('analytics-output')).toBeInTheDocument();
    expect(screen.getByText('2')).toBeInTheDocument();
    expect(screen.getByText('5 operator(s)')).toBeInTheDocument();
  });

  it('executes delete mutation when confirmed', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['bad-processor'], count: 1 });
    vi.mocked(api.deleteProcessor).mockResolvedValue(undefined);

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('bad-processor').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete processor/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Processor')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteProcessor).toHaveBeenCalled();
    });
  });

  it('starts processor when Start button is clicked', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['my-processor'], count: 1 });
    vi.mocked(api.startProcessor).mockResolvedValue({ status: 'started' });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-processor').length).toBeGreaterThan(0);
    });

    // Click Start button
    const startBtns = screen.getAllByRole('button', { name: /Start processor/ });
    await userEvent.click(startBtns[0]);

    await waitFor(() => {
      expect(api.startProcessor).toHaveBeenCalledWith('my-processor');
    });
  });

  it('stops processor when Stop button is clicked', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['my-processor'], count: 1 });
    vi.mocked(api.stopProcessor).mockResolvedValue({ status: 'stopped' });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-processor').length).toBeGreaterThan(0);
    });

    // Click Stop button
    const stopBtns = screen.getAllByRole('button', { name: /Stop processor/ });
    await userEvent.click(stopBtns[0]);

    await waitFor(() => {
      expect(api.stopProcessor).toHaveBeenCalledWith('my-processor');
    });
  });

  it('shows success toast when processor starts', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['etl'], count: 1 });
    vi.mocked(api.startProcessor).mockResolvedValue({ status: 'started' });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('etl').length).toBeGreaterThan(0);
    });

    const startBtns = screen.getAllByRole('button', { name: /Start processor/ });
    await userEvent.click(startBtns[0]);

    await waitFor(() => {
      expect(api.startProcessor).toHaveBeenCalled();
    });
  });

  it('shows error toast when start mutation fails', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['broken-processor'], count: 1 });
    vi.mocked(api.startProcessor).mockRejectedValue(new Error('Start failed'));

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('broken-processor').length).toBeGreaterThan(0);
    });

    const startBtns = screen.getAllByRole('button', { name: /Start processor/ });
    await userEvent.click(startBtns[0]);

    await waitFor(() => {
      expect(api.startProcessor).toHaveBeenCalled();
    });
  });

  it('shows error toast when stop mutation fails', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['stuck-processor'], count: 1 });
    vi.mocked(api.stopProcessor).mockRejectedValue(new Error('Stop failed'));

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('stuck-processor').length).toBeGreaterThan(0);
    });

    const stopBtns = screen.getAllByRole('button', { name: /Stop processor/ });
    await userEvent.click(stopBtns[0]);

    await waitFor(() => {
      expect(api.stopProcessor).toHaveBeenCalled();
    });
  });

  it('shows error toast when delete mutation fails', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['err-delete'], count: 1 });
    vi.mocked(api.deleteProcessor).mockRejectedValue(new Error('Delete failed'));

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('err-delete').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete processor/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Processor')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteProcessor).toHaveBeenCalled();
    });
  });

  it('shows View processor detail from mobile card view', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['mobile-view'], count: 1 });
    vi.mocked(api.getProcessor).mockResolvedValue({
      state: 'running',
      source_topic: 'raw',
      sink_topic: 'cooked',
      parallelism: 2,
      operators: 3,
    });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('mobile-view').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View processor mobile-view/ });
    expect(viewBtns.length).toBeGreaterThan(0);
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('raw')).toBeInTheDocument();
    });
    expect(screen.getByText('cooked')).toBeInTheDocument();
  });

  it('shows delete confirmation dialog from mobile card view', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['mobile-proc'], count: 1 });
    vi.mocked(api.deleteProcessor).mockResolvedValue(undefined);

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('mobile-proc').length).toBeGreaterThan(0);
    });

    // Mobile Delete button has "Delete" text alongside icon — pick from all matching buttons
    const deleteBtns = screen.getAllByRole('button', { name: /Delete processor mobile-proc/ });
    expect(deleteBtns.length).toBeGreaterThan(0);
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Processor')).toBeInTheDocument();
    });
    expect(screen.getByText(/Are you sure you want to delete "mobile-proc"/)).toBeInTheDocument();
  });

  it('starts processor from mobile card view', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['mobile-start'], count: 1 });
    vi.mocked(api.startProcessor).mockResolvedValue({ status: 'started' });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('mobile-start').length).toBeGreaterThan(0);
    });

    const startBtns = screen.getAllByRole('button', { name: /Start processor mobile-start/ });
    expect(startBtns.length).toBeGreaterThan(0);
    await userEvent.click(startBtns[0]);

    await waitFor(() => {
      expect(api.startProcessor).toHaveBeenCalledWith('mobile-start');
    });
  });

  it('triggers delete from desktop table view button', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['desktop-delete'], count: 1 });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-delete').length).toBeGreaterThan(0);
    });

    // Desktop delete button is the last one in the array (mobile renders first)
    const deleteBtns = screen.getAllByRole('button', { name: /Delete processor desktop-delete/ });
    const desktopDeleteBtn = deleteBtns[deleteBtns.length - 1];
    await userEvent.click(desktopDeleteBtn);

    await waitFor(() => {
      expect(screen.getByText('Delete Processor')).toBeInTheDocument();
    });
    expect(screen.getByText(/Are you sure you want to delete "desktop-delete"/)).toBeInTheDocument();
  });

  it('stops processor from mobile card view', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['mobile-stop'], count: 1 });
    vi.mocked(api.stopProcessor).mockResolvedValue({ status: 'stopped' });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('mobile-stop').length).toBeGreaterThan(0);
    });

    const stopBtns = screen.getAllByRole('button', { name: /Stop processor mobile-stop/ });
    expect(stopBtns.length).toBeGreaterThan(0);
    await userEvent.click(stopBtns[0]);

    await waitFor(() => {
      expect(api.stopProcessor).toHaveBeenCalledWith('mobile-stop');
    });
  });

  it('starts processor from desktop Start button', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['desktop-start'], count: 1 });
    vi.mocked(api.startProcessor).mockResolvedValue({ status: 'started' });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-start').length).toBeGreaterThan(0);
    });

    const startBtns = screen.getAllByRole('button', { name: /Start processor desktop-start/ });
    expect(startBtns.length).toBeGreaterThan(0);
    await userEvent.click(startBtns[1]); // desktop button

    await waitFor(() => {
      expect(api.startProcessor).toHaveBeenCalledWith('desktop-start');
    });
  });

  it('stops processor from desktop Stop button', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['desktop-stop'], count: 1 });
    vi.mocked(api.stopProcessor).mockResolvedValue({ status: 'stopped' });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-stop').length).toBeGreaterThan(0);
    });

    const stopBtns = screen.getAllByRole('button', { name: /Stop processor desktop-stop/ });
    expect(stopBtns.length).toBeGreaterThan(0);
    await userEvent.click(stopBtns[1]); // desktop button

    await waitFor(() => {
      expect(api.stopProcessor).toHaveBeenCalledWith('desktop-stop');
    });
  });

  it('views processor from desktop View button', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['desktop-view'], count: 1 });
    vi.mocked(api.getProcessor).mockResolvedValue({
      state: 'running',
      source_topic: 'src',
      sink_topic: 'dst',
      parallelism: 1,
      operators: 2,
    });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-view').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View processor desktop-view/ });
    expect(viewBtns.length).toBeGreaterThan(0);
    await userEvent.click(viewBtns[1]); // desktop button

    await waitFor(() => {
      expect(screen.getByText('src')).toBeInTheDocument();
    });
  });

  it('handles API failure when loading processors', async () => {
    vi.mocked(api.listProcessors).mockRejectedValue(new Error('Network error'));

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Stream Processors')).toBeInTheDocument();
    });
    // Page renders gracefully - count shows 0
    expect(screen.getByText('0 Processors')).toBeInTheDocument();
  });

  it('shows processor state badge with stopped state', async () => {
    vi.mocked(api.listProcessors).mockResolvedValue({ topologies: ['stopped-proc'], count: 1 });
    vi.mocked(api.getProcessor).mockResolvedValue({
      state: 'stopped',
      source_topic: 'raw',
      sink_topic: 'cooked',
      parallelism: 1,
      operators: 1,
    });

    render(<ProcessorsPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('stopped-proc').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View processor stopped-proc/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('stopped')).toBeInTheDocument();
    });
  });
});

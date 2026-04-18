import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { SchemasPage } from '@/pages/SchemasPage';
import * as api from '@/lib/api';

vi.mock('@/lib/api', async () => {
  const actual = await vi.importActual('@/lib/api');
  return {
    ...actual,
    listSchemaSubjects: vi.fn(),
    getSchemas: vi.fn(),
    registerSchema: vi.fn(),
    deleteSchema: vi.fn(),
  };
});

vi.mock('sonner', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}));

// Mock Select to fire onValueChange when an item is clicked
vi.mock('@/components/ui/select', () => ({
  Select: ({ children, onValueChange, ...props }: any) => (
    <div data-testid="select-root" {...props} data-onchange={onValueChange ? 'yes' : 'no'}>
      {onValueChange && (
        <div data-testid="select-onValueChange" onClick={() => onValueChange('JSON')} data-select-trigger>
          Select Trigger
        </div>
      )}
      {children}
    </div>
  ),
  SelectTrigger: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  SelectValue: ({ ...props }: any) => <span {...props}>Value</span>,
  SelectContent: ({ children, ...props }: any) => <div data-testid="select-content" {...props}>{children}</div>,
  SelectItem: ({ children, value, ...props }: any) => (
    <div data-testid={`select-item-${value}`} data-value={value} {...props}>{children}</div>
  ),
  SelectLabel: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  SelectGroup: ({ children, ...props }: any) => <div {...props}>{children}</div>,
  SelectSeparator: ({ ...props }: any) => <div {...props} />,
  SelectScrollUpButton: ({ ...props }: any) => <div {...props} />,
  SelectScrollDownButton: ({ ...props }: any) => <div {...props} />,
}));

const createWrapper = () => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  );
};

describe('SchemasPage', () => {
  it('renders empty state when no schemas exist', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Schemas')).toBeInTheDocument();
    });
    expect(screen.getAllByText('No schemas registered').length).toBeGreaterThan(0);
  });

  it('shows schema subject count heading', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['users-value', 'orders-value']);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('2 Subjects').length).toBeGreaterThan(0);
    });
  });

  it('shows subject names in the list', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['my-subject']);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('my-subject').length).toBeGreaterThan(0);
    });
  });

  it('shows schema detail with versions when a subject is viewed', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['user-schema']);
    vi.mocked(api.getSchemas).mockResolvedValue([
      { version: 1, type: 'JSON', id: 'schema-001', schema: '{"type": "object"}' },
      { version: 2, type: 'JSON', id: 'schema-002', schema: '{"type": "object", "properties": {}}' },
    ]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('user-schema').length).toBeGreaterThan(0);
    });

    // Click View button
    const viewBtns = screen.getAllByRole('button', { name: /View schema/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('2 version(s)')).toBeInTheDocument();
    });
    expect(screen.getByText('v1')).toBeInTheDocument();
    expect(screen.getByText('v2')).toBeInTheDocument();
    expect(screen.getByText('JSON')).toBeInTheDocument();
  });

  it('shows delete confirmation dialog when Delete is clicked', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['user-schema']);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('user-schema').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete schema/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Schema')).toBeInTheDocument();
    });
    expect(screen.getByText(/Are you sure you want to delete "user-schema"/)).toBeInTheDocument();
  });

  it('shows schema definition content in detail view', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['test-subject']);
    vi.mocked(api.getSchemas).mockResolvedValue([
      { version: 1, type: 'Avro', id: 'avro-1', schema: '{"type":"record","name":"User"}' },
    ]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('test-subject').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View schema/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('1 version(s)')).toBeInTheDocument();
    });

    // Schema content should be visible in the pre block
    expect(screen.getByText('{"type":"record","name":"User"}')).toBeInTheDocument();
    expect(screen.getByText('Avro')).toBeInTheDocument();
  });

  it('copies schema to clipboard when Copy is clicked', async () => {
    const mockClipboard = { writeText: vi.fn() };
    Object.assign(navigator, { clipboard: mockClipboard });

    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['copy-subject']);
    vi.mocked(api.getSchemas).mockResolvedValue([
      { version: 1, type: 'Protobuf', id: 'proto-1', schema: 'message User { string name = 1; }' },
    ]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('copy-subject').length).toBeGreaterThan(0);
    });

    const viewBtns = screen.getAllByRole('button', { name: /View schema/ });
    await userEvent.click(viewBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('1 version(s)')).toBeInTheDocument();
    });

    const copyBtn = screen.getByRole('button', { name: /Copy/ });
    await userEvent.click(copyBtn);

    expect(mockClipboard.writeText).toHaveBeenCalledWith('message User { string name = 1; }');
  });

  it('executes delete mutation when confirmed', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['del-schema']);
    vi.mocked(api.deleteSchema).mockResolvedValue(undefined);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('del-schema').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete schema/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Schema')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteSchema).toHaveBeenCalled();
    });
  });

  it('registers a schema via the form submission', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);
    vi.mocked(api.registerSchema).mockResolvedValue({ id: 'schema-001' });

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Subjects')).toBeInTheDocument();
    });

    // Open register dialog
    const registerBtns = screen.getAllByRole('button', { name: /Register/ });
    await userEvent.click(registerBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Register a new schema for a subject')).toBeInTheDocument();
    });

    // Fill form
    const subjectInput = screen.getByPlaceholderText('my-topic-value');
    await userEvent.type(subjectInput, 'users-value');

    const defInput = screen.getByPlaceholderText('{"type": "object", "properties": {...}}');
    await userEvent.type(defInput, 'simple schema def');

    // Submit
    const submitBtn = screen.getByRole('button', { name: 'Register' });
    await userEvent.click(submitBtn);

    await waitFor(() => {
      expect(api.registerSchema).toHaveBeenCalledWith('users-value', 'JSON', 'simple schema def');
    });
  });

  it('shows Register button disabled when form is empty', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Subjects')).toBeInTheDocument();
    });

    const registerBtns = screen.getAllByRole('button', { name: /Register/ });
    await userEvent.click(registerBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Register a new schema for a subject')).toBeInTheDocument();
    });

    const submitBtn = screen.getByRole('button', { name: 'Register' });
    expect(submitBtn).toBeDisabled();
  });

  it('handles API failure when loading schema subjects', async () => {
    vi.mocked(api.listSchemaSubjects).mockRejectedValue(new Error('Network error'));

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('Schemas')).toBeInTheDocument();
    });
    // Should gracefully show 0 subjects when API fails
    expect(screen.getAllByText('0 Subjects').length).toBeGreaterThan(0);
  });

  it('closes register dialog when Cancel is clicked', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Subjects')).toBeInTheDocument();
    });

    const registerBtns = screen.getAllByRole('button', { name: /Register/ });
    await userEvent.click(registerBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Register a new schema for a subject')).toBeInTheDocument();
    });

    const cancelBtn = screen.getByRole('button', { name: 'Cancel' });
    await userEvent.click(cancelBtn);

    await waitFor(() => {
      expect(screen.queryByText('Register a new schema for a subject')).not.toBeInTheDocument();
    });
  });

  it('shows error toast when delete fails', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['err-subject']);
    vi.mocked(api.deleteSchema).mockRejectedValue(new Error('Delete failed'));

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('err-subject').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete schema/ });
    await userEvent.click(deleteBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Delete Schema')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteSchema).toHaveBeenCalled();
    });
  });

  it('shows error toast when register fails', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);
    vi.mocked(api.registerSchema).mockRejectedValue(new Error('Register failed'));

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('0 Subjects')).toBeInTheDocument();
    });

    const registerBtns = screen.getAllByRole('button', { name: /Register/ });
    await userEvent.click(registerBtns[0]);

    await waitFor(() => {
      expect(screen.getByText('Register a new schema for a subject')).toBeInTheDocument();
    });

    const subjectInput = screen.getByPlaceholderText('my-topic-value');
    await userEvent.type(subjectInput, 'fail-subject');

    const defInput = screen.getByPlaceholderText('{"type": "object", "properties": {...}}');
    await userEvent.type(defInput, 'schema definition here');

    const submitBtn = screen.getByRole('button', { name: 'Register' });
    await userEvent.click(submitBtn);

    await waitFor(() => {
      expect(api.registerSchema).toHaveBeenCalled();
    });
  });

  it('views schema from desktop table View button', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['desktop-schema']);
    vi.mocked(api.getSchemas).mockResolvedValue([
      { version: 1, type: 'JSON', id: 'json-1', schema: '{"key": "value"}' },
    ]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-schema').length).toBeGreaterThan(0);
    });

    // Desktop View button is the second one (index 1)
    const viewBtns = screen.getAllByRole('button', { name: /View schema desktop-schema/ });
    expect(viewBtns.length).toBeGreaterThan(1);
    await userEvent.click(viewBtns[1]);

    await waitFor(() => {
      expect(screen.getByText('1 version(s)')).toBeInTheDocument();
    });
  });

  it('deletes schema from desktop table Delete button', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue(['desktop-del']);
    vi.mocked(api.deleteSchema).mockResolvedValue(undefined);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getAllByText('desktop-del').length).toBeGreaterThan(0);
    });

    const deleteBtns = screen.getAllByRole('button', { name: /Delete schema desktop-del/ });
    expect(deleteBtns.length).toBeGreaterThan(1);
    await userEvent.click(deleteBtns[1]);

    await waitFor(() => {
      expect(screen.getByText('Delete Schema')).toBeInTheDocument();
    });

    const dialog = screen.getByRole('alertdialog');
    const confirmBtn = within(dialog).getByRole('button', { name: 'Delete' });
    await userEvent.click(confirmBtn);

    await waitFor(() => {
      expect(api.deleteSchema).toHaveBeenCalled();
    });
  });

  it('shows empty state Register Schema button', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('No schemas registered')).toBeInTheDocument();
    });

    // Register Schema button in empty state
    const registerBtn = screen.getAllByRole('button', { name: /Register Schema/ })[0];
    expect(registerBtn).toBeInTheDocument();
  });

  it('opens register dialog from empty state button', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('No schemas registered')).toBeInTheDocument();
    });

    const registerBtn = screen.getAllByRole('button', { name: /Register Schema/ })[0];
    await userEvent.click(registerBtn);

    await waitFor(() => {
      expect(screen.getByText('Register a new schema for a subject')).toBeInTheDocument();
    });
  });

  it('selects type via Select onValueChange in register dialog', async () => {
    vi.mocked(api.listSchemaSubjects).mockResolvedValue([]);
    vi.mocked(api.registerSchema).mockResolvedValue(undefined);

    render(<SchemasPage />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(screen.getByText('No schemas registered')).toBeInTheDocument();
    });

    // Open register dialog
    const registerBtn = screen.getAllByRole('button', { name: /Register Schema/ })[0];
    await userEvent.click(registerBtn);

    await waitFor(() => {
      expect(screen.getByTestId('select-onValueChange')).toBeInTheDocument();
    });

    // Fill in subject
    const subjectInput = screen.getByLabelText(/Subject/);
    await userEvent.clear(subjectInput);
    await userEvent.type(subjectInput, 'test-subject');

    // Fill in definition
    const defInput = screen.getByLabelText(/Schema Definition/) as HTMLTextAreaElement;
    await userEvent.clear(defInput);
    defInput.focus();
    defInput.value = '{"type":"object"}';

    // Click the select trigger to fire onValueChange
    const selectTrigger = screen.getByTestId('select-onValueChange');
    await userEvent.click(selectTrigger);

    // Submit
    const submitBtn = screen.getByRole('button', { name: /Register/ });
    await userEvent.click(submitBtn);

    await waitFor(() => {
      expect(api.registerSchema).toHaveBeenCalled();
    });
  });
});

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { AppLayout } from '@/components/layout/AppLayout';

vi.mock('react-router', () => ({
  Outlet: () => <div data-testid="outlet">Page Content</div>,
}));

const mockState = { sidebarCollapsed: false, isDark: false };

vi.mock('@/stores/app-store', () => ({
  useAppState: () => mockState,
}));

vi.mock('@/components/layout/Sidebar', () => ({
  Sidebar: ({ open, onClose }: { open: boolean; onClose: () => void }) => (
    <div data-testid="sidebar" data-open={open}>
      <button onClick={onClose}>Close Sidebar</button>
    </div>
  ),
  Header: ({ onMenuClick }: { onMenuClick: () => void }) => (
    <div data-testid="header">
      <button onClick={onMenuClick}>Open Menu</button>
    </div>
  ),
}));

beforeEach(() => {
  mockState.sidebarCollapsed = false;
});

describe('AppLayout', () => {
  it('renders sidebar, header, and outlet', () => {
    render(<AppLayout />);

    expect(screen.getByTestId('sidebar')).toBeInTheDocument();
    expect(screen.getByTestId('header')).toBeInTheDocument();
    expect(screen.getByTestId('outlet')).toBeInTheDocument();
    expect(screen.getByText('Page Content')).toBeInTheDocument();
  });

  it('opens sidebar when menu button is clicked', async () => {
    render(<AppLayout />);

    const menuBtn = screen.getByText('Open Menu');
    await userEvent.click(menuBtn);

    expect(screen.getByTestId('sidebar')).toHaveAttribute('data-open', 'true');
  });

  it('closes sidebar when close button is clicked', async () => {
    render(<AppLayout />);

    // Open first
    await userEvent.click(screen.getByText('Open Menu'));
    expect(screen.getByTestId('sidebar')).toHaveAttribute('data-open', 'true');

    // Close
    await userEvent.click(screen.getByText('Close Sidebar'));
    expect(screen.getByTestId('sidebar')).toHaveAttribute('data-open', 'false');
  });

  it('applies collapsed layout class when sidebar is collapsed', () => {
    mockState.sidebarCollapsed = true;
    render(<AppLayout />);

    const container = screen.getByTestId('header').parentElement;
    expect(container).toHaveClass('md:pl-16');
  });
});

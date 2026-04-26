import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Sidebar, Header } from '@/components/layout/Sidebar';

// NavLink mock that supports className function
const NavLinkMock = ({ className, children, to, ...props }: any) => {
  const cls = typeof className === 'function'
    ? className({ isActive: false, isPending: false, isTransitioning: false })
    : className;
  return (
    <a href={to} className={cls} data-testid={`nav-link-${to}`} {...props}>{children}</a>
  );
};

vi.mock('react-router', () => ({
  NavLink: ({ className, children, to, ...props }: any) => {
    const cls = typeof className === 'function'
      ? className({ isActive: false, isPending: false, isTransitioning: false })
      : className;
    return <a href={to} className={cls} data-testid={`nav-link-${to}`} {...props}>{children}</a>;
  },
  useNavigate: () => vi.fn(),
  Link: ({ children, to, ...props }: { children: React.ReactNode; to: string }) => (
    <a href={to} {...props}>{children}</a>
  ),
}));

vi.mock('@/hooks/use-route-prefetch', () => ({
  useRoutePrefetch: () => ({ onMouseEnter: vi.fn() }),
}));

vi.mock('@/stores/app-store', () => ({
  useAppState: vi.fn(() => ({ sidebarCollapsed: false, setSidebarCollapsed: vi.fn(), isDark: false })),
}));

vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div data-testid="scroll-area">{children}</div>,
}));

vi.mock('@/components/ui/separator', () => ({
  Separator: () => <div data-testid="separator" />,
}));

vi.mock('@/components/ui/tooltip', () => ({
  Tooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  TooltipContent: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
  TooltipTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

vi.mock('@/components/shared/ThemeToggle', () => ({
  ThemeToggle: () => <button aria-label="Toggle theme">Theme</button>,
}));

const { useAppState } = await import('@/stores/app-store');

beforeEach(() => {
  vi.mocked(useAppState).mockReturnValue({
    sidebarCollapsed: false,
    setSidebarCollapsed: vi.fn(),
    isDark: false,
  });
});

describe('Sidebar', () => {
  it('renders sidebar with all navigation items', () => {
    render(<Sidebar open={false} onClose={vi.fn()} />);

    expect(screen.getByRole('navigation', { name: 'Main navigation' })).toBeInTheDocument();
    expect(screen.getByText('Overview')).toBeInTheDocument();
    expect(screen.getByText('Topics')).toBeInTheDocument();
    expect(screen.getByText('Consumers')).toBeInTheDocument();
    expect(screen.getByText('Cluster')).toBeInTheDocument();
    expect(screen.getByText('Schemas')).toBeInTheDocument();
    expect(screen.getByText('DLQ')).toBeInTheDocument();
    expect(screen.getByText('WASM')).toBeInTheDocument();
    expect(screen.getByText('Processors')).toBeInTheDocument();
  });

  it('shows mobile overlay when open', () => {
    render(<Sidebar open={true} onClose={vi.fn()} />);

    expect(screen.getByRole('complementary', { name: 'Sidebar' })).toBeInTheDocument();
    const overlays = document.querySelectorAll('.bg-background\\/80');
    expect(overlays.length).toBeGreaterThan(0);
  });

  it('shows ChimeraMQ branding when not collapsed', () => {
    render(<Sidebar open={false} onClose={vi.fn()} />);

    expect(screen.getAllByText('ChimeraMQ').length).toBeGreaterThan(0);
    expect(screen.getByText('Three Heads. One Binary.')).toBeInTheDocument();
  });

  it('hides branding when collapsed', () => {
    vi.mocked(useAppState).mockReturnValue({
      sidebarCollapsed: true,
      setSidebarCollapsed: vi.fn(),
      isDark: false,
    });

    render(<Sidebar open={false} onClose={vi.fn()} />);

    expect(screen.queryByText('Three Heads. One Binary.')).not.toBeInTheDocument();
  });

  it('shows collapse toggle button on desktop', () => {
    render(<Sidebar open={false} onClose={vi.fn()} />);

    expect(screen.getByRole('button', { name: /Collapse sidebar/ })).toBeInTheDocument();
  });

  it('calls setSidebarCollapsed when collapse button is clicked', async () => {
    const setCollapsed = vi.fn();
    vi.mocked(useAppState).mockReturnValue({
      sidebarCollapsed: false,
      setSidebarCollapsed: setCollapsed,
      isDark: false,
    });

    render(<Sidebar open={false} onClose={vi.fn()} />);

    const collapseBtn = screen.getByRole('button', { name: /Collapse sidebar/ });
    await userEvent.click(collapseBtn);

    expect(setCollapsed).toHaveBeenCalledWith(true);
  });

  it('shows expand icon when collapsed', () => {
    vi.mocked(useAppState).mockReturnValue({
      sidebarCollapsed: true,
      setSidebarCollapsed: vi.fn(),
      isDark: false,
    });

    render(<Sidebar open={false} onClose={vi.fn()} />);

    expect(screen.getByRole('button', { name: /Expand sidebar/ })).toBeInTheDocument();
  });

  it('renders nav links with active and inactive class variants', () => {
    render(<Sidebar open={false} onClose={vi.fn()} />);

    // NavLink mock renders with isActive: false, so all links should have the inactive classes
    const navLinks = screen.getAllByTestId(/^nav-link-/);
    expect(navLinks.length).toBeGreaterThan(0);

    // Each link should have the base classes from the className function
    const firstLink = navLinks[0];
    expect(firstLink).toHaveAttribute('class');
    const cls = firstLink.getAttribute('class');
    expect(cls).toContain('flex items-center gap-3');
    expect(cls).toContain('text-text-secondary');
    expect(cls).toContain('hover:bg-background-muted');
  });

  it('calls onClose when a nav link is clicked', async () => {
    const onClose = vi.fn();
    render(<Sidebar open={false} onClose={onClose} />);

    const navLinks = screen.getAllByTestId(/^nav-link-/);
    await userEvent.click(navLinks[0]);

    expect(onClose).toHaveBeenCalled();
  });
});

describe('Header', () => {
  it('renders header with menu button and theme toggle', () => {
    render(<Header onMenuClick={vi.fn()} />);

    expect(screen.getByRole('button', { name: 'Open sidebar' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Toggle theme' })).toBeInTheDocument();
  });

  it('calls onMenuClick when menu button is clicked', async () => {
    const onMenuClick = vi.fn();
    render(<Header onMenuClick={onMenuClick} />);

    const menuBtn = screen.getByRole('button', { name: 'Open sidebar' });
    await userEvent.click(menuBtn);

    expect(onMenuClick).toHaveBeenCalled();
  });
});

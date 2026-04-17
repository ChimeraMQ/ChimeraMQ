import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { App, PageFallback } from '@/App';

vi.mock('react-router', () => ({
  Routes: ({ children }: { children: React.ReactNode }) => <div data-testid="routes">{children}</div>,
  Route: () => null,
  Outlet: () => <div data-testid="outlet" />,
  useNavigate: () => vi.fn(),
  useLocation: () => ({ pathname: '/' }),
  Link: ({ children, to, ...props }: { children: React.ReactNode; to: string }) => (
    <a href={to} {...props}>{children}</a>
  ),
}));

vi.mock('sonner', () => ({
  Toaster: () => <div data-testid="toaster" />,
}));

vi.mock('@/components/ui/tooltip', () => ({
  TooltipProvider: ({ children, ...props }: { children: React.ReactNode }) => <div {...props}>{children}</div>,
}));

vi.mock('@/components/shared/SkipToContent', () => ({
  SkipToContent: () => <a href="#main-content">Skip</a>,
}));

describe('App', () => {
  it('renders routes container and toaster', () => {
    render(<App />);

    expect(screen.getByTestId('routes')).toBeInTheDocument();
    expect(screen.getByTestId('toaster')).toBeInTheDocument();
  });

  it('renders skip-to-content link', () => {
    render(<App />);

    expect(screen.getByRole('link', { name: /skip/i })).toBeInTheDocument();
  });
});

describe('PageFallback', () => {
  it('renders loading skeleton with title and grid', () => {
    render(<PageFallback />);

    // Should render container with space-y-6 class
    const container = document.querySelector('.space-y-6');
    expect(container).toBeInTheDocument();

    // Should have animate-pulse skeleton elements
    const skeletons = document.querySelectorAll('.animate-pulse');
    expect(skeletons.length).toBe(5); // 1 title + 4 grid cards
  });
});

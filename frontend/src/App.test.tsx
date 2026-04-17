import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { App } from '@/App';

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

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Skeleton } from '@/components/ui/skeleton';

describe('Skeleton', () => {
  it('renders a div with pulse animation class', () => {
    render(<Skeleton data-testid="skeleton" />);
    const el = screen.getByTestId('skeleton');
    expect(el).toBeInTheDocument();
    expect(el.className).toContain('animate-pulse');
  });

  it('applies custom className', () => {
    render(<Skeleton className="h-4 w-48" data-testid="skel" />);
    const el = screen.getByTestId('skel');
    expect(el.className).toContain('animate-pulse');
    expect(el.className).toContain('h-4');
  });

  it('passes through data attributes', () => {
    render(<Skeleton data-testid="loading-skeleton" />);
    expect(screen.getByTestId('loading-skeleton')).toBeInTheDocument();
  });

  it('uses muted background', () => {
    render(<Skeleton data-testid="bg-test" />);
    const el = screen.getByTestId('bg-test');
    expect(el.className).toContain('bg-background-muted');
  });
});

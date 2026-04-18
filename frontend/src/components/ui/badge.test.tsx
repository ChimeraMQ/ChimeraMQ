import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Badge, badgeVariants } from '@/components/ui/badge';

describe('Badge', () => {
  it('renders with default variant', () => {
    render(<Badge>Test</Badge>);
    const badge = screen.getByText('Test');
    expect(badge).toBeInTheDocument();
    expect(badge.className).toContain('bg-accent/10');
  });

  it('renders with success variant', () => {
    render(<Badge variant="success">Active</Badge>);
    const badge = screen.getByText('Active');
    expect(badge.className).toContain('bg-success/10');
    expect(badge.className).toContain('text-success');
  });

  it('renders with destructive variant', () => {
    render(<Badge variant="destructive">Error</Badge>);
    const badge = screen.getByText('Error');
    expect(badge.className).toContain('bg-error/10');
  });

  it('merges custom className', () => {
    render(<Badge className="custom-class">Tag</Badge>);
    const badge = screen.getByText('Tag');
    expect(badge.className).toContain('custom-class');
  });

  it('passes through HTML attributes', () => {
    render(<Badge data-testid="my-badge">Attr</Badge>);
    expect(screen.getByTestId('my-badge')).toBeInTheDocument();
  });
});

describe('badgeVariants', () => {
  it('returns default classes for default variant', () => {
    const result = badgeVariants({ variant: 'default' });
    expect(result).toContain('bg-accent/10');
    expect(result).toContain('text-accent');
  });

  it('returns warning classes', () => {
    const result = badgeVariants({ variant: 'warning' });
    expect(result).toContain('bg-warning/10');
    expect(result).toContain('text-warning');
  });

  it('returns outline classes', () => {
    const result = badgeVariants({ variant: 'outline' });
    expect(result).toContain('text-foreground');
  });
});

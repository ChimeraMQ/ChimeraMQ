import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Separator } from '@/components/ui/separator';

describe('Separator', () => {
  it('renders a horizontal separator by default', () => {
    const { container } = render(<Separator data-testid="sep" />);
    const sep = screen.getByTestId('sep');
    expect(sep).toBeInTheDocument();
    expect(sep.className).toContain('h-[1px]');
  });

  it('renders a vertical separator when orientation is vertical', () => {
    const { container } = render(<Separator orientation="vertical" data-testid="sep" />);
    const sep = screen.getByTestId('sep');
    expect(sep.className).toContain('w-[1px]');
  });

  it('applies custom className', () => {
    render(<Separator className="my-4" data-testid="sep" />);
    expect(screen.getByTestId('sep').className).toContain('my-4');
  });

  it('is decorative by default', () => {
    render(<Separator data-testid="sep" />);
    const sep = screen.getByTestId('sep');
    // Radix Separator sets role="none" when decorative=true (hides from a11y tree)
    expect(sep).toHaveAttribute('role', 'none');
  });
});

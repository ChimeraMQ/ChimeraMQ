import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area';

describe('ScrollArea', () => {
  it('renders children inside scroll area', () => {
    render(
      <ScrollArea className="h-72 w-48">
        <div data-testid="child">Content</div>
      </ScrollArea>,
    );

    expect(screen.getByTestId('child')).toBeInTheDocument();
    expect(screen.getByText('Content')).toBeInTheDocument();
  });

  it('applies custom className', () => {
    const { container } = render(
      <ScrollArea className="custom-class">
        <div>Content</div>
      </ScrollArea>,
    );

    expect(container.firstChild).toHaveClass('relative');
    expect(container.firstChild).toHaveClass('overflow-hidden');
    expect(container.firstChild).toHaveClass('custom-class');
  });

  it('renders ScrollBar inside ScrollArea', () => {
    render(
      <ScrollArea className="h-48">
        <div>Content</div>
      </ScrollArea>,
    );

    expect(screen.getByText('Content')).toBeInTheDocument();
  });

  it('renders ScrollBar with horizontal orientation', () => {
    render(
      <ScrollArea className="h-48">
        <ScrollBar orientation="horizontal" />
        <div>Content</div>
      </ScrollArea>,
    );

    expect(screen.getByText('Content')).toBeInTheDocument();
  });
});

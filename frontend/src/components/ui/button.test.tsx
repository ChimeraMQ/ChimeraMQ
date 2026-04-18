import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Button, buttonVariants } from '@/components/ui/button';

describe('Button', () => {
  it('renders as a button element', () => {
    render(<Button>Click me</Button>);
    expect(screen.getByRole('button', { name: /click me/i })).toBeInTheDocument();
  });

  it('renders with default variant classes', () => {
    render(<Button>Default</Button>);
    const btn = screen.getByRole('button', { name: /default/i });
    expect(btn.className).toContain('bg-accent');
  });

  it('renders with destructive variant', () => {
    render(<Button variant="destructive">Delete</Button>);
    const btn = screen.getByRole('button', { name: /delete/i });
    expect(btn.className).toContain('bg-error');
  });

  it('renders with outline variant', () => {
    render(<Button variant="outline">Border</Button>);
    const btn = screen.getByRole('button', { name: /border/i });
    expect(btn.className).toContain('border-border');
  });

  it('renders with ghost variant', () => {
    render(<Button variant="ghost">Ghost</Button>);
    const btn = screen.getByRole('button', { name: /ghost/i });
    expect(btn.className).toContain('hover:bg-background-muted');
  });

  it('renders with small size', () => {
    render(<Button size="sm">Small</Button>);
    const btn = screen.getByRole('button', { name: /small/i });
    expect(btn.className).toContain('h-8');
  });

  it('renders with large size', () => {
    render(<Button size="lg">Large</Button>);
    const btn = screen.getByRole('button', { name: /large/i });
    expect(btn.className).toContain('h-12');
  });

  it('renders with icon size', () => {
    render(<Button size="icon">Icon</Button>);
    const btn = screen.getByRole('button', { name: /icon/i });
    expect(btn.className).toContain('w-10');
  });

  it('is disabled when disabled prop set', () => {
    render(<Button disabled>Disabled</Button>);
    expect(screen.getByRole('button', { name: /disabled/i })).toBeDisabled();
  });

  it('renders as child element when asChild is true', () => {
    render(
      <Button asChild>
        <a href="/link">Link Button</a>
      </Button>,
    );
    expect(screen.getByRole('link', { name: /link button/i })).toBeInTheDocument();
  });
});

describe('buttonVariants', () => {
  it('returns link variant classes', () => {
    const result = buttonVariants({ variant: 'link' });
    expect(result).toContain('underline-offset-4');
  });

  it('combines variant and size', () => {
    const result = buttonVariants({ variant: 'secondary', size: 'sm' });
    expect(result).toContain('bg-background-muted');
    expect(result).toContain('h-8');
  });
});

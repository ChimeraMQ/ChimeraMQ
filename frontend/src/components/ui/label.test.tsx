import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Label } from '@/components/ui/label';

describe('Label', () => {
  it('renders a label element', () => {
    render(<Label>Username</Label>);
    expect(screen.getByText('Username')).toBeInTheDocument();
  });

  it('renders as a label element tag', () => {
    render(<Label>Email</Label>);
    expect(screen.getByText('Email').tagName.toLowerCase()).toBe('label');
  });

  it('applies custom className', () => {
    render(<Label className="custom">Test</Label>);
    expect(screen.getByText('Test').className).toContain('custom');
  });

  it('includes font-medium class', () => {
    render(<Label>Styled</Label>);
    expect(screen.getByText('Styled').className).toContain('font-medium');
  });
});

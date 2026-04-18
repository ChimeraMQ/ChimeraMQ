import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Textarea } from '@/components/ui/textarea';

describe('Textarea', () => {
  it('renders Textarea', () => {
    render(<Textarea data-testid="textarea" />);
    expect(screen.getByTestId('textarea')).toBeInTheDocument();
  });

  it('renders Textarea with custom className', () => {
    render(<Textarea className="custom-class" data-testid="textarea" />);
    expect(screen.getByTestId('textarea')).toHaveClass('custom-class');
  });

  it('renders Textarea with placeholder', () => {
    render(<Textarea placeholder="Enter text" data-testid="textarea" />);
    expect(screen.getByTestId('textarea')).toHaveAttribute('placeholder', 'Enter text');
  });

  it('renders Textarea with value', () => {
    render(<Textarea value="hello" readOnly data-testid="textarea" />);
    expect(screen.getByTestId('textarea')).toHaveValue('hello');
  });
});

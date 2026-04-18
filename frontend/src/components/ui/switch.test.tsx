import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Switch, SwitchThumb } from '@/components/ui/switch';

describe('Switch', () => {
  it('exports Switch', () => {
    expect(Switch).toBeDefined();
  });

  it('renders Switch with checked state', () => {
    render(<Switch checked data-testid="switch" />);
    expect(screen.getByTestId('switch')).toBeChecked();
  });

  it('renders Switch with unchecked state', () => {
    render(<Switch data-testid="switch" />);
    expect(screen.getByTestId('switch')).not.toBeChecked();
  });

  it('renders Switch with custom className', () => {
    render(<Switch className="custom-class" data-testid="switch" />);
    expect(screen.getByTestId('switch')).toHaveClass('custom-class');
  });
});

describe('SwitchThumb', () => {
  it('exports SwitchThumb', () => {
    expect(SwitchThumb).toBeDefined();
  });

  it('renders SwitchThumb inside Switch with custom className', () => {
    render(
      <Switch data-testid="switch">
        <SwitchThumb className="thumb-custom" data-testid="thumb" />
      </Switch>,
    );
    expect(screen.getByTestId('thumb')).toHaveClass('thumb-custom');
  });
});

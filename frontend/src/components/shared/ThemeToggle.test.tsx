import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { TooltipProvider } from '@/components/ui/tooltip';
import { ThemeToggle } from '@/components/shared/ThemeToggle';

beforeEach(() => {
  // Mock matchMedia for useTheme hook
  vi.stubGlobal('matchMedia', vi.fn((query) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

const renderThemeToggle = () => {
  localStorage.clear();
  return render(
    <TooltipProvider>
      <ThemeToggle />
    </TooltipProvider>,
  );
};

describe('ThemeToggle', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('renders a button with aria-label', () => {
    renderThemeToggle();
    expect(screen.getByRole('button', { name: /switch theme/i })).toBeInTheDocument();
  });

  it('opens dropdown on click', async () => {
    const user = userEvent.setup();
    renderThemeToggle();

    await user.click(screen.getByRole('button', { name: /switch theme/i }));

    expect(screen.getByRole('menu')).toBeInTheDocument();
  });

  it('shows theme options in dropdown', async () => {
    const user = userEvent.setup();
    renderThemeToggle();

    await user.click(screen.getByRole('button', { name: /switch theme/i }));

    expect(screen.getByRole('menuitem', { name: /light/i })).toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: /dark/i })).toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: /system/i })).toBeInTheDocument();
  });

  it('selects dark theme when clicked', async () => {
    const user = userEvent.setup();
    renderThemeToggle();

    await user.click(screen.getByRole('button', { name: /switch theme/i }));
    await user.click(screen.getByRole('menuitem', { name: /dark/i }));

    expect(localStorage.getItem('chimera-theme')).toBe('"dark"');
  });

  it('selects light theme when clicked', async () => {
    const user = userEvent.setup();
    renderThemeToggle();

    await user.click(screen.getByRole('button', { name: /switch theme/i }));
    await user.click(screen.getByRole('menuitem', { name: /light/i }));

    expect(localStorage.getItem('chimera-theme')).toBe('"light"');
  });

  it('selects system theme when clicked', async () => {
    const user = userEvent.setup();
    renderThemeToggle();

    await user.click(screen.getByRole('button', { name: /switch theme/i }));
    await user.click(screen.getByRole('menuitem', { name: /system/i }));

    expect(localStorage.getItem('chimera-theme')).toBe('"system"');
  });
});

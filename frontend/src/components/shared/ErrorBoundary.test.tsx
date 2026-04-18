import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ErrorBoundary } from '@/components/shared/ErrorBoundary';

describe('ErrorBoundary', () => {
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  it('renders children when no error', () => {
    render(
      <ErrorBoundary>
        <div data-testid="child">Hello</div>
      </ErrorBoundary>,
    );
    expect(screen.getByText('Hello')).toBeInTheDocument();
  });

  it('renders error message when child throws', () => {
    const Thrower = () => {
      throw new Error('Test error');
    };

    render(
      <ErrorBoundary>
        <Thrower />
      </ErrorBoundary>,
    );

    expect(screen.getByText('Something went wrong')).toBeInTheDocument();
    expect(screen.getByText('Test error')).toBeInTheDocument();
  });

  it('shows try again button after error', () => {
    const Thrower = () => {
      throw new Error('Crash');
    };

    render(
      <ErrorBoundary>
        <Thrower />
      </ErrorBoundary>,
    );

    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument();
  });

  it('resets error state when try again clicked', async () => {
    const { userEvent } = await import('@testing-library/user-event');
    let shouldThrow = true;
    const FlakyComponent = () => {
      if (shouldThrow) throw new Error('Temporary error');
      return <div>Recovered</div>;
    };

    const user = userEvent.setup();
    render(
      <ErrorBoundary>
        <FlakyComponent />
      </ErrorBoundary>,
    );

    expect(screen.getByText('Something went wrong')).toBeInTheDocument();

    shouldThrow = false;
    await user.click(screen.getByRole('button', { name: /try again/i }));

    expect(screen.getByText('Recovered')).toBeInTheDocument();
  });

  it('logs error to console in componentDidCatch', () => {
    const consoleSpy = vi.spyOn(console, 'error');

    const Thrower = () => {
      throw new Error('Logged error');
    };

    render(
      <ErrorBoundary>
        <Thrower />
      </ErrorBoundary>,
    );

    expect(consoleSpy).toHaveBeenCalledWith(
      'ErrorBoundary caught:',
      expect.any(Error),
      expect.any(String),
    );
  });
});

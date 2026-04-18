import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Card, CardHeader, CardFooter, CardTitle, CardDescription, CardContent } from '@/components/ui/card';

describe('Card', () => {
  it('renders with base classes', () => {
    render(<Card>Content</Card>);
    expect(screen.getByText('Content').className).toContain('bg-surface');
  });

  it('applies custom className', () => {
    render(<Card className="custom">Content</Card>);
    expect(screen.getByText('Content').className).toContain('custom');
  });
});

describe('CardHeader', () => {
  it('renders with padding', () => {
    render(<CardHeader>Header</CardHeader>);
    expect(screen.getByText('Header').className).toContain('p-6');
  });
});

describe('CardTitle', () => {
  it('renders as h3 element', () => {
    render(<CardTitle>Title</CardTitle>);
    const el = screen.getByText('Title');
    expect(el.tagName.toLowerCase()).toBe('h3');
  });
});

describe('CardDescription', () => {
  it('renders as paragraph with secondary text color', () => {
    render(<CardDescription>Description</CardDescription>);
    expect(screen.getByText('Description').className).toContain('text-text-secondary');
  });
});

describe('CardContent', () => {
  it('renders with padding top 0', () => {
    render(<CardContent>Body</CardContent>);
    expect(screen.getByText('Body').className).toContain('pt-0');
  });
});

describe('CardFooter', () => {
  it('renders as flex container', () => {
    render(<CardFooter>Footer</CardFooter>);
    expect(screen.getByText('Footer').className).toContain('flex');
  });
});

describe('Card composition', () => {
  it('renders all sub-components together', () => {
    render(
      <Card>
        <CardHeader>
          <CardTitle>Test Card</CardTitle>
          <CardDescription>A test description</CardDescription>
        </CardHeader>
        <CardContent>Body content</CardContent>
        <CardFooter>Footer text</CardFooter>
      </Card>,
    );

    expect(screen.getByText('Test Card')).toBeInTheDocument();
    expect(screen.getByText('A test description')).toBeInTheDocument();
    expect(screen.getByText('Body content')).toBeInTheDocument();
    expect(screen.getByText('Footer text')).toBeInTheDocument();
  });
});

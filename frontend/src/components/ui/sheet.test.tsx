import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import {
  Sheet,
  SheetTrigger,
  SheetContent,
  SheetHeader,
  SheetFooter,
  SheetTitle,
  SheetDescription,
  SheetClose,
} from '@/components/ui/sheet';

describe('Sheet', () => {
  it('exports Sheet, SheetTrigger, SheetContent, SheetHeader, SheetFooter, SheetTitle, SheetDescription, SheetClose', () => {
    // These are all Radix UI primitives — test that they export correctly
    expect(Sheet).toBeDefined();
    expect(SheetTrigger).toBeDefined();
    expect(SheetContent).toBeDefined();
    expect(SheetHeader).toBeDefined();
    expect(SheetFooter).toBeDefined();
    expect(SheetTitle).toBeDefined();
    expect(SheetDescription).toBeDefined();
    expect(SheetClose).toBeDefined();
  });

  it('renders sheet when open', () => {
    render(
      <Sheet open>
        <SheetTrigger>Open</SheetTrigger>
        <SheetContent>
          <SheetHeader>
            <SheetTitle>Sheet Title</SheetTitle>
            <SheetDescription>Sheet Description</SheetDescription>
          </SheetHeader>
          <p>Content</p>
          <SheetFooter>
            <button>Close</button>
          </SheetFooter>
        </SheetContent>
      </Sheet>,
    );

    expect(screen.getByText('Open')).toBeInTheDocument();
    expect(screen.getByText('Sheet Title')).toBeInTheDocument();
    expect(screen.getByText('Sheet Description')).toBeInTheDocument();
  });
});

import { describe, it, expect } from 'vitest';
import { cn } from '@/lib/utils';

describe('cn', () => {
  it('merges class names', () => {
    expect(cn('foo', 'bar')).toBe('foo bar');
  });

  it('handles conditional classes via clsx', () => {
    expect(cn('base', true && 'active', false && 'inactive')).toBe('base active');
  });

  it('handles object notation', () => {
    expect(cn('btn', { primary: true, disabled: false })).toBe('btn primary');
  });

  it('tailwind-merge resolves conflicts', () => {
    expect(cn('px-2', 'px-4')).toBe('px-4');
  });

  it('handles empty inputs', () => {
    expect(cn()).toBe('');
  });

  it('handles null and undefined', () => {
    expect(cn(null, undefined, 'foo')).toBe('foo');
  });
});

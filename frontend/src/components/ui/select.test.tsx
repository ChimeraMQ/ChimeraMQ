import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

describe('Select exports', () => {
  it('exports all expected components', () => {
    expect(Select).toBeDefined();
    expect(SelectContent).toBeDefined();
    expect(SelectGroup).toBeDefined();
    expect(SelectItem).toBeDefined();
    expect(SelectLabel).toBeDefined();
    expect(SelectSeparator).toBeDefined();
    expect(SelectTrigger).toBeDefined();
    expect(SelectValue).toBeDefined();
  });

  it('renders SelectLabel within SelectGroup', () => {
    render(
      <SelectGroup>
        <SelectLabel>Section</SelectLabel>
      </SelectGroup>,
    );
    expect(screen.getByText('Section')).toBeInTheDocument();
  });

  it('renders Select with Trigger, Content, Group, Item, Label, Separator', () => {
    render(
      <Select>
        <SelectTrigger data-testid="trigger">
          <SelectValue placeholder="Select..." />
        </SelectTrigger>
        <SelectContent>
          <SelectGroup>
            <SelectLabel>Options</SelectLabel>
            <SelectItem value="opt1">Option 1</SelectItem>
            <SelectSeparator />
          </SelectGroup>
        </SelectContent>
      </Select>,
    );
    expect(screen.getByTestId('trigger')).toBeInTheDocument();
  });
});

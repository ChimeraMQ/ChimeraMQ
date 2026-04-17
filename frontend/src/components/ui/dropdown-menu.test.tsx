import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuCheckboxItem,
  DropdownMenuRadioItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuGroup,
  DropdownMenuPortal,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuRadioGroup,
} from '@/components/ui/dropdown-menu';

describe('DropdownMenu exports', () => {
  it('exports all expected components', () => {
    expect(DropdownMenu).toBeDefined();
    expect(DropdownMenuTrigger).toBeDefined();
    expect(DropdownMenuContent).toBeDefined();
    expect(DropdownMenuItem).toBeDefined();
    expect(DropdownMenuCheckboxItem).toBeDefined();
    expect(DropdownMenuRadioItem).toBeDefined();
    expect(DropdownMenuLabel).toBeDefined();
    expect(DropdownMenuSeparator).toBeDefined();
    expect(DropdownMenuShortcut).toBeDefined();
    expect(DropdownMenuGroup).toBeDefined();
    expect(DropdownMenuPortal).toBeDefined();
    expect(DropdownMenuSub).toBeDefined();
    expect(DropdownMenuSubContent).toBeDefined();
    expect(DropdownMenuSubTrigger).toBeDefined();
    expect(DropdownMenuRadioGroup).toBeDefined();
  });

  it('renders DropdownMenuShortcut with custom className', () => {
    render(<DropdownMenuShortcut className="test-class">⌘S</DropdownMenuShortcut>);
    expect(screen.getByText('⌘S')).toHaveClass('test-class');
  });

  it('renders DropdownMenu with Trigger and Content', () => {
    render(
      <DropdownMenu>
        <DropdownMenuTrigger data-testid="trigger">Open Menu</DropdownMenuTrigger>
        <DropdownMenuContent>
          <DropdownMenuLabel>Actions</DropdownMenuLabel>
          <DropdownMenuItem>Menu Item</DropdownMenuItem>
          <DropdownMenuCheckboxItem checked>Checked</DropdownMenuCheckboxItem>
          <DropdownMenuRadioGroup value="opt">
            <DropdownMenuRadioItem value="opt">Radio</DropdownMenuRadioItem>
          </DropdownMenuRadioGroup>
          <DropdownMenuSeparator />
          <DropdownMenuSub>
            <DropdownMenuSubTrigger>Sub Menu</DropdownMenuSubTrigger>
            <DropdownMenuSubContent>
              <DropdownMenuItem>Sub Item</DropdownMenuItem>
            </DropdownMenuSubContent>
          </DropdownMenuSub>
          <DropdownMenuPortal>
            <DropdownMenuItem>Portal Item</DropdownMenuItem>
          </DropdownMenuPortal>
        </DropdownMenuContent>
      </DropdownMenu>,
    );
    // Trigger is in main DOM; content is in portal
    expect(screen.getByTestId('trigger')).toBeInTheDocument();
  });

  it('renders DropdownMenuGroup standalone', () => {
    render(<DropdownMenuGroup><span>Grouped</span></DropdownMenuGroup>);
    expect(screen.getByText('Grouped')).toBeInTheDocument();
  });

  it('renders DropdownMenuSubTrigger with inset prop and ChevronRight', () => {
    render(
      <DropdownMenu>
        <DropdownMenuTrigger data-testid="trigger">Open</DropdownMenuTrigger>
        <DropdownMenuContent>
          <DropdownMenuSub>
            <DropdownMenuSubTrigger inset>Inset Sub</DropdownMenuSubTrigger>
            <DropdownMenuSubContent>
              <span>Nested content</span>
            </DropdownMenuSubContent>
          </DropdownMenuSub>
        </DropdownMenuContent>
      </DropdownMenu>,
    );
    expect(screen.getByTestId('trigger')).toBeInTheDocument();
  });

  it('renders DropdownMenu with custom className on Content and Item', () => {
    render(
      <DropdownMenu>
        <DropdownMenuTrigger data-testid="trigger">Open</DropdownMenuTrigger>
        <DropdownMenuContent className="content-cls">
          <DropdownMenuItem inset className="item-cls">
            Inset item
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>,
    );
    expect(screen.getByTestId('trigger')).toBeInTheDocument();
  });

  it('renders DropdownMenuSubContent with custom className', () => {
    render(
      <DropdownMenu>
        <DropdownMenuTrigger>Open</DropdownMenuTrigger>
        <DropdownMenuContent>
          <DropdownMenuSub>
            <DropdownMenuSubTrigger>More</DropdownMenuSubTrigger>
            <DropdownMenuSubContent className="sub-content-cls">
              <span>Sub content</span>
            </DropdownMenuSubContent>
          </DropdownMenuSub>
        </DropdownMenuContent>
      </DropdownMenu>,
    );
  });
});

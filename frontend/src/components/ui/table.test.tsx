import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Table, TableHeader, TableBody, TableFooter, TableHead, TableRow, TableCell, TableCaption } from '@/components/ui/table';

describe('Table', () => {
  it('exports all table components', () => {
    expect(Table).toBeDefined();
    expect(TableHeader).toBeDefined();
    expect(TableBody).toBeDefined();
    expect(TableFooter).toBeDefined();
    expect(TableHead).toBeDefined();
    expect(TableRow).toBeDefined();
    expect(TableCell).toBeDefined();
    expect(TableCaption).toBeDefined();
  });

  it('renders Table with TableHeader, TableBody, and cells', () => {
    render(
      <Table data-testid="table">
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow>
            <TableCell>Value</TableCell>
          </TableRow>
        </TableBody>
      </Table>,
    );
    expect(screen.getByTestId('table')).toBeInTheDocument();
    expect(screen.getByText('Name')).toBeInTheDocument();
    expect(screen.getByText('Value')).toBeInTheDocument();
  });

  it('renders TableFooter', () => {
    render(
      <Table>
        <TableFooter data-testid="footer">
          <TableRow>
            <TableCell>Footer cell</TableCell>
          </TableRow>
        </TableFooter>
      </Table>,
    );
    expect(screen.getByTestId('footer')).toBeInTheDocument();
    expect(screen.getByText('Footer cell')).toBeInTheDocument();
  });

  it('renders TableCaption', () => {
    render(
      <Table>
        <TableCaption data-testid="caption">Table description</TableCaption>
      </Table>,
    );
    expect(screen.getByTestId('caption')).toBeInTheDocument();
    expect(screen.getByText('Table description')).toBeInTheDocument();
  });

  it('applies custom className to Table', () => {
    render(<Table className="custom-class" data-testid="table" />);
    expect(screen.getByTestId('table')).toHaveClass('custom-class');
  });
});

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getConsumers, getConsumerGroupDetail } from '@/lib/api';
import { useDebounce } from '@/hooks/use-debounce';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter,
} from '@/components/ui/dialog';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import { Users, Search, Eye } from 'lucide-react';

export function ConsumersPage() {
  const [search, setSearch] = useState('');
  const debouncedSearch = useDebounce(search, 300);
  const [detailGroup, setDetailGroup] = useState<string | null>(null);

  const { data, isLoading } = useQuery({
    queryKey: ['consumers'],
    queryFn: getConsumers,
    refetchInterval: 10_000,
  });

  const { data: groupDetail } = useQuery({
    queryKey: ['consumer-group', detailGroup],
    queryFn: () => getConsumerGroupDetail(detailGroup!),
    enabled: !!detailGroup,
  });

  const filtered = data?.groups?.filter((g) =>
    g.toLowerCase().includes(debouncedSearch.toLowerCase()),
  ) ?? [];

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <Card>
          <CardContent className="py-4">
            {[1, 2, 3].map((i) => <Skeleton key={i} className="h-12 w-full mb-2" />)}
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">Consumers</h1>
          <p className="text-sm text-text-secondary">Consumer groups and assignments</p>
        </div>
      </div>

      {/* Stats */}
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
            <CardTitle className="text-sm font-medium text-text-secondary">Total Groups</CardTitle>
            <Users className="h-4 w-4 text-accent" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{data?.count ?? 0}</div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
            <CardTitle className="text-sm font-medium text-text-secondary">Matching</CardTitle>
            <Search className="h-4 w-4 text-text-muted" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{filtered.length}</div>
          </CardContent>
        </Card>
      </div>

      {/* Consumer Groups */}
      <Card>
        <CardHeader>
          <CardTitle>Consumer Groups</CardTitle>
          <CardDescription>Active consumer groups across all topics</CardDescription>
          <div className="relative mt-2">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-text-muted" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Filter groups..."
              className="pl-9"
            />
          </div>
        </CardHeader>
        <CardContent>
          {data?.count === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Users className="h-12 w-12 text-text-muted mb-4" />
              <h3 className="text-lg font-medium text-foreground">No consumer groups</h3>
              <p className="text-sm text-text-secondary mt-1">
                Consumer groups appear when clients connect and subscribe
              </p>
              <code className="mt-3 text-xs font-mono bg-surface px-2 py-1 rounded">
                chimera consume --topic my-topic
              </code>
            </div>
          ) : filtered.length === 0 && search ? (
            <div className="text-center py-8 text-text-muted">
              <Search className="h-8 w-8 mx-auto mb-2" />
              <p className="text-sm">No groups matching "{search}"</p>
            </div>
          ) : (
            <>
              {/* Mobile card view */}
              <div className="space-y-2 sm:hidden">
                {filtered.map((g) => (
                  <div
                    key={g}
                    className="rounded-lg border border-border bg-background p-4 space-y-2"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Users className="h-4 w-4 text-text-muted" />
                        <span className="font-mono text-sm font-medium">{g}</span>
                      </div>
                      <Badge variant="secondary">Active</Badge>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-11 w-full min-h-[44px]"
                      onClick={() => setDetailGroup(g)}
                      aria-label={`View consumer group ${g}`}
                    >
                      <Eye className="h-4 w-4 mr-2" />
                      View Details
                    </Button>
                  </div>
                ))}
              </div>

              {/* Desktop list */}
              <div className="hidden sm:block space-y-2">
                {filtered.map((g) => (
                  <div
                    key={g}
                    className="flex items-center justify-between rounded-lg border border-border bg-background p-4 hover:bg-surface/50 transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      <Users className="h-4 w-4 text-text-muted" />
                      <span className="font-mono text-sm">{g}</span>
                      <Badge variant="secondary">Active</Badge>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => setDetailGroup(g)}
                      aria-label={`View consumer group ${g}`}
                    >
                      <Eye className="h-4 w-4" />
                    </Button>
                  </div>
                ))}
              </div>
            </>
          )}
        </CardContent>
      </Card>

      {/* Group Detail Dialog */}
      <Dialog open={!!detailGroup} onOpenChange={(open) => !open && setDetailGroup(null)}>
        <DialogContent className="max-w-3xl w-full h-full sm:h-auto sm:rounded-lg">
          <DialogHeader>
            <DialogTitle className="font-mono">{detailGroup}</DialogTitle>
            <DialogDescription>
              {groupDetail && `${groupDetail.members.length} member(s), ${Object.keys(groupDetail.assignments).length} assignment(s)`}
            </DialogDescription>
          </DialogHeader>
          {groupDetail && (
            <div className="space-y-4">
              {/* Members */}
              <div>
                <h3 className="text-sm font-medium mb-2">Members</h3>
                {groupDetail.members.length === 0 ? (
                  <p className="text-sm text-text-muted py-4 text-center">No active members</p>
                ) : (
                  <div className="space-y-2">
                    {groupDetail.members.map((m) => (
                      <div
                        key={m.id}
                        className="flex items-center gap-3 rounded-lg border border-border bg-background p-3"
                      >
                        <Users className="h-4 w-4 text-accent" />
                        <span className="font-mono text-sm flex-1">{m.id}</span>
                        <Badge variant="outline">{m.partitions.length} partition(s)</Badge>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Partition Assignments */}
              {Object.keys(groupDetail.assignments).length > 0 && (
                <div>
                  <h3 className="text-sm font-medium mb-2">Partition Assignments</h3>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Member</TableHead>
                        <TableHead>Assigned Partitions</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {Object.entries(groupDetail.assignments).map(([memberId, partitions]) => (
                        <TableRow key={memberId}>
                          <TableCell className="font-mono text-sm">{memberId}</TableCell>
                          <TableCell>
                            <div className="flex gap-1 flex-wrap">
                              {partitions.sort((a, b) => a - b).map((p) => (
                                <Badge key={p} variant="outline" className="font-mono text-xs">
                                  {p}
                                </Badge>
                              ))}
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDetailGroup(null)}>Close</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

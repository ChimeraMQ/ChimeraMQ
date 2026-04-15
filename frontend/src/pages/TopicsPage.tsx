import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import { getTopics, getTopicDetail, createTopic, deleteTopic, type TopicInfo } from '@/lib/api';
import { useDebounce } from '@/hooks/use-debounce';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger,
} from '@/components/ui/dialog';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { Plus, Trash2, Layers, Eye, HardDrive, Search, Filter, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select';

const topicSchema = z.object({
  name: z.string().min(1, 'Name is required').max(128, 'Name is too long').regex(/^[a-zA-Z0-9._-]+$/, 'Only letters, numbers, dots, hyphens, and underscores'),
  mode: z.enum(['unified', 'stream', 'queue']),
  partitions: z.coerce.number().min(1, 'At least 1 partition').max(256, 'Maximum 256 partitions'),
});

type TopicFormValues = z.infer<typeof topicSchema>;

const modeColors: Record<string, 'default' | 'success' | 'warning'> = {
  stream: 'default',
  queue: 'warning',
  unified: 'success',
};

export function TopicsPage() {
  const queryClient = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [detailTopic, setDetailTopic] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const debouncedSearch = useDebounce(search, 300);
  const [modeFilter, setModeFilter] = useState<string>('all');

  const form = useForm<TopicFormValues>({
    resolver: zodResolver(topicSchema),
    defaultValues: { name: '', mode: 'unified', partitions: 8 },
  });

  const { data: topics, isLoading } = useQuery({
    queryKey: ['topics'],
    queryFn: () => getTopics(1000, 0),
    refetchInterval: 10_000,
  });

  const { data: topicDetail } = useQuery({
    queryKey: ['topic-detail', detailTopic],
    queryFn: () => getTopicDetail(detailTopic!),
    enabled: !!detailTopic,
  });

  const filtered = topics?.filter((t) => {
    const matchesSearch = t.name.toLowerCase().includes(debouncedSearch.toLowerCase());
    const matchesMode = modeFilter === 'all' || t.mode === modeFilter;
    return matchesSearch && matchesMode;
  }) ?? [];

  const createMutation = useMutation({
    mutationFn: (data: TopicFormValues) => createTopic(data.name, data.mode, data.partitions),
    onSuccess: (_, data) => {
      toast.success(`Topic "${data.name}" created`);
      setCreateOpen(false);
      form.reset();
      queryClient.invalidateQueries({ queryKey: ['topics'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to create topic');
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteTopic(deleteTarget!),
    onSuccess: () => {
      toast.success(`Topic "${deleteTarget}" deleted`);
      setDeleteTarget(null);
      queryClient.invalidateQueries({ queryKey: ['topics'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to delete topic');
    },
  });

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <Card>
          <CardContent className="py-4">
            {[1, 2, 3, 4, 5].map((i) => (
              <Skeleton key={i} className="h-12 w-full mb-2" />
            ))}
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
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">Topics</h1>
          <p className="text-sm text-text-secondary">Manage topics and partitions</p>
        </div>
        <Dialog open={createOpen} onOpenChange={(open) => { setCreateOpen(open); if (open) form.reset(); }}>
          <DialogTrigger asChild>
            <Button size="sm">
              <Plus className="mr-2 h-4 w-4" />
              Create Topic
            </Button>
          </DialogTrigger>
          <DialogContent className="max-w-2xl sm:max-w-lg w-full h-full sm:h-auto sm:rounded-lg fixed sm:relative inset-0 sm:inset-auto">
            <DialogHeader>
              <DialogTitle>Create Topic</DialogTitle>
              <DialogDescription>Create a new ChimeraMQ topic</DialogDescription>
            </DialogHeader>
            <form onSubmit={form.handleSubmit((data) => createMutation.mutate(data))} className="space-y-4">
              <div>
                <Label htmlFor="topic-name">Name</Label>
                <Input
                  id="topic-name"
                  {...form.register('name')}
                  placeholder="my-topic"
                  aria-invalid={!!form.formState.errors.name}
                />
                {form.formState.errors.name && (
                  <p className="text-xs text-error mt-1">{form.formState.errors.name.message}</p>
                )}
              </div>
              <div>
                <Label htmlFor="topic-mode">Mode</Label>
                <Select
                  value={form.watch('mode')}
                  onValueChange={(v) => form.setValue('mode', v as TopicFormValues['mode'])}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="unified">Unified (stream + queue)</SelectItem>
                    <SelectItem value="stream">Stream (offset-based)</SelectItem>
                    <SelectItem value="queue">Queue (competing consumers)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label htmlFor="topic-partitions">Partitions</Label>
                <Input
                  id="topic-partitions"
                  type="number"
                  min="1"
                  max="256"
                  {...form.register('partitions', { valueAsNumber: true })}
                />
                {form.formState.errors.partitions && (
                  <p className="text-xs text-error mt-1">{form.formState.errors.partitions.message}</p>
                )}
              </div>
              <DialogFooter>
                <Button variant="outline" type="button" onClick={() => setCreateOpen(false)}>Cancel</Button>
                <Button
                  type="submit"
                  disabled={!form.formState.isValid || createMutation.isPending}
                >
                  {createMutation.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                  Create
                </Button>
              </DialogFooter>
            </form>
          </DialogContent>
        </Dialog>
      </div>

      {/* Topics Table */}
      <Card>
        <CardHeader>
          <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-2">
              <Layers className="h-5 w-5 text-text-muted" />
              <CardTitle className="text-base">{topics?.length ?? 0} Topics</CardTitle>
            </div>
            <div className="flex gap-2 w-full sm:w-auto">
              <div className="relative flex-1 sm:flex-initial">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-text-muted" />
                <Input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search topics..."
                  className="pl-9 w-full sm:w-48"
                />
              </div>
              <div className="relative">
                <Filter className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-text-muted pointer-events-none z-10" />
                <Select
                  value={modeFilter}
                  onValueChange={setModeFilter}
                >
                  <SelectTrigger className="w-28 pl-9">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All</SelectItem>
                    <SelectItem value="unified">Unified</SelectItem>
                    <SelectItem value="stream">Stream</SelectItem>
                    <SelectItem value="queue">Queue</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>
          <CardDescription className="sr-only">Filter and search topics</CardDescription>
        </CardHeader>
        <CardContent>
          {topics?.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Layers className="h-12 w-12 text-text-muted mb-4" />
              <h3 className="text-lg font-medium text-foreground">No topics yet</h3>
              <p className="text-sm text-text-secondary mt-1">Create your first topic to get started</p>
              <Button className="mt-4" size="sm" onClick={() => setCreateOpen(true)}>
                <Plus className="mr-2 h-4 w-4" />
                Create Topic
              </Button>
            </div>
          ) : (
            <>
              {/* Mobile card view */}
              <div className="space-y-2 sm:hidden">
                {filtered.length === 0 && (search || modeFilter !== 'all') ? (
                  <p className="text-center text-text-muted py-8 text-sm">No topics matching filters</p>
                ) : (
                  filtered?.map((t: TopicInfo) => (
                    <div key={t.name} className="rounded-lg border border-border bg-background p-4 space-y-2">
                      <div className="flex items-center justify-between">
                        <span className="font-mono text-sm font-medium">{t.name}</span>
                        <Badge variant={modeColors[t.mode] ?? 'default'}>{t.mode}</Badge>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-text-muted">{t.partitions} partition(s)</span>
                        <span className="text-text-muted text-xs">{new Date(t.created_at).toLocaleString()}</span>
                      </div>
                      <div className="flex gap-1 pt-1 border-t border-border">
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-11 w-full min-h-[44px]"
                          onClick={() => setDetailTopic(t.name)}
                          aria-label={`View topic ${t.name}`}
                        >
                          <Eye className="h-4 w-4 mr-2" />
                          View
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-11 w-full min-h-[44px] text-error hover:text-error"
                          onClick={() => setDeleteTarget(t.name)}
                          aria-label={`Delete topic ${t.name}`}
                        >
                          <Trash2 className="h-4 w-4 mr-2" />
                          Delete
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>

              {/* Desktop table */}
              <Table className="hidden sm:table">
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Mode</TableHead>
                    <TableHead>Partitions</TableHead>
                    <TableHead>Created</TableHead>
                    <TableHead className="w-28">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filtered.length === 0 && (search || modeFilter !== 'all') ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center text-text-muted py-8">
                        No topics matching filters
                      </TableCell>
                    </TableRow>
                  ) : (
                    filtered?.map((t: TopicInfo) => (
                      <TableRow key={t.name}>
                        <TableCell className="font-mono text-sm">{t.name}</TableCell>
                        <TableCell>
                          <Badge variant={modeColors[t.mode] ?? 'default'}>{t.mode}</Badge>
                        </TableCell>
                        <TableCell>{t.partitions}</TableCell>
                        <TableCell className="text-text-muted text-xs">
                          {new Date(t.created_at).toLocaleString()}
                        </TableCell>
                        <TableCell className="w-28">
                          <div className="flex gap-1">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => setDetailTopic(t.name)}
                              aria-label={`View topic ${t.name}`}
                            >
                              <Eye className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="text-error hover:text-error"
                              onClick={() => setDeleteTarget(t.name)}
                              aria-label={`Delete topic ${t.name}`}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </>
          )}
        </CardContent>
      </Card>

      {/* Topic Detail Dialog */}
      <Dialog open={!!detailTopic} onOpenChange={(open) => !open && setDetailTopic(null)}>
        <DialogContent className="max-w-3xl w-full h-full sm:h-auto sm:rounded-lg">
          <DialogHeader>
            <DialogTitle className="font-mono">{detailTopic}</DialogTitle>
            <DialogDescription>
              {topicDetail && (
                <span className="flex items-center gap-2">
                  <Badge variant={modeColors[topicDetail.mode] ?? 'default'}>{topicDetail.mode}</Badge>
                  {topicDetail.partitions} partition(s)
                </span>
              )}
            </DialogDescription>
          </DialogHeader>
          {topicDetail && (
            <div className="space-y-4">
              <div className="grid gap-2 sm:grid-cols-3 text-sm">
                <div className="rounded-lg border border-border bg-background p-3">
                  <p className="text-xs text-text-muted">Partitions</p>
                  <p className="text-lg font-semibold">{topicDetail.partitions}</p>
                </div>
                <div className="rounded-lg border border-border bg-background p-3">
                  <p className="text-xs text-text-muted">Created</p>
                  <p className="text-sm">{topicDetail.created_at ? new Date(topicDetail.created_at).toLocaleString() : 'N/A'}</p>
                </div>
                <div className="rounded-lg border border-border bg-background p-3">
                  <p className="text-xs text-text-muted">Total Messages</p>
                  <p className="text-lg font-semibold">
                    {topicDetail.partitions_detail?.reduce((sum, p) => sum + p.high_watermark, 0) ?? 0}
                  </p>
                </div>
              </div>
              {/* Partition table — cards on mobile, table on desktop */}
              <div className="space-y-2 sm:hidden">
                {topicDetail.partitions_detail?.map((p) => (
                  <div key={p.id} className="rounded-lg border border-border bg-background p-3">
                    <div className="flex items-center justify-between mb-1">
                      <span className="font-mono text-sm font-medium">Partition {p.id}</span>
                      <span className="text-xs text-text-muted">Depth: {(p.high_watermark - p.log_start_offset).toLocaleString()}</span>
                    </div>
                    <div className="grid grid-cols-2 gap-2 text-xs">
                      <div>
                        <span className="text-text-muted">High Watermark</span>
                        <p className="font-mono font-medium">{p.high_watermark.toLocaleString()}</p>
                      </div>
                      <div>
                        <span className="text-text-muted">Log Start</span>
                        <p className="font-mono font-medium">{p.log_start_offset.toLocaleString()}</p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
              <Table className="hidden sm:table">
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-16">Partition</TableHead>
                    <TableHead>
                      <span className="flex items-center gap-1">
                        <HardDrive className="h-3 w-3" />
                        High Watermark
                      </span>
                    </TableHead>
                    <TableHead>Log Start Offset</TableHead>
                    <TableHead>Depth</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {topicDetail.partitions_detail?.map((p) => (
                    <TableRow key={p.id}>
                      <TableCell className="font-mono text-sm">{p.id}</TableCell>
                      <TableCell>{p.high_watermark.toLocaleString()}</TableCell>
                      <TableCell className="text-text-muted text-xs">{p.log_start_offset.toLocaleString()}</TableCell>
                      <TableCell>{(p.high_watermark - p.log_start_offset).toLocaleString()}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDetailTopic(null)}>Close</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation */}
      <AlertDialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Topic</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete "{deleteTarget}"? This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => deleteMutation.mutate()}
              className="bg-error hover:bg-error/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

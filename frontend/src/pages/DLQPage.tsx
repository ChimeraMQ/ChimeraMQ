import { useState, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { z } from 'zod';
import { useVirtualizer } from '@tanstack/react-virtual';
import { listDLQTopics, getDLQ, clearDLQ, replayDLQ, type DLQEntry } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import {
  Tabs, TabsContent, TabsList, TabsTrigger,
} from '@/components/ui/tabs';
import { AlertTriangle, RefreshCw, Trash2, Eye, MessageSquare, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { Switch } from '@/components/ui/switch';

const replaySchema = z.object({
  targetTopic: z.string().max(256, 'Topic name is too long').optional().or(z.literal('')),
  dryRun: z.boolean(),
  deleteAfter: z.boolean(),
});

type ReplayFormValues = z.infer<typeof replaySchema>;

/**
 * Virtualized list for large DLQ entry sets (100+ rows).
 * Only renders visible rows to the DOM, keeping the page snappy.
 */
function EntriesVirtualList({ entries, onView }: { entries: DLQEntry[]; onView: (entry: DLQEntry) => void }) {
  const parentRef = useRef<HTMLDivElement>(null);

  const virtualizer = useVirtualizer({
    count: entries.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 120,
    overscan: 5,
  });

  return (
    <div
      ref={parentRef}
      className="max-h-[600px] overflow-auto space-y-2 rounded-lg border border-border p-2"
    >
      <div
        style={{ height: `${virtualizer.getTotalSize()}px`, width: '100%', position: 'relative' }}
      >
        {virtualizer.getVirtualItems().map((virtualItem) => {
          const entry = entries[virtualItem.index];
          if (!entry) return null;
          return (
            <div
              key={entry.id}
              data-index={virtualItem.index}
              ref={virtualizer.measureElement}
              className="absolute left-0 w-full rounded-lg border border-border bg-background p-4 hover:bg-surface/50 transition-colors"
              style={{ top: `${virtualItem.start}px` }}
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <code className="text-xs font-mono text-text-muted">
                      ID: {entry.id}
                    </code>
                    <Badge variant="secondary">
                      Partition {entry.partition}
                    </Badge>
                    <Badge variant="outline">
                      {entry.retries} retries
                    </Badge>
                  </div>
                  <p className="text-sm text-foreground truncate">
                    {entry.original_msg?.topic ?? entry.topic}
                  </p>
                  <p className="text-xs text-text-muted mt-1 truncate">
                    Reason: {entry.reason}
                  </p>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <span className="text-xs text-text-muted">
                    {new Date(entry.failed_at).toLocaleString()}
                  </span>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-11 min-w-[44px]"
                    onClick={() => onView(entry)}
                    aria-label={`View DLQ entry ${entry.id}`}
                  >
                    <Eye className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

async function listDLQ(): Promise<string[]> {
  try {
    const data = await listDLQTopics();
    return data.topics ?? [];
  } catch {
    return [];
  }
}

async function peekDLQ(topic: string, limit = 50): Promise<DLQEntry[]> {
  try {
    const data = await getDLQ(topic, limit);
    return data.entries ?? [];
  } catch {
    return [];
  }
}

export function DLQPage() {
  const queryClient = useQueryClient();
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const [viewOpen, setViewOpen] = useState(false);
  const [viewEntry, setViewEntry] = useState<DLQEntry | null>(null);
  const [replayOpen, setReplayOpen] = useState(false);
  const [clearOpen, setClearOpen] = useState(false);

  const replayForm = useForm<ReplayFormValues>({
    resolver: zodResolver(replaySchema),
    defaultValues: { targetTopic: '', dryRun: false, deleteAfter: false },
  });

  const { data: topics, isLoading } = useQuery({
    queryKey: ['dlq-topics'],
    queryFn: listDLQ,
    refetchInterval: 15_000,
  });

  const { data: entries, isLoading: entriesLoading } = useQuery({
    queryKey: ['dlq-entries', selectedTopic],
    queryFn: () => peekDLQ(selectedTopic!),
    enabled: !!selectedTopic,
    refetchInterval: 10_000,
  });

  const replayMutation = useMutation({
    mutationFn: (data: ReplayFormValues) => {
      return replayDLQ(selectedTopic!, {
        dry_run: data.dryRun,
        delete_after_replay: data.deleteAfter,
        ...(data.targetTopic && { target_topic: data.targetTopic }),
      });
    },
    onSuccess: (data) => {
      if (data.dry_run) {
        toast.info(`Dry run: ${data.replayed} messages would be replayed`);
      } else {
        toast.success(`Replayed ${data.replayed} messages from "${selectedTopic}"`);
      }
      setReplayOpen(false);
      replayForm.reset();
      queryClient.invalidateQueries({ queryKey: ['dlq-entries', selectedTopic] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to replay messages');
    },
  });

  const clearMutation = useMutation({
    mutationFn: () => clearDLQ(selectedTopic!),
    onSuccess: () => {
      toast.success(`DLQ "${selectedTopic}" cleared`);
      setClearOpen(false);
      setSelectedTopic(null);
      queryClient.invalidateQueries({ queryKey: ['dlq-topics'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to clear DLQ');
    },
  });

  if (isLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <Card><CardContent className="py-4"><Skeleton className="h-12 w-full mb-2" /></CardContent></Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">Dead Letter Queue</h1>
          <p className="text-sm text-text-secondary">Inspect and replay failed messages</p>
        </div>
      </div>

      {/* Topics List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-text-muted" />
            {topics?.length ?? 0} DLQ Topics
          </CardTitle>
          <CardDescription>
            Messages that exceeded max retries are routed here
          </CardDescription>
        </CardHeader>
        <CardContent>
          {(!topics || topics.length === 0) ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <AlertTriangle className="h-12 w-12 text-text-muted mb-4" />
              <h3 className="text-lg font-medium text-foreground">No DLQ topics active</h3>
              <p className="text-sm text-text-secondary mt-1">
                Failed messages will appear here when DLQ is enabled
              </p>
            </div>
          ) : (
            <>
              {/* Mobile card view */}
              <div className="space-y-2 sm:hidden">
                {topics.map((t) => (
                  <div
                    key={t}
                    className="rounded-lg border border-border bg-background p-4 space-y-2"
                  >
                    <div className="flex items-center justify-between">
                      <Badge variant="destructive">{t}</Badge>
                      <span className="text-xs text-text-muted">Tap to inspect</span>
                    </div>
                    <div className="flex gap-1 pt-1 border-t border-border">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px]"
                        onClick={() => setSelectedTopic(selectedTopic === t ? null : t)}
                        aria-label={`Inspect DLQ topic ${t}`}
                      >
                        <Eye className="h-4 w-4 mr-2" />
                        Inspect
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px]"
                        onClick={() => { setSelectedTopic(t); setReplayOpen(true); }}
                        aria-label={`Replay DLQ topic ${t}`}
                      >
                        <RefreshCw className="h-4 w-4 mr-2" />
                        Replay
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px] text-error hover:text-error"
                        onClick={() => { setSelectedTopic(t); setClearOpen(true); }}
                        aria-label={`Clear DLQ topic ${t}`}
                      >
                        <Trash2 className="h-4 w-4 mr-2" />
                        Clear
                      </Button>
                    </div>
                  </div>
                ))}
              </div>

              {/* Desktop list */}
              <div className="hidden sm:block space-y-2">
                {topics.map((t) => (
                  <div
                    key={t}
                    className="flex items-center justify-between rounded-lg border border-border bg-background p-4 hover:bg-surface/50 cursor-pointer transition-colors"
                    onClick={() => setSelectedTopic(selectedTopic === t ? null : t)}
                  >
                    <div className="flex items-center gap-3">
                      <Badge variant="destructive">{t}</Badge>
                      <span className="text-sm text-text-muted">Click to inspect</span>
                    </div>
                    <div className="flex gap-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedTopic(selectedTopic === t ? null : t);
                        }}
                        aria-label={`Inspect DLQ topic ${t}`}
                      >
                        <Eye className="h-4 w-4 mr-1" />
                        Inspect
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedTopic(t);
                          setReplayOpen(true);
                        }}
                        aria-label={`Replay DLQ topic ${t}`}
                      >
                        <RefreshCw className="h-4 w-4 mr-1" />
                        Replay
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="text-error hover:text-error"
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedTopic(t);
                          setClearOpen(true);
                        }}
                        aria-label={`Clear DLQ topic ${t}`}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </CardContent>
      </Card>

      {/* Entries for Selected Topic */}
      {selectedTopic && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between">
              <span>{selectedTopic}</span>
              <div className="flex gap-2">
                <Badge variant="destructive">
                  {entriesLoading ? '...' : entries?.length ?? 0} entries
                </Badge>
              </div>
            </CardTitle>
            <CardDescription>
              Failed messages in DLQ topic
            </CardDescription>
          </CardHeader>
          <CardContent>
            {entriesLoading ? (
              <div className="space-y-2">
                {Array.from({ length: 3 }).map((_, i) => (
                  <Skeleton key={i} className="h-12 w-full" />
                ))}
              </div>
            ) : !entries || entries.length === 0 ? (
              <div className="text-center py-8 text-text-muted">
                <MessageSquare className="h-8 w-8 mx-auto mb-2" />
                <p className="text-sm">No entries in this DLQ topic</p>
              </div>
            ) : (
              <>
                {entries.length > 100 ? (
                  /* Virtualized list for 100+ rows */
                  <EntriesVirtualList entries={entries} onView={setViewEntry} />
                ) : (
                  /* Standard list for smaller datasets */
                  <div className="space-y-2">
                    {entries.map((entry) => (
                      <div
                        key={entry.id}
                        className="rounded-lg border border-border bg-background p-4 hover:bg-surface/50 transition-colors"
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <code className="text-xs font-mono text-text-muted">
                                ID: {entry.id}
                              </code>
                              <Badge variant="secondary">
                                Partition {entry.partition}
                              </Badge>
                              <Badge variant="outline">
                                {entry.retries} retries
                              </Badge>
                            </div>
                            <p className="text-sm text-foreground truncate">
                              {entry.original_msg?.topic ?? entry.topic}
                            </p>
                            <p className="text-xs text-text-muted mt-1 truncate">
                              Reason: {entry.reason}
                            </p>
                          </div>
                          <div className="flex items-center gap-2 shrink-0">
                            <span className="text-xs text-text-muted">
                              {new Date(entry.failed_at).toLocaleString()}
                            </span>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-11 min-w-[44px]"
                              onClick={() => {
                                setViewEntry(entry);
                                setViewOpen(true);
                              }}
                              aria-label={`View DLQ entry ${entry.id}`}
                            >
                              <Eye className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </>
            )}
          </CardContent>
        </Card>
      )}

      {/* View Entry Dialog */}
      <Dialog open={viewOpen} onOpenChange={setViewOpen}>
        <DialogContent className="max-w-3xl w-full h-full sm:h-auto sm:rounded-lg">
          <DialogHeader>
            <DialogTitle>DLQ Entry #{viewEntry?.id}</DialogTitle>
            <DialogDescription>
              {viewEntry?.original_msg?.topic} — Partition {viewEntry?.partition}
            </DialogDescription>
          </DialogHeader>
          {viewEntry && (
            <Tabs defaultValue="message">
              <TabsList>
                <TabsTrigger value="message">Message</TabsTrigger>
                <TabsTrigger value="details">Details</TabsTrigger>
                <TabsTrigger value="raw">Raw JSON</TabsTrigger>
              </TabsList>
              <TabsContent value="message" className="mt-4">
                <div className="space-y-3">
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
                    <div>
                      <Label className="text-text-muted">Topic</Label>
                      <p className="font-mono">{viewEntry.original_msg?.topic}</p>
                    </div>
                    <div>
                      <Label className="text-text-muted">Message ID</Label>
                      <p className="font-mono">{viewEntry.original_msg?.id}</p>
                    </div>
                    <div>
                      <Label className="text-text-muted">Failed At</Label>
                      <p>{new Date(viewEntry.failed_at).toLocaleString()}</p>
                    </div>
                    <div>
                      <Label className="text-text-muted">Retries</Label>
                      <p>{viewEntry.retries}</p>
                    </div>
                  </div>
                  <div>
                    <Label className="text-text-muted">Failure Reason</Label>
                    <p className="text-sm text-error">{viewEntry.reason}</p>
                  </div>
                  {viewEntry.original_msg?.body && (
                    <div>
                      <Label className="text-text-muted">Body</Label>
                      <pre className="rounded-lg border border-border bg-background p-3 text-xs overflow-auto max-h-64 font-mono mt-1">
                        {viewEntry.original_msg.body}
                      </pre>
                    </div>
                  )}
                </div>
              </TabsContent>
              <TabsContent value="details" className="mt-4">
                <div className="space-y-3 text-sm">
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                    <div>
                      <Label className="text-text-muted">DLQ Entry ID</Label>
                      <p className="font-mono">{viewEntry.id}</p>
                    </div>
                    <div>
                      <Label className="text-text-muted">Partition</Label>
                      <p>{viewEntry.partition}</p>
                    </div>
                  </div>
                  {viewEntry.original_msg?.headers && Object.keys(viewEntry.original_msg.headers).length > 0 && (
                    <div>
                      <Label className="text-text-muted">Headers</Label>
                      <pre className="rounded-lg border border-border bg-background p-3 text-xs overflow-auto max-h-48 font-mono mt-1">
                        {JSON.stringify(viewEntry.original_msg.headers, null, 2)}
                      </pre>
                    </div>
                  )}
                </div>
              </TabsContent>
              <TabsContent value="raw" className="mt-4">
                <pre className="rounded-lg border border-border bg-background p-3 text-xs overflow-auto max-h-96 font-mono">
                  {JSON.stringify(viewEntry, null, 2)}
                </pre>
              </TabsContent>
            </Tabs>
          )}
        </DialogContent>
      </Dialog>

      {/* Replay Dialog */}
      <Dialog open={replayOpen} onOpenChange={(open) => { setReplayOpen(open); if (open) replayForm.reset(); }}>
        <DialogContent className="w-full h-full sm:h-auto sm:rounded-lg">
          <DialogHeader>
            <DialogTitle>Replay DLQ Messages</DialogTitle>
            <DialogDescription>
              Replay failed messages from "{selectedTopic}"
            </DialogDescription>
          </DialogHeader>
          <form onSubmit={replayForm.handleSubmit((data) => replayMutation.mutate(data))} className="space-y-4">
            <div>
              <Label htmlFor="replay-target">Target Topic (optional)</Label>
              <Input
                id="replay-target"
                {...replayForm.register('targetTopic')}
                placeholder="Leave empty for original topic"
              />
              {replayForm.formState.errors.targetTopic && (
                <p className="text-xs text-error mt-1">{replayForm.formState.errors.targetTopic.message}</p>
              )}
              <p className="text-xs text-text-muted mt-1">
                Messages will be replayed to their original topic if not specified
              </p>
            </div>
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <Switch
                  id="dry-run"
                  checked={replayForm.watch('dryRun')}
                  onCheckedChange={(v) => replayForm.setValue('dryRun', v)}
                />
                <Label htmlFor="dry-run" className="text-sm">Dry run (preview only)</Label>
              </div>
              <div className="flex items-center gap-2">
                <Switch
                  id="delete-after"
                  checked={replayForm.watch('deleteAfter')}
                  onCheckedChange={(v) => replayForm.setValue('deleteAfter', v)}
                />
                <Label htmlFor="delete-after" className="text-sm">Delete after replay</Label>
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" type="button" onClick={() => setReplayOpen(false)}>Cancel</Button>
              <Button
                type="submit"
                disabled={!replayForm.formState.isValid || replayMutation.isPending}
                variant={replayForm.watch('dryRun') ? 'outline' : 'default'}
              >
                {replayMutation.isPending && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
                <RefreshCw className="h-4 w-4 mr-2" />
                {replayForm.watch('dryRun') ? 'Preview' : 'Replay'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      {/* Clear Confirmation */}
      <AlertDialog open={clearOpen} onOpenChange={setClearOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Clear DLQ Topic</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to clear all entries from "{selectedTopic}"? This action cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => clearMutation.mutate()}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Clear
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

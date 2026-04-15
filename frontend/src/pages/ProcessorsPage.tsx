import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listProcessors, getProcessor, deleteProcessor, startProcessor, stopProcessor } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { Workflow, Trash2, Play, Pause, Eye } from 'lucide-react';
import { toast } from 'sonner';

const stateColors: Record<string, 'success' | 'warning' | 'default' | 'destructive'> = {
  running: 'success',
  stopped: 'default',
  error: 'destructive',
};

export function ProcessorsPage() {
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [detailTarget, setDetailTarget] = useState<string | null>(null);

  const { data, isLoading } = useQuery({
    queryKey: ['processors'],
    queryFn: listProcessors,
    refetchInterval: 15_000,
  });

  const { data: processorDetail } = useQuery({
    queryKey: ['processor-detail', detailTarget],
    queryFn: () => getProcessor(detailTarget!),
    enabled: !!detailTarget,
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteProcessor(deleteTarget!),
    onSuccess: () => {
      toast.success(`Processor "${deleteTarget}" deleted`);
      setDeleteTarget(null);
      queryClient.invalidateQueries({ queryKey: ['processors'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to delete processor');
    },
  });

  const startMutation = useMutation({
    mutationFn: (name: string) => startProcessor(name),
    onSuccess: (_, name) => {
      toast.success(`Processor "${name}" started`);
      queryClient.invalidateQueries({ queryKey: ['processors'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to start processor');
    },
  });

  const stopMutation = useMutation({
    mutationFn: (name: string) => stopProcessor(name),
    onSuccess: (_, name) => {
      toast.success(`Processor "${name}" stopped`);
      queryClient.invalidateQueries({ queryKey: ['processors'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to stop processor');
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
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">Stream Processors</h1>
          <p className="text-sm text-text-secondary">Stream processing topologies for real-time data transforms</p>
        </div>
      </div>

      {/* Processor List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Workflow className="h-5 w-5 text-text-muted" />
            {data?.count ?? 0} Processors
          </CardTitle>
          <CardDescription>
            Active stream processing topologies
          </CardDescription>
        </CardHeader>
        <CardContent>
          {data?.count === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Workflow className="h-12 w-12 text-text-muted mb-4" />
              <h3 className="text-lg font-medium text-foreground">No processors configured</h3>
              <p className="text-sm text-text-secondary mt-1">
                Create a processing topology via the CLI to enable stream transforms
              </p>
              <code className="mt-3 text-xs font-mono bg-surface px-2 py-1 rounded">
                chimera processor create my-topology
              </code>
            </div>
          ) : (
            <>
              {/* Mobile card view */}
              <div className="space-y-2 sm:hidden">
                {data?.topologies.map((name) => (
                  <div
                    key={name}
                    className="rounded-lg border border-border bg-background p-4 space-y-2"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Workflow className="h-4 w-4 text-accent shrink-0" />
                        <span className="font-mono text-sm font-medium">{name}</span>
                      </div>
                      <Badge variant="secondary">Processor</Badge>
                    </div>
                    <div className="grid grid-cols-2 gap-1 pt-1 border-t border-border">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px]"
                        onClick={() => setDetailTarget(name)}
                        aria-label={`View processor ${name}`}
                      >
                        <Eye className="h-4 w-4 mr-2" />
                        View
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px]"
                        onClick={() => startMutation.mutate(name)}
                        aria-label={`Start processor ${name}`}
                      >
                        <Play className="h-4 w-4 mr-2" />
                        Start
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px]"
                        onClick={() => stopMutation.mutate(name)}
                        aria-label={`Stop processor ${name}`}
                      >
                        <Pause className="h-4 w-4 mr-2" />
                        Stop
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-11 w-full min-h-[44px] text-error hover:text-error"
                        onClick={() => setDeleteTarget(name)}
                        aria-label={`Delete processor ${name}`}
                      >
                        <Trash2 className="h-4 w-4 mr-2" />
                        Delete
                      </Button>
                    </div>
                  </div>
                ))}
              </div>

              {/* Desktop list */}
              <div className="hidden sm:block space-y-2">
                {data?.topologies.map((name) => (
                  <div
                    key={name}
                    className="flex items-center justify-between rounded-lg border border-border bg-background p-4 hover:bg-surface/50 transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      <Workflow className="h-4 w-4 text-accent" />
                      <div>
                        <span className="font-mono text-sm">{name}</span>
                        <Badge variant="secondary" className="ml-2">Processor</Badge>
                      </div>
                    </div>
                    <div className="flex gap-1">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setDetailTarget(name)}
                        aria-label={`View processor ${name}`}
                      >
                        <Eye className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => startMutation.mutate(name)}
                        aria-label={`Start processor ${name}`}
                      >
                        <Play className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => stopMutation.mutate(name)}
                        aria-label={`Stop processor ${name}`}
                      >
                        <Pause className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="text-error hover:text-error"
                        onClick={() => setDeleteTarget(name)}
                        aria-label={`Delete processor ${name}`}
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

      {/* Processor Detail */}
      {processorDetail && (
        <Card>
          <CardHeader>
            <CardTitle className="font-mono">{detailTarget}</CardTitle>
            <CardDescription>
              <Badge variant={stateColors[processorDetail.state] ?? 'default'} className="mr-2">
                {processorDetail.state}
              </Badge>
              {processorDetail.operators} operator(s)
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">Source Topic</p>
                <p className="text-sm font-mono mt-1">{processorDetail.source_topic}</p>
              </div>
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">Sink Topic</p>
                <p className="text-sm font-mono mt-1">{processorDetail.sink_topic}</p>
              </div>
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">Parallelism</p>
                <p className="text-lg font-semibold mt-1">{processorDetail.parallelism}</p>
              </div>
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">Operators</p>
                <p className="text-lg font-semibold mt-1">{processorDetail.operators}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Delete Confirmation */}
      <AlertDialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Processor</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete "{deleteTarget}"? This will stop and remove the topology.
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

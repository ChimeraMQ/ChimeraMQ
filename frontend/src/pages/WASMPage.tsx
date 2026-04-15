import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listWASMModules, deleteWASMModule } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { Cpu, Trash2, Package } from 'lucide-react';
import { toast } from 'sonner';

export function WASMPage() {
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

  const { data, isLoading } = useQuery({
    queryKey: ['wasm-modules'],
    queryFn: listWASMModules,
    refetchInterval: 15_000,
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteWASMModule(deleteTarget!),
    onSuccess: () => {
      toast.success(`Module "${deleteTarget}" deleted`);
      setDeleteTarget(null);
      queryClient.invalidateQueries({ queryKey: ['wasm-modules'] });
    },
    onError: (err: { message?: string }) => {
      toast.error(err.message ?? 'Failed to delete module');
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
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">WASM Modules</h1>
          <p className="text-sm text-text-secondary">WebAssembly transform modules for publish/consume pipelines</p>
        </div>
      </div>

      {/* Modules List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Cpu className="h-5 w-5 text-text-muted" />
            {data?.count ?? 0} Modules
          </CardTitle>
          <CardDescription>
            Loaded WASM transform modules
          </CardDescription>
        </CardHeader>
        <CardContent>
          {data?.count === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <Package className="h-12 w-12 text-text-muted mb-4" />
              <h3 className="text-lg font-medium text-foreground">No WASM modules loaded</h3>
              <p className="text-sm text-text-secondary mt-1">
                Upload a WASM module via the CLI to enable message transforms
              </p>
              <code className="mt-3 text-xs font-mono bg-surface px-2 py-1 rounded">
                chimera wasm upload my-transform.wasm
              </code>
            </div>
          ) : (
            <>
              {/* Mobile card view */}
              <div className="space-y-2 sm:hidden">
                {data?.modules.map((name) => (
                  <div
                    key={name}
                    className="rounded-lg border border-border bg-background p-4 space-y-2"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Cpu className="h-4 w-4 text-accent shrink-0" />
                        <span className="font-mono text-sm font-medium">{name}</span>
                      </div>
                      <Badge variant="secondary">Active</Badge>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-11 w-full min-h-[44px] text-error hover:text-error"
                      onClick={() => setDeleteTarget(name)}
                      aria-label={`Delete WASM module ${name}`}
                    >
                      <Trash2 className="h-4 w-4 mr-2" />
                      Delete
                    </Button>
                  </div>
                ))}
              </div>

              {/* Desktop list */}
              <div className="hidden sm:block space-y-2">
                {data?.modules.map((name) => (
                  <div
                    key={name}
                    className="flex items-center justify-between rounded-lg border border-border bg-background p-4 hover:bg-surface/50 transition-colors"
                  >
                    <div className="flex items-center gap-3">
                      <Cpu className="h-4 w-4 text-accent" />
                      <div>
                        <span className="font-mono text-sm">{name}</span>
                        <Badge variant="secondary" className="ml-2">Active</Badge>
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => setDeleteTarget(name)}
                      aria-label={`Delete WASM module ${name}`}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                ))}
              </div>
            </>
          )}
        </CardContent>
      </Card>

      {/* Delete Confirmation */}
      <AlertDialog open={!!deleteTarget} onOpenChange={() => setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete WASM Module</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete "{deleteTarget}"? This will remove the transform from all pipelines.
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

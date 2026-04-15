import { useQuery } from '@tanstack/react-query';
import { getCluster, getHealth } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Server, Radio } from 'lucide-react';

const stateColors: Record<string, 'success' | 'warning' | 'destructive' | 'default'> = {
  Alive: 'success',
  Suspect: 'warning',
  Dead: 'destructive',
};

export function ClusterPage() {
  const { data: cluster, isLoading: clusterLoading } = useQuery({
    queryKey: ['cluster'],
    queryFn: getCluster,
    refetchInterval: 5_000,
  });

  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: getHealth,
    refetchInterval: 10_000,
  });

  if (clusterLoading) {
    return (
      <div className="space-y-6">
        <Skeleton className="h-8 w-48" />
        <Card><CardContent className="py-4"><Skeleton className="h-20 w-full" /></CardContent></Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">Cluster</h1>
          <p className="text-sm text-text-secondary">
            {cluster?.mode === 'cluster' ? 'Clustered mode' : 'Single-node mode'}
          </p>
        </div>
      </div>

      {cluster?.mode === 'single-node' && (
        <Card>
          <CardHeader>
            <CardTitle>Single-Node Mode</CardTitle>
            <CardDescription>
              This broker is running standalone without clustering
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="rounded-lg border border-border bg-background p-6 text-center">
              <Radio className="h-12 w-12 text-text-muted mx-auto mb-4" />
              <p className="text-sm text-text-secondary">
                Enable clustering via the config to see cluster members here
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {cluster?.mode === 'cluster' && (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-text-secondary">Members</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold">{cluster.alive_count}</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-text-secondary">Leader</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold font-mono">{cluster.leader_id ?? 'N/A'}</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-text-secondary">This Node</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold font-mono">{health?.name ?? 'N/A'}</p>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Server className="h-5 w-5 text-text-muted" />
                Members
              </CardTitle>
            </CardHeader>
            <CardContent>
              {/* Mobile card view */}
              <div className="space-y-2 sm:hidden">
                {cluster.members?.map((m) => (
                  <div
                    key={m.id}
                    className="rounded-lg border border-border bg-background p-4 space-y-2"
                  >
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Server className="h-4 w-4 text-text-muted shrink-0" />
                        <span className="font-mono text-sm font-medium">{m.id}</span>
                      </div>
                      <Badge variant={stateColors[m.state] ?? 'default'}>
                        {m.state}
                      </Badge>
                    </div>
                    <p className="text-xs text-text-muted font-mono">{m.addr}:{m.port}</p>
                  </div>
                ))}
              </div>

              {/* Desktop list */}
              <div className="hidden sm:block space-y-2">
                {cluster.members?.map((m) => (
                  <div
                    key={m.id}
                    className="flex items-center gap-4 rounded-lg border border-border bg-background p-4"
                  >
                    <Server className="h-5 w-5 text-text-muted" />
                    <div className="flex flex-col">
                      <span className="text-sm font-medium">{m.id}</span>
                      <span className="text-xs text-text-muted font-mono">{m.addr}:{m.port}</span>
                    </div>
                    <Badge variant={stateColors[m.state] ?? 'default'} className="ml-auto">
                      {m.state}
                    </Badge>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}

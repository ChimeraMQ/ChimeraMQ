import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getHealth, getTopics, getConsumers, getCluster } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Badge } from '@/components/ui/badge';
import { Layers, Users, Server, HardDrive, Zap, Activity } from 'lucide-react';
import { cn } from '@/lib/utils';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

interface ChartPoint {
  time: string;
  topics: number;
  consumers: number;
}

function StatsCard({ icon: Icon, label, value, variant }: {
  icon: React.ElementType;
  label: string;
  value: string | number;
  variant?: 'default' | 'success' | 'warning';
}) {
  const variantColors: Record<string, string> = {
    default: 'text-accent',
    success: 'text-success',
    warning: 'text-warning',
  };
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
        <CardTitle className="text-sm font-medium text-text-secondary">{label}</CardTitle>
        <Icon className={cn('h-4 w-4', variantColors[variant ?? 'default'])} />
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value}</div>
      </CardContent>
    </Card>
  );
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export function OverviewPage() {
  const [chartData, setChartData] = useState<ChartPoint[]>([]);

  const { data: health, isLoading: healthLoading } = useQuery({
    queryKey: ['health'],
    queryFn: getHealth,
    refetchInterval: 10_000,
  });

  const { data: topics, isLoading: topicsLoading } = useQuery({
    queryKey: ['topics'],
    queryFn: () => getTopics(1000, 0),
    refetchInterval: 15_000,
  });

  const { data: consumers, isLoading: consumersLoading } = useQuery({
    queryKey: ['consumers'],
    queryFn: getConsumers,
    refetchInterval: 15_000,
  });

  const { data: cluster, isLoading: clusterLoading } = useQuery({
    queryKey: ['cluster'],
    queryFn: getCluster,
    refetchInterval: 10_000,
  });

  // Append a data point to the chart on each health+topics refresh
  useEffect(() => {
    if (topics && consumers !== undefined) {
      const now = new Date();
      const timeLabel = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      setChartData((prev) => {
        const next = [...prev, { time: timeLabel, topics: topics.length, consumers: consumers?.count ?? 0 }];
        // Keep last 20 points
        return next.slice(-20);
      });
    }
  }, [topics?.length, consumers?.count]);

  const loading = healthLoading || topicsLoading || consumersLoading || clusterLoading;

  if (loading) {
    return (
      <div className="space-y-6">
        <div>
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-64 mt-2" />
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {[1, 2, 3, 4].map((i) => (
            <Card key={i}>
              <CardHeader className="pb-2">
                <Skeleton className="h-4 w-24" />
              </CardHeader>
              <CardContent>
                <Skeleton className="h-8 w-16" />
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  const statusColor = health?.status === 'healthy' ? 'success' : health?.status === 'draining' ? 'warning' : 'destructive';

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-foreground">Overview</h1>
          <p className="text-sm text-text-secondary">
            {health?.name} — {health?.version} — {health?.uptime}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={statusColor as "success" | "warning" | "destructive"}>
            {health?.status ?? 'unknown'}
          </Badge>
          {health?.mode === 'single-node' && (
            <Badge variant="secondary">Single Node</Badge>
          )}
          {cluster?.mode === 'cluster' && (
            <Badge variant="secondary">
              Cluster — {cluster?.alive_count} alive
            </Badge>
          )}
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        <StatsCard
          icon={Layers}
          label="Topics"
          value={topics?.length ?? 0}
        />
        <StatsCard
          icon={Users}
          label="Consumer Groups"
          value={consumers?.count ?? 0}
        />
        <StatsCard
          icon={HardDrive}
          label="Storage (Hot)"
          value={health?.storage ? formatBytes(health.storage.hot_size_bytes) : 'N/A'}
        />
        <StatsCard
          icon={Zap}
          label="Partitions"
          value={topics?.reduce((sum, t) => sum + t.partitions, 0) ?? 0}
        />
      </div>

      {/* Resource Chart */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="h-5 w-5 text-text-muted" />
            Resources Over Time
          </CardTitle>
          <CardDescription>Topics and consumer groups sampled every 10–15s</CardDescription>
        </CardHeader>
        <CardContent>
          {chartData.length === 0 ? (
            <div className="flex h-64 items-center justify-center text-text-muted text-sm">
              Collecting data points...
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <AreaChart data={chartData}>
                <defs>
                  <linearGradient id="colorTopics" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--accent)" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="var(--accent)" stopOpacity={0} />
                  </linearGradient>
                  <linearGradient id="colorConsumers" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="var(--success)" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="var(--success)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="time" stroke="var(--text-muted)" fontSize={12} tickLine={false} axisLine={false} />
                <YAxis stroke="var(--text-muted)" fontSize={12} tickLine={false} axisLine={false} allowDecimals={false} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'var(--surface)',
                    border: '1px solid var(--border)',
                    borderRadius: '8px',
                    color: 'var(--text-primary)',
                    fontSize: '12px',
                  }}
                />
                <Area type="monotone" dataKey="topics" stroke="var(--accent)" fillOpacity={1} fill="url(#colorTopics)" />
                <Area type="monotone" dataKey="consumers" stroke="var(--success)" fillOpacity={1} fill="url(#colorConsumers)" />
              </AreaChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* Cluster Health */}
      {cluster && (
        <Card>
          <CardHeader>
            <CardTitle>Cluster Health</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {cluster.members?.map((m) => (
                <div
                  key={m.id}
                  className="flex items-center gap-3 rounded-lg border border-border bg-background p-4"
                >
                  <Server className="h-5 w-5 text-text-muted" />
                  <div className="flex flex-col">
                    <span className="text-sm font-medium">{m.id}</span>
                    <span className="text-xs text-text-muted">{m.addr}:{m.port}</span>
                  </div>
                  <Badge
                    variant={m.state === 'Alive' ? 'success' : m.state === 'Suspect' ? 'warning' : 'destructive'}
                    className="ml-auto"
                  >
                    {m.state}
                  </Badge>
                </div>
              ))}
              {cluster.members?.length === 0 && (
                <p className="text-sm text-text-muted col-span-full text-center py-8">
                  No cluster members — running in single-node mode
                </p>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Raft Status */}
      {health?.raft && (
        <Card>
          <CardHeader>
            <CardTitle>Raft State</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">State</p>
                <p className="text-sm font-medium">{health.raft.state}</p>
              </div>
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">Term</p>
                <p className="text-sm font-medium">{health.raft.term}</p>
              </div>
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">Leader</p>
                <p className="text-sm font-medium">{health.raft.is_leader ? 'This node' : health.raft.leader_id}</p>
              </div>
              <div className="rounded-lg border border-border bg-background p-4">
                <p className="text-xs text-text-muted">Commit Index</p>
                <p className="text-sm font-medium">{health.raft.commit_index}</p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

export interface ApiError {
  code: number;
  message: string;
  details?: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/v1${path}`, {
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const err: ApiError = {
      code: res.status,
      message: body.error ?? res.statusText,
    };
    throw err;
  }

  return res.json();
}

export const api = {
  get: <T>(path: string) => request<T>(path, { method: 'GET' }),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: 'POST',
      body: body ? JSON.stringify(body) : undefined,
    }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: 'PUT',
      body: body ? JSON.stringify(body) : undefined,
    }),
  del: <T>(path: string) => request<T>(path, { method: 'DELETE' }),
};

// --- Typed API functions ---

export interface HealthResponse {
  status: string;
  node_id: number;
  name: string;
  version: string;
  uptime: string;
  mode?: string;
  raft?: { state: string; term: number; is_leader: boolean; leader_id: string; commit_index: number };
  cluster?: { alive_members: number; members: number };
  storage?: { hot_size_bytes: number; partitions: number };
  warm?: { enabled: boolean; size: number };
  dlq_topics?: number;
}

export interface TopicInfo {
  name: string;
  mode: string;
  partitions: number;
  created_at: string;
}

export interface TopicPartition {
  id: number;
  high_watermark: number;
  log_start_offset: number;
}

export interface TopicDetail extends TopicInfo {
  partitions_detail?: TopicPartition[];
}

export interface ConsumerGroupInfo {
  groups: string[];
  count: number;
}

export interface ConsumerMember {
  id: string;
  partitions: number[];
}

export interface ConsumerGroupDetail {
  group: string;
  members: ConsumerMember[];
  assignments: Record<string, number[]>;
}

export interface MemberInfo {
  id: string;
  addr: string;
  port: number;
  state: string;
  incarnation: number;
}

export interface ClusterResponse {
  mode: string;
  is_leader?: boolean;
  leader_id?: string;
  alive_count: number;
  members: MemberInfo[];
}

export interface SchemaVersion {
  id: number;
  subject: string;
  version: number;
  type: string;
  schema: string;
}

export interface WASMModule {
  name: string;
}

export interface WASMListResponse {
  modules: string[];
  count: number;
}

export interface ProcessorInfo {
  name: string;
  state: string;
  parallelism: number;
  source_topic: string;
  sink_topic: string;
  operators: number;
}

export interface ProcessorListResponse {
  topologies: string[];
  count: number;
}

export interface DLQEntry {
  id: number;
  original_msg: {
    id: string;
    topic: string;
    body: string;
    headers?: Record<string, string>;
    content_type?: string;
    created_at?: string;
  };
  topic: string;
  partition: number;
  reason: string;
  retries: number;
  failed_at: string;
}

export interface DLQPeekResponse {
  topic: string;
  count: number;
  entries: DLQEntry[];
}

export interface DLQReplayOptions {
  dry_run?: boolean;
  max_messages?: number;
  target_topic?: string;
  delete_after_replay?: boolean;
  add_dlq_metadata?: boolean;
}

export interface GeoStatusResponse {
  enabled: boolean;
  local_dc?: string;
  mode?: string;
  remote_dcs?: number;
  sender?: { events_sent: number; events_failed: number };
  receiver?: { events_received: number; events_rejected: number };
}

export function getHealth() {
  return api.get<HealthResponse>('/health');
}

export function getTopics(limit = 100, offset = 0) {
  return api.get<TopicInfo[]>(`/topics?limit=${limit}&offset=${offset}`);
}

export function getTopicDetail(name: string) {
  return api.get<TopicDetail>(`/topics/${name}`);
}

export function getTopic(name: string) {
  return api.get<{ name: string; partitions: { id: number; high_watermark: number; log_start_offset: number }[] }>(`/topics/${name}`);
}

export function createTopic(name: string, mode: string, partitions: number) {
  return api.post<{ name: string; mode: string; partitions: number }>('/topics', { name, mode, partitions });
}

export function deleteTopic(name: string) {
  return api.del<{ status: string }>(`/topics/${name}`);
}

export function getConsumers() {
  return api.get<ConsumerGroupInfo>('/consumers');
}

export function getConsumerGroupDetail(group: string) {
  return api.get<ConsumerGroupDetail>(`/consumers/${group}`);
}

export function getCluster() {
  return api.get<ClusterResponse>('/cluster/members');
}

export function getSchemas(subject: string) {
  return api.get<SchemaVersion[]>(`/schemas/${subject}`);
}

export function registerSchema(subject: string, type: string, schema: string) {
  return api.post<SchemaVersion>(`/schemas/${subject}`, { type, schema });
}

export function deleteSchema(subject: string) {
  return api.del<void>(`/schemas/${subject}`);
}

export function listSchemaSubjects() {
  // We need a way to list subjects — for now the UI will use a known set
  return api.get<string[]>('/schemas');
}

export function getDLQ(topic: string, limit = 50) {
  return api.get<DLQPeekResponse>(`/dlq/${topic}?limit=${limit}`);
}

export function listDLQTopics() {
  return api.get<{ topics: string[] }>('/dlq');
}

export function clearDLQ(topic: string) {
  return api.del<{ status: string }>(`/dlq/${topic}`);
}

export function replayDLQ(topic: string, options: DLQReplayOptions = {}) {
  return api.post<{ replayed: number; dry_run: boolean }>(`/dlq/${topic}/replay`, options);
}

export function getGeoStatus() {
  return api.get<GeoStatusResponse>('/geo-replication/status');
}

export function getGeoLag() {
  return api.get<{ enabled: boolean; lag: unknown }>('/geo-replication/lag');
}

export function listWASMModules() {
  return api.get<WASMListResponse>('/wasm/modules');
}

export function deleteWASMModule(name: string) {
  return api.del<void>(`/wasm/modules/${name}`);
}

export function listProcessors() {
  return api.get<ProcessorListResponse>('/processors');
}

export function getProcessor(name: string) {
  return api.get<ProcessorInfo>(`/processors/${name}`);
}

export function deleteProcessor(name: string) {
  return api.del<void>(`/processors/${name}`);
}

export function startProcessor(name: string) {
  return api.post<void>(`/processors/${name}/start`);
}

export function stopProcessor(name: string) {
  return api.post<void>(`/processors/${name}/stop`);
}

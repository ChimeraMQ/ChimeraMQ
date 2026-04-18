import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  api, getHealth, getTopics, getConsumers, getCluster,
  getTopicDetail, createTopic, deleteTopic, getConsumerGroupDetail,
  getSchemas, registerSchema, deleteSchema, listSchemaSubjects,
  getDLQ, listDLQTopics, clearDLQ, replayDLQ,
  listProcessors, getProcessor, deleteProcessor, startProcessor, stopProcessor,
  getTopic, listWASMModules, deleteWASMModule, getGeoStatus, getGeoLag,
} from '@/lib/api';

describe('api', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('makes a GET request and returns parsed JSON', async () => {
    const mockData = { status: 'healthy', node_id: 1, name: 'test', version: '0.1.0', uptime: '1h' };
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => mockData,
    } as Response);

    const result = await api.get('/health');
    expect(result).toEqual(mockData);
    expect(fetch).toHaveBeenCalledWith('/v1/health', {
      headers: { 'Content-Type': 'application/json' },
      method: 'GET',
    });
  });

  it('makes a POST request with JSON body', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ name: 'orders', mode: 'unified', partitions: 4 }),
    } as Response);

    const result = await api.post('/topics', { name: 'orders', mode: 'unified', partitions: 4 });
    expect(result).toEqual({ name: 'orders', mode: 'unified', partitions: 4 });
    expect(fetch).toHaveBeenCalledWith('/v1/topics', {
      headers: { 'Content-Type': 'application/json' },
      method: 'POST',
      body: '{"name":"orders","mode":"unified","partitions":4}',
    });
  });

  it('makes a POST request without body', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'ok' }),
    } as Response);

    const result = await api.post('/trigger');
    expect(result).toEqual({ status: 'ok' });
    const callArgs = vi.mocked(fetch).mock.calls[0][1];
    expect(callArgs?.body).toBeUndefined();
  });

  it('makes a DELETE request', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'deleted' }),
    } as Response);

    const result = await api.del('/topics/orders');
    expect(result).toEqual({ status: 'deleted' });
  });

  it('throws ApiError on non-ok response', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: false,
      status: 404,
      statusText: 'Not Found',
      json: async () => ({ error: 'Topic not found' }),
    } as Response);

    await expect(api.get('/topics/missing')).rejects.toEqual({
      code: 404,
      message: 'Topic not found',
    });
  });

  it('throws with statusText when body has no error field', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
      json: async () => ({}),
    } as Response);

    await expect(api.get('/health')).rejects.toEqual({
      code: 500,
      message: 'Internal Server Error',
    });
  });
});

describe('typed API functions', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('getHealth returns parsed health response', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ status: 'healthy', node_id: 1, name: 'chimera-01', version: '0.1.0', uptime: '1h 23m' }),
    } as Response);

    const result = await getHealth();
    expect(result.status).toBe('healthy');
    expect(result.node_id).toBe(1);
  });

  it('getTopics passes query params', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => [{ name: 'orders', mode: 'unified', partitions: 4, created_at: '2026-04-16' }],
    } as Response);

    await getTopics(50, 10);
    expect(fetch).toHaveBeenCalledWith('/v1/topics?limit=50&offset=10', {
      headers: { 'Content-Type': 'application/json' },
      method: 'GET',
    });
  });

  it('getConsumers returns group info', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ groups: ['group-1'], count: 1 }),
    } as Response);

    const result = await getConsumers();
    expect(result.count).toBe(1);
    expect(result.groups).toEqual(['group-1']);
  });

  it('getCluster returns member list', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        mode: 'cluster',
        is_leader: true,
        leader_id: 'node-1',
        alive_count: 3,
        members: [{ id: 'node-1', addr: '10.0.0.1', port: 5672, state: 'Alive', incarnation: 1 }],
      }),
    } as Response);

    const result = await getCluster();
    expect(result.mode).toBe('cluster');
    expect(result.members).toHaveLength(1);
    expect(result.members[0].state).toBe('Alive');
  });
});

describe('schema API functions', () => {
  beforeEach(() => { vi.stubGlobal('fetch', vi.fn()); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('listSchemaSubjects returns array', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ['subj-a', 'subj-b'] } as Response);
    const result = await listSchemaSubjects();
    expect(result).toEqual(['subj-a', 'subj-b']);
    expect(fetch).toHaveBeenCalledWith('/v1/schemas', expect.any(Object));
  });

  it('getSchemas returns versions', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => [{ version: 1, type: 'JSON', id: 'x', schema: '{}' }] } as Response);
    const result = await getSchemas('my-subject');
    expect(result).toHaveLength(1);
    expect(result[0].version).toBe(1);
  });

  it('registerSchema POSTs schema data', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ version: 1 }) } as Response);
    await registerSchema('my-subject', 'Avro', '{"type":"record"}');
    expect(fetch).toHaveBeenCalledWith('/v1/schemas/my-subject', expect.objectContaining({ method: 'POST' }));
  });

  it('deleteSchema sends DELETE', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({}) } as Response);
    await deleteSchema('my-subject');
    expect(fetch).toHaveBeenCalledWith('/v1/schemas/my-subject', expect.objectContaining({ method: 'DELETE' }));
  });
});

describe('DLQ API functions', () => {
  beforeEach(() => { vi.stubGlobal('fetch', vi.fn()); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('listDLQTopics returns topics array', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ topics: ['dlq-a', 'dlq-b'] }) } as Response);
    const result = await listDLQTopics();
    expect(result.topics).toEqual(['dlq-a', 'dlq-b']);
  });

  it('getDLQ returns entries', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ topic: 'dlq-a', count: 1, entries: [] }) } as Response);
    const result = await getDLQ('dlq-a', 25);
    expect(result.count).toBe(1);
    expect(fetch).toHaveBeenCalledWith('/v1/dlq/dlq-a?limit=25', expect.any(Object));
  });

  it('clearDLQ sends DELETE', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'cleared' }) } as Response);
    await clearDLQ('dlq-a');
    expect(fetch).toHaveBeenCalledWith('/v1/dlq/dlq-a', expect.objectContaining({ method: 'DELETE' }));
  });

  it('replayDLQ POSTs options', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ replayed: 5, dry_run: true }) } as Response);
    await replayDLQ('dlq-a', { dry_run: true, max_messages: 100 });
    expect(fetch).toHaveBeenCalledWith('/v1/dlq/dlq-a/replay', expect.objectContaining({ method: 'POST' }));
  });
});

describe('processor API functions', () => {
  beforeEach(() => { vi.stubGlobal('fetch', vi.fn()); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('listProcessors returns topologies', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ topologies: ['proc-a'], count: 1 }) } as Response);
    const result = await listProcessors();
    expect(result.count).toBe(1);
  });

  it('getProcessor returns detail', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ state: 'running', source_topic: 'src', sink_topic: 'dst', parallelism: 1, operators: 2 }) } as Response);
    const result = await getProcessor('proc-a');
    expect(result.state).toBe('running');
  });

  it('deleteProcessor sends DELETE', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => {} } as Response);
    await deleteProcessor('proc-a');
    expect(fetch).toHaveBeenCalledWith('/v1/processors/proc-a', expect.objectContaining({ method: 'DELETE' }));
  });

  it('startProcessor sends POST', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => {} } as Response);
    await startProcessor('proc-a');
    expect(fetch).toHaveBeenCalledWith('/v1/processors/proc-a/start', expect.objectContaining({ method: 'POST' }));
  });

  it('stopProcessor sends POST', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => {} } as Response);
    await stopProcessor('proc-a');
    expect(fetch).toHaveBeenCalledWith('/v1/processors/proc-a/stop', expect.objectContaining({ method: 'POST' }));
  });
});

describe('topic API functions', () => {
  beforeEach(() => { vi.stubGlobal('fetch', vi.fn()); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('getTopicDetail returns partition detail', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ name: 'orders', mode: 'unified', partitions: 2, created_at: '2026-04-16', partitions_detail: [{ id: 0, high_watermark: 100, log_start_offset: 0 }, { id: 1, high_watermark: 200, log_start_offset: 0 }] }),
    } as Response);
    const result = await getTopicDetail('orders');
    expect(result.name).toBe('orders');
    expect(result.partitions_detail).toHaveLength(2);
  });

  it('createTopic POSTs topic data', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ name: 'new-topic', mode: 'stream', partitions: 4 }) } as Response);
    const result = await createTopic('new-topic', 'stream', 4);
    expect(result.name).toBe('new-topic');
  });

  it('deleteTopic sends DELETE', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'deleted' }) } as Response);
    await deleteTopic('orders');
    expect(fetch).toHaveBeenCalledWith('/v1/topics/orders', expect.objectContaining({ method: 'DELETE' }));
  });

  it('getConsumerGroupDetail returns members', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ group: 'g1', state: 'Stable', members: [{ id: 'm1', topics: ['orders'] }] }),
    } as Response);
    const result = await getConsumerGroupDetail('g1');
    expect(result.group).toBe('g1');
  });
});

describe('WASM and geo API functions', () => {
  beforeEach(() => { vi.stubGlobal('fetch', vi.fn()); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('listWASMModules returns module list', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ modules: ['compress'], count: 1 }) } as Response);
    const result = await listWASMModules();
    expect(result.count).toBe(1);
  });

  it('deleteWASMModule sends DELETE', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => {} } as Response);
    await deleteWASMModule('compress');
    expect(fetch).toHaveBeenCalledWith('/v1/wasm/modules/compress', expect.objectContaining({ method: 'DELETE' }));
  });

  it('getGeoStatus returns geo config', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ enabled: true, local_dc: 'us-east' }) } as Response);
    const result = await getGeoStatus();
    expect(result.enabled).toBe(true);
  });

  it('getGeoLag returns lag info', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ enabled: true, lag: {} }) } as Response);
    const result = await getGeoLag();
    expect(result.enabled).toBe(true);
  });
});

describe('api.put', () => {
  beforeEach(() => { vi.stubGlobal('fetch', vi.fn()); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('makes a PUT request with JSON body', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'updated' }) } as Response);
    const result = await api.put('/config', { key: 'val' });
    expect(result).toEqual({ status: 'updated' });
    expect(fetch).toHaveBeenCalledWith('/v1/config', expect.objectContaining({ method: 'PUT' }));
  });

  it('makes a PUT request without body', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({ ok: true, json: async () => ({ status: 'reset' }) } as Response);
    const result = await api.put('/reset');
    expect(result).toEqual({ status: 'reset' });
    const callArgs = vi.mocked(fetch).mock.calls[0][1];
    expect(callArgs?.body).toBeUndefined();
  });
});

describe('getTopic', () => {
  beforeEach(() => { vi.stubGlobal('fetch', vi.fn()); });
  afterEach(() => { vi.unstubAllGlobals(); });

  it('returns topic detail with partitions', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ name: 'orders', partitions: [{ id: 0, high_watermark: 100, log_start_offset: 0 }] }),
    } as Response);
    const result = await getTopic('orders');
    expect(result.name).toBe('orders');
    expect(result.partitions).toHaveLength(1);
  });
});

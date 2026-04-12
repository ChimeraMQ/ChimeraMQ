# Technical Deep-Dive: ChimeraMQ Core Components

> Detailed technical analysis of critical subsystems
> Version: v0.9.0
> Date: 2026-04-11

---

## 1. Storage Engine Architecture

### 1.1 Three-Tier Storage Model

ChimeraMQ implements a sophisticated tiered storage system optimizing for access patterns:

```
┌─────────────────────────────────────────────────────────────┐
│                         HOT TIER                             │
│              Memory-Mapped Log Segments                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ Segment N   │  │ Segment N-1 │  │ Segment N-2 │         │
│  │ (active)    │  │ (frozen)    │  │ (frozen)    │         │
│  │ mmap READ/  │  │ mmap READ   │  │ mmap READ   │         │
│  │ WRITE       │  │             │  │             │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│  Retention: 1 hour (configurable)                           │
└──────────────────────────┬──────────────────────────────────┘
                           │ age/size threshold
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                        WARM TIER                             │
│                    LSM-Tree Storage                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ L0 SSTables │  │ L1 SSTables │  │ L2+ SSTables│         │
│  │ (recent)    │  │ (merged)    │  │ (compacted) │         │
│  │ Bloom filter│  │ Bloom filter│  │ Bloom filter│         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│  Retention: 24 hours (configurable)                         │
└──────────────────────────┬──────────────────────────────────┘
                           │ age threshold
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                        COLD TIER                             │
│                   Compressed Archives                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Zstd Compressed Segments (Dictionary Trained)      │   │
│  │  Archive Size: 1GB per file                         │   │
│  └─────────────────────────────────────────────────────┘   │
│  Retention: 7 days (configurable)                           │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 Hot Tier: Segment Architecture

**File Format:**
```
Segment File Layout:
┌─────────────────────────────────────────────────────────────┐
│ Header (32 bytes)                                           │
│  - Magic (4 bytes): 0x43534731 "CSG1"                       │
│  - Version (4 bytes): 1                                     │
│  - BaseOffset (8 bytes): First message offset               │
│  - Created (8 bytes): Timestamp                             │
│  - Reserved (8 bytes)                                       │
├─────────────────────────────────────────────────────────────┤
│ Record 0: [Length:4][Data:N]                               │
│ Record 1: [Length:4][Data:N]                               │
│ Record 2: [Length:4][Data:N]                               │
│ ...                                                         │
└─────────────────────────────────────────────────────────────┘
```

**Sparse Index:**
- Index entry every 256 messages (configurable)
- Entry: `{Offset, Position, Timestamp}` (20 bytes)
- Binary search for O(log n) offset lookup
- In-memory only (rebuilt on startup if missing)

**Performance Characteristics:**
| Operation | Latency | Throughput |
|-----------|---------|------------|
| Append | ~1μs | 500K+ msg/s |
| Read (cached) | ~500ns | 2M+ msg/s |
| Read (uncached) | ~5μs | 200K+ msg/s |
| Index lookup | ~100ns | 10M+ ops/s |

### 1.3 Warm Tier: LSM-Tree Implementation

**Architecture Decisions:**

1. **MemTable**: Red-black tree in memory
   - Capacity: 4MB default
   - Flush trigger: Size threshold or time-based
   - WAL ensures durability

2. **SSTable Format:**
```
SSTable File Layout:
┌─────────────────────────────────────────────────────────────┐
│ Data Blocks (64KB each)                                     │
│  - Compressed (Snappy)                                      │
│  - Key-Value pairs sorted                                   │
├─────────────────────────────────────────────────────────────┤
│ Index Block                                                │
│  - Block offsets for binary search                         │
├─────────────────────────────────────────────────────────────┤
│ Bloom Filter                                               │
│  - 1% false positive rate default                          │
├─────────────────────────────────────────────────────────────┤
│ Footer (metadata)                                          │
│  - Offset ranges, timestamps, count                        │
└─────────────────────────────────────────────────────────────┘
```

3. **Compaction Strategies:**
   - **Size-Tiered** (default): Merge similar-size SSTables
   - **Leveled**: Key-based dedup, latest wins
   - **Tombstone**: Aggressive dead record removal

**Block Cache:**
- FIFO cache with 256 entries
- Block-level reads (not full SSTable)
- Eliminates OOM risk from large SSTables

### 1.4 Tier Migration

**Hot → Warm Migration:**
```go
// Trigger: Segment frozen (age > hot_retention)
Process:
1. Freeze segment (read-only)
2. Create SSTable from segment
3. Build bloom filter
4. Update manifest
5. Delete segment file
```

**Warm → Cold Migration:**
```go
// Trigger: SSTable age > warm_retention
Process:
1. Collect eligible SSTables
2. Train Zstd dictionary (every 100 archives)
3. Compress with dictionary
4. Write archive file (1GB)
5. Update manifest
6. Delete SSTable files
```

---

## 2. Raft Consensus Implementation

### 2.1 Architecture Overview

Custom Raft implementation for metadata consensus:

```
┌─────────────────────────────────────────────────────────────┐
│                     Raft Node State                          │
├─────────────────────────────────────────────────────────────┤
│ Persistent State (all nodes)                                 │
│  - currentTerm: Latest term seen                            │
│  - votedFor: Candidate voted for in current term            │
│  - log[]: Log entries (index, term, command)                │
├─────────────────────────────────────────────────────────────┤
│ Volatile State (all nodes)                                   │
│  - commitIndex: Highest log entry known committed           │
│  - lastApplied: Highest log entry applied to FSM            │
├─────────────────────────────────────────────────────────────┤
│ Leader State (volatile)                                      │
│  - nextIndex[]: For each follower, next log index to send   │
│  - matchIndex[]: For each follower, highest replicated      │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Leader Election

**Quorum Calculation:**
```go
// Fixed in v0.9.0: Correct majority calculation
quorum := (len(peers)+1)/2 + 1  // For odd-sized clusters
```

**Election Process:**
1. Node starts as Follower
2. Election timeout expires (randomized 150-300ms)
3. Increment term, convert to Candidate
4. Send RequestVote RPCs to all peers
5. If quorum votes received → become Leader
6. If AppendEntries from new leader → convert to Follower

**Timer Management:**
- Election timer: Reset on AppendEntries from current leader
- Heartbeat ticker: 150ms default (leader only)
- Randomization: 150-300ms to prevent split votes

### 2.3 Log Replication

**Log Entry:**
```go
type Entry struct {
    Index   Index       // Position in log (1-indexed)
    Term    Term        // Term when entry was created
    Command []byte      // Serialized command for FSM
}
```

**Replication Flow:**
```
1. Client sends command to Leader
2. Leader appends to local log
3. Leader sends AppendEntries RPCs to followers
4. Followers append to local logs
5. Leader waits for quorum acknowledgments
6. Leader commits entry (advances commitIndex)
7. Leader applies to FSM
8. Leader notifies followers of new commitIndex
```

**Binary Log Format (v0.9.0 optimization):**
- Previous: JSON with base64 encoding (bloated)
- Current: gob-encoded binary (compact, fast)

### 2.4 Snapshotting

**Trigger:**
- Log size exceeds `MaxLogEntries` (default 100,000)
- Time-based (`SnapshotInterval`: 5 minutes)

**Process:**
1. Leader takes snapshot of FSM state
2. Save to disk with metadata (last index/term)
3. Truncate log before snapshot index
4. Followers receive snapshot via RPC

**FSM State:**
- Topics metadata
- Schema registry
- ACLs
- Consumer group assignments
- Partition assignments

### 2.5 Multi-Node Test Results

| Test | Result | Notes |
|------|--------|-------|
| Leader Election | ✅ Pass | 3-node cluster elects leader in <500ms |
| Log Replication | ✅ Pass | 10K entries replicated with no loss |
| Failover | ✅ Pass | Leader kill → new leader elected <1s |
| Network Partition | ✅ Pass | Majority partition continues |
| Partition Assignment | ✅ Pass | Metadata consistent across nodes |

---

## 3. Protocol Multiplexing

### 3.1 Protocol Detection Order

Detection proceeds in order of specificity:

```
┌─────────────────────────────────────────────────────────────┐
│              Protocol Detection Order                        │
├─────────────────────────────────────────────────────────────┤
│ 1. TLS ClientHello (0x16 0x03)                              │
│    → Unwrap TLS, re-detect inner protocol                   │
├─────────────────────────────────────────────────────────────┤
│ 2. AMQP 1.0 (AMQP\x00\x01\x00\x00)                          │
│    → AMQP handler                                           │
├─────────────────────────────────────────────────────────────┤
│ 3. MQTT (0x10 = CONNECT packet)                             │
│    → MQTT handler                                           │
├─────────────────────────────────────────────────────────────┤
│ 4. HTTP/WebSocket (GET / POST / PUT)                        │
│    → HTTP handler (WebSocket upgrade detected separately)   │
├─────────────────────────────────────────────────────────────┤
│ 5. Chimera Protocol (CHMR magic)                            │
│    → Native binary protocol handler                         │
├─────────────────────────────────────────────────────────────┤
│ 6. Fallback                                                 │
│    → Reject with protocol hint                              │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Connection Handling

**Accept Loop:**
```go
func (m *ProtocolMux) acceptLoop() {
    for {
        conn, err := m.listener.Accept()
        if err != nil {
            return // Shutdown
        }
        
        // Check max connections
        if m.connections.Load() >= maxConn {
            conn.Close()
            continue
        }
        
        m.wg.Add(1)
        go m.handleConnection(conn)
    }
}
```

**Protocol Detection:**
```go
func (m *ProtocolMux) detectProtocol(conn net.Conn) (handler, peeked []byte, error) {
    // Read up to max bytes needed by any detector
    peek := make([]byte, maxPeekBytes)
    n, err := conn.Read(peek)
    if err != nil {
        return nil, nil, err
    }
    peek = peek[:n]
    
    // Try detectors in order
    for _, entry := range m.detectors {
        if entry.detector.Detect(peek) {
            return entry.handler, peek, nil
        }
    }
    
    return nil, peek, ErrUnknownProtocol
}
```

### 3.3 TLS Support

**Configuration:**
```go
type TLSConfig struct {
    Enabled    bool
    CertFile   string
    KeyFile    string
    ClientCA   string  // For mutual TLS
    Mutual     bool    // Require client certificates
}
```

**Implementation:**
- TLS 1.2+ minimum
- Client certificate verification (when mutual TLS enabled)
- Certificate reloading (future enhancement)

### 3.4 Protocol Statistics

| Protocol | Bytes Needed | Detection Time | Handler |
|----------|-------------|----------------|---------|
| TLS | 2 bytes | ~1μs | TLS unwrap + re-detect |
| AMQP | 8 bytes | ~500ns | `internal/protocol/amqp/` |
| MQTT | 1 byte | ~100ns | `internal/protocol/mqtt/` |
| HTTP | 4 bytes | ~200ns | `internal/protocol/http/` |
| Chimera | 4 bytes | ~100ns | `internal/protocol/chimera/` |

---

## 4. Queue Engine (Lion Head)

### 4.1 Message Dispatch Flow

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   Message   │────▶│  Dispatcher  │────▶│  Consumer A  │
│   Arrives   │     │  (Round-     │     │  (prefetch)  │
└─────────────┘     │   Robin)     │────▶├──────────────┤
                    └──────────────┘     │  Consumer B  │
                           │             │  (prefetch)  │
                           ▼             ├──────────────┤
                    ┌──────────────┐     │  Consumer C  │
                    │   In-Flight  │     │  (prefetch)  │
                    │    Tracking  │     └──────────────┘
                    └──────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Ack/Nack   │
                    │   Handling   │
                    └──────────────┘
```

### 4.2 Acknowledgment Tracking

**Visibility Timeout:**
- Default: 30 seconds
- Tracked per message offset
- Requeue on timeout

**Redelivery Count:**
- Tracked in envelope (`DeliverCount`)
- DLQ routing after `MaxRetries`

**Bitmap Optimization:**
- Roaring bitmap for ack tracking
- Memory efficient for sparse acks
- Fast union/intersection operations

### 4.3 Delay Scheduler

**Min-Heap Implementation:**
```go
type DelayScheduler struct {
    heap     *MinHeap[DelayedMsg]  // Sorted by DeliverAt
    ticker   *time.Ticker          // 100ms check interval
    readyCh  chan *Envelope        // Promoted messages
}
```

**Complexity:**
- Schedule: O(log n)
- Promote: O(1) (amortized)
- Memory: O(n) for n delayed messages

---

## 5. Stream Engine (Goat Head)

### 5.1 Consumer Group Rebalancing

**Strategies:**

1. **Range (default):**
   ```
   Partitions: [0,1,2,3,4,5,6,7]
   Consumers:  [C1, C2, C3]
   Assignment: C1: [0,1,2], C2: [3,4,5], C3: [6,7]
   ```
   - Best for: Co-partitioned joins
   - Pros: Contiguous ranges
   - Cons: Uneven if partitions % consumers != 0

2. **RoundRobin:**
   ```
   Partitions: [0,1,2,3,4,5,6,7]
   Consumers:  [C1, C2, C3]
   Assignment: C1: [0,3,6], C2: [1,4,7], C3: [2,5]
   ```
   - Best for: Even distribution
   - Pros: Balanced
   - Cons: More complex offset tracking

3. **Sticky (v0.9.0):**
   - Preserves existing assignments
   - Minimizes partition movement
   - Best for: Stable consumer groups

### 5.2 Offset Management

**Storage Options:**
1. **Local JSON** (default): `consumers/{group}/offsets.json`
2. **Raft-backed** (clustered): Replicated via consensus

**Commit Semantics:**
- At-least-once delivery (acks after commit)
- Manual commit via API
- Auto-commit option (future)

### 5.3 Waiter Registry

**Long-Poll Implementation:**
```go
func (e *Engine) Fetch(topic string, partition uint32, offset uint64, maxWait time.Duration) ([]*Envelope, error) {
    // Try immediate read
    msgs, err := e.storage.Read(topic, partition, offset)
    if len(msgs) > 0 {
        return msgs, nil
    }
    
    // Register waiter
    waiter := e.waiters.Register(topic, partition)
    defer e.waiters.Unregister(topic, partition, waiter)
    
    // Wait for new message or timeout
    select {
    case <-waiter:
        return e.storage.Read(topic, partition, offset)
    case <-time.After(maxWait):
        return nil, nil // Empty response
    }
}
```

---

## 6. Security Implementation

### 6.1 OAuth Fix (v0.9.0)

**Vulnerability:** `alg:none` JWT bypass

**Fix Implementation:**
```go
func validateAlg(alg string) error {
    // Reject empty and "none" algorithms
    if alg == "" || strings.ToLower(alg) == "none" {
        return fmt.Errorf("algorithm not allowed: %s", alg)
    }
    
    // Validate alg matches key type
    switch alg {
    case "RS256", "RS384", "RS512":
        // RSA key required
    case "ES256", "ES384", "ES512":
        // EC key required
    case "EdDSA":
        // Ed25519 key required
    default:
        return fmt.Errorf("unsupported algorithm: %s", alg)
    }
    return nil
}
```

### 6.2 Authentication Providers

| Provider | Mechanism | Use Case |
|----------|-----------|----------|
| Static | API keys in config | Development, single-node |
| File | User database file | Small teams |
| OAuth/OIDC | JWT validation | Enterprise SSO |
| LDAP | Active Directory | Corporate environments |
| mTLS | Client certificates | Service-to-service |
| SCRAM-SHA-256 | SASL challenge | MQTT, AMQP |

### 6.3 ACL Engine

**Permission Model:**
```go
type ACLEntry struct {
    Principal    string          // User or group
    ResourceType ResourceType    // Topic, Group, Cluster, Schema
    ResourceName string          // Pattern (supports wildcards)
    Operation    OperationType   // Read, Write, Create, Delete, Alter, Describe
    Permission   PermissionType  // Allow, Deny
}
```

**Wildcard Support:**
- `*` matches any single segment
- `>` matches any remaining segments
- Example: `orders.*.validated` matches `orders.us.validated`

---

## 7. Performance Optimizations (v0.9.0)

### 7.1 Publish Path Optimizations

| Optimization | Before | After | Improvement |
|--------------|--------|-------|-------------|
| CRC32 table | Allocated per call | Pre-computed package var | -1KB alloc |
| Segment write | 2 WriteAt calls | Pooled buffer, single write | -50% syscalls |
| highWatermark | Mutex protected | atomic.Uint64 | Zero contention |
| Sequential scan | Per-offset lock | Single lock cycle | -N lock cycles |

**Result:** 23-30% latency improvement
- Before: 9.6μs (unified)
- After: 7.0μs (unified)

### 7.2 Storage Optimizations

| Component | Optimization | Impact |
|-----------|--------------|--------|
| SSTable | Block-level reads | No OOM risk |
| SSTable | FIFO block cache (256 entries) | Better locality |
| Raft log | Binary (gob) format | No base64 bloat |
| Gossip | HMAC-SHA256 | Authenticated messages |

### 7.3 Memory Management

**Buffer Pools:**
- Message codec: `sync.Pool` for marshal buffers
- Record writes: `sync.Pool` for combined length+data
- SSTable blocks: FIFO cache, not LRU (predictable memory)

---

## 8. Deployment Architecture

### 8.1 Single-Node Deployment

```yaml
# Minimal configuration
node:
  id: 1
  name: chimera-01
  data_dir: /var/lib/chimera

listener:
  bind: 127.0.0.1
  port: 5672
  admin_port: 9090

storage:
  hot:
    segment_size: 268435456  # 256MB
    sync_mode: interval
```

### 8.2 Cluster Deployment

```yaml
# 3-node cluster configuration
node:
  id: 1  # Unique per node
  name: chimera-01
  data_dir: /var/lib/chimera

cluster:
  raft:
    peers:
      - "chimera-01:5673"
      - "chimera-02:5673"
      - "chimera-03:5673"
  gossip:
    seeds:
      - "chimera-01:5674"
      - "chimera-02:5674"
  replication:
    default_factor: 3
    min_isr: 2
    ack_policy: quorum
```

### 8.3 Kubernetes Deployment

**Helm Chart Features:**
- StatefulSet with persistent volumes
- Headless service for Raft/gossip
- ConfigMap for configuration
- Secret for TLS certs
- ServiceMonitor for Prometheus

**Resource Requirements:**
| Component | CPU | Memory | Storage |
|-----------|-----|--------|---------|
| Minimal | 0.5 | 512Mi | 10GB |
| Production | 2+ | 4Gi+ | 100GB+ |
| Cluster node | 4+ | 8Gi+ | 500GB+ |

---

## 9. Troubleshooting Guide

### 9.1 Common Issues

**High Memory Usage:**
```bash
# Check memory profile
curl http://localhost:9090/debug/pprof/heap > heap.prof

# Common causes:
# 1. Large messages (check max_message_size)
# 2. Many partitions (check partition count)
# 3. Slow consumers (check consumer lag)
```

**Disk Full:**
```bash
# Check tier usage
curl http://localhost:9090/v1/metrics | grep chimera_storage

# Mitigation:
# 1. Reduce retention settings
# 2. Enable tier migration
# 3. Add storage
```

**Consumer Lag:**
```bash
# Check lag
curl http://localhost:9090/v1/consumers/my-group

# Mitigation:
# 1. Add more consumers
# 2. Increase prefetch
# 3. Check for slow processing
```

### 9.2 Debug Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/v1/health` | Health status |
| `/v1/metrics` | Prometheus metrics |
| `/debug/pprof/heap` | Heap profile |
| `/debug/pprof/profile` | CPU profile |
| `/debug/pprof/goroutine` | Goroutine dump |

---

## 10. References

### 10.1 Key Files

| Component | Key Files |
|-----------|-----------|
| Storage | `internal/storage/hot/segment.go`, `internal/storage/warm/lsm.go` |
| Raft | `internal/cluster/raft/node.go`, `internal/cluster/raft/log_binary.go` |
| Protocol | `internal/protocol/mux.go` |
| Queue | `internal/engine/queue/dispatcher.go`, `internal/engine/queue/ack.go` |
| Stream | `internal/engine/stream/consumer_group.go` |
| Auth | `internal/auth/oauth.go`, `internal/auth/scram.go` |

### 10.2 Architecture Decision Records

See `docs/adr/`:
- ADR 0001: Offset Storage
- ADR 0002: WASM Processor Wiring
- ADR 0003: Binary Log Format
- ADR 0004: Sequential Scan
- ADR 0005: Hot Path Optimizations
- ADR 0006: Raft Timer Fix

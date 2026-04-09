# ChimeraMQ — SPECIFICATION

> **The Unified Messaging Beast**
> Queue + Stream + Multi-Protocol — Single Binary, Zero Dependencies, Pure Go.

**Version:** 1.0.0-draft
**Author:** Ersin Koç / ECOSTACK TECHNOLOGY OÜ
**Domain:** chimeramq.com
**GitHub:** github.com/chimeramq/chimera
**License:** Apache 2.0
**Tagline:** "Three Heads. One Binary. All Messages."

---

## 1. VISION & PHILOSOPHY

### 1.1 Problem Statement

The messaging infrastructure landscape is fractured and bloated:

| System     | Language    | Dependencies                        | Core Problem                                    |
|------------|-------------|-------------------------------------|-------------------------------------------------|
| Kafka      | Java/Scala  | JVM + ZooKeeper/KRaft               | Massive footprint, ops nightmare, stream-only    |
| RabbitMQ   | Erlang/OTP  | Erlang runtime + Mnesia             | Split-brain, cluster fragility, queue-only       |
| Pulsar     | Java        | JVM + BookKeeper + ZooKeeper        | 3-tier complexity, operational overhead           |
| ActiveMQ   | Java        | JVM                                 | Legacy, security history, declining ecosystem    |
| NATS       | Go          | Minimal                             | Feature-light, JetStream feels bolted-on         |
| Redis Streams | C        | Redis server                        | Not a real MQ, no consumer group semantics       |

**The fundamental problem:** You must choose between queue semantics (RabbitMQ) OR stream semantics (Kafka). No system unifies both with multi-protocol support in a single, dependency-free binary.

### 1.2 ChimeraMQ Solution

ChimeraMQ is a **unified message queue and event streaming platform** built from scratch in pure Go:

- **Lion Head (Queue Engine):** Competing consumers, ack/nack, dead-letter, delayed messages, priority queues — everything RabbitMQ does.
- **Goat Head (Stream Engine):** Append-only log, partitions, consumer groups, replay from offset, compaction — everything Kafka does.
- **Serpent Head (Protocol Engine):** Native binary protocol + AMQP 1.0 + MQTT 3.1.1/5.0 + WebSocket + HTTP/REST — all protocols, one port multiplexer.

### 1.3 Design Principles

1. **#NOFORKANYMORE** — Every component built from scratch. No Sarama, no Paho, no borrowed code.
2. **Zero External Dependencies** — Only `golang.org/x/crypto`, `golang.org/x/sys`, YAML parser. Nothing else.
3. **Single Binary** — `chimera` is everything: broker, CLI, admin API, schema registry, WASM runtime.
4. **Unified Semantics** — A single topic can be consumed as a stream (log replay) OR as a queue (competing consumers) simultaneously.
5. **Protocol Agnostic Core** — Internal message format is protocol-independent; protocol adapters translate at the edge.
6. **Tiered Storage** — Hot (mmap), Warm (LSM-Tree), Cold (compressed archive) — automatic data lifecycle.
7. **Embedded Everything** — Raft consensus, Gossip protocol, Schema Registry, WASM runtime — all built-in, no sidecars.

---

## 2. CORE ARCHITECTURE

### 2.1 High-Level Architecture

```
                         ┌─────────────────────────────────────────────┐
                         │              ChimeraMQ Binary               │
                         │                                             │
   Clients ──────────────┤  ┌─────────────────────────────────────┐   │
                         │  │       Protocol Multiplexer           │   │
   AMQP 1.0 ──────┐     │  │  (TLS/TCP port 5672 + auto-detect)  │   │
   MQTT 3.1/5.0 ───┤     │  └─────────┬───────────────────────────┘   │
   WebSocket ──────┤     │            │                               │
   Chimera Proto ──┤     │  ┌─────────▼───────────────────────────┐   │
   HTTP/REST ──────┘     │  │        Message Router                │   │
                         │  │  (exchanges, topic routing, filters) │   │
                         │  └─────────┬───────────────────────────┘   │
                         │            │                               │
                         │  ┌─────────▼───────┐ ┌─────────────────┐  │
                         │  │  Queue Engine    │ │  Stream Engine  │  │
                         │  │  (Lion Head)     │ │  (Goat Head)    │  │
                         │  │  - Competing     │ │  - Partitions   │  │
                         │  │  - Ack/Nack      │ │  - Offsets      │  │
                         │  │  - Priority      │ │  - Compaction   │  │
                         │  │  - DLQ           │ │  - Replay       │  │
                         │  │  - Delayed       │ │  - Windowing    │  │
                         │  └─────────┬───────┘ └────────┬────────┘  │
                         │            │                   │           │
                         │  ┌─────────▼───────────────────▼────────┐  │
                         │  │       Hybrid Storage Engine           │  │
                         │  │  Hot:  Memory-mapped log segments    │  │
                         │  │  Warm: LSM-Tree (indexed, compacted) │  │
                         │  │  Cold: Compressed archives (zstd)    │  │
                         │  └─────────┬───────────────────────────┘  │
                         │            │                               │
                         │  ┌─────────▼───────────────────────────┐  │
                         │  │     Cluster Fabric                   │  │
                         │  │  Metadata: Raft consensus            │  │
                         │  │  Data:     Gossip (SWIM protocol)    │  │
                         │  │  Discovery + Health + Rebalancing    │  │
                         │  └─────────────────────────────────────┘  │
                         │                                             │
                         │  ┌──────────┐ ┌──────────┐ ┌────────────┐  │
                         │  │ Schema   │ │  WASM    │ │  Stream    │  │
                         │  │ Registry │ │ Runtime  │ │ Processing │  │
                         │  └──────────┘ └──────────┘ └────────────┘  │
                         └─────────────────────────────────────────────┘
```

### 2.2 Internal Message Envelope

All messages are stored in a **protocol-agnostic internal format** regardless of ingestion protocol:

```go
type MessageEnvelope struct {
    // Identity
    MessageID   [16]byte          // UUIDv7 (time-sortable)
    Timestamp   int64             // Unix nanoseconds
    Sequence    uint64            // Per-partition monotonic sequence

    // Routing
    Topic       string            // Target topic/queue name
    PartitionID uint32            // Resolved partition (0 for queue mode)
    RoutingKey  string            // AMQP-style routing key
    Headers     map[string][]byte // Arbitrary key-value headers

    // Payload
    SchemaID    uint32            // Schema Registry reference (0 = no schema)
    ContentType string            // MIME type hint
    Encoding    EncodingType      // Raw, Snappy, Zstd, LZ4
    Payload     []byte            // Actual message body

    // Delivery semantics
    Priority    uint8             // 0-9 (queue mode)
    TTL         int64             // Nanoseconds, 0 = no expiry
    DeliverAt   int64             // Delayed delivery timestamp (0 = immediate)
    DeliverCount uint32           // Redelivery counter
    MaxRetries  uint32            // Max redelivery before DLQ

    // Tracing
    TraceID     [16]byte          // Distributed trace ID
    SpanID      [8]byte           // Span ID
    SourceProto ProtocolType      // Ingestion protocol (Chimera/AMQP/MQTT/WS/HTTP)
}

type EncodingType uint8
const (
    EncodingRaw    EncodingType = 0
    EncodingSnappy EncodingType = 1
    EncodingZstd   EncodingType = 2
    EncodingLZ4    EncodingType = 3
)

type ProtocolType uint8
const (
    ProtoChimera ProtocolType = 0
    ProtoAMQP    ProtocolType = 1
    ProtoMQTT    ProtocolType = 2
    ProtoWS      ProtocolType = 3
    ProtoHTTP    ProtocolType = 4
)
```

Binary wire format: **Fixed 64-byte header + variable-length fields.** Header layout:

```
Offset  Size   Field
0       16     MessageID (UUIDv7)
16      8      Timestamp (int64 nanos)
24      8      Sequence (uint64)
32      4      PartitionID (uint32)
36      4      SchemaID (uint32)
40      1      Priority (uint8)
41      1      Encoding (uint8)
42      1      SourceProto (uint8)
43      1      Flags (uint8: bit0=has-headers, bit1=has-routing-key, bit2=has-trace, bit3=has-ttl, bit4=has-delay)
44      4      PayloadLength (uint32)
48      4      HeadersLength (uint32)
52      4      TopicLength (uint16) + RoutingKeyLength (uint16)
56      8      TTL / DeliverAt (conditional based on flags)
--- variable length fields follow ---
```

### 2.3 Topic / Queue Unified Model

ChimeraMQ introduces the concept of a **Chimera Topic** — a single data structure that supports both semantics:

```go
type ChimeraTopic struct {
    Name          string
    Mode          TopicMode       // Stream, Queue, Unified
    Partitions    uint32          // Number of partitions (1 for pure queue)
    Replication   uint32          // Replication factor
    RetentionTime time.Duration   // Time-based retention
    RetentionSize int64           // Size-based retention (bytes)
    CompactionMode CompactionMode // None, KeyBased, Tombstone
    TierPolicy    TierPolicy      // Hot→Warm→Cold thresholds
    SchemaPolicy  SchemaPolicy    // None, Validate, Enforce
    DLQTopic      string          // Dead-letter topic name ("" = disabled)
    MaxRetries    uint32          // Before DLQ routing
    DelaySupport  bool            // Enable delayed message scheduling
}

type TopicMode uint8
const (
    ModeStream  TopicMode = 0 // Kafka-like: partitioned log, consumer groups, replay
    ModeQueue   TopicMode = 1 // RabbitMQ-like: competing consumers, ack/nack, round-robin
    ModeUnified TopicMode = 2 // Both: same data consumed as stream OR queue simultaneously
)
```

**Unified Mode Behavior:**
- Writes go to a partitioned append-only log (stream storage)
- Stream consumers read via offsets (Kafka-style consumer groups)
- Queue consumers get messages dispatched round-robin with individual ack/nack
- Both consumer types operate on the **same underlying data** — no duplication
- Queue consumers maintain a "virtual cursor" backed by an ack bitmap

---

## 3. PROTOCOL ENGINE (Serpent Head)

### 3.1 Protocol Multiplexer

Single TCP port with automatic protocol detection:

```go
type ProtocolMux struct {
    listener    net.Listener
    tlsConfig   *tls.Config
    detectors   []ProtocolDetector
    handlers    map[ProtocolType]ProtocolHandler
}

type ProtocolDetector interface {
    // Detect reads the first N bytes and determines protocol
    Detect(peek []byte) (ProtocolType, bool)
    BytesNeeded() int
}

type ProtocolHandler interface {
    HandleConnection(conn net.Conn, detected []byte) error
    Protocol() ProtocolType
}
```

**Detection order** (by first bytes):
1. **TLS ClientHello** (0x16 0x03) → unwrap TLS, re-detect inner protocol
2. **AMQP 1.0** (`AMQP\x00\x01\x00\x00`) → AMQP handler
3. **MQTT** (0x10 = CONNECT packet) → MQTT handler
4. **HTTP/WebSocket** (`GET ` / `POST ` / `PUT `) → HTTP/WS handler
5. **Chimera Protocol** (`CHMR` magic bytes) → native handler
6. **Fallback** → reject with protocol hint

### 3.2 Chimera Native Protocol

Custom binary protocol optimized for maximum throughput:

```
Frame Layout:
┌──────────┬──────────┬──────────┬──────────┬─────────────┐
│ Magic(4) │ Version  │ OpCode   │ Flags    │ Length(4)    │
│ "CHMR"   │ (1 byte) │ (1 byte) │ (1 byte) │ (uint32)    │
├──────────┴──────────┴──────────┴──────────┴─────────────┤
│                    Payload (variable)                     │
├─────────────────────────────────────────────────────────┤
│                    CRC32C (4 bytes)                       │
└─────────────────────────────────────────────────────────┘
```

**OpCodes:**
```
0x01 CONNECT          0x02 CONNACK
0x03 PUBLISH          0x04 PUBACK
0x05 SUBSCRIBE        0x06 SUBACK
0x07 UNSUBSCRIBE      0x08 UNSUBACK
0x09 FETCH            0x0A FETCHRESP
0x0B ACK              0x0C NACK
0x0D SEEK             0x0E SEEKACK
0x0F PING             0x10 PONG
0x11 CREATE_TOPIC     0x12 DELETE_TOPIC
0x13 BATCH_PUBLISH    0x14 BATCH_PUBACK
0x15 SCHEMA_REG       0x16 SCHEMA_RESP
0x17 COMMIT_OFFSET    0x18 COMMIT_ACK
0x19 DISCONNECT       0x1A ERROR
```

**Features:**
- Pipelining: Multiple requests without waiting for responses
- Batching: BATCH_PUBLISH sends N messages in single frame
- Flow control: Credit-based (consumer advertises capacity)
- Zero-copy: Large payloads use sendfile(2) / splice(2) where possible

### 3.3 AMQP 1.0 Adapter

Full AMQP 1.0 implementation (not 0-9-1):

- **Connection** → maps to ChimeraMQ client session
- **Session** → maps to channel with flow control
- **Link (Sender)** → maps to producer on a topic
- **Link (Receiver)** → maps to consumer (queue or stream mode)
- **Exchanges** → ChimeraMQ routing rules (direct, topic, fanout, headers)
- **Disposition** → maps to ack/nack/reject

AMQP-specific routing:

```go
type Exchange struct {
    Name    string
    Type    ExchangeType // Direct, Topic, Fanout, Headers
    Durable bool
    Bindings []Binding
}

type Binding struct {
    Source      string // Exchange name
    Destination string // ChimeraTopic name
    RoutingKey  string // Binding key pattern
    Arguments   map[string]interface{}
}
```

### 3.4 MQTT Adapter

MQTT 3.1.1 and 5.0 support:

- **QoS 0** → fire-and-forget publish
- **QoS 1** → at-least-once with PUBACK
- **QoS 2** → exactly-once with full 4-step handshake
- **Retained messages** → stored in compacted topic `$chimera/retained/{topic}`
- **Will messages** → triggered on ungraceful disconnect
- **MQTT 5.0 features:** Shared subscriptions, message expiry, topic alias, flow control
- **Topic mapping:** MQTT `a/b/c` → ChimeraMQ `a.b.c` (configurable separator)

### 3.5 WebSocket Adapter

WebSocket upgrade on HTTP endpoint:

- **Text frames:** JSON-encoded messages (developer-friendly)
- **Binary frames:** Chimera Protocol frames (performance mode)
- **Sub-protocols:** `chimera-json-v1`, `chimera-binary-v1`
- **Heartbeat:** WebSocket ping/pong + Chimera-level keepalive
- **Auth:** JWT token in query param or first message

### 3.6 HTTP/REST Admin API

```
POST   /v1/messages/{topic}              Publish message(s)
GET    /v1/messages/{topic}?offset=N     Fetch messages (stream mode)
POST   /v1/messages/{topic}/ack          Acknowledge messages (queue mode)

POST   /v1/topics                        Create topic
GET    /v1/topics                        List topics
GET    /v1/topics/{name}                 Topic details + stats
PUT    /v1/topics/{name}                 Update topic config
DELETE /v1/topics/{name}                 Delete topic

GET    /v1/consumers                     List consumer groups
GET    /v1/consumers/{group}             Consumer group details + lag

POST   /v1/schemas                       Register schema
GET    /v1/schemas/{id}                  Get schema by ID
GET    /v1/schemas/subjects/{subject}    Get schemas by subject

GET    /v1/cluster/status                Cluster health
GET    /v1/cluster/nodes                 Node list
GET    /v1/metrics                       Prometheus metrics
GET    /v1/health                        Health check
```

---

## 4. STORAGE ENGINE (Hybrid Tiered)

### 4.1 Tiered Architecture

```
                    Write Path
                       │
                       ▼
              ┌─────────────────┐
              │   Write Buffer  │  (in-memory batch accumulator)
              │   (per partition)│
              └────────┬────────┘
                       │ flush
                       ▼
        ┌──────────────────────────────┐
        │     HOT TIER                 │
        │  Memory-mapped log segments  │
        │  - Active writes             │
        │  - Recent reads (< 1 hour)   │
        │  - mmap + sendfile zero-copy │
        │  - Segment size: 256MB       │
        └──────────────┬───────────────┘
                       │ age/size threshold
                       ▼
        ┌──────────────────────────────┐
        │     WARM TIER                │
        │  LSM-Tree indexed storage    │
        │  - Sorted by timestamp       │
        │  - Bloom filters per block   │
        │  - Sparse index for seeking  │
        │  - Block size: 64KB          │
        └──────────────┬───────────────┘
                       │ age/access threshold
                       ▼
        ┌──────────────────────────────┐
        │     COLD TIER                │
        │  Compressed archives         │
        │  - Zstd dictionary compress  │
        │  - Concatenated segments     │
        │  - Read requires decompress  │
        │  - Archive size: 1GB         │
        └──────────────────────────────┘
```

### 4.2 Hot Tier — Memory-Mapped Log Segments

```go
type HotSegment struct {
    file     *os.File
    mmap     []byte           // mmap'd region
    size     int64
    baseOff  uint64           // First message offset in this segment
    maxSize  int64            // Default 256MB
    index    *SparseIndex     // In-memory offset→position index
    created  time.Time
    frozen   bool             // true = read-only, pending tier migration
}

type SparseIndex struct {
    entries  []IndexEntry     // Every Nth message (default N=256)
    interval uint32
}

type IndexEntry struct {
    Offset   uint64
    Position uint32           // Byte position in segment file
    Timestamp int64
}
```

**Write path:**
1. Message serialized to binary envelope
2. Appended to active segment's mmap region (single writer per partition)
3. Sparse index updated every 256 messages
4. fsync based on config: `immediate` / `interval` (default 200ms) / `os` (let OS decide)

**Read path (zero-copy):**
1. Binary search sparse index for nearest offset
2. Linear scan from nearest position to target
3. `sendfile(2)` directly from mmap to socket — no userspace copy

### 4.3 Warm Tier — LSM-Tree

LSM-Tree for compacted, indexed, queryable storage:

```go
type LSMTree struct {
    memtable    *MemTable           // Active writes (red-black tree)
    immutables  []*MemTable         // Frozen, pending flush
    levels      []*Level            // L0..L6 sorted run levels
    manifest    *Manifest           // SSTable metadata
    bloomFilter *BloomFilter        // Per-SSTable bloom filters
    compactor   *Compactor          // Background compaction goroutine
}

type SSTable struct {
    file        *os.File
    index       *BlockIndex         // Block offset index
    bloom       *BloomFilter        // Key membership test
    metadata    SSTMetadata         // Min/max timestamp, offset range, count
    compression CompressionType     // Snappy for warm tier
}
```

**Compaction strategies:**
- **Size-tiered** (default for stream topics): Merge similar-size SSTables
- **Leveled** (for compacted topics): Key-based dedup, latest value wins
- **Tombstone** (for delete-heavy workloads): Aggressive dead record removal

### 4.4 Cold Tier — Compressed Archives

```go
type ColdArchive struct {
    path         string
    offsetRange  OffsetRange        // First..Last offset
    timeRange    TimeRange          // First..Last timestamp
    compression  CompressionType    // Zstd with trained dictionary
    dictID       uint32             // Zstd dictionary reference
    segmentIndex []ArchiveSegIndex  // Segment boundaries within archive
    size         int64              // Compressed size on disk
}
```

**Zstd dictionary training:** Every 100 archives, train a new dictionary from recent message samples for better compression ratios on similar message patterns.

### 4.5 Tier Migration

```go
type TierPolicy struct {
    HotRetention    time.Duration  // Keep in hot tier (default: 1 hour)
    WarmRetention   time.Duration  // Keep in warm tier (default: 24 hours)
    ColdRetention   time.Duration  // Keep in cold tier (default: 7 days)
    HotMaxSize      int64          // Max hot tier size per partition
    WarmMaxSize     int64          // Max warm tier size per partition
    CompactOnMigrate bool          // Run compaction during hot→warm
}
```

Background goroutine per node:
1. **Hot→Warm:** Frozen segments converted to SSTables, bloom filters built
2. **Warm→Cold:** SSTables merged into compressed archives, dictionary applied
3. **Cold→Delete:** Archives past retention purged

### 4.6 Write-Ahead Log (WAL)

Separate from storage tiers — WAL ensures durability before hot tier write:

```go
type WAL struct {
    file     *os.File
    offset   uint64
    maxSize  int64     // Default 128MB, then rotate
    syncMode SyncMode  // Immediate, Interval, OS
}
```

Write path: WAL append → Hot segment write → WAL checkpoint → WAL truncate

---

## 5. QUEUE ENGINE (Lion Head)

### 5.1 Queue Semantics

```go
type QueueState struct {
    topic       *ChimeraTopic
    consumers   map[string]*QueueConsumer  // ConsumerID → state
    unacked     *AckTracker                // In-flight message tracking
    dlq         *DLQManager                // Dead-letter routing
    delayHeap   *DelayScheduler            // Delayed message min-heap
    prefetchCap int                        // Max in-flight per consumer
}

type QueueConsumer struct {
    ID          string
    Connection  *ClientConnection
    Prefetch    int              // Consumer-specific prefetch
    InFlight    map[uint64]time.Time // Offset → delivery time
    AckBitmap   *roaring.Bitmap  // Acknowledged message offsets (pure Go impl)
}
```

### 5.2 Message Dispatch

Round-robin with prefetch awareness:

```
1. Message arrives in partition
2. Dispatcher checks consumer prefetch capacity
3. Selects next consumer with available capacity (round-robin)
4. Marks message as in-flight, starts ack timeout
5. On ACK: remove from in-flight, advance consumer cursor
6. On NACK: requeue with incremented DeliverCount
7. On timeout: requeue (configurable visibility timeout)
8. On MaxRetries exceeded: route to DLQ topic
```

### 5.3 Priority Queue

When `Priority > 0`, messages enter a per-partition priority skip list:

```go
type PriorityDispatcher struct {
    levels [10]*MessageList  // Priority 0-9, 9 = highest
    total  int64
}
```

Dispatch always drains highest priority first (starvation-aware with configurable fairness ratio).

### 5.4 Dead-Letter Queue (DLQ)

```go
type DLQManager struct {
    dlqTopic     string
    maxRetries   uint32
    captureHeaders bool  // Include original headers + error info
}
```

DLQ message includes extra headers:
- `x-chimera-original-topic`
- `x-chimera-death-reason` (rejected | expired | max-retries)
- `x-chimera-death-count`
- `x-chimera-first-death-time`
- `x-chimera-original-routing-key`

### 5.5 Delayed / Scheduled Messages

```go
type DelayScheduler struct {
    heap     *MinHeap[DelayedMsg]  // Min-heap sorted by DeliverAt
    ticker   *time.Ticker          // Check interval (default 100ms)
    storage  *DelayStore           // Persisted delayed messages
}

type DelayedMsg struct {
    DeliverAt time.Time
    Envelope  *MessageEnvelope
}
```

Messages with `DeliverAt > now` are stored in the delay scheduler instead of immediate dispatch. Background goroutine promotes messages to the main dispatch queue when their time arrives.

---

## 6. STREAM ENGINE (Goat Head)

### 6.1 Stream Semantics

```go
type StreamPartition struct {
    id          uint32
    activeHot   *HotSegment         // Current write segment
    segments    []*HotSegment       // All hot segments
    warmStore   *LSMTree            // Warm tier
    coldStore   *ColdArchiveManager // Cold tier
    highWater   uint64              // Highest committed offset
    logStart    uint64              // Earliest available offset
}

type ConsumerGroup struct {
    Name          string
    Topics        []string
    Members       map[string]*GroupMember
    Assignments   map[uint32]string        // PartitionID → MemberID
    CommittedOff  map[uint32]uint64         // PartitionID → committed offset
    Rebalancer    RebalanceStrategy         // Range, RoundRobin, Sticky
    SessionTimeout time.Duration
}

type GroupMember struct {
    ID            string
    Connection    *ClientConnection
    Partitions    []uint32
    LastHeartbeat time.Time
}
```

### 6.2 Consumer Group Rebalancing

Three strategies:

- **Range:** Consecutive partition ranges assigned to members (good for co-partitioned joins)
- **RoundRobin:** Partitions distributed evenly across members
- **Sticky:** Minimize partition movement during rebalance (preserves locality)

Rebalance triggered by: member join/leave, heartbeat timeout, manual trigger.

### 6.3 Log Compaction

For compacted topics (event sourcing, changelog):

```go
type Compactor struct {
    strategy   CompactionStrategy
    interval   time.Duration       // Default 5 minutes
    minDirty   float64             // Min dirty ratio to trigger (default 0.5)
    tombTTL    time.Duration       // Tombstone retention (default 24h)
}
```

Key-based compaction keeps latest value per key, removes duplicates and expired tombstones.

### 6.4 Offset Management

```go
type OffsetStore struct {
    // In-memory: fast reads
    cache    map[string]map[uint32]uint64 // Group → Partition → Offset
    // Durable: persisted in internal topic "$chimera/offsets"
    storage  *ChimeraTopic
}
```

Consumer offsets stored in internal compacted topic `$chimera/offsets` — self-bootstrapping, no external dependency.

---

## 7. CLUSTER FABRIC

### 7.1 Hybrid Architecture

```
┌────────────────────────────────────────────┐
│              Cluster Fabric                 │
│                                            │
│  ┌──────────────────────────────────────┐  │
│  │         Raft Layer (Control Plane)   │  │
│  │  - Topic/queue metadata             │  │
│  │  - Schema registry state            │  │
│  │  - ACL / auth state                 │  │
│  │  - Partition assignments            │  │
│  │  - Consumer group coordination      │  │
│  │  - 3 or 5 node Raft group           │  │
│  └──────────────────────────────────────┘  │
│                                            │
│  ┌──────────────────────────────────────┐  │
│  │       Gossip Layer (Data Plane)      │  │
│  │  - Node discovery & membership      │  │
│  │  - Health monitoring (SWIM)         │  │
│  │  - Partition replica sync status    │  │
│  │  - Load metrics dissemination       │  │
│  │  - Failure detection (φ accrual)    │  │
│  └──────────────────────────────────────┘  │
│                                            │
│  ┌──────────────────────────────────────┐  │
│  │      Replication Engine              │  │
│  │  - Leader-follower per partition     │  │
│  │  - Sync/async replication modes     │  │
│  │  - ISR (In-Sync Replicas) tracking  │  │
│  │  - Automatic leader election         │  │
│  └──────────────────────────────────────┘  │
└────────────────────────────────────────────┘
```

### 7.2 Raft Consensus (Metadata)

Embedded Raft implementation (from scratch, like other ECOSTACK projects):

```go
type RaftNode struct {
    id          uint64
    state       RaftState          // Follower, Candidate, Leader
    currentTerm uint64
    votedFor    uint64
    log         *RaftLog
    commitIndex uint64
    lastApplied uint64
    peers       map[uint64]*RaftPeer
    fsm         *MetadataFSM       // State machine for metadata
    transport   *RaftTransport     // TCP-based RPC
}

type MetadataFSM struct {
    topics       map[string]*ChimeraTopic
    schemas      *SchemaRegistry
    assignments  *PartitionAssigner
    acls         *ACLStore
    consumers    map[string]*ConsumerGroup
}
```

### 7.3 Gossip Protocol (SWIM)

```go
type GossipNode struct {
    id          uint64
    addr        string
    state       NodeState          // Alive, Suspect, Dead
    metadata    NodeMetadata       // CPU, memory, partition count, throughput
    incarnation uint32             // Monotonic counter for state disambiguation
}

type SWIM struct {
    members     map[uint64]*GossipNode
    suspicions  map[uint64]*SuspicionTimer
    probeInterval time.Duration    // Default 1 second
    probeTimeout  time.Duration    // Default 500ms
    indirectNodes int              // K=3 indirect probes
}
```

**Failure detection:**
1. Direct probe (TCP ping) every `probeInterval`
2. On timeout → K indirect probes via random healthy members
3. On all indirect timeouts → mark as `Suspect`
4. Suspicion timer (tunable, default 5s) → `Dead`
5. Dead nodes removed after configurable grace period

### 7.4 Data Replication

Per-partition leader-follower:

```go
type ReplicationConfig struct {
    Factor      uint32             // Replication factor (default 3)
    MinISR      uint32             // Minimum in-sync replicas for ack (default 2)
    AckPolicy   AckPolicy          // Leader, Quorum, All
    SyncMode    ReplicationSync    // Sync (wait for ISR), Async (fire-and-forget)
    MaxLag      uint64             // Max offset lag before ISR removal
}

type AckPolicy uint8
const (
    AckLeader AckPolicy = 0 // Ack after leader write (fastest, least safe)
    AckQuorum AckPolicy = 1 // Ack after majority ISR write (balanced)
    AckAll    AckPolicy = 2 // Ack after all ISR write (safest, slowest)
)
```

---

## 8. SCHEMA REGISTRY

### 8.1 Built-in Schema Registry

No need for Confluent Schema Registry — built directly into ChimeraMQ:

```go
type SchemaRegistry struct {
    schemas     map[uint32]*Schema        // SchemaID → Schema
    subjects    map[string]*SubjectSchemas // Subject → version chain
    compat      map[string]Compatibility   // Subject → compatibility level
    nextID      uint32
    raftBacked  bool                       // Persisted via Raft FSM
}

type Schema struct {
    ID         uint32
    Subject    string
    Version    uint32
    Type       SchemaType    // Avro, Protobuf, JSONSchema
    Definition []byte        // Raw schema definition
    Hash       [32]byte      // SHA-256 of definition
    CreatedAt  time.Time
}

type SchemaType uint8
const (
    SchemaAvro     SchemaType = 0
    SchemaProtobuf SchemaType = 1
    SchemaJSON     SchemaType = 2
)

type Compatibility uint8
const (
    CompatNone           Compatibility = 0
    CompatBackward       Compatibility = 1 // New schema can read old data
    CompatForward        Compatibility = 2 // Old schema can read new data
    CompatFull           Compatibility = 3 // Both directions
    CompatBackwardTransitive Compatibility = 4
    CompatForwardTransitive  Compatibility = 5
    CompatFullTransitive     Compatibility = 6
)
```

### 8.2 Schema Enforcement

Per-topic policy:

- **None:** No schema validation
- **Validate:** Check against schema, log warnings on mismatch, allow through
- **Enforce:** Reject messages that don't match registered schema

Validation happens at the protocol adapter layer — before message enters storage.

---

## 9. WASM TRANSFORM PIPELINE

### 9.1 In-Flight Message Transformation

Embedded WASM runtime (pure Go interpreter, no CGO):

```go
type WASMRuntime struct {
    modules   map[string]*WASMModule
    pool      *ModulePool              // Pre-instantiated module pool
    sandbox   *WASMSandbox             // Memory/CPU limits
}

type WASMModule struct {
    Name       string
    Binary     []byte                   // .wasm binary
    Entrypoint string                   // Export function name
    MemLimit   uint32                   // Max memory pages (64KB each)
    Timeout    time.Duration            // Max execution time per message
    Config     map[string]string        // Module-specific config
}

type TransformPipeline struct {
    Topic    string
    Stages   []TransformStage          // Ordered list of transforms
    OnError  TransformErrorPolicy      // Skip, DLQ, Reject
}

type TransformStage struct {
    ModuleName string
    Filter     string                   // Optional: header-based filter expression
    Order      int
}
```

### 9.2 WASM ABI

```
// ChimeraMQ WASM Transform ABI
// Guest exports:
//   transform(ptr: i32, len: i32) -> i64
//     Input:  MessageEnvelope bytes at (ptr, len)
//     Output: high 32 bits = ptr, low 32 bits = len of transformed message
//             OR 0 = drop message (filter)
//             OR -1 = pass through unchanged
//
// Host imports:
//   chimera_log(level: i32, ptr: i32, len: i32)
//   chimera_get_config(key_ptr: i32, key_len: i32) -> i64
//   chimera_set_header(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32)
//   chimera_get_header(key_ptr: i32, key_len: i32) -> i64
//   chimera_emit(topic_ptr: i32, topic_len: i32, msg_ptr: i32, msg_len: i32)
```

Use cases:
- **Message enrichment:** Add headers, transform payload format
- **Filtering:** Drop messages based on content rules
- **Routing:** Dynamic routing based on message inspection
- **Fan-out:** Emit copies to multiple topics
- **PII redaction:** Strip sensitive fields before storage

---

## 10. STREAM PROCESSING ENGINE

### 10.1 Embedded Stream Processing

Lightweight stream processing — not a full Flink replacement, but handles common patterns:

```go
type StreamProcessor struct {
    ID          string
    Sources     []string            // Input topics
    Sink        string              // Output topic
    Pipeline    []ProcessorStage    // Processing stages
    Parallelism int                 // Concurrent workers
    Checkpoint  time.Duration       // State checkpoint interval
}

type ProcessorStage interface {
    Process(msg *MessageEnvelope, state *StateStore) ([]*MessageEnvelope, error)
    Init(state *StateStore) error
    Close() error
}
```

### 10.2 Built-in Operators

```go
// Window types
type WindowType uint8
const (
    TumblingWindow  WindowType = 0 // Fixed-size, non-overlapping
    SlidingWindow   WindowType = 1 // Fixed-size, overlapping by slide interval
    SessionWindow   WindowType = 2 // Gap-based, dynamic size
    HoppingWindow   WindowType = 3 // Fixed-size, custom hop interval
)

// Built-in operators
type FilterOp struct { Predicate func(*MessageEnvelope) bool }
type MapOp struct { Transform func(*MessageEnvelope) *MessageEnvelope }
type FlatMapOp struct { Transform func(*MessageEnvelope) []*MessageEnvelope }
type WindowAggregateOp struct {
    WindowType WindowType
    Size       time.Duration
    Slide      time.Duration           // For sliding/hopping
    Gap        time.Duration           // For session
    Init       func() interface{}
    Accumulate func(state interface{}, msg *MessageEnvelope) interface{}
    Emit       func(state interface{}, window TimeRange) *MessageEnvelope
}
type JoinOp struct {
    LeftTopic  string
    RightTopic string
    JoinKey    func(*MessageEnvelope) string
    Window     time.Duration
    JoinFn     func(left, right *MessageEnvelope) *MessageEnvelope
}
```

### 10.3 State Store

```go
type StateStore struct {
    local    map[string][]byte     // In-memory state
    backend  *LSMTree              // Persistent state (survives restarts)
    changelog *ChimeraTopic         // State changelog topic for fault tolerance
}
```

State is backed by a local LSM-Tree and replicated via internal changelog topic — on failure, state is rebuilt from changelog (Kafka Streams pattern).

---

## 11. SECURITY

### 11.1 Authentication

```go
type AuthProvider interface {
    Authenticate(credentials Credentials) (*Identity, error)
}

// Built-in providers
type PlaintextAuth struct { ... }    // Username/password (SCRAM-SHA-256)
type TLSAuth struct { ... }          // Mutual TLS (client certificates)
type JWTAuth struct { ... }          // JWT token validation
type OAuthAuth struct { ... }        // OAuth 2.0 / OIDC
type LDAPAuth struct { ... }         // LDAP/AD integration
```

### 11.2 Authorization (ACL)

```go
type ACLEntry struct {
    Principal   string              // User or group
    Resource    ResourceType        // Topic, ConsumerGroup, Cluster, Schema
    ResourceName string             // Specific name or wildcard
    Operation   OperationType       // Read, Write, Create, Delete, Alter, Describe, All
    Permission  PermissionType      // Allow, Deny
}
```

### 11.3 Encryption

- **In-transit:** TLS 1.3 (all protocols), optional mutual TLS
- **At-rest:** AES-256-GCM per-segment encryption (configurable per-topic)
- **Key management:** Built-in key rotation, external KMS plugin interface

---

## 12. OBSERVABILITY

### 12.1 Metrics (Prometheus)

```
# Broker metrics
chimera_messages_in_total{topic, partition, protocol}
chimera_messages_out_total{topic, partition, consumer_group}
chimera_bytes_in_total{topic}
chimera_bytes_out_total{topic}
chimera_active_connections{protocol}
chimera_partitions_total{topic, state}

# Storage metrics
chimera_storage_hot_bytes{topic}
chimera_storage_warm_bytes{topic}
chimera_storage_cold_bytes{topic}
chimera_tier_migrations_total{from, to}
chimera_compaction_duration_seconds{topic}

# Queue metrics
chimera_queue_depth{topic}
chimera_messages_unacked{topic, consumer}
chimera_dlq_messages_total{topic}
chimera_delayed_messages_pending{topic}

# Consumer lag
chimera_consumer_lag{topic, partition, consumer_group}
chimera_consumer_lag_seconds{topic, partition, consumer_group}

# Cluster metrics
chimera_raft_term
chimera_raft_leader
chimera_gossip_members{state}
chimera_replication_lag{topic, partition, replica}
chimera_isr_count{topic, partition}

# WASM metrics
chimera_wasm_executions_total{module, topic}
chimera_wasm_execution_duration_seconds{module}
chimera_wasm_errors_total{module, topic}

# Stream processing metrics
chimera_processor_records_in_total{processor}
chimera_processor_records_out_total{processor}
chimera_processor_state_size_bytes{processor}
chimera_processor_window_count{processor, type}
```

### 12.2 Built-in Dashboard

Embedded web UI (React, served from binary):

- Cluster overview (node health, leader status)
- Topic browser (messages, partitions, consumer lag)
- Consumer group management
- Schema registry browser
- WASM module management
- Stream processor monitoring
- Real-time message inspector

### 12.3 Distributed Tracing

OpenTelemetry-compatible trace propagation:
- TraceID/SpanID in message envelope
- Automatic span creation per protocol adapter
- W3C Trace Context header support
- Export: OTLP gRPC/HTTP, Jaeger, Zipkin

---

## 13. CLI

### 13.1 Command Structure

```
chimera server                          Start broker
chimera server --config chimera.yaml    Start with config file

chimera topic create <name> [flags]     Create topic
chimera topic list                      List topics
chimera topic describe <name>           Topic details
chimera topic delete <name>             Delete topic

chimera produce <topic> [flags]         Produce messages (stdin/file)
chimera consume <topic> [flags]         Consume messages (stdout)

chimera consumer-group list             List consumer groups
chimera consumer-group describe <name>  Group details + lag
chimera consumer-group reset <name>     Reset offsets

chimera schema register <subject> <file>  Register schema
chimera schema list                       List schemas
chimera schema get <id>                   Get schema

chimera wasm deploy <name> <file.wasm>  Deploy WASM module
chimera wasm list                       List modules
chimera wasm remove <name>              Remove module

chimera processor create <config.yaml>  Create stream processor
chimera processor list                  List processors
chimera processor describe <name>       Processor details

chimera cluster status                  Cluster status
chimera cluster nodes                   List nodes
chimera cluster rebalance               Trigger rebalance

chimera bench produce <topic> [flags]   Benchmark producer
chimera bench consume <topic> [flags]   Benchmark consumer

chimera mcp-server                      Start MCP server
chimera version                         Version info
```

---

## 14. CONFIGURATION

### 14.1 chimera.yaml

```yaml
node:
  id: 1                              # Unique node ID
  name: "chimera-01"                 # Human-readable name
  data_dir: "/var/lib/chimera"       # Data directory
  
listener:
  bind: "0.0.0.0"
  port: 5672                         # Primary port (all protocols)
  admin_port: 9090                   # HTTP admin API
  max_connections: 100000
  
tls:
  enabled: false
  cert_file: ""
  key_file: ""
  ca_file: ""
  mutual: false
  
protocols:
  chimera:
    enabled: true
    max_frame_size: 1048576          # 1MB
  amqp:
    enabled: true
    max_frame_size: 131072           # 128KB
  mqtt:
    enabled: true
    max_packet_size: 268435456       # 256MB
    max_qos: 2
  websocket:
    enabled: true
    path: "/ws"
    
storage:
  hot:
    segment_size: 268435456          # 256MB
    sync_mode: "interval"            # immediate, interval, os
    sync_interval: "200ms"
    max_segments: 10                 # Per partition
  warm:
    block_size: 65536                # 64KB
    bloom_fp_rate: 0.01
    compaction_strategy: "size_tiered"
    compaction_interval: "5m"
  cold:
    archive_size: 1073741824         # 1GB
    compression: "zstd"
    compression_level: 3
    dict_training_interval: 100      # Every N archives
  tier_policy:
    hot_retention: "1h"
    warm_retention: "24h"
    cold_retention: "168h"           # 7 days
  wal:
    max_size: 134217728              # 128MB
    sync_mode: "interval"
    sync_interval: "100ms"
    
cluster:
  raft:
    peers:                           # Initial Raft peers
      - "chimera-01:5673"
      - "chimera-02:5673"
      - "chimera-03:5673"
    election_timeout: "1s"
    heartbeat_interval: "150ms"
    snapshot_interval: "5m"
    max_log_entries: 100000
  gossip:
    bind_port: 5674
    seeds:                           # Bootstrap nodes
      - "chimera-01:5674"
      - "chimera-02:5674"
    probe_interval: "1s"
    probe_timeout: "500ms"
    indirect_nodes: 3
    suspicion_timeout: "5s"
  replication:
    default_factor: 3
    min_isr: 2
    ack_policy: "quorum"
    sync_mode: "sync"
    max_lag: 10000
    
defaults:
  topic:
    partitions: 8
    replication: 3
    retention_time: "168h"
    retention_size: 0                # 0 = unlimited
    mode: "unified"
    
schema_registry:
  enabled: true
  default_compatibility: "backward"
  
wasm:
  enabled: true
  max_memory_pages: 256              # 16MB per module
  execution_timeout: "100ms"
  module_pool_size: 4                # Pre-instantiated per module
  
stream_processing:
  enabled: true
  checkpoint_interval: "30s"
  state_dir: "/var/lib/chimera/state"
  
auth:
  enabled: false
  provider: "plaintext"              # plaintext, tls, jwt, oauth, ldap
  
acl:
  enabled: false
  default_policy: "allow"            # allow, deny
  
observability:
  metrics:
    enabled: true
    path: "/metrics"
  tracing:
    enabled: false
    exporter: "otlp"
    endpoint: ""
  dashboard:
    enabled: true
    path: "/ui"
    
logging:
  level: "info"                      # debug, info, warn, error
  format: "json"                     # json, text
  output: "stdout"                   # stdout, file
  file: "/var/log/chimera/chimera.log"
```

---

## 15. MCP SERVER

### 15.1 Model Context Protocol Integration

```go
type ChimeraMCPServer struct {
    broker  *Broker
    tools   []MCPTool
}

// MCP Tools:
// - chimera_publish        Publish message to topic
// - chimera_consume        Consume N messages from topic
// - chimera_create_topic   Create a new topic
// - chimera_list_topics    List all topics with stats
// - chimera_topic_stats    Get detailed topic statistics
// - chimera_consumer_lag   Get consumer group lag
// - chimera_cluster_status Get cluster health
// - chimera_search_messages Search messages by header/content
// - chimera_schema_register Register a schema
// - chimera_deploy_wasm    Deploy WASM transform
```

---

## 16. PHASED IMPLEMENTATION

### Phase 1 — Core Engine (MVP)
- Internal message envelope & binary serialization
- Chimera native protocol (connect, publish, subscribe, ack)
- Hot tier storage (mmap log segments, sparse index)
- WAL for durability
- Queue mode (competing consumers, ack/nack, round-robin)
- Stream mode (partitions, offsets, consumer groups)
- Unified mode (dual consumption)
- Single-node operation
- CLI (server, topic, produce, consume)
- HTTP admin API (topics, health, metrics)
- Prometheus metrics (basic set)
- YAML configuration

### Phase 2 — Multi-Protocol
- Protocol multiplexer (auto-detection)
- AMQP 1.0 adapter (connections, sessions, links, exchanges, bindings)
- MQTT adapter (3.1.1 + 5.0, QoS 0/1/2, retained, will)
- WebSocket adapter (JSON + binary sub-protocols)
- Protocol-level authentication

### Phase 3 — Clustering
- Embedded Raft (leader election, log replication, snapshots)
- Metadata FSM (topics, schemas, ACLs, assignments)
- Gossip / SWIM (node discovery, health, failure detection)
- Partition replication (leader-follower, ISR tracking)
- Consumer group rebalancing (range, round-robin, sticky)
- Automatic leader election on failure

### Phase 4 — Advanced Storage
- Warm tier (LSM-Tree, SSTables, bloom filters)
- Cold tier (compressed archives, zstd dictionaries)
- Automatic tier migration
- Log compaction (key-based, tombstone)
- Size-based retention
- At-rest encryption (AES-256-GCM)

### Phase 5 — Schema & DLQ
- Schema Registry (Avro, Protobuf, JSON Schema)
- Compatibility checking (backward, forward, full, transitive)
- Schema enforcement per-topic
- Dead-letter queues
- Delayed / scheduled messages
- Priority queues
- Message TTL

### Phase 6 — WASM & Stream Processing
- Embedded WASM runtime (pure Go)
- Transform pipeline (filter, enrich, route, fan-out)
- WASM module management (deploy, remove, update)
- Stream processing engine (filter, map, flatMap)
- Windowing (tumbling, sliding, session, hopping)
- Aggregations & joins
- State store (LSM-backed, changelog replicated)

### Phase 7 — Production Hardening
- OAuth 2.0 / OIDC authentication
- LDAP integration
- Full ACL engine
- Mutual TLS
- Embedded Web UI (React dashboard)
- OpenTelemetry tracing
- MCP server
- Benchmark suite
- Chaos testing framework

---

## 17. PERFORMANCE TARGETS

| Metric                          | Target                    |
|---------------------------------|---------------------------|
| Single-node throughput (publish) | 1M+ messages/sec          |
| Single-node throughput (consume) | 2M+ messages/sec          |
| P99 latency (publish)           | < 5ms                     |
| P99 latency (consume, hot tier) | < 2ms                     |
| Binary size                     | < 30MB                    |
| Memory (idle, 100 topics)       | < 100MB                   |
| Cold start time                 | < 1 second                |
| Max partitions per node         | 10,000+                   |
| Max connections per node        | 100,000+                  |
| Max message size                | 256MB (configurable)      |

---

## 18. DIRECTORY STRUCTURE

```
chimera/
├── cmd/
│   └── chimera/
│       └── main.go                  # Single entry point
├── internal/
│   ├── broker/
│   │   ├── broker.go                # Core broker orchestration
│   │   ├── topic.go                 # Topic management
│   │   └── config.go                # Configuration loading
│   ├── protocol/
│   │   ├── mux.go                   # Protocol multiplexer
│   │   ├── chimera/                 # Native protocol
│   │   │   ├── server.go
│   │   │   ├── codec.go
│   │   │   └── handler.go
│   │   ├── amqp/                    # AMQP 1.0 adapter
│   │   │   ├── server.go
│   │   │   ├── session.go
│   │   │   ├── link.go
│   │   │   └── exchange.go
│   │   ├── mqtt/                    # MQTT adapter
│   │   │   ├── server.go
│   │   │   ├── session.go
│   │   │   └── handler.go
│   │   ├── ws/                      # WebSocket adapter
│   │   │   ├── server.go
│   │   │   └── handler.go
│   │   └── http/                    # REST API
│   │       ├── server.go
│   │       └── routes.go
│   ├── engine/
│   │   ├── queue/                   # Queue engine (Lion Head)
│   │   │   ├── dispatcher.go
│   │   │   ├── ack.go
│   │   │   ├── dlq.go
│   │   │   ├── delay.go
│   │   │   └── priority.go
│   │   └── stream/                  # Stream engine (Goat Head)
│   │       ├── partition.go
│   │       ├── consumer_group.go
│   │       ├── rebalance.go
│   │       ├── compaction.go
│   │       └── offset.go
│   ├── storage/
│   │   ├── hot/                     # Memory-mapped log segments
│   │   │   ├── segment.go
│   │   │   ├── index.go
│   │   │   └── mmap.go
│   │   ├── warm/                    # LSM-Tree
│   │   │   ├── lsm.go
│   │   │   ├── memtable.go
│   │   │   ├── sstable.go
│   │   │   ├── bloom.go
│   │   │   └── compaction.go
│   │   ├── cold/                    # Compressed archives
│   │   │   ├── archive.go
│   │   │   ├── compress.go
│   │   │   └── dict.go
│   │   ├── wal/                     # Write-ahead log
│   │   │   └── wal.go
│   │   └── tier/                    # Tier migration
│   │       └── migrator.go
│   ├── cluster/
│   │   ├── raft/                    # Raft consensus
│   │   │   ├── node.go
│   │   │   ├── log.go
│   │   │   ├── fsm.go
│   │   │   ├── transport.go
│   │   │   └── snapshot.go
│   │   ├── gossip/                  # SWIM gossip
│   │   │   ├── swim.go
│   │   │   ├── member.go
│   │   │   └── detector.go
│   │   └── replication/             # Data replication
│   │       ├── replicator.go
│   │       └── isr.go
│   ├── schema/                      # Schema Registry
│   │   ├── registry.go
│   │   ├── avro.go
│   │   ├── protobuf.go
│   │   ├── jsonschema.go
│   │   └── compat.go
│   ├── wasm/                        # WASM runtime
│   │   ├── runtime.go
│   │   ├── module.go
│   │   ├── pipeline.go
│   │   └── abi.go
│   ├── stream/                      # Stream processing
│   │   ├── processor.go
│   │   ├── window.go
│   │   ├── aggregate.go
│   │   ├── join.go
│   │   └── state.go
│   ├── auth/                        # Authentication
│   │   ├── auth.go
│   │   ├── scram.go
│   │   ├── jwt.go
│   │   └── acl.go
│   ├── message/                     # Message envelope
│   │   ├── envelope.go
│   │   ├── codec.go
│   │   └── uuid.go
│   ├── mcp/                         # MCP server
│   │   └── server.go
│   └── ui/                          # Embedded web UI
│       └── embed.go
├── web/                             # React dashboard source
│   └── ...
├── docs/
│   ├── SPECIFICATION.md
│   ├── IMPLEMENTATION.md
│   ├── TASKS.md
│   └── BRANDING.md
├── configs/
│   └── chimera.yaml.example
├── go.mod
├── go.sum
├── Makefile
├── Dockerfile
├── LICENSE
└── README.md
```

---

## 19. DEPENDENCY POLICY

### Allowed External Dependencies (Extended Stdlib Only)

| Package              | Purpose                | Justification                     |
|----------------------|------------------------|-----------------------------------|
| golang.org/x/crypto  | SCRAM, TLS helpers     | Go extended stdlib                |
| golang.org/x/sys     | mmap, sendfile, epoll  | Go extended stdlib                |
| gopkg.in/yaml.v3     | YAML config parsing    | Standard YAML parser              |

**Everything else is built from scratch:**
- Raft consensus ✋ (no hashicorp/raft)
- Gossip/SWIM ✋ (no memberlist)
- LSM-Tree ✋ (no badger, pebble)
- AMQP 1.0 codec ✋ (no azure/go-amqp)
- MQTT codec ✋ (no paho)
- WASM runtime ✋ (no wazero — evaluate; if pure Go and lightweight, consider exception)
- Bloom filter ✋ (no external)
- UUID generation ✋ (no google/uuid)

> **WASM Exception Note:** If `wazero` (pure Go, zero CGO) proves essential for WASM compliance and performance, it may be accepted as a controlled exception — same category as YAML parser. Decision deferred to Phase 6.

---

## 20. TAGLINE & POSITIONING

**Primary tagline:** "Three Heads. One Binary. All Messages."

**Secondary taglines:**
- "The beast that devours Kafka, RabbitMQ, and Pulsar."
- "Queue + Stream + Multi-Protocol. One `go install` away."
- "Stop choosing between queues and streams."

**Positioning statement:**
ChimeraMQ is a unified message queue and event streaming platform that replaces Kafka, RabbitMQ, and Pulsar with a single Go binary. No JVM. No Erlang. No ZooKeeper. No BookKeeper. Just one binary that speaks every protocol and handles every messaging pattern.

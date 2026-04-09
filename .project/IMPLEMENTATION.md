# ChimeraMQ — IMPLEMENTATION GUIDE

> **Phase 1: Core Engine (MVP)**
> Single-node broker with native protocol, hot storage, queue + stream + unified semantics.

**Scope:** This document covers Phase 1 implementation in precise detail — every algorithm, data structure, goroutine lifecycle, and wire format needed to build a working single-node ChimeraMQ broker.

---

## TABLE OF CONTENTS

1. [Bootstrap & Lifecycle](#1-bootstrap--lifecycle)
2. [Configuration System](#2-configuration-system)
3. [Message Envelope & Codec](#3-message-envelope--codec)
4. [UUIDv7 Generator](#4-uuidv7-generator)
5. [Write-Ahead Log (WAL)](#5-write-ahead-log-wal)
6. [Hot Tier Storage Engine](#6-hot-tier-storage-engine)
7. [Topic Manager](#7-topic-manager)
8. [Queue Engine (Lion Head)](#8-queue-engine-lion-head)
9. [Stream Engine (Goat Head)](#9-stream-engine-goat-head)
10. [Unified Mode](#10-unified-mode)
11. [Chimera Native Protocol](#11-chimera-native-protocol)
12. [Client Connection Manager](#12-client-connection-manager)
13. [HTTP Admin API](#13-http-admin-api)
14. [Prometheus Metrics](#14-prometheus-metrics)
15. [CLI](#15-cli)
16. [Graceful Shutdown](#16-graceful-shutdown)
17. [Testing Strategy](#17-testing-strategy)
18. [Build & Distribution](#18-build--distribution)

---

## 1. BOOTSTRAP & LIFECYCLE

### 1.1 Entry Point

```
cmd/chimera/main.go
```

The binary has one entry point. Subcommand routing determines behavior:

```go
func main() {
    if len(os.Args) < 2 {
        printUsage()
        os.Exit(1)
    }

    switch os.Args[1] {
    case "server":
        runServer(os.Args[2:])
    case "topic":
        runTopicCLI(os.Args[2:])
    case "produce":
        runProduceCLI(os.Args[2:])
    case "consume":
        runConsumeCLI(os.Args[2:])
    case "version":
        printVersion()
    default:
        printUsage()
        os.Exit(1)
    }
}
```

### 1.2 Server Bootstrap Sequence

The broker starts in a strict ordered sequence. Each component depends on the previous:

```
Step 1:  Parse CLI flags & load chimera.yaml
Step 2:  Initialize logger (structured JSON or text)
Step 3:  Create/validate data directory structure
Step 4:  Open WAL (recover if dirty shutdown detected)
Step 5:  Initialize Hot Storage Engine
Step 6:  Replay WAL entries into Hot Storage (crash recovery)
Step 7:  Initialize Topic Manager (load topic metadata from disk)
Step 8:  Initialize Queue Engine
Step 9:  Initialize Stream Engine
Step 10: Start Chimera Protocol listener (TCP)
Step 11: Start HTTP Admin API listener
Step 12: Start metrics collector goroutine
Step 13: Start background goroutines (segment roller, fsync ticker)
Step 14: Register OS signal handlers (SIGINT, SIGTERM)
Step 15: Log "ChimeraMQ started" with listen addresses
Step 16: Block on signal / context cancellation
```

### 1.3 Broker Struct

```go
// internal/broker/broker.go

type Broker struct {
    config       *Config
    logger       *Logger
    wal          *wal.WAL
    storage      *hot.Engine
    topics       *TopicManager
    queueEngine  *queue.Engine
    streamEngine *stream.Engine
    protoServer  *chimera.Server
    httpServer   *http.Server
    metrics      *metrics.Collector
    
    ctx          context.Context
    cancel       context.CancelFunc
    wg           sync.WaitGroup
}

func NewBroker(cfg *Config) (*Broker, error) {
    ctx, cancel := context.WithCancel(context.Background())
    b := &Broker{
        config: cfg,
        ctx:    ctx,
        cancel: cancel,
    }
    return b, nil
}

func (b *Broker) Start() error {
    // Execute bootstrap sequence steps 2-15
    // Each step returns error; fail fast on any error
    // Use b.wg.Add() for each background goroutine
    return nil
}

func (b *Broker) Stop() error {
    // Graceful shutdown (see Section 16)
    b.cancel()
    b.wg.Wait()
    return nil
}
```

### 1.4 Data Directory Layout

```
{data_dir}/
├── wal/
│   ├── 000000000001.wal          # WAL segment files
│   ├── 000000000002.wal
│   └── checkpoint                 # Last checkpointed WAL offset
├── topics/
│   ├── meta.json                  # Topic registry (name→config)
│   └── {topic_name}/
│       ├── partition-0/
│       │   ├── 00000000000000000000.log    # Hot segment (base offset = 0)
│       │   ├── 00000000000000000000.idx    # Sparse index
│       │   ├── 00000000000256000000.log    # Next segment
│       │   ├── 00000000000256000000.idx
│       │   └── state.json                   # Partition state (highWater, etc.)
│       ├── partition-1/
│       │   └── ...
│       └── partition-2/
│           └── ...
├── consumers/
│   └── {group_name}/
│       └── offsets.json            # Committed offsets per partition
└── chimera.lock                    # PID lock file (prevent double start)
```

### 1.5 Lock File

Prevent two broker instances from using the same data directory:

```go
func acquireLockFile(dataDir string) (*os.File, error) {
    lockPath := filepath.Join(dataDir, "chimera.lock")
    f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_WRONLY, 0600)
    if err != nil {
        return nil, err
    }
    // Try exclusive flock
    if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
        f.Close()
        return nil, fmt.Errorf("data directory already locked by another process")
    }
    // Write PID
    f.Truncate(0)
    f.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
    f.Sync()
    return f, nil
}
```

---

## 2. CONFIGURATION SYSTEM

### 2.1 Config Struct

```go
// internal/broker/config.go

type Config struct {
    Node     NodeConfig     `yaml:"node"`
    Listener ListenerConfig `yaml:"listener"`
    Storage  StorageConfig  `yaml:"storage"`
    Defaults DefaultsConfig `yaml:"defaults"`
    Logging  LoggingConfig  `yaml:"logging"`
}

type NodeConfig struct {
    ID      uint64 `yaml:"id"`
    Name    string `yaml:"name"`
    DataDir string `yaml:"data_dir"`
}

type ListenerConfig struct {
    Bind           string `yaml:"bind"`
    Port           int    `yaml:"port"`
    AdminPort      int    `yaml:"admin_port"`
    MaxConnections int    `yaml:"max_connections"`
}

type StorageConfig struct {
    Hot        HotConfig        `yaml:"hot"`
    WAL        WALConfig        `yaml:"wal"`
    TierPolicy TierPolicyConfig `yaml:"tier_policy"`
}

type HotConfig struct {
    SegmentSize  int64  `yaml:"segment_size"`   // bytes
    SyncMode     string `yaml:"sync_mode"`      // immediate, interval, os
    SyncInterval string `yaml:"sync_interval"`  // duration string
    MaxSegments  int    `yaml:"max_segments"`   // per partition
}

type WALConfig struct {
    MaxSize      int64  `yaml:"max_size"`
    SyncMode     string `yaml:"sync_mode"`
    SyncInterval string `yaml:"sync_interval"`
}

type TierPolicyConfig struct {
    HotRetention string `yaml:"hot_retention"`
}

type DefaultsConfig struct {
    Topic TopicDefaults `yaml:"topic"`
}

type TopicDefaults struct {
    Partitions    uint32 `yaml:"partitions"`
    RetentionTime string `yaml:"retention_time"`
    Mode          string `yaml:"mode"` // stream, queue, unified
}

type LoggingConfig struct {
    Level  string `yaml:"level"`
    Format string `yaml:"format"`
    Output string `yaml:"output"`
    File   string `yaml:"file"`
}
```

### 2.2 Config Loading

Priority: CLI flags > environment variables > config file > defaults.

```go
func LoadConfig(configPath string, flags *CLIFlags) (*Config, error) {
    cfg := defaultConfig()
    
    // Load YAML if path provided
    if configPath != "" {
        data, err := os.ReadFile(configPath)
        if err != nil {
            return nil, fmt.Errorf("reading config: %w", err)
        }
        if err := yaml.Unmarshal(data, cfg); err != nil {
            return nil, fmt.Errorf("parsing config: %w", err)
        }
    }
    
    // Override with env vars (CHIMERA_NODE_ID, CHIMERA_DATA_DIR, etc.)
    applyEnvOverrides(cfg)
    
    // Override with CLI flags
    if flags != nil {
        applyCLIOverrides(cfg, flags)
    }
    
    // Validate
    if err := cfg.Validate(); err != nil {
        return nil, fmt.Errorf("invalid config: %w", err)
    }
    
    return cfg, nil
}

func defaultConfig() *Config {
    return &Config{
        Node: NodeConfig{
            ID:      1,
            Name:    "chimera-01",
            DataDir: "/var/lib/chimera",
        },
        Listener: ListenerConfig{
            Bind:           "0.0.0.0",
            Port:           5672,
            AdminPort:      9090,
            MaxConnections: 100000,
        },
        Storage: StorageConfig{
            Hot: HotConfig{
                SegmentSize:  256 * 1024 * 1024, // 256MB
                SyncMode:     "interval",
                SyncInterval: "200ms",
                MaxSegments:  10,
            },
            WAL: WALConfig{
                MaxSize:      128 * 1024 * 1024, // 128MB
                SyncMode:     "interval",
                SyncInterval: "100ms",
            },
            TierPolicy: TierPolicyConfig{
                HotRetention: "1h",
            },
        },
        Defaults: DefaultsConfig{
            Topic: TopicDefaults{
                Partitions:    8,
                RetentionTime: "168h",
                Mode:          "unified",
            },
        },
        Logging: LoggingConfig{
            Level:  "info",
            Format: "json",
            Output: "stdout",
        },
    }
}
```

### 2.3 Environment Variable Mapping

Pattern: `CHIMERA_{SECTION}_{KEY}` in uppercase.

```
CHIMERA_NODE_ID          → Node.ID
CHIMERA_NODE_NAME        → Node.Name
CHIMERA_DATA_DIR         → Node.DataDir
CHIMERA_LISTEN_PORT      → Listener.Port
CHIMERA_ADMIN_PORT       → Listener.AdminPort
CHIMERA_LOG_LEVEL        → Logging.Level
CHIMERA_LOG_FORMAT       → Logging.Format
```

---

## 3. MESSAGE ENVELOPE & CODEC

### 3.1 Constants & Types

```go
// internal/message/envelope.go

const (
    FixedHeaderSize = 64 // bytes

    FlagHasHeaders    uint8 = 1 << 0
    FlagHasRoutingKey uint8 = 1 << 1
    FlagHasTrace      uint8 = 1 << 2
    FlagHasTTL        uint8 = 1 << 3
    FlagHasDelay      uint8 = 1 << 4
)

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

type Envelope struct {
    // Identity
    MessageID [16]byte
    Timestamp int64    // Unix nanoseconds
    Sequence  uint64   // Per-partition monotonic

    // Routing
    Topic       string
    PartitionID uint32
    RoutingKey  string
    Headers     map[string][]byte

    // Payload
    SchemaID    uint32
    ContentType string
    Encoding    EncodingType
    Payload     []byte

    // Delivery
    Priority     uint8
    TTL          int64  // Nanoseconds, 0 = no expiry
    DeliverAt    int64  // Delayed delivery timestamp
    DeliverCount uint32
    MaxRetries   uint32

    // Tracing
    TraceID     [16]byte
    SpanID      [8]byte
    SourceProto ProtocolType
}
```

### 3.2 Binary Codec

The codec must be allocation-efficient. Use a shared buffer pool:

```go
// internal/message/codec.go

var bufferPool = sync.Pool{
    New: func() interface{} {
        buf := make([]byte, 0, 4096)
        return &buf
    },
}

// Marshal serializes an Envelope to binary format.
// Returns a byte slice from the pool — caller must release via ReleaseBuffer.
func Marshal(e *Envelope) ([]byte, error) {
    // Calculate total size
    size := FixedHeaderSize
    size += len(e.Topic)
    if e.RoutingKey != "" {
        size += len(e.RoutingKey)
    }
    size += len(e.Payload)
    if len(e.Headers) > 0 {
        size += headersSize(e.Headers)
    }
    if e.TraceID != [16]byte{} {
        size += 24 // TraceID(16) + SpanID(8)
    }

    bufPtr := bufferPool.Get().(*[]byte)
    buf := *bufPtr
    if cap(buf) < size {
        buf = make([]byte, size)
    } else {
        buf = buf[:size]
    }

    // Encode fixed header (64 bytes)
    pos := 0
    
    // Bytes 0-15: MessageID
    copy(buf[pos:], e.MessageID[:])
    pos += 16
    
    // Bytes 16-23: Timestamp
    binary.BigEndian.PutUint64(buf[pos:], uint64(e.Timestamp))
    pos += 8
    
    // Bytes 24-31: Sequence
    binary.BigEndian.PutUint64(buf[pos:], e.Sequence)
    pos += 8
    
    // Bytes 32-35: PartitionID
    binary.BigEndian.PutUint32(buf[pos:], e.PartitionID)
    pos += 4
    
    // Bytes 36-39: SchemaID
    binary.BigEndian.PutUint32(buf[pos:], e.SchemaID)
    pos += 4
    
    // Byte 40: Priority
    buf[pos] = e.Priority
    pos++
    
    // Byte 41: Encoding
    buf[pos] = byte(e.Encoding)
    pos++
    
    // Byte 42: SourceProto
    buf[pos] = byte(e.SourceProto)
    pos++
    
    // Byte 43: Flags
    var flags uint8
    if len(e.Headers) > 0 {
        flags |= FlagHasHeaders
    }
    if e.RoutingKey != "" {
        flags |= FlagHasRoutingKey
    }
    if e.TraceID != [16]byte{} {
        flags |= FlagHasTrace
    }
    if e.TTL > 0 {
        flags |= FlagHasTTL
    }
    if e.DeliverAt > 0 {
        flags |= FlagHasDelay
    }
    buf[pos] = flags
    pos++
    
    // Bytes 44-47: PayloadLength
    binary.BigEndian.PutUint32(buf[pos:], uint32(len(e.Payload)))
    pos += 4
    
    // Bytes 48-51: HeadersLength
    hdrBytes := marshalHeaders(e.Headers)
    binary.BigEndian.PutUint32(buf[pos:], uint32(len(hdrBytes)))
    pos += 4
    
    // Bytes 52-53: TopicLength (uint16)
    binary.BigEndian.PutUint16(buf[pos:], uint16(len(e.Topic)))
    pos += 2
    
    // Bytes 54-55: RoutingKeyLength (uint16)
    binary.BigEndian.PutUint16(buf[pos:], uint16(len(e.RoutingKey)))
    pos += 2
    
    // Bytes 56-63: TTL or DeliverAt or DeliverCount+MaxRetries
    if flags&FlagHasTTL != 0 {
        binary.BigEndian.PutUint64(buf[pos:], uint64(e.TTL))
    } else if flags&FlagHasDelay != 0 {
        binary.BigEndian.PutUint64(buf[pos:], uint64(e.DeliverAt))
    } else {
        binary.BigEndian.PutUint32(buf[pos:], e.DeliverCount)
        binary.BigEndian.PutUint32(buf[pos+4:], e.MaxRetries)
    }
    pos += 8
    
    // Variable fields
    // Topic
    copy(buf[pos:], e.Topic)
    pos += len(e.Topic)
    
    // RoutingKey (if present)
    if e.RoutingKey != "" {
        copy(buf[pos:], e.RoutingKey)
        pos += len(e.RoutingKey)
    }
    
    // Headers (if present)
    if len(hdrBytes) > 0 {
        copy(buf[pos:], hdrBytes)
        pos += len(hdrBytes)
    }
    
    // Trace (if present)
    if e.TraceID != [16]byte{} {
        copy(buf[pos:], e.TraceID[:])
        pos += 16
        copy(buf[pos:], e.SpanID[:])
        pos += 8
    }
    
    // Payload (always last)
    copy(buf[pos:], e.Payload)
    
    *bufPtr = buf
    return buf, nil
}

// Unmarshal deserializes binary data into an Envelope.
// Zero-copy for payload — references the input slice.
func Unmarshal(data []byte) (*Envelope, error) {
    if len(data) < FixedHeaderSize {
        return nil, fmt.Errorf("data too short: %d < %d", len(data), FixedHeaderSize)
    }
    
    e := &Envelope{}
    pos := 0
    
    // Fixed header decode (mirrors Marshal)
    copy(e.MessageID[:], data[pos:pos+16])
    pos += 16
    
    e.Timestamp = int64(binary.BigEndian.Uint64(data[pos:]))
    pos += 8
    
    e.Sequence = binary.BigEndian.Uint64(data[pos:])
    pos += 8
    
    e.PartitionID = binary.BigEndian.Uint32(data[pos:])
    pos += 4
    
    e.SchemaID = binary.BigEndian.Uint32(data[pos:])
    pos += 4
    
    e.Priority = data[pos]
    pos++
    
    e.Encoding = EncodingType(data[pos])
    pos++
    
    e.SourceProto = ProtocolType(data[pos])
    pos++
    
    flags := data[pos]
    pos++
    
    payloadLen := binary.BigEndian.Uint32(data[pos:])
    pos += 4
    
    headersLen := binary.BigEndian.Uint32(data[pos:])
    pos += 4
    
    topicLen := binary.BigEndian.Uint16(data[pos:])
    pos += 2
    
    rkLen := binary.BigEndian.Uint16(data[pos:])
    pos += 2
    
    // Bytes 56-63: conditional
    if flags&FlagHasTTL != 0 {
        e.TTL = int64(binary.BigEndian.Uint64(data[pos:]))
    } else if flags&FlagHasDelay != 0 {
        e.DeliverAt = int64(binary.BigEndian.Uint64(data[pos:]))
    } else {
        e.DeliverCount = binary.BigEndian.Uint32(data[pos:])
        e.MaxRetries = binary.BigEndian.Uint32(data[pos+4:])
    }
    pos += 8
    
    // Variable fields
    e.Topic = string(data[pos : pos+int(topicLen)])
    pos += int(topicLen)
    
    if rkLen > 0 {
        e.RoutingKey = string(data[pos : pos+int(rkLen)])
        pos += int(rkLen)
    }
    
    if headersLen > 0 {
        e.Headers = unmarshalHeaders(data[pos : pos+int(headersLen)])
        pos += int(headersLen)
    }
    
    if flags&FlagHasTrace != 0 {
        copy(e.TraceID[:], data[pos:pos+16])
        pos += 16
        copy(e.SpanID[:], data[pos:pos+8])
        pos += 8
    }
    
    // Payload — zero-copy reference to input slice
    e.Payload = data[pos : pos+int(payloadLen)]
    
    return e, nil
}
```

### 3.3 Header Encoding

Headers use a simple TLV (Type-Length-Value) encoding:

```go
// Header binary format (per header):
// [key_len: uint16][key: bytes][val_len: uint32][val: bytes]

func marshalHeaders(headers map[string][]byte) []byte {
    if len(headers) == 0 {
        return nil
    }
    
    size := 0
    for k, v := range headers {
        size += 2 + len(k) + 4 + len(v)
    }
    
    buf := make([]byte, size)
    pos := 0
    for k, v := range headers {
        binary.BigEndian.PutUint16(buf[pos:], uint16(len(k)))
        pos += 2
        copy(buf[pos:], k)
        pos += len(k)
        binary.BigEndian.PutUint32(buf[pos:], uint32(len(v)))
        pos += 4
        copy(buf[pos:], v)
        pos += len(v)
    }
    return buf
}

func unmarshalHeaders(data []byte) map[string][]byte {
    headers := make(map[string][]byte)
    pos := 0
    for pos < len(data) {
        if pos+2 > len(data) {
            break
        }
        keyLen := int(binary.BigEndian.Uint16(data[pos:]))
        pos += 2
        key := string(data[pos : pos+keyLen])
        pos += keyLen
        valLen := int(binary.BigEndian.Uint32(data[pos:]))
        pos += 4
        val := make([]byte, valLen)
        copy(val, data[pos:pos+valLen])
        pos += valLen
        headers[key] = val
    }
    return headers
}
```

### 3.4 Size Estimation

For pre-allocating buffers and capacity planning:

```go
func (e *Envelope) EstimateSize() int {
    size := FixedHeaderSize
    size += len(e.Topic)
    size += len(e.RoutingKey)
    size += len(e.Payload)
    for k, v := range e.Headers {
        size += 2 + len(k) + 4 + len(v)
    }
    if e.TraceID != [16]byte{} {
        size += 24
    }
    return size
}
```

---

## 4. UUIDv7 GENERATOR

### 4.1 Implementation

UUIDv7: time-sortable, 48-bit millisecond timestamp + 74-bit random.

```go
// internal/message/uuid.go

import (
    "crypto/rand"
    "encoding/binary"
    "sync"
    "time"
)

type UUIDv7Generator struct {
    mu       sync.Mutex
    lastMS   int64
    counter  uint16 // Monotonic counter within same millisecond
}

var defaultGenerator = &UUIDv7Generator{}

func NewUUIDv7() [16]byte {
    return defaultGenerator.Generate()
}

func (g *UUIDv7Generator) Generate() [16]byte {
    g.mu.Lock()
    defer g.mu.Unlock()
    
    var uuid [16]byte
    
    now := time.Now().UnixMilli()
    
    if now == g.lastMS {
        g.counter++
    } else {
        g.lastMS = now
        g.counter = 0
    }
    
    // Bytes 0-5: 48-bit timestamp (milliseconds)
    binary.BigEndian.PutUint16(uuid[0:], uint16(now>>32))
    binary.BigEndian.PutUint32(uuid[2:], uint32(now))
    
    // Bytes 6-7: version (7) + 12-bit counter/random
    binary.BigEndian.PutUint16(uuid[6:], g.counter)
    uuid[6] = (uuid[6] & 0x0F) | 0x70 // Version 7
    
    // Bytes 8-15: variant (10) + 62-bit random
    rand.Read(uuid[8:])
    uuid[8] = (uuid[8] & 0x3F) | 0x80 // Variant 10
    
    return uuid
}

// String formats UUID as standard hex string.
func UUIDString(uuid [16]byte) string {
    return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
        uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}
```

---

## 5. WRITE-AHEAD LOG (WAL)

### 5.1 WAL Architecture

The WAL guarantees durability before hot tier writes. Every message first goes to WAL, then to the hot segment.

```go
// internal/storage/wal/wal.go

const (
    WALMagic      = uint32(0x43574C31) // "CWL1" — Chimera WAL v1
    WALHeaderSize = 17                 // type(1) + size(4) + crc(4) + timestamp(8)
)

type EntryType uint8
const (
    EntryMessage    EntryType = 1 // Message append
    EntryTopicMeta  EntryType = 2 // Topic create/delete/modify
    EntryCheckpoint EntryType = 3 // Checkpoint marker
)

type WAL struct {
    mu           sync.Mutex
    dir          string
    activeFile   *os.File
    activeSize   int64
    maxSize      int64
    syncMode     SyncMode
    syncInterval time.Duration
    
    offset       uint64      // Global WAL byte offset
    segmentSeq   uint64      // Current segment sequence number
    
    writeBuf     *bufio.Writer
    
    // Fsync ticker (for interval mode)
    syncTicker   *time.Ticker
    syncStop     chan struct{}
}

type SyncMode uint8
const (
    SyncImmediate SyncMode = 0 // fsync every write
    SyncInterval  SyncMode = 1 // fsync on timer
    SyncOS        SyncMode = 2 // let OS decide
)
```

### 5.2 WAL Entry Format

```
┌───────────┬──────────┬──────────────┬───────────┬─────────────┐
│ Type (1B) │ Size(4B) │ Timestamp(8B)│ Data (var)│ CRC32C (4B) │
└───────────┴──────────┴──────────────┴───────────┴─────────────┘
```

### 5.3 WAL Operations

```go
func NewWAL(dir string, maxSize int64, syncMode SyncMode, syncInterval time.Duration) (*WAL, error) {
    if err := os.MkdirAll(dir, 0750); err != nil {
        return nil, err
    }
    
    w := &WAL{
        dir:          dir,
        maxSize:      maxSize,
        syncMode:     syncMode,
        syncInterval: syncInterval,
        syncStop:     make(chan struct{}),
    }
    
    // Find latest segment or create first one
    if err := w.openOrCreateSegment(); err != nil {
        return nil, err
    }
    
    // Start fsync ticker if interval mode
    if syncMode == SyncInterval {
        w.syncTicker = time.NewTicker(syncInterval)
        go w.syncLoop()
    }
    
    return w, nil
}

// Append writes an entry to the WAL. Returns the WAL offset.
func (w *WAL) Append(entryType EntryType, data []byte) (uint64, error) {
    w.mu.Lock()
    defer w.mu.Unlock()
    
    // Rotate if needed
    entrySize := int64(WALHeaderSize + len(data))
    if w.activeSize+entrySize > w.maxSize {
        if err := w.rotate(); err != nil {
            return 0, err
        }
    }
    
    // Write entry
    var header [WALHeaderSize]byte
    header[0] = byte(entryType)
    binary.BigEndian.PutUint32(header[1:], uint32(len(data)))
    binary.BigEndian.PutUint64(header[5:], uint64(time.Now().UnixNano()))
    
    // CRC covers type + size + timestamp + data
    crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
    crc.Write(header[:13])
    crc.Write(data)
    binary.BigEndian.PutUint32(header[13:], crc.Sum32())
    
    if _, err := w.writeBuf.Write(header[:]); err != nil {
        return 0, err
    }
    if _, err := w.writeBuf.Write(data); err != nil {
        return 0, err
    }
    
    offset := w.offset
    w.offset += uint64(entrySize)
    w.activeSize += entrySize
    
    // Immediate sync if configured
    if w.syncMode == SyncImmediate {
        if err := w.writeBuf.Flush(); err != nil {
            return 0, err
        }
        if err := w.activeFile.Sync(); err != nil {
            return 0, err
        }
    }
    
    return offset, nil
}

// Recover iterates all entries from a WAL offset for crash recovery.
func (w *WAL) Recover(fromOffset uint64, fn func(EntryType, []byte) error) error {
    segments, err := w.listSegments()
    if err != nil {
        return err
    }
    
    for _, seg := range segments {
        f, err := os.Open(seg)
        if err != nil {
            return err
        }
        reader := bufio.NewReader(f)
        
        for {
            var header [WALHeaderSize]byte
            if _, err := io.ReadFull(reader, header[:]); err != nil {
                if err == io.EOF {
                    break
                }
                // Partial write — truncate here (crash recovery)
                break
            }
            
            entryType := EntryType(header[0])
            dataSize := binary.BigEndian.Uint32(header[1:])
            storedCRC := binary.BigEndian.Uint32(header[13:])
            
            data := make([]byte, dataSize)
            if _, err := io.ReadFull(reader, data); err != nil {
                break // Partial entry
            }
            
            // Verify CRC
            crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
            crc.Write(header[:13])
            crc.Write(data)
            if crc.Sum32() != storedCRC {
                break // Corruption — stop here
            }
            
            if err := fn(entryType, data); err != nil {
                f.Close()
                return err
            }
        }
        f.Close()
    }
    return nil
}

// Checkpoint marks the WAL position as durable (hot storage has synced).
// Segments before checkpoint can be deleted.
func (w *WAL) Checkpoint(offset uint64) error {
    cpPath := filepath.Join(w.dir, "checkpoint")
    data := []byte(fmt.Sprintf("%d\n", offset))
    return os.WriteFile(cpPath, data, 0640)
}

// Truncate removes WAL segments that are fully before the checkpoint.
func (w *WAL) Truncate() error {
    cpOffset, err := w.readCheckpoint()
    if err != nil {
        return nil // No checkpoint yet
    }
    
    segments, err := w.listSegments()
    if err != nil {
        return err
    }
    
    // Remove segments fully before checkpoint
    // (keep at least one segment)
    for _, seg := range segments[:len(segments)-1] {
        segEnd := segmentEndOffset(seg)
        if segEnd <= cpOffset {
            os.Remove(seg)
        }
    }
    return nil
}

func (w *WAL) rotate() error {
    if err := w.writeBuf.Flush(); err != nil {
        return err
    }
    if err := w.activeFile.Sync(); err != nil {
        return err
    }
    w.activeFile.Close()
    
    w.segmentSeq++
    return w.openOrCreateSegment()
}

func (w *WAL) syncLoop() {
    for {
        select {
        case <-w.syncTicker.C:
            w.mu.Lock()
            w.writeBuf.Flush()
            w.activeFile.Sync()
            w.mu.Unlock()
        case <-w.syncStop:
            return
        }
    }
}

func (w *WAL) segmentPath(seq uint64) string {
    return filepath.Join(w.dir, fmt.Sprintf("%012d.wal", seq))
}
```

---

## 6. HOT TIER STORAGE ENGINE

### 6.1 Segment Structure

Each partition has a series of log segments. The active segment accepts writes; older segments are read-only.

```go
// internal/storage/hot/segment.go

const (
    SegmentMagic   = uint32(0x43534731) // "CSG1" — Chimera Segment v1
    SegmentHeaderLen = 32
)

type Segment struct {
    mu        sync.RWMutex
    file      *os.File
    path      string
    mmap      []byte         // Memory-mapped region (read path)
    size      int64          // Current written size
    maxSize   int64          // Max segment size (from config)
    baseOff   uint64         // First message offset in this segment
    nextOff   uint64         // Next offset to assign
    index     *SparseIndex   // In-memory offset→position index
    created   time.Time
    frozen    bool           // true = read-only
}

type SparseIndex struct {
    mu       sync.RWMutex
    entries  []IndexEntry
    interval uint32         // Index every N messages (default 256)
}

type IndexEntry struct {
    Offset    uint64 // Message offset
    Position  uint32 // Byte position in segment file
    Timestamp int64  // Message timestamp (for time-based seek)
}
```

### 6.2 Segment File Format

```
Segment file layout:
┌──────────────────────────────────────┐
│ File Header (32 bytes)               │
│  Magic (4B) + Version (4B) +         │
│  BaseOffset (8B) + Created (8B) +    │
│  Reserved (8B)                       │
├──────────────────────────────────────┤
│ Record 0                             │
│  RecordLen (4B) + Data (var)         │
├──────────────────────────────────────┤
│ Record 1                             │
│  RecordLen (4B) + Data (var)         │
├──────────────────────────────────────┤
│ ...                                  │
└──────────────────────────────────────┘

Each record:
┌────────────┬─────────────────┐
│ Length (4B) │ Envelope bytes  │
└────────────┴─────────────────┘
```

### 6.3 Segment Operations

```go
func OpenSegment(path string, baseOffset uint64, maxSize int64) (*Segment, error) {
    f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
    if err != nil {
        return nil, err
    }
    
    info, _ := f.Stat()
    seg := &Segment{
        file:    f,
        path:    path,
        size:    info.Size(),
        maxSize: maxSize,
        baseOff: baseOffset,
        nextOff: baseOffset,
        created: time.Now(),
        index: &SparseIndex{
            entries:  make([]IndexEntry, 0, 1024),
            interval: 256,
        },
    }
    
    if info.Size() == 0 {
        // New segment — write header
        if err := seg.writeHeader(); err != nil {
            f.Close()
            return nil, err
        }
    } else {
        // Existing segment — read header, rebuild index
        if err := seg.readHeader(); err != nil {
            f.Close()
            return nil, err
        }
        if err := seg.rebuildIndex(); err != nil {
            f.Close()
            return nil, err
        }
    }
    
    return seg, nil
}

// Append writes a serialized message envelope to the segment.
// Returns the assigned offset and byte position.
func (s *Segment) Append(data []byte) (offset uint64, position int64, err error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    recordSize := 4 + len(data) // length prefix + data
    
    if s.size+int64(recordSize) > s.maxSize {
        return 0, 0, ErrSegmentFull
    }
    
    position = s.size
    offset = s.nextOff
    
    // Write length prefix
    var lenBuf [4]byte
    binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
    if _, err := s.file.WriteAt(lenBuf[:], position); err != nil {
        return 0, 0, err
    }
    
    // Write data
    if _, err := s.file.WriteAt(data, position+4); err != nil {
        return 0, 0, err
    }
    
    s.size += int64(recordSize)
    s.nextOff++
    
    // Update sparse index
    msgCount := offset - s.baseOff
    if msgCount%uint64(s.index.interval) == 0 {
        s.index.mu.Lock()
        s.index.entries = append(s.index.entries, IndexEntry{
            Offset:   offset,
            Position: uint32(position),
            Timestamp: time.Now().UnixNano(),
        })
        s.index.mu.Unlock()
    }
    
    return offset, position, nil
}

// ReadAt reads a message at the given byte position.
func (s *Segment) ReadAt(position int64) ([]byte, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    // If mmap is available, use it (zero-copy read)
    if s.mmap != nil {
        return s.readFromMmap(position)
    }
    
    // Fallback to file read
    var lenBuf [4]byte
    if _, err := s.file.ReadAt(lenBuf[:], position); err != nil {
        return nil, err
    }
    dataLen := binary.BigEndian.Uint32(lenBuf[:])
    
    data := make([]byte, dataLen)
    if _, err := s.file.ReadAt(data, position+4); err != nil {
        return nil, err
    }
    return data, nil
}

// FindPosition locates the byte position for a given offset using sparse index.
func (s *Segment) FindPosition(targetOffset uint64) (int64, error) {
    s.index.mu.RLock()
    defer s.index.mu.RUnlock()
    
    if targetOffset < s.baseOff {
        return 0, ErrOffsetTooOld
    }
    
    // Binary search sparse index for nearest entry <= targetOffset
    entries := s.index.entries
    lo, hi := 0, len(entries)-1
    nearest := int64(SegmentHeaderLen) // Start of first record
    
    for lo <= hi {
        mid := (lo + hi) / 2
        if entries[mid].Offset == targetOffset {
            return int64(entries[mid].Position), nil
        }
        if entries[mid].Offset < targetOffset {
            nearest = int64(entries[mid].Position)
            lo = mid + 1
        } else {
            hi = mid - 1
        }
    }
    
    // Linear scan from nearest position to target
    pos := nearest
    currentOffset := s.baseOff
    if len(entries) > 0 && lo > 0 {
        currentOffset = entries[lo-1].Offset
    }
    
    for currentOffset < targetOffset {
        var lenBuf [4]byte
        if _, err := s.file.ReadAt(lenBuf[:], pos); err != nil {
            return 0, err
        }
        dataLen := binary.BigEndian.Uint32(lenBuf[:])
        pos += 4 + int64(dataLen)
        currentOffset++
    }
    
    return pos, nil
}

// Freeze marks the segment as read-only and creates mmap for zero-copy reads.
func (s *Segment) Freeze() error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    s.frozen = true
    
    // Sync to disk
    if err := s.file.Sync(); err != nil {
        return err
    }
    
    // Create mmap for zero-copy reads
    data, err := syscall.Mmap(
        int(s.file.Fd()), 0, int(s.size),
        syscall.PROT_READ, syscall.MAP_SHARED,
    )
    if err != nil {
        return err
    }
    s.mmap = data
    
    // Advise kernel for sequential reads
    syscall.Madvise(data, syscall.MADV_SEQUENTIAL)
    
    return nil
}

func (s *Segment) readFromMmap(position int64) ([]byte, error) {
    if position+4 > int64(len(s.mmap)) {
        return nil, ErrPositionOutOfBounds
    }
    dataLen := binary.BigEndian.Uint32(s.mmap[position:])
    start := position + 4
    end := start + int64(dataLen)
    if end > int64(len(s.mmap)) {
        return nil, ErrPositionOutOfBounds
    }
    // Return slice of mmap — zero-copy
    return s.mmap[start:end], nil
}

// Close unmaps and closes the segment file.
func (s *Segment) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    if s.mmap != nil {
        syscall.Munmap(s.mmap)
        s.mmap = nil
    }
    return s.file.Close()
}
```

### 6.4 Sparse Index Persistence

Sparse index saved alongside segment for fast recovery:

```go
// Index file format: binary array of IndexEntry
// Filename: {base_offset}.idx

func (s *Segment) SaveIndex() error {
    s.index.mu.RLock()
    defer s.index.mu.RUnlock()
    
    idxPath := strings.TrimSuffix(s.path, ".log") + ".idx"
    f, err := os.Create(idxPath)
    if err != nil {
        return err
    }
    defer f.Close()
    
    buf := make([]byte, 20) // 8 + 4 + 8 per entry
    for _, entry := range s.index.entries {
        binary.BigEndian.PutUint64(buf[0:], entry.Offset)
        binary.BigEndian.PutUint32(buf[8:], entry.Position)
        binary.BigEndian.PutUint64(buf[12:], uint64(entry.Timestamp))
        if _, err := f.Write(buf); err != nil {
            return err
        }
    }
    return f.Sync()
}

func (s *Segment) LoadIndex() error {
    idxPath := strings.TrimSuffix(s.path, ".log") + ".idx"
    data, err := os.ReadFile(idxPath)
    if err != nil {
        if os.IsNotExist(err) {
            return s.rebuildIndex()
        }
        return err
    }
    
    s.index.mu.Lock()
    defer s.index.mu.Unlock()
    
    entryCount := len(data) / 20
    s.index.entries = make([]IndexEntry, 0, entryCount)
    
    for i := 0; i < entryCount; i++ {
        pos := i * 20
        s.index.entries = append(s.index.entries, IndexEntry{
            Offset:    binary.BigEndian.Uint64(data[pos:]),
            Position:  binary.BigEndian.Uint32(data[pos+8:]),
            Timestamp: int64(binary.BigEndian.Uint64(data[pos+12:])),
        })
    }
    return nil
}
```

### 6.5 Partition Manager

Manages segments for a single partition:

```go
// internal/storage/hot/partition.go

type Partition struct {
    mu           sync.RWMutex
    topicName    string
    partitionID  uint32
    dir          string
    segments     []*Segment       // Ordered by baseOffset
    active       *Segment         // Current write segment
    highWater    uint64           // Highest committed offset
    logStart     uint64           // Earliest available offset
    maxSegSize   int64
}

func OpenPartition(dir string, topic string, id uint32, maxSegSize int64) (*Partition, error) {
    partDir := filepath.Join(dir, fmt.Sprintf("partition-%d", id))
    if err := os.MkdirAll(partDir, 0750); err != nil {
        return nil, err
    }
    
    p := &Partition{
        topicName:   topic,
        partitionID: id,
        dir:         partDir,
        segments:    make([]*Segment, 0),
        maxSegSize:  maxSegSize,
    }
    
    // Load existing segments
    if err := p.loadSegments(); err != nil {
        return nil, err
    }
    
    // Ensure there's an active segment
    if len(p.segments) == 0 {
        if err := p.createNewSegment(0); err != nil {
            return nil, err
        }
    }
    p.active = p.segments[len(p.segments)-1]
    p.highWater = p.active.nextOff - 1
    
    return p, nil
}

// Append writes a message to the partition. Returns assigned offset.
func (p *Partition) Append(data []byte) (uint64, error) {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    offset, _, err := p.active.Append(data)
    if err == ErrSegmentFull {
        // Roll to new segment
        p.active.Freeze()
        p.active.SaveIndex()
        
        newBase := p.active.nextOff
        if err := p.createNewSegment(newBase); err != nil {
            return 0, err
        }
        p.active = p.segments[len(p.segments)-1]
        
        offset, _, err = p.active.Append(data)
        if err != nil {
            return 0, err
        }
    } else if err != nil {
        return 0, err
    }
    
    p.highWater = offset
    return offset, nil
}

// Read reads a message at the given offset.
func (p *Partition) Read(offset uint64) ([]byte, error) {
    p.mu.RLock()
    defer p.mu.RUnlock()
    
    seg := p.findSegment(offset)
    if seg == nil {
        return nil, fmt.Errorf("offset %d not found", offset)
    }
    
    pos, err := seg.FindPosition(offset)
    if err != nil {
        return nil, err
    }
    
    return seg.ReadAt(pos)
}

// ReadRange reads messages from startOffset to endOffset (inclusive).
func (p *Partition) ReadRange(startOffset, endOffset uint64, maxMessages int) ([][]byte, error) {
    p.mu.RLock()
    defer p.mu.RUnlock()
    
    var results [][]byte
    
    for offset := startOffset; offset <= endOffset && len(results) < maxMessages; offset++ {
        data, err := p.Read(offset)
        if err != nil {
            break
        }
        results = append(results, data)
    }
    
    return results, nil
}

// findSegment returns the segment containing the given offset.
func (p *Partition) findSegment(offset uint64) *Segment {
    // Binary search segments by baseOffset
    lo, hi := 0, len(p.segments)-1
    for lo <= hi {
        mid := (lo + hi) / 2
        seg := p.segments[mid]
        if offset >= seg.baseOff && offset < seg.nextOff {
            return seg
        }
        if offset < seg.baseOff {
            hi = mid - 1
        } else {
            lo = mid + 1
        }
    }
    return nil
}

func (p *Partition) createNewSegment(baseOffset uint64) error {
    name := fmt.Sprintf("%020d.log", baseOffset)
    path := filepath.Join(p.dir, name)
    seg, err := OpenSegment(path, baseOffset, p.maxSegSize)
    if err != nil {
        return err
    }
    p.segments = append(p.segments, seg)
    return nil
}

func (p *Partition) HighWatermark() uint64 {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.highWater
}

func (p *Partition) LogStartOffset() uint64 {
    p.mu.RLock()
    defer p.mu.RUnlock()
    if len(p.segments) == 0 {
        return 0
    }
    return p.segments[0].baseOff
}
```

### 6.6 Storage Engine

Top-level engine managing all partitions:

```go
// internal/storage/hot/engine.go

type Engine struct {
    mu         sync.RWMutex
    baseDir    string
    partitions map[string]map[uint32]*Partition // topic → partitionID → Partition
    config     HotConfig
}

func NewEngine(baseDir string, cfg HotConfig) *Engine {
    return &Engine{
        baseDir:    baseDir,
        partitions: make(map[string]map[uint32]*Partition),
        config:     cfg,
    }
}

func (e *Engine) GetOrCreatePartition(topic string, partID uint32) (*Partition, error) {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    topicParts, ok := e.partitions[topic]
    if !ok {
        topicParts = make(map[uint32]*Partition)
        e.partitions[topic] = topicParts
    }
    
    part, ok := topicParts[partID]
    if !ok {
        dir := filepath.Join(e.baseDir, "topics", topic)
        var err error
        part, err = OpenPartition(dir, topic, partID, e.config.SegmentSize)
        if err != nil {
            return nil, err
        }
        topicParts[partID] = part
    }
    
    return part, nil
}

func (e *Engine) Close() error {
    e.mu.Lock()
    defer e.mu.Unlock()
    
    for _, topicParts := range e.partitions {
        for _, part := range topicParts {
            part.Close()
        }
    }
    return nil
}
```

---

## 7. TOPIC MANAGER

### 7.1 Topic Metadata

```go
// internal/broker/topic.go

type TopicMode uint8
const (
    ModeStream  TopicMode = 0
    ModeQueue   TopicMode = 1
    ModeUnified TopicMode = 2
)

type TopicConfig struct {
    Name          string        `json:"name"`
    Mode          TopicMode     `json:"mode"`
    Partitions    uint32        `json:"partitions"`
    RetentionTime time.Duration `json:"retention_time"`
    RetentionSize int64         `json:"retention_size"`
    DLQTopic      string        `json:"dlq_topic,omitempty"`
    MaxRetries    uint32        `json:"max_retries"`
    DelaySupport  bool          `json:"delay_support"`
    CreatedAt     time.Time     `json:"created_at"`
}

type TopicManager struct {
    mu       sync.RWMutex
    topics   map[string]*TopicConfig
    metaPath string         // Path to meta.json
    storage  *hot.Engine
    wal      *wal.WAL
}

func NewTopicManager(dataDir string, storage *hot.Engine, wal *wal.WAL) (*TopicManager, error) {
    tm := &TopicManager{
        topics:   make(map[string]*TopicConfig),
        metaPath: filepath.Join(dataDir, "topics", "meta.json"),
        storage:  storage,
        wal:      wal,
    }
    
    // Load existing topic metadata
    if err := tm.loadMetadata(); err != nil && !os.IsNotExist(err) {
        return nil, err
    }
    
    // Initialize partitions for each topic
    for _, topic := range tm.topics {
        for i := uint32(0); i < topic.Partitions; i++ {
            if _, err := storage.GetOrCreatePartition(topic.Name, i); err != nil {
                return nil, fmt.Errorf("init partition %s/%d: %w", topic.Name, i, err)
            }
        }
    }
    
    return tm, nil
}

func (tm *TopicManager) CreateTopic(cfg TopicConfig) error {
    tm.mu.Lock()
    defer tm.mu.Unlock()
    
    if _, exists := tm.topics[cfg.Name]; exists {
        return fmt.Errorf("topic %q already exists", cfg.Name)
    }
    
    // Validate
    if err := validateTopicName(cfg.Name); err != nil {
        return err
    }
    if cfg.Partitions == 0 {
        return fmt.Errorf("partitions must be > 0")
    }
    
    cfg.CreatedAt = time.Now()
    
    // WAL entry for crash safety
    data, _ := json.Marshal(cfg)
    if _, err := tm.wal.Append(wal.EntryTopicMeta, data); err != nil {
        return err
    }
    
    // Create partitions
    for i := uint32(0); i < cfg.Partitions; i++ {
        if _, err := tm.storage.GetOrCreatePartition(cfg.Name, i); err != nil {
            return err
        }
    }
    
    tm.topics[cfg.Name] = &cfg
    return tm.saveMetadata()
}

func (tm *TopicManager) DeleteTopic(name string) error {
    tm.mu.Lock()
    defer tm.mu.Unlock()
    
    if _, exists := tm.topics[name]; !exists {
        return fmt.Errorf("topic %q not found", name)
    }
    
    delete(tm.topics, name)
    // Note: Physical file cleanup is async (background goroutine)
    return tm.saveMetadata()
}

func (tm *TopicManager) GetTopic(name string) (*TopicConfig, bool) {
    tm.mu.RLock()
    defer tm.mu.RUnlock()
    cfg, ok := tm.topics[name]
    return cfg, ok
}

func (tm *TopicManager) ListTopics() []*TopicConfig {
    tm.mu.RLock()
    defer tm.mu.RUnlock()
    result := make([]*TopicConfig, 0, len(tm.topics))
    for _, cfg := range tm.topics {
        result = append(result, cfg)
    }
    return result
}

// Partition key routing
func (tm *TopicManager) ResolvePartition(topic string, routingKey string, partitionCount uint32) uint32 {
    if routingKey == "" {
        // Round-robin (using atomic counter per topic)
        return tm.roundRobinPartition(topic, partitionCount)
    }
    // Murmur3 hash of routing key
    h := murmur3Hash([]byte(routingKey))
    return h % partitionCount
}

func validateTopicName(name string) error {
    if len(name) == 0 || len(name) > 255 {
        return fmt.Errorf("topic name must be 1-255 characters")
    }
    for _, c := range name {
        if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_') {
            return fmt.Errorf("topic name contains invalid character: %c", c)
        }
    }
    if name[0] == '.' || name[0] == '-' {
        return fmt.Errorf("topic name cannot start with '.' or '-'")
    }
    return nil
}
```

### 7.2 Murmur3 Hash (Partition Routing)

Pure Go murmur3 — no external dependency:

```go
// internal/broker/murmur3.go

func murmur3Hash(data []byte) uint32 {
    const (
        c1 = 0xcc9e2d51
        c2 = 0x1b873593
    )
    
    h := uint32(0) // seed
    length := len(data)
    nblocks := length / 4
    
    for i := 0; i < nblocks; i++ {
        k := binary.LittleEndian.Uint32(data[i*4:])
        k *= c1
        k = (k << 15) | (k >> 17)
        k *= c2
        h ^= k
        h = (h << 13) | (h >> 19)
        h = h*5 + 0xe6546b64
    }
    
    tail := data[nblocks*4:]
    var k uint32
    switch len(tail) {
    case 3:
        k ^= uint32(tail[2]) << 16
        fallthrough
    case 2:
        k ^= uint32(tail[1]) << 8
        fallthrough
    case 1:
        k ^= uint32(tail[0])
        k *= c1
        k = (k << 15) | (k >> 17)
        k *= c2
        h ^= k
    }
    
    h ^= uint32(length)
    h ^= h >> 16
    h *= 0x85ebca6b
    h ^= h >> 13
    h *= 0xc2b2ae35
    h ^= h >> 16
    
    return h
}
```

---

## 8. QUEUE ENGINE (Lion Head)

### 8.1 Queue Engine Structure

```go
// internal/engine/queue/engine.go

type Engine struct {
    mu         sync.RWMutex
    queues     map[string]*QueueState // topic → queue state
    storage    *hot.Engine
    topics     *broker.TopicManager
    metrics    *metrics.Collector
}

type QueueState struct {
    mu          sync.Mutex
    topicName   string
    consumers   map[string]*QueueConsumer
    dispatcher  *Dispatcher
    ackTracker  *AckTracker
    dlqManager  *DLQManager
    delayHeap   *DelayScheduler
}

type QueueConsumer struct {
    ID         string
    conn       *ClientConnection
    prefetch   int
    inFlight   map[uint64]time.Time // offset → delivery time
    ackBitmap  *Bitmap              // Custom roaring bitmap (pure Go)
    mu         sync.Mutex
}
```

### 8.2 Dispatcher

The dispatcher assigns messages to consumers in round-robin order with prefetch awareness:

```go
// internal/engine/queue/dispatcher.go

type Dispatcher struct {
    mu            sync.Mutex
    consumers     []*QueueConsumer
    nextIdx       int
    visTimeout    time.Duration   // Default 30s
    maxRetries    uint32
}

// Dispatch assigns a message to the next available consumer.
// Returns the consumer ID, or error if no consumer has capacity.
func (d *Dispatcher) Dispatch(offset uint64, envelope *message.Envelope) (string, error) {
    d.mu.Lock()
    defer d.mu.Unlock()
    
    if len(d.consumers) == 0 {
        return "", ErrNoConsumers
    }
    
    // Round-robin with capacity check
    checked := 0
    for checked < len(d.consumers) {
        consumer := d.consumers[d.nextIdx]
        d.nextIdx = (d.nextIdx + 1) % len(d.consumers)
        checked++
        
        consumer.mu.Lock()
        if len(consumer.inFlight) < consumer.prefetch {
            consumer.inFlight[offset] = time.Now()
            consumer.mu.Unlock()
            return consumer.ID, nil
        }
        consumer.mu.Unlock()
    }
    
    return "", ErrAllConsumersBusy
}

// AddConsumer registers a new queue consumer.
func (d *Dispatcher) AddConsumer(c *QueueConsumer) {
    d.mu.Lock()
    defer d.mu.Unlock()
    d.consumers = append(d.consumers, c)
}

// RemoveConsumer unregisters a consumer. In-flight messages are requeued.
func (d *Dispatcher) RemoveConsumer(id string) {
    d.mu.Lock()
    defer d.mu.Unlock()
    
    for i, c := range d.consumers {
        if c.ID == id {
            d.consumers = append(d.consumers[:i], d.consumers[i+1:]...)
            if d.nextIdx >= len(d.consumers) && len(d.consumers) > 0 {
                d.nextIdx = 0
            }
            // Return in-flight offsets for redelivery
            c.mu.Lock()
            for offset := range c.inFlight {
                delete(c.inFlight, offset)
                // These will be re-dispatched by the visibility timeout checker
            }
            c.mu.Unlock()
            return
        }
    }
}
```

### 8.3 Ack Tracker

Tracks acknowledgment state for queue consumers:

```go
// internal/engine/queue/ack.go

type AckTracker struct {
    mu             sync.Mutex
    pending        map[uint64]*PendingMsg // offset → pending info
    visTimeout     time.Duration
    redeliverChan  chan uint64            // Offsets to redeliver
    stopChan       chan struct{}
}

type PendingMsg struct {
    Offset       uint64
    ConsumerID   string
    DeliveredAt  time.Time
    DeliverCount uint32
    MaxRetries   uint32
}

func NewAckTracker(visTimeout time.Duration) *AckTracker {
    at := &AckTracker{
        pending:       make(map[uint64]*PendingMsg),
        visTimeout:    visTimeout,
        redeliverChan: make(chan uint64, 10000),
        stopChan:      make(chan struct{}),
    }
    go at.visibilityTimeoutLoop()
    return at
}

// Track adds a message to the pending ack set.
func (at *AckTracker) Track(offset uint64, consumerID string, deliverCount, maxRetries uint32) {
    at.mu.Lock()
    defer at.mu.Unlock()
    at.pending[offset] = &PendingMsg{
        Offset:       offset,
        ConsumerID:   consumerID,
        DeliveredAt:  time.Now(),
        DeliverCount: deliverCount,
        MaxRetries:   maxRetries,
    }
}

// Ack acknowledges a message. Returns true if the offset was pending.
func (at *AckTracker) Ack(offset uint64) bool {
    at.mu.Lock()
    defer at.mu.Unlock()
    _, ok := at.pending[offset]
    if ok {
        delete(at.pending, offset)
    }
    return ok
}

// Nack negative-acknowledges a message for redelivery.
// Returns (shouldDLQ, deliverCount).
func (at *AckTracker) Nack(offset uint64) (bool, uint32) {
    at.mu.Lock()
    defer at.mu.Unlock()
    
    pending, ok := at.pending[offset]
    if !ok {
        return false, 0
    }
    
    pending.DeliverCount++
    delete(at.pending, offset)
    
    if pending.MaxRetries > 0 && pending.DeliverCount >= pending.MaxRetries {
        return true, pending.DeliverCount // Route to DLQ
    }
    
    // Requeue for redelivery
    at.redeliverChan <- offset
    return false, pending.DeliverCount
}

// visibilityTimeoutLoop checks for expired in-flight messages.
func (at *AckTracker) visibilityTimeoutLoop() {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            at.mu.Lock()
            now := time.Now()
            for offset, pending := range at.pending {
                if now.Sub(pending.DeliveredAt) > at.visTimeout {
                    delete(at.pending, offset)
                    at.redeliverChan <- offset
                }
            }
            at.mu.Unlock()
        case <-at.stopChan:
            return
        }
    }
}
```

### 8.4 Dead-Letter Queue Manager

```go
// internal/engine/queue/dlq.go

type DLQManager struct {
    dlqTopic string
    storage  *hot.Engine
    topics   *broker.TopicManager
}

func (dm *DLQManager) Route(original *message.Envelope, reason string, deliverCount uint32) error {
    if dm.dlqTopic == "" {
        return nil // DLQ disabled
    }
    
    // Clone envelope with DLQ headers
    dlqEnv := *original
    if dlqEnv.Headers == nil {
        dlqEnv.Headers = make(map[string][]byte)
    }
    dlqEnv.Headers["x-chimera-original-topic"] = []byte(original.Topic)
    dlqEnv.Headers["x-chimera-death-reason"] = []byte(reason)
    dlqEnv.Headers["x-chimera-death-count"] = []byte(fmt.Sprintf("%d", deliverCount))
    dlqEnv.Headers["x-chimera-first-death-time"] = []byte(time.Now().Format(time.RFC3339Nano))
    if original.RoutingKey != "" {
        dlqEnv.Headers["x-chimera-original-routing-key"] = []byte(original.RoutingKey)
    }
    
    dlqEnv.Topic = dm.dlqTopic
    dlqEnv.MessageID = message.NewUUIDv7()
    dlqEnv.Timestamp = time.Now().UnixNano()
    
    // Serialize and write to DLQ topic
    data, err := message.Marshal(&dlqEnv)
    if err != nil {
        return err
    }
    
    topicCfg, ok := dm.topics.GetTopic(dm.dlqTopic)
    if !ok {
        return fmt.Errorf("DLQ topic %q not found", dm.dlqTopic)
    }
    
    partID := dm.topics.ResolvePartition(dm.dlqTopic, "", topicCfg.Partitions)
    part, err := dm.storage.GetOrCreatePartition(dm.dlqTopic, partID)
    if err != nil {
        return err
    }
    
    _, err = part.Append(data)
    return err
}
```

### 8.5 Delay Scheduler

```go
// internal/engine/queue/delay.go

type DelayScheduler struct {
    mu       sync.Mutex
    heap     delayHeap
    ticker   *time.Ticker
    readyCh  chan *message.Envelope
    stopCh   chan struct{}
}

type delayedMsg struct {
    deliverAt time.Time
    envelope  *message.Envelope
    index     int // heap index
}

// Min-heap implementation
type delayHeap []*delayedMsg

func (h delayHeap) Len() int            { return len(h) }
func (h delayHeap) Less(i, j int) bool   { return h[i].deliverAt.Before(h[j].deliverAt) }
func (h delayHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *delayHeap) Push(x interface{})  { item := x.(*delayedMsg); item.index = len(*h); *h = append(*h, item) }
func (h *delayHeap) Pop() interface{} {
    old := *h
    n := len(old)
    item := old[n-1]
    old[n-1] = nil
    item.index = -1
    *h = old[:n-1]
    return item
}

func NewDelayScheduler() *DelayScheduler {
    ds := &DelayScheduler{
        heap:    make(delayHeap, 0),
        ticker:  time.NewTicker(100 * time.Millisecond),
        readyCh: make(chan *message.Envelope, 10000),
        stopCh:  make(chan struct{}),
    }
    go ds.promotionLoop()
    return ds
}

func (ds *DelayScheduler) Schedule(env *message.Envelope) {
    ds.mu.Lock()
    defer ds.mu.Unlock()
    
    deliverAt := time.Unix(0, env.DeliverAt)
    heap.Push(&ds.heap, &delayedMsg{
        deliverAt: deliverAt,
        envelope:  env,
    })
}

func (ds *DelayScheduler) promotionLoop() {
    for {
        select {
        case <-ds.ticker.C:
            ds.mu.Lock()
            now := time.Now()
            for ds.heap.Len() > 0 && ds.heap[0].deliverAt.Before(now) {
                item := heap.Pop(&ds.heap).(*delayedMsg)
                ds.readyCh <- item.envelope
            }
            ds.mu.Unlock()
        case <-ds.stopCh:
            return
        }
    }
}

// Ready returns the channel that receives messages ready for delivery.
func (ds *DelayScheduler) Ready() <-chan *message.Envelope {
    return ds.readyCh
}
```

---

## 9. STREAM ENGINE (Goat Head)

### 9.1 Consumer Group

```go
// internal/engine/stream/consumer_group.go

type ConsumerGroup struct {
    mu           sync.RWMutex
    name         string
    topic        string
    members      map[string]*GroupMember
    assignments  map[uint32]string        // partitionID → memberID
    committed    map[uint32]uint64        // partitionID → committed offset
    strategy     RebalanceStrategy
    sessionTimeout time.Duration
    
    rebalanceCh  chan struct{}
    storage      *hot.Engine
    offsetStore  *OffsetStore
}

type GroupMember struct {
    ID            string
    conn          *ClientConnection
    partitions    []uint32
    lastHeartbeat time.Time
    fetchCh       chan *FetchResponse
}

type RebalanceStrategy uint8
const (
    StrategyRange      RebalanceStrategy = 0
    StrategyRoundRobin RebalanceStrategy = 1
    StrategySticky     RebalanceStrategy = 2
)

type FetchResponse struct {
    PartitionID uint32
    Messages    []*message.Envelope
    HighWater   uint64
}
```

### 9.2 Consumer Group Operations

```go
func NewConsumerGroup(name, topic string, strategy RebalanceStrategy, storage *hot.Engine, offsetStore *OffsetStore) *ConsumerGroup {
    cg := &ConsumerGroup{
        name:           name,
        topic:          topic,
        members:        make(map[string]*GroupMember),
        assignments:    make(map[uint32]string),
        committed:      make(map[uint32]uint64),
        strategy:       strategy,
        sessionTimeout: 30 * time.Second,
        rebalanceCh:    make(chan struct{}, 1),
        storage:        storage,
        offsetStore:    offsetStore,
    }
    
    // Load committed offsets
    cg.loadCommittedOffsets()
    
    // Start heartbeat checker
    go cg.heartbeatLoop()
    
    return cg
}

// Join adds a member to the group and triggers rebalance.
func (cg *ConsumerGroup) Join(memberID string, conn *ClientConnection) {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    cg.members[memberID] = &GroupMember{
        ID:            memberID,
        conn:          conn,
        lastHeartbeat: time.Now(),
        fetchCh:       make(chan *FetchResponse, 100),
    }
    
    cg.triggerRebalance()
}

// Leave removes a member and triggers rebalance.
func (cg *ConsumerGroup) Leave(memberID string) {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    delete(cg.members, memberID)
    cg.triggerRebalance()
}

// Heartbeat updates the member's last heartbeat time.
func (cg *ConsumerGroup) Heartbeat(memberID string) error {
    cg.mu.Lock()
    defer cg.mu.Unlock()
    
    member, ok := cg.members[memberID]
    if !ok {
        return fmt.Errorf("member %q not in group", memberID)
    }
    member.lastHeartbeat = time.Now()
    return nil
}

// CommitOffset persists the consumer's offset for a partition.
func (cg *ConsumerGroup) CommitOffset(partitionID uint32, offset uint64) error {
    cg.mu.Lock()
    cg.committed[partitionID] = offset
    cg.mu.Unlock()
    
    return cg.offsetStore.Save(cg.name, partitionID, offset)
}

// GetCommittedOffset returns the last committed offset for a partition.
func (cg *ConsumerGroup) GetCommittedOffset(partitionID uint32) uint64 {
    cg.mu.RLock()
    defer cg.mu.RUnlock()
    return cg.committed[partitionID]
}

func (cg *ConsumerGroup) triggerRebalance() {
    select {
    case cg.rebalanceCh <- struct{}{}:
    default:
    }
    cg.rebalance()
}
```

### 9.3 Rebalancing Algorithms

```go
// internal/engine/stream/rebalance.go

func (cg *ConsumerGroup) rebalance() {
    // Clear current assignments
    for k := range cg.assignments {
        delete(cg.assignments, k)
    }
    for _, member := range cg.members {
        member.partitions = nil
    }
    
    if len(cg.members) == 0 {
        return
    }
    
    topicCfg, ok := cg.topicManager.GetTopic(cg.topic)
    if !ok {
        return
    }
    
    partitions := make([]uint32, topicCfg.Partitions)
    for i := uint32(0); i < topicCfg.Partitions; i++ {
        partitions[i] = i
    }
    
    memberIDs := make([]string, 0, len(cg.members))
    for id := range cg.members {
        memberIDs = append(memberIDs, id)
    }
    sort.Strings(memberIDs) // Deterministic ordering
    
    switch cg.strategy {
    case StrategyRange:
        cg.rebalanceRange(partitions, memberIDs)
    case StrategyRoundRobin:
        cg.rebalanceRoundRobin(partitions, memberIDs)
    case StrategySticky:
        cg.rebalanceSticky(partitions, memberIDs)
    }
}

// Range: consecutive partition ranges per member
func (cg *ConsumerGroup) rebalanceRange(partitions []uint32, members []string) {
    n := len(partitions)
    m := len(members)
    perMember := n / m
    remainder := n % m
    
    idx := 0
    for i, memberID := range members {
        count := perMember
        if i < remainder {
            count++
        }
        for j := 0; j < count && idx < n; j++ {
            cg.assignments[partitions[idx]] = memberID
            cg.members[memberID].partitions = append(cg.members[memberID].partitions, partitions[idx])
            idx++
        }
    }
}

// RoundRobin: distribute partitions evenly
func (cg *ConsumerGroup) rebalanceRoundRobin(partitions []uint32, members []string) {
    for i, partID := range partitions {
        memberID := members[i%len(members)]
        cg.assignments[partID] = memberID
        cg.members[memberID].partitions = append(cg.members[memberID].partitions, partID)
    }
}

// Sticky: minimize movement from previous assignment
func (cg *ConsumerGroup) rebalanceSticky(partitions []uint32, members []string) {
    // Start with round-robin as base
    // (Full sticky implementation tracks previous assignments and minimizes changes)
    cg.rebalanceRoundRobin(partitions, members)
}
```

### 9.4 Offset Store

```go
// internal/engine/stream/offset.go

type OffsetStore struct {
    mu      sync.RWMutex
    dir     string
    cache   map[string]map[uint32]uint64 // group → partition → offset
}

func NewOffsetStore(dataDir string) *OffsetStore {
    dir := filepath.Join(dataDir, "consumers")
    os.MkdirAll(dir, 0750)
    
    store := &OffsetStore{
        dir:   dir,
        cache: make(map[string]map[uint32]uint64),
    }
    store.loadAll()
    return store
}

func (os *OffsetStore) Save(group string, partitionID uint32, offset uint64) error {
    os.mu.Lock()
    defer os.mu.Unlock()
    
    if os.cache[group] == nil {
        os.cache[group] = make(map[uint32]uint64)
    }
    os.cache[group][partitionID] = offset
    
    return os.persist(group)
}

func (os *OffsetStore) Get(group string, partitionID uint32) uint64 {
    os.mu.RLock()
    defer os.mu.RUnlock()
    
    if g, ok := os.cache[group]; ok {
        return g[partitionID]
    }
    return 0
}

func (os *OffsetStore) persist(group string) error {
    path := filepath.Join(os.dir, group, "offsets.json")
    osutil.MkdirAll(filepath.Dir(path), 0750)
    data, _ := json.Marshal(os.cache[group])
    return osutil.WriteFile(path, data, 0640)
}
```

### 9.5 Fetch Loop

Stream consumers use a long-poll fetch loop:

```go
// Fetch returns messages starting from the given offset.
// If no messages are available, blocks up to maxWait.
func (se *StreamEngine) Fetch(topic string, partitionID uint32, fromOffset uint64, maxMessages int, maxWait time.Duration) ([]*message.Envelope, uint64, error) {
    part, err := se.storage.GetOrCreatePartition(topic, partitionID)
    if err != nil {
        return nil, 0, err
    }
    
    hw := part.HighWatermark()
    
    // If data available, return immediately
    if fromOffset <= hw {
        return se.readMessages(part, fromOffset, hw, maxMessages)
    }
    
    // Long-poll: wait for new data or timeout
    waitCh := se.registerWaiter(topic, partitionID)
    defer se.unregisterWaiter(topic, partitionID, waitCh)
    
    select {
    case <-waitCh:
        hw = part.HighWatermark()
        return se.readMessages(part, fromOffset, hw, maxMessages)
    case <-time.After(maxWait):
        return nil, fromOffset, nil
    }
}

func (se *StreamEngine) readMessages(part *hot.Partition, from, to uint64, max int) ([]*message.Envelope, uint64, error) {
    var msgs []*message.Envelope
    
    end := to
    if from+uint64(max) < end {
        end = from + uint64(max) - 1
    }
    
    for offset := from; offset <= end; offset++ {
        data, err := part.Read(offset)
        if err != nil {
            break
        }
        env, err := message.Unmarshal(data)
        if err != nil {
            continue // Skip corrupt messages
        }
        env.Sequence = offset
        msgs = append(msgs, env)
    }
    
    nextOffset := from + uint64(len(msgs))
    return msgs, nextOffset, nil
}
```

### 9.6 Waiter Registry (Notification on New Messages)

```go
// internal/engine/stream/waiter.go

type WaiterRegistry struct {
    mu      sync.RWMutex
    waiters map[string]map[uint32][]chan struct{} // topic → partition → channels
}

func (wr *WaiterRegistry) Register(topic string, partID uint32) chan struct{} {
    wr.mu.Lock()
    defer wr.mu.Unlock()
    
    ch := make(chan struct{}, 1)
    if wr.waiters[topic] == nil {
        wr.waiters[topic] = make(map[uint32][]chan struct{})
    }
    wr.waiters[topic][partID] = append(wr.waiters[topic][partID], ch)
    return ch
}

// Notify wakes up all waiters for a topic/partition (called after message append).
func (wr *WaiterRegistry) Notify(topic string, partID uint32) {
    wr.mu.RLock()
    defer wr.mu.RUnlock()
    
    if topicWaiters, ok := wr.waiters[topic]; ok {
        if channels, ok := topicWaiters[partID]; ok {
            for _, ch := range channels {
                select {
                case ch <- struct{}{}:
                default:
                }
            }
        }
    }
}
```

---

## 10. UNIFIED MODE

### 10.1 Dual Consumption

Unified mode is the key differentiator. Same underlying data, two consumption patterns:

```go
// internal/broker/publish.go

// Publish handles message ingestion for all topic modes.
func (b *Broker) Publish(env *message.Envelope) (uint64, error) {
    topicCfg, ok := b.topics.GetTopic(env.Topic)
    if !ok {
        return 0, fmt.Errorf("topic %q not found", env.Topic)
    }
    
    // Resolve partition
    partID := b.topics.ResolvePartition(env.Topic, env.RoutingKey, topicCfg.Partitions)
    env.PartitionID = partID
    
    // Check for delayed message
    if env.DeliverAt > 0 && time.Unix(0, env.DeliverAt).After(time.Now()) {
        if topicCfg.Mode == ModeQueue || topicCfg.Mode == ModeUnified {
            b.queueEngine.ScheduleDelayed(env)
            return 0, nil // Will be written to storage when delay expires
        }
    }
    
    // Assign sequence
    env.Sequence = 0 // Will be assigned by partition
    env.MessageID = message.NewUUIDv7()
    if env.Timestamp == 0 {
        env.Timestamp = time.Now().UnixNano()
    }
    
    // Serialize
    data, err := message.Marshal(env)
    if err != nil {
        return 0, err
    }
    
    // WAL first
    if _, err := b.wal.Append(wal.EntryMessage, data); err != nil {
        return 0, fmt.Errorf("WAL append: %w", err)
    }
    
    // Hot storage
    part, err := b.storage.GetOrCreatePartition(env.Topic, partID)
    if err != nil {
        return 0, err
    }
    
    offset, err := part.Append(data)
    if err != nil {
        return 0, err
    }
    
    env.Sequence = offset
    
    // Notify stream waiters (for long-poll fetch)
    b.streamEngine.NotifyWaiters(env.Topic, partID)
    
    // Dispatch to queue consumers (if queue or unified mode)
    if topicCfg.Mode == ModeQueue || topicCfg.Mode == ModeUnified {
        b.queueEngine.TryDispatch(env.Topic, partID, offset, env)
    }
    
    // Update metrics
    b.metrics.MessageIn(env.Topic, partID, env.SourceProto)
    
    return offset, nil
}
```

### 10.2 Unified Mode Behavior Summary

```
              PUBLISH
                │
                ▼
         ┌─────────────┐
         │  WAL Append  │
         └──────┬───────┘
                │
                ▼
         ┌─────────────────┐
         │  Hot Segment     │  ← Single copy of data
         │  Append          │
         └──────┬───────────┘
                │
         ┌──────┴──────────────────────────┐
         │                                  │
         ▼                                  ▼
  ┌──────────────┐                ┌──────────────────┐
  │ Stream Path   │                │  Queue Path       │
  │               │                │                    │
  │ - Notify      │                │ - Find consumer    │
  │   waiters     │                │   with capacity    │
  │ - Consumer    │                │ - Mark in-flight   │
  │   fetches by  │                │ - Send to consumer │
  │   offset      │                │ - Wait for ACK     │
  │ - Commits     │                │ - Redeliver on     │
  │   offset      │                │   NACK/timeout     │
  └──────────────┘                └──────────────────┘
```

**Key insight:** There is no data duplication. Stream consumers and queue consumers read from the exact same hot segments. The only difference is the consumption cursor:
- Stream consumers track their position via committed offsets (per consumer group)
- Queue consumers track their position via the ack bitmap (per individual consumer)

---

## 11. CHIMERA NATIVE PROTOCOL

### 11.1 Frame Format

```go
// internal/protocol/chimera/codec.go

const (
    FrameMagic   = [4]byte{'C', 'H', 'M', 'R'}
    FrameVersion = uint8(1)
    FrameHeaderLen = 11 // magic(4) + version(1) + opcode(1) + flags(1) + length(4)
    FrameTrailerLen = 4 // CRC32C
)

type OpCode uint8
const (
    OpConnect       OpCode = 0x01
    OpConnAck       OpCode = 0x02
    OpPublish       OpCode = 0x03
    OpPubAck        OpCode = 0x04
    OpSubscribe     OpCode = 0x05
    OpSubAck        OpCode = 0x06
    OpUnsubscribe   OpCode = 0x07
    OpUnsubAck      OpCode = 0x08
    OpFetch         OpCode = 0x09
    OpFetchResp     OpCode = 0x0A
    OpAck           OpCode = 0x0B
    OpNack          OpCode = 0x0C
    OpSeek          OpCode = 0x0D
    OpSeekAck       OpCode = 0x0E
    OpPing          OpCode = 0x0F
    OpPong          OpCode = 0x10
    OpCreateTopic   OpCode = 0x11
    OpDeleteTopic   OpCode = 0x12
    OpBatchPublish  OpCode = 0x13
    OpBatchPubAck   OpCode = 0x14
    OpCommitOffset  OpCode = 0x17
    OpCommitAck     OpCode = 0x18
    OpDisconnect    OpCode = 0x19
    OpError         OpCode = 0x1A
)

type Frame struct {
    Version  uint8
    OpCode   OpCode
    Flags    uint8
    Payload  []byte
}
```

### 11.2 Frame Codec

```go
func EncodeFrame(f *Frame) ([]byte, error) {
    totalLen := FrameHeaderLen + len(f.Payload) + FrameTrailerLen
    buf := make([]byte, totalLen)
    
    // Header
    copy(buf[0:4], FrameMagic[:])
    buf[4] = f.Version
    buf[5] = byte(f.OpCode)
    buf[6] = f.Flags
    binary.BigEndian.PutUint32(buf[7:], uint32(len(f.Payload)))
    
    // Payload
    copy(buf[FrameHeaderLen:], f.Payload)
    
    // CRC32C over header + payload
    crc := crc32.Checksum(buf[:FrameHeaderLen+len(f.Payload)], crc32.MakeTable(crc32.Castagnoli))
    binary.BigEndian.PutUint32(buf[FrameHeaderLen+len(f.Payload):], crc)
    
    return buf, nil
}

func DecodeFrame(reader io.Reader) (*Frame, error) {
    var header [FrameHeaderLen]byte
    if _, err := io.ReadFull(reader, header[:]); err != nil {
        return nil, err
    }
    
    // Validate magic
    if header[0] != 'C' || header[1] != 'H' || header[2] != 'M' || header[3] != 'R' {
        return nil, fmt.Errorf("invalid frame magic")
    }
    
    f := &Frame{
        Version: header[4],
        OpCode:  OpCode(header[5]),
        Flags:   header[6],
    }
    
    payloadLen := binary.BigEndian.Uint32(header[7:])
    if payloadLen > 16*1024*1024 { // 16MB max frame
        return nil, fmt.Errorf("frame too large: %d", payloadLen)
    }
    
    // Read payload + CRC
    buf := make([]byte, payloadLen+4)
    if _, err := io.ReadFull(reader, buf); err != nil {
        return nil, err
    }
    
    f.Payload = buf[:payloadLen]
    
    // Verify CRC
    expectedCRC := binary.BigEndian.Uint32(buf[payloadLen:])
    actualCRC := crc32.Checksum(header[:], crc32.MakeTable(crc32.Castagnoli))
    actualCRC = crc32.Update(actualCRC, crc32.MakeTable(crc32.Castagnoli), f.Payload)
    if actualCRC != expectedCRC {
        return nil, fmt.Errorf("CRC mismatch")
    }
    
    return f, nil
}
```

### 11.3 Protocol Handler Payloads

```go
// internal/protocol/chimera/payloads.go

// CONNECT payload
type ConnectPayload struct {
    ClientID  string
    Username  string // Future: auth
    Password  string // Future: auth
    Keepalive uint16 // Seconds
}

func encodeConnect(p *ConnectPayload) []byte {
    var buf bytes.Buffer
    writeString(&buf, p.ClientID)
    writeString(&buf, p.Username)
    writeString(&buf, p.Password)
    binary.Write(&buf, binary.BigEndian, p.Keepalive)
    return buf.Bytes()
}

// PUBLISH payload
type PublishPayload struct {
    Topic      string
    RoutingKey string
    Priority   uint8
    TTL        int64
    DeliverAt  int64
    Headers    map[string][]byte
    Body       []byte
}

// SUBSCRIBE payload
type SubscribePayload struct {
    Topic         string
    Mode          uint8  // 0=stream, 1=queue
    ConsumerGroup string // Stream mode only
    Prefetch      int    // Queue mode only
    StartOffset   int64  // Stream mode: -1=latest, -2=earliest, N=specific
}

// FETCH payload (stream mode)
type FetchPayload struct {
    Topic       string
    PartitionID uint32
    Offset      uint64
    MaxMessages uint32
    MaxWaitMS   uint32
}

// ACK/NACK payload
type AckPayload struct {
    Topic       string
    PartitionID uint32
    Offsets     []uint64 // Multiple offsets can be acked at once
}

// Helper: length-prefixed string encoding
func writeString(buf *bytes.Buffer, s string) {
    binary.Write(buf, binary.BigEndian, uint16(len(s)))
    buf.WriteString(s)
}

func readString(buf *bytes.Reader) (string, error) {
    var length uint16
    if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
        return "", err
    }
    data := make([]byte, length)
    if _, err := io.ReadFull(buf, data); err != nil {
        return "", err
    }
    return string(data), nil
}
```

### 11.4 Connection Handler

```go
// internal/protocol/chimera/server.go

type Server struct {
    listener net.Listener
    broker   *broker.Broker
    clients  sync.Map // clientID → *ClientConn
    metrics  *metrics.Collector
    
    ctx      context.Context
    cancel   context.CancelFunc
}

func NewServer(broker *broker.Broker, bind string, port int) (*Server, error) {
    addr := fmt.Sprintf("%s:%d", bind, port)
    listener, err := net.Listen("tcp", addr)
    if err != nil {
        return nil, err
    }
    
    ctx, cancel := context.WithCancel(context.Background())
    return &Server{
        listener: listener,
        broker:   broker,
        ctx:      ctx,
        cancel:   cancel,
    }, nil
}

func (s *Server) Serve() error {
    for {
        conn, err := s.listener.Accept()
        if err != nil {
            select {
            case <-s.ctx.Done():
                return nil
            default:
                continue
            }
        }
        go s.handleConnection(conn)
    }
}

func (s *Server) handleConnection(conn net.Conn) {
    defer conn.Close()
    
    client := &ClientConn{
        conn:    conn,
        reader:  bufio.NewReaderSize(conn, 64*1024),
        writer:  bufio.NewWriterSize(conn, 64*1024),
        subs:    make(map[string]*Subscription),
        created: time.Now(),
    }
    
    // First frame must be CONNECT
    frame, err := DecodeFrame(client.reader)
    if err != nil {
        return
    }
    if frame.OpCode != OpConnect {
        s.sendError(client, "first frame must be CONNECT")
        return
    }
    
    connectPayload := decodeConnect(frame.Payload)
    client.clientID = connectPayload.ClientID
    if client.clientID == "" {
        client.clientID = generateClientID()
    }
    
    // Store client
    s.clients.Store(client.clientID, client)
    defer s.clients.Delete(client.clientID)
    
    // Send CONNACK
    s.sendFrame(client, &Frame{
        Version: FrameVersion,
        OpCode:  OpConnAck,
        Payload: encodeConnAck(client.clientID),
    })
    
    // Start keepalive timer
    if connectPayload.Keepalive > 0 {
        conn.SetReadDeadline(time.Now().Add(time.Duration(connectPayload.Keepalive*2) * time.Second))
    }
    
    // Main read loop
    for {
        frame, err := DecodeFrame(client.reader)
        if err != nil {
            return
        }
        
        // Reset keepalive deadline
        if connectPayload.Keepalive > 0 {
            conn.SetReadDeadline(time.Now().Add(time.Duration(connectPayload.Keepalive*2) * time.Second))
        }
        
        switch frame.OpCode {
        case OpPublish:
            s.handlePublish(client, frame)
        case OpBatchPublish:
            s.handleBatchPublish(client, frame)
        case OpSubscribe:
            s.handleSubscribe(client, frame)
        case OpUnsubscribe:
            s.handleUnsubscribe(client, frame)
        case OpFetch:
            s.handleFetch(client, frame)
        case OpAck:
            s.handleAck(client, frame)
        case OpNack:
            s.handleNack(client, frame)
        case OpSeek:
            s.handleSeek(client, frame)
        case OpCommitOffset:
            s.handleCommitOffset(client, frame)
        case OpCreateTopic:
            s.handleCreateTopic(client, frame)
        case OpDeleteTopic:
            s.handleDeleteTopic(client, frame)
        case OpPing:
            s.sendFrame(client, &Frame{Version: FrameVersion, OpCode: OpPong})
        case OpDisconnect:
            return
        }
    }
}
```

### 11.5 Publish Handler

```go
func (s *Server) handlePublish(client *ClientConn, frame *Frame) {
    payload := decodePublish(frame.Payload)
    
    env := &message.Envelope{
        Topic:       payload.Topic,
        RoutingKey:  payload.RoutingKey,
        Priority:    payload.Priority,
        TTL:         payload.TTL,
        DeliverAt:   payload.DeliverAt,
        Headers:     payload.Headers,
        Payload:     payload.Body,
        SourceProto: message.ProtoChimera,
    }
    
    offset, err := s.broker.Publish(env)
    if err != nil {
        s.sendError(client, err.Error())
        return
    }
    
    // Send PUBACK
    ackPayload := encodePubAck(env.Topic, env.PartitionID, offset)
    s.sendFrame(client, &Frame{
        Version: FrameVersion,
        OpCode:  OpPubAck,
        Payload: ackPayload,
    })
}
```

### 11.6 Subscribe Handler

```go
func (s *Server) handleSubscribe(client *ClientConn, frame *Frame) {
    payload := decodeSubscribe(frame.Payload)
    
    sub := &Subscription{
        topic:         payload.Topic,
        mode:          payload.Mode,
        consumerGroup: payload.ConsumerGroup,
        prefetch:      payload.Prefetch,
    }
    
    if payload.Mode == 1 { // Queue mode
        consumer := &queue.QueueConsumer{
            ID:       client.clientID,
            Prefetch: payload.Prefetch,
            InFlight: make(map[uint64]time.Time),
        }
        s.broker.QueueEngine().AddConsumer(payload.Topic, consumer, client)
    } else { // Stream mode
        startOffset := payload.StartOffset
        s.broker.StreamEngine().JoinGroup(
            payload.ConsumerGroup,
            payload.Topic,
            client.clientID,
            startOffset,
            client,
        )
    }
    
    client.subs[payload.Topic] = sub
    
    // Send SUBACK
    s.sendFrame(client, &Frame{
        Version: FrameVersion,
        OpCode:  OpSubAck,
        Payload: encodeSubAck(payload.Topic, true),
    })
}
```

---

## 12. CLIENT CONNECTION MANAGER

### 12.1 Client Connection Struct

```go
// internal/protocol/chimera/client.go

type ClientConn struct {
    mu       sync.Mutex
    conn     net.Conn
    reader   *bufio.Reader
    writer   *bufio.Writer
    clientID string
    subs     map[string]*Subscription
    created  time.Time
    
    // Write coalescing
    writeCh  chan *Frame
    done     chan struct{}
}

type Subscription struct {
    topic         string
    mode          uint8  // 0=stream, 1=queue
    consumerGroup string
    prefetch      int
}

// StartWriteLoop starts a goroutine that batches and writes frames.
func (c *ClientConn) StartWriteLoop() {
    c.writeCh = make(chan *Frame, 1024)
    c.done = make(chan struct{})
    
    go func() {
        defer close(c.done)
        for frame := range c.writeCh {
            data, err := EncodeFrame(frame)
            if err != nil {
                continue
            }
            c.mu.Lock()
            c.writer.Write(data)
            // Flush if channel is empty (write coalescing)
            if len(c.writeCh) == 0 {
                c.writer.Flush()
            }
            c.mu.Unlock()
        }
    }()
}

// Send queues a frame for async writing.
func (c *ClientConn) Send(f *Frame) {
    select {
    case c.writeCh <- f:
    default:
        // Channel full — drop frame (client is too slow)
    }
}

// SendSync writes a frame immediately (for CONNACK, errors).
func (c *ClientConn) SendSync(f *Frame) error {
    data, err := EncodeFrame(f)
    if err != nil {
        return err
    }
    c.mu.Lock()
    defer c.mu.Unlock()
    if _, err := c.writer.Write(data); err != nil {
        return err
    }
    return c.writer.Flush()
}
```

---

## 13. HTTP ADMIN API

### 13.1 Router

Pure stdlib `net/http` — no gorilla/mux, no chi.

```go
// internal/protocol/http/server.go

type AdminServer struct {
    broker  *broker.Broker
    server  *http.Server
    mux     *http.ServeMux
}

func NewAdminServer(broker *broker.Broker, bind string, port int) *AdminServer {
    mux := http.NewServeMux()
    s := &AdminServer{
        broker: broker,
        mux:    mux,
        server: &http.Server{
            Addr:         fmt.Sprintf("%s:%d", bind, port),
            Handler:      mux,
            ReadTimeout:  30 * time.Second,
            WriteTimeout: 30 * time.Second,
            IdleTimeout:  120 * time.Second,
        },
    }
    
    s.registerRoutes()
    return s
}

func (s *AdminServer) registerRoutes() {
    // Topics
    s.mux.HandleFunc("POST /v1/topics", s.handleCreateTopic)
    s.mux.HandleFunc("GET /v1/topics", s.handleListTopics)
    s.mux.HandleFunc("GET /v1/topics/{name}", s.handleGetTopic)
    s.mux.HandleFunc("DELETE /v1/topics/{name}", s.handleDeleteTopic)
    
    // Messages
    s.mux.HandleFunc("POST /v1/messages/{topic}", s.handlePublish)
    s.mux.HandleFunc("GET /v1/messages/{topic}", s.handleFetch)
    s.mux.HandleFunc("POST /v1/messages/{topic}/ack", s.handleAck)
    
    // Consumer groups
    s.mux.HandleFunc("GET /v1/consumers", s.handleListConsumers)
    s.mux.HandleFunc("GET /v1/consumers/{group}", s.handleGetConsumerGroup)
    
    // Operations
    s.mux.HandleFunc("GET /v1/health", s.handleHealth)
    s.mux.HandleFunc("GET /v1/metrics", s.handleMetrics)
}

func (s *AdminServer) Serve() error {
    return s.server.ListenAndServe()
}

func (s *AdminServer) Shutdown(ctx context.Context) error {
    return s.server.Shutdown(ctx)
}
```

### 13.2 Topic Endpoints

```go
func (s *AdminServer) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
    var req struct {
        Name          string `json:"name"`
        Mode          string `json:"mode"`
        Partitions    uint32 `json:"partitions"`
        RetentionTime string `json:"retention_time,omitempty"`
        DLQTopic      string `json:"dlq_topic,omitempty"`
        MaxRetries    uint32 `json:"max_retries,omitempty"`
    }
    
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
        return
    }
    
    mode := broker.ModeUnified
    switch req.Mode {
    case "stream":
        mode = broker.ModeStream
    case "queue":
        mode = broker.ModeQueue
    case "unified", "":
        mode = broker.ModeUnified
    default:
        writeError(w, http.StatusBadRequest, "invalid mode: "+req.Mode)
        return
    }
    
    retention, _ := time.ParseDuration(req.RetentionTime)
    if retention == 0 {
        retention, _ = time.ParseDuration(s.broker.Config().Defaults.Topic.RetentionTime)
    }
    
    if req.Partitions == 0 {
        req.Partitions = s.broker.Config().Defaults.Topic.Partitions
    }
    
    cfg := broker.TopicConfig{
        Name:          req.Name,
        Mode:          mode,
        Partitions:    req.Partitions,
        RetentionTime: retention,
        DLQTopic:      req.DLQTopic,
        MaxRetries:    req.MaxRetries,
    }
    
    if err := s.broker.Topics().CreateTopic(cfg); err != nil {
        writeError(w, http.StatusConflict, err.Error())
        return
    }
    
    writeJSON(w, http.StatusCreated, cfg)
}

func (s *AdminServer) handleListTopics(w http.ResponseWriter, r *http.Request) {
    topics := s.broker.Topics().ListTopics()
    
    type topicInfo struct {
        Name       string `json:"name"`
        Mode       string `json:"mode"`
        Partitions uint32 `json:"partitions"`
        CreatedAt  string `json:"created_at"`
    }
    
    result := make([]topicInfo, len(topics))
    for i, t := range topics {
        modeStr := "unified"
        switch t.Mode {
        case broker.ModeStream:
            modeStr = "stream"
        case broker.ModeQueue:
            modeStr = "queue"
        }
        result[i] = topicInfo{
            Name:       t.Name,
            Mode:       modeStr,
            Partitions: t.Partitions,
            CreatedAt:  t.CreatedAt.Format(time.RFC3339),
        }
    }
    
    writeJSON(w, http.StatusOK, result)
}

func (s *AdminServer) handleGetTopic(w http.ResponseWriter, r *http.Request) {
    name := r.PathValue("name")
    
    topic, ok := s.broker.Topics().GetTopic(name)
    if !ok {
        writeError(w, http.StatusNotFound, "topic not found")
        return
    }
    
    // Include partition stats
    type partStat struct {
        ID        uint32 `json:"id"`
        HighWater uint64 `json:"high_watermark"`
        LogStart  uint64 `json:"log_start_offset"`
    }
    
    stats := make([]partStat, topic.Partitions)
    for i := uint32(0); i < topic.Partitions; i++ {
        part, err := s.broker.Storage().GetOrCreatePartition(name, i)
        if err == nil {
            stats[i] = partStat{
                ID:        i,
                HighWater: part.HighWatermark(),
                LogStart:  part.LogStartOffset(),
            }
        }
    }
    
    writeJSON(w, http.StatusOK, map[string]interface{}{
        "topic":      topic,
        "partitions": stats,
    })
}

func (s *AdminServer) handlePublish(w http.ResponseWriter, r *http.Request) {
    topicName := r.PathValue("topic")
    
    body, err := io.ReadAll(io.LimitReader(r.Body, 16*1024*1024)) // 16MB max
    if err != nil {
        writeError(w, http.StatusBadRequest, "read body: "+err.Error())
        return
    }
    
    env := &message.Envelope{
        Topic:       topicName,
        RoutingKey:  r.Header.Get("X-Routing-Key"),
        Payload:     body,
        ContentType: r.Header.Get("Content-Type"),
        SourceProto: message.ProtoHTTP,
    }
    
    offset, err := s.broker.Publish(env)
    if err != nil {
        writeError(w, http.StatusInternalServerError, err.Error())
        return
    }
    
    writeJSON(w, http.StatusOK, map[string]interface{}{
        "offset":    offset,
        "partition": env.PartitionID,
        "topic":     topicName,
    })
}

func (s *AdminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]interface{}{
        "status":  "healthy",
        "node_id": s.broker.Config().Node.ID,
        "name":    s.broker.Config().Node.Name,
        "uptime":  time.Since(s.broker.StartTime()).String(),
    })
}

// Helpers
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
    writeJSON(w, status, map[string]string{"error": msg})
}
```

---

## 14. PROMETHEUS METRICS

### 14.1 Metrics Collector

Pure Go implementation — Prometheus text format exposition without external library:

```go
// internal/metrics/collector.go

type Collector struct {
    mu       sync.RWMutex
    counters map[string]*Counter
    gauges   map[string]*Gauge
    histos   map[string]*Histogram
}

type Counter struct {
    mu     sync.Mutex
    values map[string]uint64 // label combo → value
}

type Gauge struct {
    mu     sync.Mutex
    values map[string]float64
}

func NewCollector() *Collector {
    return &Collector{
        counters: make(map[string]*Counter),
        gauges:   make(map[string]*Gauge),
        histos:   make(map[string]*Histogram),
    }
}

func (c *Collector) IncrCounter(name string, labels map[string]string, delta uint64) {
    c.mu.RLock()
    counter, ok := c.counters[name]
    c.mu.RUnlock()
    
    if !ok {
        c.mu.Lock()
        counter = &Counter{values: make(map[string]uint64)}
        c.counters[name] = counter
        c.mu.Unlock()
    }
    
    key := labelsToKey(labels)
    counter.mu.Lock()
    counter.values[key] += delta
    counter.mu.Unlock()
}

func (c *Collector) SetGauge(name string, labels map[string]string, value float64) {
    c.mu.RLock()
    gauge, ok := c.gauges[name]
    c.mu.RUnlock()
    
    if !ok {
        c.mu.Lock()
        gauge = &Gauge{values: make(map[string]float64)}
        c.gauges[name] = gauge
        c.mu.Unlock()
    }
    
    key := labelsToKey(labels)
    gauge.mu.Lock()
    gauge.values[key] = value
    gauge.mu.Unlock()
}

// Expose returns Prometheus text format exposition.
func (c *Collector) Expose() string {
    var buf strings.Builder
    
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    for name, counter := range c.counters {
        buf.WriteString(fmt.Sprintf("# TYPE %s counter\n", name))
        counter.mu.Lock()
        for labels, value := range counter.values {
            if labels == "" {
                buf.WriteString(fmt.Sprintf("%s %d\n", name, value))
            } else {
                buf.WriteString(fmt.Sprintf("%s{%s} %d\n", name, labels, value))
            }
        }
        counter.mu.Unlock()
    }
    
    for name, gauge := range c.gauges {
        buf.WriteString(fmt.Sprintf("# TYPE %s gauge\n", name))
        gauge.mu.Lock()
        for labels, value := range gauge.values {
            if labels == "" {
                buf.WriteString(fmt.Sprintf("%s %g\n", name, value))
            } else {
                buf.WriteString(fmt.Sprintf("%s{%s} %g\n", name, labels, value))
            }
        }
        gauge.mu.Unlock()
    }
    
    return buf.String()
}

func labelsToKey(labels map[string]string) string {
    if len(labels) == 0 {
        return ""
    }
    keys := make([]string, 0, len(labels))
    for k := range labels {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    var buf strings.Builder
    for i, k := range keys {
        if i > 0 {
            buf.WriteByte(',')
        }
        buf.WriteString(k)
        buf.WriteString(`="`)
        buf.WriteString(labels[k])
        buf.WriteByte('"')
    }
    return buf.String()
}

// Convenience methods for broker
func (c *Collector) MessageIn(topic string, partition uint32, proto message.ProtocolType) {
    c.IncrCounter("chimera_messages_in_total", map[string]string{
        "topic":     topic,
        "partition": fmt.Sprintf("%d", partition),
        "protocol":  proto.String(),
    }, 1)
}

func (c *Collector) MessageOut(topic string, partition uint32, group string) {
    c.IncrCounter("chimera_messages_out_total", map[string]string{
        "topic":          topic,
        "partition":      fmt.Sprintf("%d", partition),
        "consumer_group": group,
    }, 1)
}

func (c *Collector) ActiveConnections(proto string, count int) {
    c.SetGauge("chimera_active_connections", map[string]string{
        "protocol": proto,
    }, float64(count))
}

func (c *Collector) QueueDepth(topic string, depth int) {
    c.SetGauge("chimera_queue_depth", map[string]string{
        "topic": topic,
    }, float64(depth))
}

func (c *Collector) ConsumerLag(topic string, partition uint32, group string, lag uint64) {
    c.SetGauge("chimera_consumer_lag", map[string]string{
        "topic":          topic,
        "partition":      fmt.Sprintf("%d", partition),
        "consumer_group": group,
    }, float64(lag))
}
```

---

## 15. CLI

### 15.1 Server Command

```go
// internal/cli/server.go

func runServer(args []string) {
    flags := flag.NewFlagSet("server", flag.ExitOnError)
    configPath := flags.String("config", "", "Path to chimera.yaml")
    dataDir := flags.String("data-dir", "", "Data directory override")
    bindAddr := flags.String("bind", "", "Bind address override")
    port := flags.Int("port", 0, "Port override")
    adminPort := flags.Int("admin-port", 0, "Admin port override")
    logLevel := flags.String("log-level", "", "Log level override")
    flags.Parse(args)
    
    cliFlags := &CLIFlags{
        DataDir:   *dataDir,
        Bind:      *bindAddr,
        Port:      *port,
        AdminPort: *adminPort,
        LogLevel:  *logLevel,
    }
    
    cfg, err := broker.LoadConfig(*configPath, cliFlags)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }
    
    b, err := broker.NewBroker(cfg)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }
    
    if err := b.Start(); err != nil {
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }
    
    // Wait for signal
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    
    fmt.Println("\nShutting down...")
    if err := b.Stop(); err != nil {
        fmt.Fprintf(os.Stderr, "Shutdown error: %v\n", err)
        os.Exit(1)
    }
    fmt.Println("Goodbye.")
}
```

### 15.2 Topic CLI

```go
// internal/cli/topic.go

func runTopicCLI(args []string) {
    if len(args) < 1 {
        fmt.Println("Usage: chimera topic [create|list|describe|delete]")
        os.Exit(1)
    }
    
    adminAddr := getAdminAddr() // From env or flag
    
    switch args[0] {
    case "create":
        flags := flag.NewFlagSet("create", flag.ExitOnError)
        name := flags.String("name", "", "Topic name")
        mode := flags.String("mode", "unified", "Topic mode: stream, queue, unified")
        partitions := flags.Uint("partitions", 8, "Number of partitions")
        flags.Parse(args[1:])
        
        body, _ := json.Marshal(map[string]interface{}{
            "name":       *name,
            "mode":       *mode,
            "partitions": *partitions,
        })
        
        resp, err := http.Post(adminAddr+"/v1/topics", "application/json", bytes.NewReader(body))
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            os.Exit(1)
        }
        defer resp.Body.Close()
        printResponse(resp)
        
    case "list":
        resp, err := http.Get(adminAddr + "/v1/topics")
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            os.Exit(1)
        }
        defer resp.Body.Close()
        printResponse(resp)
        
    case "describe":
        if len(args) < 2 {
            fmt.Println("Usage: chimera topic describe <name>")
            os.Exit(1)
        }
        resp, err := http.Get(adminAddr + "/v1/topics/" + args[1])
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            os.Exit(1)
        }
        defer resp.Body.Close()
        printResponse(resp)
        
    case "delete":
        if len(args) < 2 {
            fmt.Println("Usage: chimera topic delete <name>")
            os.Exit(1)
        }
        req, _ := http.NewRequest("DELETE", adminAddr+"/v1/topics/"+args[1], nil)
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            os.Exit(1)
        }
        defer resp.Body.Close()
        printResponse(resp)
    }
}
```

### 15.3 Produce / Consume CLI

```go
// internal/cli/produce.go

func runProduceCLI(args []string) {
    flags := flag.NewFlagSet("produce", flag.ExitOnError)
    topic := flags.String("topic", "", "Target topic")
    message := flags.String("message", "", "Message body (or stdin if empty)")
    count := flags.Int("count", 1, "Number of messages to send")
    flags.Parse(args)
    
    // For Phase 1, use HTTP API (simple but functional)
    adminAddr := getAdminAddr()
    
    var body []byte
    if *message != "" {
        body = []byte(*message)
    } else {
        body, _ = io.ReadAll(os.Stdin)
    }
    
    for i := 0; i < *count; i++ {
        resp, err := http.Post(
            adminAddr+"/v1/messages/"+*topic,
            "application/octet-stream",
            bytes.NewReader(body),
        )
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            os.Exit(1)
        }
        
        var result map[string]interface{}
        json.NewDecoder(resp.Body).Decode(&result)
        resp.Body.Close()
        
        fmt.Printf("Published: partition=%v offset=%v\n", result["partition"], result["offset"])
    }
}

// internal/cli/consume.go

func runConsumeCLI(args []string) {
    flags := flag.NewFlagSet("consume", flag.ExitOnError)
    topic := flags.String("topic", "", "Source topic")
    partition := flags.Int("partition", 0, "Partition ID")
    offset := flags.Int64("offset", -1, "Start offset (-1=latest, -2=earliest)")
    maxMessages := flags.Int("max", 10, "Max messages to fetch")
    follow := flags.Bool("follow", false, "Follow mode (continuous)")
    flags.Parse(args)
    
    adminAddr := getAdminAddr()
    
    currentOffset := *offset
    
    for {
        url := fmt.Sprintf("%s/v1/messages/%s?partition=%d&offset=%d&max=%d",
            adminAddr, *topic, *partition, currentOffset, *maxMessages)
        
        resp, err := http.Get(url)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
            os.Exit(1)
        }
        
        var result struct {
            Messages   []json.RawMessage `json:"messages"`
            NextOffset int64             `json:"next_offset"`
        }
        json.NewDecoder(resp.Body).Decode(&result)
        resp.Body.Close()
        
        for _, msg := range result.Messages {
            fmt.Println(string(msg))
        }
        
        if !*follow {
            break
        }
        
        currentOffset = result.NextOffset
        if len(result.Messages) == 0 {
            time.Sleep(500 * time.Millisecond)
        }
    }
}
```

---

## 16. GRACEFUL SHUTDOWN

### 16.1 Shutdown Sequence

Reverse order of startup — each component given a deadline:

```go
func (b *Broker) Stop() error {
    shutdownTimeout := 30 * time.Second
    ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
    defer cancel()
    
    b.logger.Info("initiating graceful shutdown")
    
    // 1. Stop accepting new connections
    b.protoServer.StopAccepting()
    
    // 2. Drain HTTP server
    b.httpServer.Shutdown(ctx)
    b.logger.Info("HTTP server stopped")
    
    // 3. Wait for in-flight messages to complete (max 10s)
    drainCtx, drainCancel := context.WithTimeout(ctx, 10*time.Second)
    b.drainInFlight(drainCtx)
    drainCancel()
    b.logger.Info("in-flight messages drained")
    
    // 4. Disconnect all clients
    b.protoServer.DisconnectAll()
    b.logger.Info("clients disconnected")
    
    // 5. Stop background goroutines
    b.cancel()
    b.wg.Wait()
    b.logger.Info("background goroutines stopped")
    
    // 6. Flush storage
    b.storage.FlushAll()
    b.logger.Info("storage flushed")
    
    // 7. Checkpoint and close WAL
    b.wal.Checkpoint(b.wal.Offset())
    b.wal.Close()
    b.logger.Info("WAL closed")
    
    // 8. Close storage engine
    b.storage.Close()
    b.logger.Info("storage closed")
    
    // 9. Release lock file
    b.lockFile.Close()
    os.Remove(b.lockFile.Name())
    
    b.logger.Info("shutdown complete")
    return nil
}
```

---

## 17. TESTING STRATEGY

### 17.1 Unit Test Coverage

Each package must have comprehensive unit tests:

```
internal/message/     → codec_test.go (marshal/unmarshal roundtrip, fuzzing)
                      → uuid_test.go (uniqueness, ordering, performance)
internal/storage/wal/ → wal_test.go (append, recover, checkpoint, truncate, corruption)
internal/storage/hot/ → segment_test.go (append, read, freeze, mmap, index)
                      → partition_test.go (multi-segment, rollover, offset lookup)
internal/broker/      → topic_test.go (CRUD, validation, metadata persistence)
                      → murmur3_test.go (known vectors, distribution)
internal/engine/queue/ → dispatcher_test.go (round-robin, prefetch, capacity)
                       → ack_test.go (ack, nack, timeout, DLQ routing)
                       → delay_test.go (scheduling, promotion, ordering)
internal/engine/stream/ → consumer_group_test.go (join, leave, rebalance)
                        → offset_test.go (save, load, persistence)
internal/protocol/chimera/ → codec_test.go (frame encode/decode, CRC)
                           → server_test.go (connect, publish, subscribe, ack flow)
```

### 17.2 Integration Tests

```go
// test/integration/publish_consume_test.go

func TestPublishConsumeQueue(t *testing.T) {
    // Start embedded broker
    // Create queue topic
    // Publish N messages via HTTP
    // Consume via Chimera protocol
    // Verify all messages received
    // Verify ack/nack behavior
}

func TestPublishConsumeStream(t *testing.T) {
    // Start embedded broker
    // Create stream topic
    // Publish N messages
    // Consumer group with 3 members
    // Verify partition assignment
    // Verify offset commit
    // Verify replay from offset
}

func TestUnifiedMode(t *testing.T) {
    // Start embedded broker
    // Create unified topic
    // Publish N messages
    // Stream consumer reads by offset
    // Queue consumer reads by dispatch
    // Verify both see same messages
    // Verify no data duplication
}

func TestCrashRecovery(t *testing.T) {
    // Start broker, publish messages
    // Kill broker (simulate crash)
    // Restart broker
    // Verify WAL replay
    // Verify all committed messages present
    // Verify no duplicates
}
```

### 17.3 Benchmark Tests

```go
// test/bench/publish_bench_test.go

func BenchmarkPublish(b *testing.B) {
    // Benchmark: messages/sec, latency, allocation
    // Message sizes: 100B, 1KB, 10KB, 100KB
}

func BenchmarkConsume(b *testing.B) {
    // Benchmark: messages/sec from hot tier
}

func BenchmarkEnvelopeMarshal(b *testing.B) {
    // Benchmark: serialization throughput
}
```

---

## 18. BUILD & DISTRIBUTION

### 18.1 Makefile

```makefile
BINARY   := chimera
VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE     := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS  := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.PHONY: build test bench clean

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/chimera/

test:
	go test -race -count=1 ./...

bench:
	go test -bench=. -benchmem ./...

lint:
	go vet ./...
	staticcheck ./...

clean:
	rm -rf bin/

docker:
	docker build -t chimeramq/chimera:$(VERSION) .

release: clean
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-linux-amd64 ./cmd/chimera/
	GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-linux-arm64 ./cmd/chimera/
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-darwin-amd64 ./cmd/chimera/
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-darwin-arm64 ./cmd/chimera/
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-windows-amd64.exe ./cmd/chimera/
```

### 18.2 Dockerfile

```dockerfile
FROM golang:1.23-alpine AS builder
RUN apk add --no-cache git make
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /src/bin/chimera /usr/local/bin/chimera
RUN mkdir -p /var/lib/chimera /etc/chimera
EXPOSE 5672 9090
VOLUME /var/lib/chimera
ENTRYPOINT ["chimera"]
CMD ["server", "--config", "/etc/chimera/chimera.yaml"]
```

### 18.3 go.mod

```
module github.com/chimeramq/chimera

go 1.23

require (
    golang.org/x/crypto v0.28.0
    golang.org/x/sys v0.26.0
    gopkg.in/yaml.v3 v3.0.1
)
```

Three dependencies. Zero external. Pure Go. #NOFORKANYMORE.

---

## IMPLEMENTATION NOTES

### Critical Path (Must Get Right)

1. **WAL → Storage ordering:** WAL MUST be durable before hot segment write. Violation = data loss.
2. **Offset monotonicity:** Partition offsets MUST be strictly monotonic. Single writer per partition enforces this.
3. **Segment boundary handling:** Read operations spanning two segments must be seamless.
4. **Mmap lifecycle:** Munmap before file close. Segment resize invalidates existing mmaps.
5. **CRC validation:** Every WAL entry and protocol frame verified on read. No silent corruption.
6. **Graceful shutdown ordering:** Storage flush before WAL close. WAL close before file unlock.

### Performance-Critical Decisions

1. **Single writer per partition:** No locks on write path (only the partition owner goroutine writes).
2. **Buffer pool for serialization:** Reduce GC pressure on high-throughput publish path.
3. **Write coalescing on client connections:** Batch TCP writes when channel has pending frames.
4. **Sparse index interval tuning:** 256 messages default, tunable per workload.
5. **Fsync interval vs durability:** Default 200ms balances throughput and safety.

### Known Phase 1 Limitations

- Single-node only (no replication, no failover)
- No TLS (Phase 2 with multi-protocol)
- No authentication/authorization (Phase 7)
- No warm/cold tier (Phase 4)
- No log compaction (Phase 4)
- Hot tier retention only by segment count (time-based in Phase 4)
- CLI uses HTTP API (native protocol client in Phase 2)

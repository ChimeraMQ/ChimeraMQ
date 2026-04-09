# ChimeraMQ — TASKS.md

> **Phase 1: Core Engine (MVP) — Granular Task Breakdown**
> Each task is atomic, testable, and Claude Code ready.
> Estimated total: ~85 tasks across 18 milestones.

---

## CONVENTIONS

- **Task ID format:** `P1-{milestone}.{sequence}` (e.g., P1-01.03 = Phase 1, Milestone 1, Task 3)
- **Status:** `[ ]` = pending, `[x]` = done, `[~]` = in progress
- **Deps:** Tasks that must be completed before this task
- **Test:** Every task includes its test expectation
- **Files:** Exact file paths to create/modify

---

## MILESTONE 1: Project Scaffold & Configuration

### P1-01.01 — Initialize Go module and directory structure
- **Status:** `[ ]`
- **Deps:** None
- **Files:**
  ```
  go.mod
  go.sum
  cmd/chimera/main.go
  internal/broker/
  internal/message/
  internal/storage/wal/
  internal/storage/hot/
  internal/engine/queue/
  internal/engine/stream/
  internal/protocol/chimera/
  internal/protocol/http/
  internal/metrics/
  internal/cli/
  configs/chimera.yaml.example
  Makefile
  Dockerfile
  LICENSE (Apache 2.0)
  README.md (placeholder)
  ```
- **Action:** `go mod init github.com/chimeramq/chimera` with Go 1.23. Add three deps: `golang.org/x/crypto`, `golang.org/x/sys`, `gopkg.in/yaml.v3`. Create all directories with `.gitkeep`. Write minimal `main.go` that prints version. Write `Makefile` with `build`, `test`, `bench`, `lint`, `clean`, `release`, `docker` targets. Write `Dockerfile` (multi-stage, alpine). Write Apache 2.0 LICENSE.
- **Test:** `go build ./...` succeeds. `make build` produces `bin/chimera`. `bin/chimera version` prints version string.

### P1-01.02 — Configuration struct and YAML loader
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/broker/config.go
  internal/broker/config_test.go
  configs/chimera.yaml.example
  ```
- **Action:** Implement `Config` struct hierarchy: `NodeConfig`, `ListenerConfig`, `StorageConfig` (with `HotConfig`, `WALConfig`, `TierPolicyConfig`), `DefaultsConfig` (with `TopicDefaults`), `LoggingConfig`. Implement `LoadConfig(configPath string, flags *CLIFlags) (*Config, error)` with priority: CLI flags > env vars > YAML > defaults. Implement `defaultConfig()` returning sensible defaults. Implement `Validate()` method. Env var pattern: `CHIMERA_{SECTION}_{KEY}`.
- **Test:** Test default config values. Test YAML loading overrides defaults. Test env var overrides YAML. Test CLI flag overrides env. Test validation catches: port=0, empty data_dir, invalid sync_mode. Test partial YAML (missing sections use defaults).

### P1-01.03 — Structured logger
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/broker/logger.go
  internal/broker/logger_test.go
  ```
- **Action:** Implement simple structured logger wrapping `log/slog`. Support JSON and text format. Support levels: debug, info, warn, error. Support output: stdout, file. Logger instance created from `LoggingConfig`. No external dependency.
- **Test:** Test JSON output format contains expected keys (level, msg, time). Test text format. Test level filtering (debug messages hidden at info level). Test file output writes to file.

### P1-01.04 — CLI subcommand router and version command
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  cmd/chimera/main.go
  internal/cli/version.go
  ```
- **Action:** `main()` routes by `os.Args[1]`: `server`, `topic`, `produce`, `consume`, `version`. Unknown subcommand prints usage and exits 1. `version` command prints version, commit, date (injected via `-ldflags`). Add build-time variables: `var version, commit, date string`.
- **Test:** `chimera version` outputs version string. Unknown subcommand exits with code 1. No args prints usage.

---

## MILESTONE 2: Message Envelope & Codec

### P1-02.01 — Message envelope types and constants
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/message/envelope.go
  ```
- **Action:** Define `Envelope` struct with all fields (MessageID, Timestamp, Sequence, Topic, PartitionID, RoutingKey, Headers, SchemaID, ContentType, Encoding, Payload, Priority, TTL, DeliverAt, DeliverCount, MaxRetries, TraceID, SpanID, SourceProto). Define `EncodingType` enum (Raw, Snappy, Zstd, LZ4). Define `ProtocolType` enum (Chimera, AMQP, MQTT, WS, HTTP) with `String()` method. Define flag constants (FlagHasHeaders, FlagHasRoutingKey, FlagHasTrace, FlagHasTTL, FlagHasDelay). Define `FixedHeaderSize = 64`. Define `EstimateSize() int` method.
- **Test:** N/A (types only), but verify `EstimateSize()` returns correct values for various envelope configurations.

### P1-02.02 — UUIDv7 generator
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/message/uuid.go
  internal/message/uuid_test.go
  ```
- **Action:** Implement `UUIDv7Generator` struct with mutex, lastMS, counter. Implement `Generate() [16]byte` — 48-bit ms timestamp + version 7 + 12-bit monotonic counter + variant 10 + 62-bit random. Implement `UUIDString([16]byte) string` for standard hex format. Package-level `NewUUIDv7()` via default generator.
- **Test:** Generate 100K UUIDs — all unique. UUIDs generated in same millisecond have monotonically increasing counter. `UUIDString` format matches `xxxxxxxx-xxxx-7xxx-[89ab]xxx-xxxxxxxxxxxx`. Version bits = 0x70. Variant bits = 0x80. Benchmark: > 1M UUIDs/sec.

### P1-02.03 — Header TLV encoding/decoding
- **Status:** `[ ]`
- **Deps:** P1-02.01
- **Files:**
  ```
  internal/message/headers.go
  internal/message/headers_test.go
  ```
- **Action:** Implement `marshalHeaders(map[string][]byte) []byte` — TLV format: `[key_len:uint16][key][val_len:uint32][val]` per header. Implement `unmarshalHeaders([]byte) map[string][]byte`. Implement `headersSize(map[string][]byte) int`.
- **Test:** Roundtrip empty headers. Roundtrip single header. Roundtrip multiple headers. Roundtrip headers with empty values. Roundtrip headers with binary values. Verify `headersSize` matches actual marshaled length.

### P1-02.04 — Envelope binary codec (Marshal/Unmarshal)
- **Status:** `[ ]`
- **Deps:** P1-02.01, P1-02.02, P1-02.03
- **Files:**
  ```
  internal/message/codec.go
  internal/message/codec_test.go
  ```
- **Action:** Implement `Marshal(e *Envelope) ([]byte, error)` with buffer pool (`sync.Pool`). Encode fixed 64-byte header: MessageID(16) + Timestamp(8) + Sequence(8) + PartitionID(4) + SchemaID(4) + Priority(1) + Encoding(1) + SourceProto(1) + Flags(1) + PayloadLength(4) + HeadersLength(4) + TopicLength(2) + RoutingKeyLength(2) + TTL/DeliverAt/DeliverCount(8). Variable fields: Topic + RoutingKey + Headers + Trace + Payload. Implement `Unmarshal([]byte) (*Envelope, error)` — zero-copy for Payload (slice reference). Implement `ReleaseBuffer(*[]byte)` for pool return.
- **Test:** Roundtrip minimal envelope (topic + payload only). Roundtrip with all optional fields. Roundtrip with headers. Roundtrip with trace. Unmarshal rejects data < 64 bytes. Zero-copy verified: payload slice shares underlying array with input. Fuzz test: random bytes don't panic Unmarshal. Benchmark: > 2M marshal/unmarshal ops/sec for 1KB payload.

---

## MILESTONE 3: Write-Ahead Log

### P1-03.01 — WAL types and constants
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/storage/wal/types.go
  ```
- **Action:** Define `WALMagic`, `WALHeaderSize = 17`, `EntryType` enum (Message, TopicMeta, Checkpoint), `SyncMode` enum (Immediate, Interval, OS), `WAL` struct fields.
- **Test:** N/A (types only).

### P1-03.02 — WAL segment file management
- **Status:** `[ ]`
- **Deps:** P1-03.01
- **Files:**
  ```
  internal/storage/wal/wal.go
  internal/storage/wal/wal_test.go
  ```
- **Action:** Implement `NewWAL(dir, maxSize, syncMode, syncInterval)`. Create WAL directory. Implement `openOrCreateSegment()` — find latest segment or create `000000000001.wal`. Implement `rotate()` — flush, sync, close current, increment sequence, open new. Implement `listSegments()` — glob `*.wal` sorted. Implement `segmentPath(seq)`. Implement `Close()` — flush, sync, close, stop ticker.
- **Test:** New WAL creates directory and first segment. Segment file has correct name format. After `maxSize` bytes, rotate creates new segment. `listSegments` returns sorted order. Close flushes pending data.

### P1-03.03 — WAL append with CRC32C
- **Status:** `[ ]`
- **Deps:** P1-03.02
- **Files:**
  ```
  internal/storage/wal/wal.go (extend)
  internal/storage/wal/wal_test.go (extend)
  ```
- **Action:** Implement `Append(entryType, data) (uint64, error)`. Entry format: `Type(1) + Size(4) + Timestamp(8) + Data(var) + CRC32C(4)`. CRC computed over Type+Size+Timestamp+Data using Castagnoli polynomial. Use `bufio.Writer` for batching. Rotate when `activeSize + entrySize > maxSize`. Immediate fsync if `SyncImmediate`. Update `offset` tracking.
- **Test:** Append returns monotonically increasing offsets. Entry bytes on disk match expected format. CRC is correct (verify manually). Append triggers rotate at maxSize boundary. Append with SyncImmediate calls fsync (verify via file mod time).

### P1-03.04 — WAL fsync interval ticker
- **Status:** `[ ]`
- **Deps:** P1-03.03
- **Files:**
  ```
  internal/storage/wal/wal.go (extend)
  internal/storage/wal/wal_test.go (extend)
  ```
- **Action:** Implement `syncLoop()` goroutine — ticker fires at `syncInterval`, flushes `bufio.Writer` and calls `file.Sync()`. Start in `NewWAL` if mode is `SyncInterval`. Stop via `syncStop` channel on `Close()`.
- **Test:** With SyncInterval=50ms, data visible on disk after 50ms even without explicit flush. Stop channel terminates goroutine.

### P1-03.05 — WAL recovery (read and replay)
- **Status:** `[ ]`
- **Deps:** P1-03.03
- **Files:**
  ```
  internal/storage/wal/wal.go (extend)
  internal/storage/wal/wal_test.go (extend)
  ```
- **Action:** Implement `Recover(fromOffset, fn) error`. Iterate all segments. For each segment, read entries sequentially: header(17 bytes) → data(Size bytes) → verify CRC. On EOF: move to next segment. On partial read (< header size or < data size): truncate file at last valid entry (crash recovery). On CRC mismatch: truncate at last valid entry. Call `fn(EntryType, data)` for each valid entry.
- **Test:** Write 100 entries, recover reads all 100. Simulate crash (truncate mid-entry): recover reads N-1 valid entries. Corrupt CRC in last entry: recover reads N-1 entries. Empty WAL: recover returns no entries. Multi-segment recovery (write enough to rotate, then recover all).

### P1-03.06 — WAL checkpoint and truncation
- **Status:** `[ ]`
- **Deps:** P1-03.05
- **Files:**
  ```
  internal/storage/wal/wal.go (extend)
  internal/storage/wal/wal_test.go (extend)
  ```
- **Action:** Implement `Checkpoint(offset) error` — write offset to `{dir}/checkpoint` file. Implement `readCheckpoint() (uint64, error)`. Implement `Truncate() error` — read checkpoint, delete segments fully before checkpoint (keep at least one).
- **Test:** Checkpoint writes correct value. Truncate removes old segments. Truncate keeps current segment. No checkpoint file = Truncate is no-op.

---

## MILESTONE 4: Hot Tier Storage

### P1-04.01 — Segment file header and creation
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/storage/hot/segment.go
  internal/storage/hot/errors.go
  internal/storage/hot/segment_test.go
  ```
- **Action:** Define `SegmentMagic = 0x43534731`, `SegmentHeaderLen = 32`. Implement `Segment` struct. Implement `OpenSegment(path, baseOffset, maxSize)`. New file: write 32-byte header (Magic(4) + Version(4) + BaseOffset(8) + Created(8) + Reserved(8)). Existing file: read header, validate magic, extract baseOffset. Define errors: `ErrSegmentFull`, `ErrPositionOutOfBounds`, `ErrOffsetTooOld`.
- **Test:** New segment creates file with 32-byte header. Magic bytes correct. Re-open existing segment reads baseOffset correctly. Invalid magic returns error.

### P1-04.02 — Segment append (write path)
- **Status:** `[ ]`
- **Deps:** P1-04.01
- **Files:**
  ```
  internal/storage/hot/segment.go (extend)
  internal/storage/hot/segment_test.go (extend)
  ```
- **Action:** Implement `Append(data []byte) (offset uint64, position int64, err error)`. Record format: `Length(4 bytes, BigEndian) + Data(variable)`. Check `size + recordSize > maxSize` → return `ErrSegmentFull`. Write at current `size` position using `WriteAt`. Increment `size`, `nextOff`. No mutex in Append — caller (Partition) holds lock.
- **Test:** Append returns sequential offsets (0, 1, 2...). File size grows by 4+len(data) per append. Append at maxSize boundary returns ErrSegmentFull. Data on disk matches input.

### P1-04.03 — Sparse index (in-memory + persistence)
- **Status:** `[ ]`
- **Deps:** P1-04.02
- **Files:**
  ```
  internal/storage/hot/index.go
  internal/storage/hot/index_test.go
  ```
- **Action:** Implement `SparseIndex` struct with `entries []IndexEntry`, `interval uint32` (default 256). `IndexEntry`: Offset(uint64) + Position(uint32) + Timestamp(int64). Implement `Add(offset, position, timestamp)` — only adds if `offset % interval == 0`. Implement `Search(targetOffset) (position int64, found bool)` — binary search for nearest entry <= target. Implement `SaveIndex(path)` — write binary: 20 bytes per entry (8+4+8). Implement `LoadIndex(path)` — read binary back.
- **Test:** Index adds entry every 256th message. Search finds exact match. Search finds nearest-before for non-indexed offset. Save/Load roundtrip preserves all entries. Empty index returns position 0. Benchmark: search 100K-entry index < 1μs.

### P1-04.04 — Segment read and offset lookup
- **Status:** `[ ]`
- **Deps:** P1-04.02, P1-04.03
- **Files:**
  ```
  internal/storage/hot/segment.go (extend)
  internal/storage/hot/segment_test.go (extend)
  ```
- **Action:** Implement `ReadAt(position int64) ([]byte, error)` — read 4-byte length prefix, then data. Implement `FindPosition(targetOffset) (int64, error)` — binary search sparse index for nearest entry, then linear scan from there to target offset (read length-prefixed records to skip forward). Return `ErrOffsetTooOld` if target < baseOff.
- **Test:** Write 1000 messages, read each by offset — data matches. FindPosition for indexed offset is exact. FindPosition for non-indexed offset requires linear scan (verify correct). FindPosition for offset < baseOff returns error. Random access pattern works correctly.

### P1-04.05 — Segment freeze and mmap
- **Status:** `[ ]`
- **Deps:** P1-04.04
- **Files:**
  ```
  internal/storage/hot/segment.go (extend)
  internal/storage/hot/segment_test.go (extend)
  ```
- **Action:** Implement `Freeze() error` — set `frozen=true`, fsync file, create mmap with `PROT_READ | MAP_SHARED`, call `MADV_SEQUENTIAL`. Implement `readFromMmap(position) ([]byte, error)` — zero-copy read from mmap slice. Modify `ReadAt` to prefer mmap when available. Implement `Close()` — munmap if mapped, close file.
- **Test:** After Freeze, segment is read-only (Append returns error). Mmap read returns same data as file read. Zero-copy verified: returned slice points into mmap region. Close releases mmap. Freeze on non-Linux (if applicable): fallback to file reads.

### P1-04.06 — Segment index rebuild on recovery
- **Status:** `[ ]`
- **Deps:** P1-04.03, P1-04.04
- **Files:**
  ```
  internal/storage/hot/segment.go (extend)
  internal/storage/hot/segment_test.go (extend)
  ```
- **Action:** Implement `rebuildIndex() error` — scan segment from `SegmentHeaderLen` reading all records, rebuild sparse index entries. Count messages to determine `nextOff = baseOff + count`. Used when `.idx` file is missing or corrupt.
- **Test:** Write 1000 messages, delete .idx file, reopen segment — index rebuilt correctly. nextOff matches expected value. Read after rebuild returns correct data.

### P1-04.07 — Partition manager
- **Status:** `[ ]`
- **Deps:** P1-04.01 through P1-04.06
- **Files:**
  ```
  internal/storage/hot/partition.go
  internal/storage/hot/partition_test.go
  ```
- **Action:** Implement `Partition` struct managing multiple segments for one topic-partition. Implement `OpenPartition(dir, topic, id, maxSegSize)` — create directory, load existing segments (sorted by filename/baseOffset), set active = last segment, compute highWater. Implement `Append(data) (uint64, error)` — write to active, on `ErrSegmentFull` freeze active + save index + create new segment + retry. Implement `Read(offset) ([]byte, error)` — find segment via `findSegment` (binary search by baseOffset), then FindPosition + ReadAt. Implement `ReadRange(start, end, max) ([][]byte, error)`. Implement `HighWatermark()`, `LogStartOffset()`. Implement `Close()` — close all segments.
- **Test:** Append across segment boundary works seamlessly. Read from any segment works. HighWatermark tracks latest offset. Multi-segment partition: write 10K messages (multiple rollovers), read all back. LogStartOffset returns first segment's base. Close all segments without leak.

### P1-04.08 — Storage engine (partition registry)
- **Status:** `[ ]`
- **Deps:** P1-04.07
- **Files:**
  ```
  internal/storage/hot/engine.go
  internal/storage/hot/engine_test.go
  ```
- **Action:** Implement `Engine` struct with `partitions map[string]map[uint32]*Partition`, `baseDir`, `config`. Implement `NewEngine(baseDir, cfg)`. Implement `GetOrCreatePartition(topic, partID) (*Partition, error)` — lazy creation with double-checked locking. Implement `Close()` — close all partitions.
- **Test:** GetOrCreatePartition creates on first call, returns same on second. Multiple topics with multiple partitions. Close releases all resources. Concurrent GetOrCreatePartition is safe.

---

## MILESTONE 5: Topic Manager

### P1-05.01 — Topic metadata persistence
- **Status:** `[ ]`
- **Deps:** P1-04.08, P1-03.02
- **Files:**
  ```
  internal/broker/topic.go
  internal/broker/topic_test.go
  ```
- **Action:** Define `TopicMode` (Stream=0, Queue=1, Unified=2), `TopicConfig` struct. Implement `TopicManager` with `topics map`, `metaPath`, references to storage + WAL. Implement `loadMetadata()` — read `meta.json`, unmarshal. Implement `saveMetadata()` — marshal to JSON, write atomically (write temp + rename). Implement `NewTopicManager(dataDir, storage, wal)` — load metadata, initialize partitions for each existing topic.
- **Test:** Create topic manager with empty dir — no error. Save then load roundtrip preserves topics. Atomic write: crash during save doesn't corrupt existing meta.json.

### P1-05.02 — Topic CRUD operations
- **Status:** `[ ]`
- **Deps:** P1-05.01
- **Files:**
  ```
  internal/broker/topic.go (extend)
  internal/broker/topic_test.go (extend)
  ```
- **Action:** Implement `CreateTopic(cfg) error` — validate name (alphanumeric + . - _, 1-255 chars, no leading . or -), check duplicate, WAL append, create partitions, add to map, save metadata. Implement `DeleteTopic(name) error` — remove from map, save metadata (physical cleanup deferred). Implement `GetTopic(name) (*TopicConfig, bool)`. Implement `ListTopics() []*TopicConfig`.
- **Test:** Create topic succeeds and persists. Duplicate name returns error. Invalid name (empty, special chars, too long) returns error. Delete removes from list. Get returns nil for unknown topic. List returns all topics.

### P1-05.03 — Partition routing (Murmur3 + round-robin)
- **Status:** `[ ]`
- **Deps:** P1-05.02
- **Files:**
  ```
  internal/broker/murmur3.go
  internal/broker/murmur3_test.go
  internal/broker/topic.go (extend)
  ```
- **Action:** Implement pure Go `murmur3Hash([]byte) uint32` — standard Murmur3-32 with seed=0. Implement `ResolvePartition(topic, routingKey, partitionCount) uint32` — if routingKey empty, use atomic round-robin counter per topic; if routingKey present, use `murmur3Hash(key) % partitionCount`.
- **Test:** Murmur3 known test vectors (verify against reference implementation). Same key always maps to same partition. Empty routing key distributes evenly (round-robin). Distribution across 8 partitions with 10K random keys is roughly uniform (chi-square test, p > 0.01). Benchmark: > 50M hashes/sec.

---

## MILESTONE 6: Queue Engine (Lion Head)

### P1-06.01 — Queue consumer struct and registration
- **Status:** `[ ]`
- **Deps:** P1-05.02
- **Files:**
  ```
  internal/engine/queue/consumer.go
  internal/engine/queue/engine.go
  ```
- **Action:** Define `QueueConsumer` struct (ID, prefetch, inFlight map, ackBitmap, mutex). Define `QueueState` per topic. Implement `Engine` struct with `queues map[string]*QueueState`, storage and topics references. Implement `AddConsumer(topic, consumer, conn)`, `RemoveConsumer(topic, consumerID)`.
- **Test:** Add consumer appears in queue state. Remove consumer removes from state. Remove non-existent consumer is no-op.

### P1-06.02 — Round-robin dispatcher with prefetch
- **Status:** `[ ]`
- **Deps:** P1-06.01
- **Files:**
  ```
  internal/engine/queue/dispatcher.go
  internal/engine/queue/dispatcher_test.go
  ```
- **Action:** Implement `Dispatcher` struct with consumer list, nextIdx, visTimeout. Implement `Dispatch(offset, envelope) (consumerID string, error)` — round-robin through consumers, skip those at prefetch capacity, add to consumer's inFlight map. Return `ErrNoConsumers` if empty, `ErrAllConsumersBusy` if all at capacity.
- **Test:** Single consumer gets all messages. Two consumers get alternating messages. Consumer at prefetch limit is skipped. All consumers busy returns error. No consumers returns error. Remove consumer mid-dispatch rebalances correctly.

### P1-06.03 — Ack tracker with visibility timeout
- **Status:** `[ ]`
- **Deps:** P1-06.01
- **Files:**
  ```
  internal/engine/queue/ack.go
  internal/engine/queue/ack_test.go
  ```
- **Action:** Implement `AckTracker` with pending map, visTimeout, redeliverChan, stopChan. Implement `NewAckTracker(visTimeout)` — starts `visibilityTimeoutLoop` goroutine (1s ticker). Implement `Track(offset, consumerID, deliverCount, maxRetries)`. Implement `Ack(offset) bool`. Implement `Nack(offset) (shouldDLQ bool, deliverCount uint32)` — increment count, check maxRetries, push to redeliverChan or return shouldDLQ=true.
- **Test:** Track → Ack removes from pending. Track → Nack requeues (appears on redeliverChan). Nack after maxRetries returns shouldDLQ=true. Visibility timeout: Track without ack → after timeout, offset appears on redeliverChan. Ack unknown offset returns false. Stop closes goroutine cleanly.

### P1-06.04 — Dead-letter queue manager
- **Status:** `[ ]`
- **Deps:** P1-06.03, P1-04.08, P1-02.04
- **Files:**
  ```
  internal/engine/queue/dlq.go
  internal/engine/queue/dlq_test.go
  ```
- **Action:** Implement `DLQManager` with dlqTopic, storage, topics references. Implement `Route(original *Envelope, reason string, deliverCount uint32) error` — clone envelope, add x-chimera-original-topic, x-chimera-death-reason, x-chimera-death-count, x-chimera-first-death-time, x-chimera-original-routing-key headers. New MessageID, new Timestamp. Marshal and append to DLQ topic partition.
- **Test:** Route writes message to DLQ topic. DLQ message has all required headers. Original message unchanged. DLQ disabled (empty topic) = no-op. DLQ topic not found returns error.

### P1-06.05 — Delay scheduler (min-heap)
- **Status:** `[ ]`
- **Deps:** P1-02.01
- **Files:**
  ```
  internal/engine/queue/delay.go
  internal/engine/queue/delay_test.go
  ```
- **Action:** Implement `DelayScheduler` with `container/heap`-compatible min-heap sorted by deliverAt. Implement `NewDelayScheduler()` — starts promotionLoop goroutine (100ms ticker). Implement `Schedule(env)` — push to heap. `promotionLoop`: pop all items where `deliverAt <= now`, send to `readyCh`. Implement `Ready() <-chan *Envelope`. Implement `Stop()`.
- **Test:** Schedule message with 200ms delay → appears on Ready() after ~200ms. Schedule multiple → delivered in time order. Schedule with past time → immediate delivery. Stop terminates goroutine. Benchmark: schedule+promote 100K messages.

### P1-06.06 — Queue engine integration (TryDispatch)
- **Status:** `[ ]`
- **Deps:** P1-06.02, P1-06.03, P1-06.04, P1-06.05
- **Files:**
  ```
  internal/engine/queue/engine.go (extend)
  internal/engine/queue/engine_test.go
  ```
- **Action:** Implement `TryDispatch(topic, partID, offset, envelope)` — get/create QueueState, call dispatcher.Dispatch, if consumer found: track in ackTracker, send envelope to consumer's connection. Implement `HandleAck(topic, offset)` — ack in tracker, update consumer inFlight. Implement `HandleNack(topic, offset)` — nack in tracker, if shouldDLQ route to DLQ, else redeliver. Implement `ScheduleDelayed(env)` — add to delay scheduler. Wire delay scheduler Ready channel to dispatch loop.
- **Test:** Full flow: add consumer → publish → consumer receives → ack → done. Nack → redeliver to same/other consumer. Max retries → DLQ. Delayed message → delivered after delay. No consumers → message waits until consumer joins.

---

## MILESTONE 7: Stream Engine (Goat Head)

### P1-07.01 — Offset store (persistence)
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/engine/stream/offset.go
  internal/engine/stream/offset_test.go
  ```
- **Action:** Implement `OffsetStore` with dir, cache (`map[string]map[uint32]uint64`). Implement `NewOffsetStore(dataDir)` — create consumers dir, loadAll from disk. Implement `Save(group, partID, offset)` — update cache, persist to `{dir}/{group}/offsets.json`. Implement `Get(group, partID) uint64` — return from cache (0 if not found). Implement `loadAll()` — scan consumer dirs.
- **Test:** Save then Get returns correct offset. Persist survives restart (new OffsetStore reads saved data). Multiple groups isolated. Unknown group returns 0.

### P1-07.02 — Consumer group struct and join/leave
- **Status:** `[ ]`
- **Deps:** P1-07.01
- **Files:**
  ```
  internal/engine/stream/consumer_group.go
  internal/engine/stream/consumer_group_test.go
  ```
- **Action:** Define `ConsumerGroup` struct (name, topic, members map, assignments map, committed map, strategy, sessionTimeout). Define `GroupMember` (ID, conn, partitions, lastHeartbeat, fetchCh). Implement `NewConsumerGroup(...)`. Implement `Join(memberID, conn)` — add member, trigger rebalance. Implement `Leave(memberID)` — remove member, trigger rebalance. Implement `Heartbeat(memberID) error`. Implement `CommitOffset(partID, offset)`. Implement `GetCommittedOffset(partID) uint64`.
- **Test:** Join adds member and triggers rebalance. Leave removes member. Heartbeat updates timestamp. Commit persists offset. Unknown member heartbeat returns error.

### P1-07.03 — Rebalancing algorithms (Range, RoundRobin, Sticky)
- **Status:** `[ ]`
- **Deps:** P1-07.02
- **Files:**
  ```
  internal/engine/stream/rebalance.go
  internal/engine/stream/rebalance_test.go
  ```
- **Action:** Implement `rebalance()` on ConsumerGroup — clears assignments, calls strategy-specific method. **Range:** divide partitions into consecutive ranges per member (remainder distributed to first N members). **RoundRobin:** partition `i` → member `i % numMembers`. **Sticky:** for Phase 1, same as RoundRobin (full sticky in later phase). All methods sort member IDs for determinism.
- **Test:** Range: 8 partitions / 3 members → [0,1,2], [3,4,5], [6,7]. RoundRobin: 8/3 → [0,3,6], [1,4,7], [2,5]. All partitions assigned exactly once. Member join/leave → reassignment. Single member gets all partitions. Zero members → empty assignments.

### P1-07.04 — Heartbeat timeout and session expiry
- **Status:** `[ ]`
- **Deps:** P1-07.02
- **Files:**
  ```
  internal/engine/stream/consumer_group.go (extend)
  internal/engine/stream/consumer_group_test.go (extend)
  ```
- **Action:** Implement `heartbeatLoop()` goroutine — every `sessionTimeout/3`, check all members. If `time.Since(lastHeartbeat) > sessionTimeout`, call `Leave(memberID)` (triggers rebalance). Start in `NewConsumerGroup`, stop via context cancellation.
- **Test:** Member with no heartbeat for > sessionTimeout is removed. Regular heartbeats keep member alive. Removal triggers rebalance.

### P1-07.05 — Waiter registry (new message notification)
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/engine/stream/waiter.go
  internal/engine/stream/waiter_test.go
  ```
- **Action:** Implement `WaiterRegistry` with `waiters map[string]map[uint32][]chan struct{}`. Implement `Register(topic, partID) chan struct{}`. Implement `Unregister(topic, partID, ch)`. Implement `Notify(topic, partID)` — non-blocking send to all registered channels.
- **Test:** Register returns channel. Notify triggers channel. Unregister removes channel. Notify with no waiters is no-op. Multiple waiters all notified. Non-blocking: Notify doesn't block if channel full.

### P1-07.06 — Stream engine with Fetch (long-poll)
- **Status:** `[ ]`
- **Deps:** P1-07.01, P1-07.02, P1-07.05, P1-04.08
- **Files:**
  ```
  internal/engine/stream/engine.go
  internal/engine/stream/engine_test.go
  ```
- **Action:** Implement `StreamEngine` with groups map, storage, offsetStore, waiterRegistry. Implement `Fetch(topic, partID, fromOffset, maxMessages, maxWait) ([]*Envelope, uint64, error)` — if data available (fromOffset <= highWater), read immediately; else register waiter, select on waiter channel or timeout. Implement `JoinGroup(group, topic, memberID, startOffset, conn)`. Implement `NotifyWaiters(topic, partID)`. Implement consumer group management methods.
- **Test:** Fetch with available data returns immediately. Fetch with no data blocks until timeout. Fetch with no data returns when new message arrives (Notify). Long-poll doesn't leak goroutines. Multiple concurrent fetches work correctly.

---

## MILESTONE 8: Unified Mode & Broker Integration

### P1-08.01 — Broker struct and lifecycle
- **Status:** `[ ]`
- **Deps:** P1-01.02, P1-01.03, P1-03.02, P1-04.08, P1-05.02, P1-06.06, P1-07.06
- **Files:**
  ```
  internal/broker/broker.go
  ```
- **Action:** Implement `Broker` struct holding config, logger, WAL, storage engine, topic manager, queue engine, stream engine, protocol server, HTTP server, metrics. Implement `NewBroker(cfg)`. Implement `Start() error` — full bootstrap sequence (15 steps from IMPLEMENTATION.md Section 1.2). Lock file acquisition. Implement `Stop() error` — graceful shutdown sequence. Implement accessor methods: `Config()`, `Topics()`, `Storage()`, `QueueEngine()`, `StreamEngine()`, `Metrics()`, `StartTime()`.
- **Test:** Broker starts and stops without error. Lock file prevents second instance. Start initializes all components. Stop releases all resources.

### P1-08.02 — Unified publish path
- **Status:** `[ ]`
- **Deps:** P1-08.01
- **Files:**
  ```
  internal/broker/publish.go
  internal/broker/publish_test.go
  ```
- **Action:** Implement `Publish(env *Envelope) (uint64, error)` — validate topic exists, resolve partition, handle delayed messages, assign MessageID + Timestamp + Sequence, marshal, WAL append, hot storage append, notify stream waiters, try dispatch to queue consumers (if queue/unified mode), update metrics. Return assigned offset.
- **Test:** Publish to stream topic: stored, waiters notified, no queue dispatch. Publish to queue topic: stored, queue dispatched. Publish to unified topic: stored, waiters notified AND queue dispatched. Publish to nonexistent topic: error. Delayed message: routed to delay scheduler. MessageID assigned. Timestamp assigned if zero.

### P1-08.03 — Unified mode integration test
- **Status:** `[ ]`
- **Deps:** P1-08.02
- **Files:**
  ```
  internal/broker/unified_test.go
  ```
- **Action:** Write comprehensive integration test: create unified topic → publish 100 messages → stream consumer reads all 100 by offset → queue consumer receives all 100 by dispatch → verify same data, no duplication → stream consumer replays from offset 50 → queue consumer acks all → verify ack tracker empty.
- **Test:** This IS the test. All assertions must pass.

---

## MILESTONE 9: Chimera Native Protocol

### P1-09.01 — Frame codec (encode/decode)
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/protocol/chimera/codec.go
  internal/protocol/chimera/codec_test.go
  ```
- **Action:** Define `FrameMagic = "CHMR"`, `FrameVersion = 1`, `FrameHeaderLen = 11`, `FrameTrailerLen = 4`. Define all `OpCode` constants. Implement `EncodeFrame(f *Frame) ([]byte, error)` — header(11) + payload(var) + CRC32C(4). Implement `DecodeFrame(reader io.Reader) (*Frame, error)` — read header, validate magic, read payload + CRC, verify CRC. Max frame size check (16MB).
- **Test:** Roundtrip encode/decode for each OpCode. Invalid magic rejected. CRC mismatch rejected. Oversized frame rejected. Empty payload frame works. Benchmark: > 5M frames/sec encode+decode.

### P1-09.02 — Protocol payload encoders/decoders
- **Status:** `[ ]`
- **Deps:** P1-09.01
- **Files:**
  ```
  internal/protocol/chimera/payloads.go
  internal/protocol/chimera/payloads_test.go
  ```
- **Action:** Implement encode/decode for each payload type: `ConnectPayload` (ClientID, Username, Password, Keepalive), `ConnAckPayload` (ClientID, Status), `PublishPayload` (Topic, RoutingKey, Priority, TTL, DeliverAt, Headers, Body), `PubAckPayload` (Topic, PartitionID, Offset), `SubscribePayload` (Topic, Mode, ConsumerGroup, Prefetch, StartOffset), `SubAckPayload` (Topic, Success), `FetchPayload` (Topic, PartitionID, Offset, MaxMessages, MaxWaitMS), `FetchRespPayload` (PartitionID, Messages, HighWater, NextOffset), `AckPayload` / `NackPayload` (Topic, PartitionID, Offsets), `CommitOffsetPayload` (Topic, Group, PartitionID, Offset), `ErrorPayload` (Code, Message). Length-prefixed strings: `uint16 length + bytes`.
- **Test:** Roundtrip each payload type. Empty optional fields handled. Multiple offsets in Ack. Binary safety (payload with zero bytes).

### P1-09.03 — TCP server and connection accept loop
- **Status:** `[ ]`
- **Deps:** P1-09.01
- **Files:**
  ```
  internal/protocol/chimera/server.go
  ```
- **Action:** Implement `Server` struct with listener, broker ref, clients sync.Map, context. Implement `NewServer(broker, bind, port)` — create TCP listener. Implement `Serve()` — accept loop, spawn goroutine per connection. Implement `StopAccepting()` — close listener. Implement `DisconnectAll()` — iterate clients, close connections.
- **Test:** Server accepts TCP connections. StopAccepting prevents new connections. DisconnectAll closes all.

### P1-09.04 — Client connection handler (CONNECT flow)
- **Status:** `[ ]`
- **Deps:** P1-09.02, P1-09.03
- **Files:**
  ```
  internal/protocol/chimera/handler.go
  internal/protocol/chimera/client.go
  ```
- **Action:** Implement `ClientConn` struct (conn, reader, writer, clientID, subs, writeCh, done). Implement `StartWriteLoop()` — goroutine reads from writeCh, writes to conn with write coalescing (flush when channel empty). Implement `Send(frame)` async. Implement `SendSync(frame)` immediate. Implement `handleConnection(conn)` — expect CONNECT as first frame, extract ClientID (generate if empty), store in clients map, send CONNACK, enter main read loop dispatching by OpCode.
- **Test:** Client connects and receives CONNACK. Auto-generated ClientID if empty. Non-CONNECT first frame → error + disconnect. Keepalive timeout disconnects idle client.

### P1-09.05 — Publish/PubAck handler
- **Status:** `[ ]`
- **Deps:** P1-09.04, P1-08.02
- **Files:**
  ```
  internal/protocol/chimera/handler.go (extend)
  ```
- **Action:** Implement `handlePublish(client, frame)` — decode PublishPayload, create Envelope (SourceProto=Chimera), call `broker.Publish()`, encode PubAck with offset, send to client.
- **Test:** Publish via protocol → PubAck received with correct offset. Message persisted in storage. Error publish → Error frame returned.

### P1-09.06 — Subscribe handler (queue + stream modes)
- **Status:** `[ ]`
- **Deps:** P1-09.04, P1-06.06, P1-07.06
- **Files:**
  ```
  internal/protocol/chimera/handler.go (extend)
  ```
- **Action:** Implement `handleSubscribe(client, frame)` — decode SubscribePayload. If mode=queue: create QueueConsumer, register with queue engine, wire dispatch to client's Send. If mode=stream: join consumer group, wire fetch responses to client's Send. Send SubAck.
- **Test:** Subscribe queue mode: consumer registered, receives dispatched messages. Subscribe stream mode: consumer joins group, partitions assigned.

### P1-09.07 — Fetch handler (stream long-poll)
- **Status:** `[ ]`
- **Deps:** P1-09.06
- **Files:**
  ```
  internal/protocol/chimera/handler.go (extend)
  ```
- **Action:** Implement `handleFetch(client, frame)` — decode FetchPayload, call `streamEngine.Fetch(topic, partID, offset, maxMessages, maxWait)`, encode FetchResp with messages + highWater + nextOffset, send to client.
- **Test:** Fetch returns available messages. Fetch with no data waits up to maxWait then returns empty. Fetch returns when new message arrives.

### P1-09.08 — Ack/Nack/CommitOffset handlers
- **Status:** `[ ]`
- **Deps:** P1-09.06
- **Files:**
  ```
  internal/protocol/chimera/handler.go (extend)
  ```
- **Action:** Implement `handleAck` — decode, call queueEngine.HandleAck for each offset. Implement `handleNack` — decode, call queueEngine.HandleNack. Implement `handleCommitOffset` — decode, call consumerGroup.CommitOffset.
- **Test:** Ack clears in-flight. Nack redelivers. Nack after max retries → DLQ. CommitOffset persists.

### P1-09.09 — Ping/Pong and Disconnect
- **Status:** `[ ]`
- **Deps:** P1-09.04
- **Files:**
  ```
  internal/protocol/chimera/handler.go (extend)
  ```
- **Action:** Ping → respond with Pong. Disconnect → clean up subscriptions (leave consumer groups, remove queue consumers), close connection.
- **Test:** Ping/Pong roundtrip. Disconnect cleans up all state.

### P1-09.10 — CreateTopic/DeleteTopic over protocol
- **Status:** `[ ]`
- **Deps:** P1-09.04, P1-05.02
- **Files:**
  ```
  internal/protocol/chimera/handler.go (extend)
  ```
- **Action:** Implement `handleCreateTopic` — decode, call topicManager.CreateTopic. Implement `handleDeleteTopic` — decode, call topicManager.DeleteTopic. Both respond with success frame or Error frame.
- **Test:** Create topic via protocol → topic exists. Delete via protocol → topic gone. Duplicate create → error. Delete nonexistent → error.

---

## MILESTONE 10: HTTP Admin API

### P1-10.01 — HTTP server with stdlib router
- **Status:** `[ ]`
- **Deps:** P1-08.01
- **Files:**
  ```
  internal/protocol/http/server.go
  ```
- **Action:** Implement `AdminServer` with `http.ServeMux` (Go 1.22+ pattern routing). Register all routes. Implement `Serve()` and `Shutdown(ctx)`. Set timeouts (read=30s, write=30s, idle=120s). Helper functions: `writeJSON(w, status, v)`, `writeError(w, status, msg)`.
- **Test:** Server starts and responds to health check.

### P1-10.02 — Topic CRUD endpoints
- **Status:** `[ ]`
- **Deps:** P1-10.01, P1-05.02
- **Files:**
  ```
  internal/protocol/http/topics.go
  internal/protocol/http/topics_test.go
  ```
- **Action:** `POST /v1/topics` — create topic (JSON body: name, mode, partitions). `GET /v1/topics` — list all topics. `GET /v1/topics/{name}` — topic details with partition stats (highWater, logStart per partition). `DELETE /v1/topics/{name}` — delete topic.
- **Test:** Full CRUD cycle via HTTP. Invalid JSON → 400. Duplicate → 409. Not found → 404. Partition stats correct.

### P1-10.03 — Message publish/fetch endpoints
- **Status:** `[ ]`
- **Deps:** P1-10.01, P1-08.02
- **Files:**
  ```
  internal/protocol/http/messages.go
  internal/protocol/http/messages_test.go
  ```
- **Action:** `POST /v1/messages/{topic}` — publish (body = payload, X-Routing-Key header). `GET /v1/messages/{topic}?partition=N&offset=N&max=N` — fetch messages (stream mode). `POST /v1/messages/{topic}/ack` — ack offsets (JSON body: consumer_id, offsets[]).
- **Test:** Publish returns offset+partition. Fetch returns messages with next_offset. Fetch with no data returns empty array. Ack succeeds.

### P1-10.04 — Consumer group and health endpoints
- **Status:** `[ ]`
- **Deps:** P1-10.01
- **Files:**
  ```
  internal/protocol/http/consumers.go
  internal/protocol/http/health.go
  ```
- **Action:** `GET /v1/consumers` — list consumer groups. `GET /v1/consumers/{group}` — group details (members, assignments, committed offsets, lag). `GET /v1/health` — status, node_id, name, uptime.
- **Test:** Health returns 200 with correct node info. Consumer endpoints return expected data.

---

## MILESTONE 11: Prometheus Metrics

### P1-11.01 — Metrics collector (pure Go Prometheus exposition)
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  internal/metrics/collector.go
  internal/metrics/collector_test.go
  ```
- **Action:** Implement `Collector` with counters (`map[string]*Counter`) and gauges (`map[string]*Gauge`). Counter: `IncrCounter(name, labels, delta)`. Gauge: `SetGauge(name, labels, value)`. Labels encoded as sorted `key="val"` string. Implement `Expose() string` — Prometheus text format (`# TYPE` lines + `metric{labels} value` lines). Thread-safe with fine-grained mutexes.
- **Test:** IncrCounter increments value. SetGauge sets value. Expose produces valid Prometheus format. Concurrent access is safe. Labels sorted deterministically.

### P1-11.02 — Broker metric instrumentation
- **Status:** `[ ]`
- **Deps:** P1-11.01, P1-08.02
- **Files:**
  ```
  internal/metrics/broker_metrics.go
  ```
- **Action:** Add convenience methods: `MessageIn(topic, partition, proto)`, `MessageOut(topic, partition, group)`, `ActiveConnections(proto, count)`, `QueueDepth(topic, depth)`, `ConsumerLag(topic, partition, group, lag)`. Wire into Broker.Publish, queue dispatch, stream fetch, connection handler. Expose via `GET /v1/metrics`.
- **Test:** After publish, `chimera_messages_in_total` counter increments. After consume, `chimera_messages_out_total` increments. Metrics endpoint returns valid Prometheus text.

---

## MILESTONE 12: CLI Commands

### P1-12.01 — Server command
- **Status:** `[ ]`
- **Deps:** P1-08.01
- **Files:**
  ```
  internal/cli/server.go
  ```
- **Action:** Implement `runServer(args)` — parse flags (--config, --data-dir, --bind, --port, --admin-port, --log-level), load config, create broker, start, wait for SIGINT/SIGTERM, stop.
- **Test:** `chimera server --config test.yaml` starts and stops cleanly. Signal handling works.

### P1-12.02 — Topic CLI commands
- **Status:** `[ ]`
- **Deps:** P1-10.02
- **Files:**
  ```
  internal/cli/topic.go
  ```
- **Action:** Implement `runTopicCLI(args)` — subcommands: `create` (--name, --mode, --partitions), `list`, `describe <name>`, `delete <name>`. All use HTTP admin API. Support `--admin-addr` flag and `CHIMERA_ADMIN_ADDR` env var (default `http://localhost:9090`).
- **Test:** Create/list/describe/delete cycle works against running broker.

### P1-12.03 — Produce/Consume CLI commands
- **Status:** `[ ]`
- **Deps:** P1-10.03
- **Files:**
  ```
  internal/cli/produce.go
  internal/cli/consume.go
  ```
- **Action:** `chimera produce --topic T --message "hello"` — POST to HTTP API. Support `--count N` for multiple sends. Support stdin if --message empty. `chimera consume --topic T --partition P --offset N --max M` — GET from HTTP API. Support `--follow` for continuous mode (loop with 500ms sleep on empty). Support `--offset -1` (latest), `--offset -2` (earliest, default 0).
- **Test:** Produce sends message, consume reads it back. Follow mode shows new messages as they arrive. Count mode sends multiple.

---

## MILESTONE 13: Integration Testing

### P1-13.01 — End-to-end queue mode test
- **Status:** `[ ]`
- **Deps:** All previous milestones
- **Files:**
  ```
  test/integration/queue_test.go
  ```
- **Action:** Start embedded broker. Create queue topic. Connect two consumers via Chimera protocol with prefetch=5. Publish 100 messages. Verify: messages distributed ~50/50. Ack all from consumer 1. Nack 5 from consumer 2. Verify redelivery. Verify DLQ after max retries. Verify queue depth metric.
- **Test:** This IS the test.

### P1-13.02 — End-to-end stream mode test
- **Status:** `[ ]`
- **Deps:** All previous milestones
- **Files:**
  ```
  test/integration/stream_test.go
  ```
- **Action:** Start embedded broker. Create stream topic (8 partitions). Connect 3 stream consumers in one group. Verify partition assignment (range or round-robin). Publish 1000 messages with routing keys. Verify all consumed. Commit offsets. Disconnect one consumer → verify rebalance. Reconnect → verify resume from committed offset. Replay from offset 0 → all messages available.
- **Test:** This IS the test.

### P1-13.03 — End-to-end unified mode test
- **Status:** `[ ]`
- **Deps:** All previous milestones
- **Files:**
  ```
  test/integration/unified_test.go
  ```
- **Action:** Start embedded broker. Create unified topic. Connect 1 stream consumer group + 2 queue consumers. Publish 200 messages. Verify: stream consumer sees all 200 via offset. Queue consumers see all 200 via dispatch (competing, ~100 each). No data duplication on disk. Both can operate simultaneously without interference.
- **Test:** This IS the test.

### P1-13.04 — Crash recovery test
- **Status:** `[ ]`
- **Deps:** All previous milestones
- **Files:**
  ```
  test/integration/recovery_test.go
  ```
- **Action:** Start broker. Create topic. Publish 500 messages. Force kill (no graceful shutdown — simulate os.Exit). Start new broker on same data directory. Verify: WAL replayed. All 500 messages readable. Topic metadata intact. No duplicate messages. HighWatermark correct.
- **Test:** This IS the test.

### P1-13.05 — HTTP API integration test
- **Status:** `[ ]`
- **Deps:** All previous milestones
- **Files:**
  ```
  test/integration/http_test.go
  ```
- **Action:** Start embedded broker. Full lifecycle via HTTP only: create topic → publish 50 messages → fetch → ack → list topics → describe topic → check metrics → health check → delete topic.
- **Test:** All HTTP endpoints return expected status codes and bodies.

---

## MILESTONE 14: Benchmarks

### P1-14.01 — Message codec benchmarks
- **Status:** `[ ]`
- **Deps:** P1-02.04
- **Files:**
  ```
  internal/message/codec_bench_test.go
  ```
- **Action:** Benchmark Marshal/Unmarshal for payloads: 100B, 1KB, 10KB, 100KB. Track: ops/sec, bytes/op, allocs/op. Target: > 2M ops/sec for 1KB.
- **Test:** Benchmarks run. Report results.

### P1-14.02 — Storage benchmarks
- **Status:** `[ ]`
- **Deps:** P1-04.07
- **Files:**
  ```
  internal/storage/hot/bench_test.go
  ```
- **Action:** Benchmark: sequential append throughput (messages/sec, MB/sec), random read throughput, sequential read throughput, FindPosition latency. Message sizes: 100B, 1KB, 10KB. Target: > 1M appends/sec for 1KB.
- **Test:** Benchmarks run. Report results.

### P1-14.03 — End-to-end publish/consume benchmark
- **Status:** `[ ]`
- **Deps:** P1-08.02
- **Files:**
  ```
  test/bench/e2e_bench_test.go
  ```
- **Action:** Benchmark: publish throughput (single publisher), consume throughput (single consumer), publish with 10 concurrent publishers, consume with 10 concurrent consumers. Track: messages/sec, P50/P99 latency, memory usage. Compare queue vs stream vs unified mode.
- **Test:** Benchmarks run. Report results.

### P1-14.04 — CLI benchmark command
- **Status:** `[ ]`
- **Deps:** P1-12.03
- **Files:**
  ```
  internal/cli/bench.go
  ```
- **Action:** `chimera bench produce --topic T --message-size 1024 --count 100000 --concurrency 4` — publish N messages with C concurrent producers, report throughput and latency. `chimera bench consume --topic T --count 100000 --concurrency 4` — consume N messages.
- **Test:** Bench command runs against live broker and reports results.

---

## MILESTONE 15: Documentation & Polish

### P1-15.01 — README.md
- **Status:** `[ ]`
- **Deps:** P1-13.05
- **Files:**
  ```
  README.md
  ```
- **Action:** Write comprehensive README: project description, tagline, architecture diagram (ASCII), features list, quick start (install, create topic, produce, consume), configuration reference, benchmarks section (placeholder), comparison table vs Kafka/RabbitMQ/NATS, license, contributing.
- **Test:** Quickstart instructions work on fresh machine.

### P1-15.02 — chimera.yaml.example with full comments
- **Status:** `[ ]`
- **Deps:** P1-01.02
- **Files:**
  ```
  configs/chimera.yaml.example
  ```
- **Action:** Write fully commented example config covering all Phase 1 options.
- **Test:** Example config loads without error.

### P1-15.03 — Code documentation (godoc)
- **Status:** `[ ]`
- **Deps:** All code milestones
- **Files:**
  All `.go` files
- **Action:** Ensure every exported type, function, and method has godoc comment. Package-level doc comments for each package in `doc.go`. Run `go vet ./...` clean. Run `staticcheck ./...` clean.
- **Test:** `go vet ./...` passes. `staticcheck ./...` passes. `go doc ./...` renders clean docs.

---

## MILESTONE 16: CI/CD & Release

### P1-16.01 — GitHub Actions CI
- **Status:** `[ ]`
- **Deps:** P1-01.01
- **Files:**
  ```
  .github/workflows/ci.yml
  ```
- **Action:** CI pipeline: checkout → setup Go 1.23 → `go vet ./...` → `staticcheck ./...` → `go test -race ./...` → `make build` → upload binary artifact. Run on push to main and PRs.
- **Test:** CI passes on push.

### P1-16.02 — Release workflow
- **Status:** `[ ]`
- **Deps:** P1-16.01
- **Files:**
  ```
  .github/workflows/release.yml
  ```
- **Action:** On tag push (`v*`): build for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64. Create GitHub release with binaries. Build and push Docker image to ghcr.io.
- **Test:** Tag push produces release with all binaries.

---

## DEPENDENCY GRAPH (Critical Path)

```
P1-01 (Scaffold) ──────────────────────────────────────┐
  │                                                      │
  ├─→ P1-02 (Envelope) ────────────────────────┐        │
  │                                              │        │
  ├─→ P1-03 (WAL) ─────────────────────────┐   │        │
  │                                          │   │        │
  ├─→ P1-04 (Hot Storage) ──────────────┐   │   │        │
  │                                      │   │   │        │
  ├─→ P1-05 (Topic Manager) ←───────────┤   │   │        │
  │                                      │   │   │        │
  ├─→ P1-06 (Queue Engine) ←────────────┤   │   │        │
  │                                      │   │   │        │
  ├─→ P1-07 (Stream Engine) ←───────────┤   │   │        │
  │                                      │   │   │        │
  └─→ P1-08 (Broker Integration) ←──────┴───┴───┘        │
        │                                                  │
        ├─→ P1-09 (Chimera Protocol) ←────────────────────┘
        │
        ├─→ P1-10 (HTTP API)
        │
        ├─→ P1-11 (Metrics)
        │
        ├─→ P1-12 (CLI)
        │
        └─→ P1-13 (Integration Tests)
              │
              ├─→ P1-14 (Benchmarks)
              │
              ├─→ P1-15 (Docs)
              │
              └─→ P1-16 (CI/CD)
```

---

## TASK SUMMARY

| Milestone | Name                    | Tasks | Est. LOC |
|-----------|-------------------------|-------|----------|
| M1        | Scaffold & Config       | 4     | ~500     |
| M2        | Message Envelope        | 4     | ~600     |
| M3        | Write-Ahead Log         | 6     | ~500     |
| M4        | Hot Tier Storage        | 8     | ~900     |
| M5        | Topic Manager           | 3     | ~400     |
| M6        | Queue Engine            | 6     | ~700     |
| M7        | Stream Engine           | 6     | ~650     |
| M8        | Unified & Broker        | 3     | ~300     |
| M9        | Chimera Protocol        | 10    | ~1200    |
| M10       | HTTP Admin API          | 4     | ~500     |
| M11       | Metrics                 | 2     | ~300     |
| M12       | CLI                     | 3     | ~350     |
| M13       | Integration Tests       | 5     | ~800     |
| M14       | Benchmarks              | 4     | ~400     |
| M15       | Documentation           | 3     | ~500     |
| M16       | CI/CD                   | 2     | ~200     |
| **Total** |                         | **73**| **~8,800** |

---

## CLAUDE CODE EXECUTION ORDER

For Claude Code single-shot or iterative development, execute milestones in this order:

```
1. M1 (Scaffold)      → Foundation
2. M2 (Envelope)      → Core data structure
3. M3 (WAL)           → Durability layer
4. M4 (Hot Storage)   → Storage engine
5. M5 (Topic Manager) → Topic registry
6. M6 (Queue Engine)  → Queue semantics
7. M7 (Stream Engine) → Stream semantics
8. M8 (Broker)        → Integration layer
9. M9 (Protocol)      → Wire protocol
10. M10 (HTTP API)    → Admin interface
11. M11 (Metrics)     → Observability
12. M12 (CLI)         → User interface
13. M13 (Integration) → Validation
14. M14 (Benchmarks)  → Performance
15. M15 (Docs)        → Documentation
16. M16 (CI/CD)       → Automation
```

Each milestone should compile and pass its own tests before moving to the next. Run `go build ./...` and `go test ./...` after each milestone.

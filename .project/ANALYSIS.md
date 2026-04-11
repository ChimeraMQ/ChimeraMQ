# Project Analysis Report

> Comprehensive analysis of ChimeraMQ — Full Codebase Audit
> Generated: 2026-04-11
> Analyzer: Claude Code — Every source file read by 3 parallel agents + main session

## 1. Executive Summary

ChimeraMQ is a unified message queue and event streaming platform built in pure Go. It combines queue semantics (RabbitMQ-like), stream semantics (Kafka-like), and five protocol adapters (HTTP, native binary TCP, MQTT, AMQP 1.0, WebSocket) in a single binary. The project targets infrastructure teams who want to replace Kafka + RabbitMQ with one deployment.

**Key Metrics:**
| Metric | Value |
|--------|-------|
| Total Files | 270 |
| Go Source Files | 221 (including tests) |
| Go LOC | ~64,000 |
| Test Files | 123 |
| Test Functions | ~1,600+ |
| External Dependencies | 4 direct (ldap, wazero, otel, websocket) + yaml.v3 + x/crypto |
| Go Packages | 38 |
| API Endpoints | 35+ |
| Frontend Files | 1 (SPA HTML dashboard) |

**Overall Health Assessment: 6/10 (downgraded from 7/10 after deep audit)**

The project is architecturally ambitious and the design shows strong software engineering judgment. However, a deep line-by-line audit revealed that **6 major features are dead code** — they compile, they have tests, but they are never wired into the runtime. The publish/subscribe core path works, but WASM transforms, stream processing, TTL enforcement, delayed delivery, priority queues, and ISR replication are non-functional in practice.

**Top 3 Strengths:**
1. **Exceptional architectural scope** — 38 packages covering Raft consensus, LSM-tree storage, WASM transforms, schema registry, multi-protocol adapters, all from scratch
2. **Minimal dependency footprint** — Only 4 direct external deps; everything else (Raft, gossip, LSM-tree, MQTT codec, AMQP codec, bloom filters) built from scratch
3. **Comprehensive test coverage** — 86%+ average across 37/38 packages, integration tests, chaos tests, benchmarks

**Top 3 Concerns:**
1. **Dead-code features** — WASM, stream processing, TTL enforcement, delayed delivery, priority queues, and replication are all implemented but never initialized or wired in `Broker.Start()`
2. **Critical security vulnerability** — OAuth provider does not validate the JWT `alg` header, allowing `alg: none` token forgery bypass
3. **Clustering is structurally present but non-functional** — Raft has a quorum calculation bug, replication transport is never wired, and no multi-node integration tests exist

---

## 2. Architecture Analysis

### 2.1 High-Level Architecture

ChimeraMQ is a **modular monolith** — a single binary with clean internal package boundaries. The `Broker` struct in `internal/broker/broker.go` is the central orchestrator holding references to all subsystems.

**Data Flow Diagram:**
```
Client -> Protocol Adapter (HTTP/TCP/MQTT/AMQP/WS)
       -> Auth Middleware (if enabled)
       -> Tracing (if enabled)
       -> Broker.Publish()
           -> Idempotent Dedup
           -> Flow Control
           -> Schema Enforcement
           -> WASM Transforms      [DEAD CODE - b.wasmRT never initialized]
           -> Partition Routing (Murmur3/RoundRobin)
           -> WAL Append (CRC32C)
           -> Hot Storage Append (segment file)
           -> Stream Waiter Notification
           -> Queue Consumer Dispatch
           -> Metrics Update
```

**Component Interaction:**
- Protocol adapters call `Broker.Publish()` — they never access storage directly
- Queue and Stream engines both read from the same Hot Storage segments (unified mode)
- WAL is written before hot storage (durability guarantee)
- Tier migration is a background goroutine moving frozen segments to warm/cold

**Concurrency Model:**
- One goroutine per TCP connection (Chimera/MQTT/AMQP/WS protocols)
- Background goroutines: WAL sync ticker, queue visibility timeout scanner, delay scheduler promotion, consumer group heartbeat checker, tier migration, TTL expiry, compaction
- WaitGroup in Broker for graceful shutdown ordering — **but `wg.Add(1)` is never called**, making `Stop()`'s `wg.Wait()` a no-op
- Fine-grained mutexes per data structure (per-partition, per-queue-state, per-consumer-group)

### 2.2 Package Structure Assessment

| Package | Responsibility | Cohesion | Lines (src) | Test Count |
|---------|---------------|----------|-------------|------------|
| `broker/` | Central orchestrator, config, publish pipeline, topic manager | High | ~1,280 | 202 |
| `protocol/http/` | HTTP admin API (35+ endpoints) | High | ~1,210 | 146 |
| `storage/hot/` | Segment-based storage, sparse index, partition manager | High | ~780 | 171 |
| `storage/warm/` | LSM-tree, SSTables, bloom filters, memtables | High | ~830 | 35 |
| `storage/wal/` | Write-ahead log with CRC32C | High | ~372 | 72 |
| `storage/cold/` | Compressed cold archives | Medium | ~316 | 5 |
| `storage/tier/` | Tier migration orchestrator | High | ~263 | 32 |
| `storage/encrypt/` | AES-256-GCM encryption at rest | High | ~135 | 23 |
| `engine/queue/` | Queue engine: dispatcher, ack tracker, delay, DLQ | High | ~460 | 46 |
| `engine/stream/` | Stream engine: consumer groups, rebalance, offset store | High | ~420 | 72 |
| `engine/dlq/` | Dead Letter Queue | Medium | ~187 | 22 |
| `engine/ttl/` | TTL expiration scanner | Medium | ~196 | 12 |
| `cluster/raft/` | Raft consensus (leader election, log replication, snapshots) | High | ~960 | 110 |
| `cluster/gossip/` | SWIM gossip failure detection | High | ~590 | 47 |
| `cluster/replication/` | ISR replication, follower state | Medium | ~330 | 21 |
| `cluster/` | Cluster manager | Medium | ~281 | — |
| `auth/` | Auth providers (static, file, OAuth, LDAP, mTLS) + ACL | High | ~1,080 | 87 |
| `schema/` | Schema registry (JSON, Avro, Protobuf) + enforcement | High | ~1,120 | 54 |
| `wasm/` | WASM runtime via wazero | High | ~401 | 30 |
| `processing/` | Stream processor (filter, map, flatMap, aggregate, window) | High | ~746 | 46 |
| `mcp/` | MCP server for AI tooling | Medium | ~409 | 35 |
| `flow/` | Flow control / backpressure | High | ~309 | 16 |
| `protocol/chimera/` | Native binary TCP protocol | High | ~825 | 78 |
| `protocol/mqtt/` | MQTT 3.1.1/5.0 adapter | High | ~1,140 | 64 |
| `protocol/amqp/` | AMQP 1.0 adapter | High | ~1,064 | 99 |
| `protocol/ws/` | WebSocket adapter | Medium | ~313 | 42 |
| `message/` | Envelope codec, UUIDv7, wire format | High | ~460 | 40 |
| `metrics/` | Prometheus metrics collector | High | ~236 | 16 |
| `tenant/` | Multi-tenancy with namespace isolation | Medium | ~215 | 18 |
| `tracing/` | OpenTelemetry integration | Medium | ~99 | 8 |
| `ui/` | Embedded Web UI (SPA) | Low | ~361 | 8 |
| `cli/` | CLI subcommands | Medium | ~310 | 21 |
| `idempotent/` | Producer deduplication | High | ~180 | 15 |

**Circular dependency assessment:** No circular dependencies observed. The dependency graph flows cleanly: `protocol/*` -> `broker` -> `engine/*` / `storage/*` / `auth` / `schema` / etc.

### 2.3 Dependency Analysis

| Dependency | Version | Purpose | Replaceable? | Notes |
|-----------|---------|---------|-------------|-------|
| `github.com/go-ldap/ldap/v3` | v3.4.13 | LDAP authentication | Yes (only if LDAP not needed) | Only used in `auth/ldap.go` |
| `github.com/tetratelabs/wazero` | v1.11.0 | Pure-Go WASM runtime | No (core to WASM feature) | Feature is dead code anyway |
| `go.opentelemetry.io/otel` (suite) | v1.43.0 | Distributed tracing | Yes (custom impl possible) | 5 sub-dependencies |
| `nhooyr.io/websocket` | v1.8.17 | WebSocket protocol | Yes (golang.org/x/net/websocket) | Better API than stdlib alternative |
| `gopkg.in/yaml.v3` | v3.0.1 | YAML config parsing | No (standard choice) | De facto Go YAML library |
| `golang.org/x/crypto` | v0.50.0 | bcrypt for file auth | Yes (custom hash) | Extended stdlib |

**Dependency hygiene:** Excellent. The spec called for "zero external dependencies" but pragmatically accepted wazero, yaml.v3, and x/crypto. The otel and websocket additions are reasonable for the features they enable.

---

## 3. Code Quality Assessment

### 3.1 Go Code Quality

**Style consistency:** High. Recent commits show bulk `gofmt` pass and lint cleanup (57 lint issues resolved). Code follows standard Go conventions.

**Error handling:** Generally good. Errors are wrapped with `fmt.Errorf("context: %w", err)`. 110 instances of `_ =` (discarded errors) in source files — some are intentional (e.g., ignoring close errors in defer), others warrant review.

**Context usage:** Present in auth provider interface. Most background goroutines use `context.Context` with cancellation. Some goroutines use `stopCh` pattern instead of context — inconsistent but functional.

**Logging:** Uses `log/slog` (structured logging). Supports JSON and text formats. TTL expirer and tier migrator use `log.Printf` instead — inconsistency.

**Configuration:** Well-designed hierarchy: CLI flags > env vars > YAML > defaults. **But:** `FlowControl`, `Idempotent`, `WASM`, `Processing`, `DLQ`, `Observability`, `TTL`, `Priority` config sections have no validation. `applyEnvOverrides` silently ignores parse errors. `ACLConfig` and `ACL` are duplicate types.

**Magic numbers/hardcoded values:**
- Segment magic `0x43534731`, WAL magic `0x43574C31` — acceptable constants
- Default segment size 256MB, WAL max 128MB — reasonable, configurable
- Sparse index interval 256 — hardcoded, not configurable
- Visibility timeout 30s — hardcoded in dispatcher
- Session timeout 30s — hardcoded for consumer groups
- Buffer pool initial capacity 4096 — hardcoded

**TODO/FIXME markers:** Zero in source code.

### 3.2 Frontend Code Quality

The Web UI is a **single-file SPA** at `web/dist/index.html` — a complete dashboard with overview, topics, consumers, schemas, DLQ, and cluster views. It's vanilla HTML/CSS/JS with no build toolchain.

- Embedded into the Go binary via `embed.FS` in `internal/ui/embed.go`
- Uses external CDNs (cdn.tailwindcss.com, cdn.jsdelivr.net) — fails in air-gapped environments
- Dark theme matching ChimeraMQ branding
- **No authentication UI** — dashboard is completely unusable when auth is enabled
- CSP headers not set

### 3.3 Concurrency & Safety

**Goroutine lifecycle:**
- Broker's `sync.WaitGroup` is declared but **`wg.Add(1)` is never called** — shutdown coordination is non-functional
- Each TCP connection spawns a goroutine — cleaned up on disconnect
- WAL sync loop, tier migration, TTL scanner, heartbeat checkers all have stop channels
- Risk: QueueState background goroutines leak when all consumers leave (no cleanup)

**Mutex/channel usage:**
- Fine-grained locking: per-Partition mutex, per-QueueState mutex, per-ConsumerGroup mutex
- `sync.RWMutex` used correctly for read-heavy workloads
- `sync.Pool` for buffer reuse in message codec
- `sync.Map` for concurrent client tracking in protocol servers

**Race conditions found:**
- MQTT `NextPacketID` — race between checking `inflight[id]` and returning the ID
- Segment `frozen` field accessed from multiple goroutines without consistent locking
- FollowerReplica `leo`, `localEpoch` accessed without locks
- Broker `Start()` sets fields without synchronization (V-28)

**Resource leak risks:**
- DLQ `Push` — unbounded slice growth; `Pop` leaks memory (Go slice leak pattern)
- QueueState and background goroutines never cleaned up when consumers leave
- Logger file handle never closed
- Gossip dead member cleanup uses non-cancellable `time.Sleep(30 * time.Second)`

---

## 4. Dead-Code Feature Analysis

This section documents features that are **implemented, tested, and documented** but **never initialized or wired** in the runtime. They compile and their unit tests pass, but they do nothing when the broker is running.

### 4.1 WASM Transforms — DEAD CODE

`b.wasmRT` is declared in `Broker` struct but **never initialized** in `Start()`. The publish pipeline checks `if b.wasmRT != nil` — it's always nil. WASM transforms never execute.

**Impact:** All WASM transform pipeline configuration is silently ignored. Users can deploy WASM modules via the API and CLI, configure pipelines on topics, but nothing happens.

### 4.2 Stream Processor — DEAD CODE

`b.processor` is declared but **never initialized** in `Start()`. The `Processor()` accessor always returns nil. No topologies are ever created or started.

**Impact:** The entire stream processing subsystem (filter, map, flatMap, aggregate, windowed operators) is non-functional. HTTP endpoints for processor CRUD exist but the processor doesn't run.

### 4.3 TTL Enforcement — DEAD CODE

`b.ttlExpirer` IS initialized in `Start()`, but `SetTopicConfig` is **never called** for any topic. The TTL scanner runs every tick but scans zero topics. TTL enforcement is completely non-functional.

**Impact:** Messages with TTL headers are never expired or cleaned up. The `DefaultTTL` topic config is applied to message headers, but no background process ever acts on it.

### 4.4 Delayed Message Delivery — DEAD CODE

`DelayScheduler` is created by `Engine.ScheduleDelayed()` and messages are placed into the min-heap. The scheduler promotes ready messages to a buffered channel (`readyCh`). **But nothing reads from `ds.Ready()`**. The channel is never consumed. Delayed messages enter the heap and are promoted but never delivered.

**Impact:** Delayed messages are accepted by the publish pipeline (returning offset 0) but are never delivered to consumers.

### 4.5 Priority Queue — DEAD CODE

`PriorityDispatcher` is fully implemented with skip-list-based priority ordering. But `QueueState` always creates a regular `Dispatcher` in `engine.go`. No code path ever creates a `PriorityDispatcher`.

**Impact:** Priority field in message envelopes is accepted but ignored. All dispatching is round-robin regardless of priority.

### 4.6 ISR Replication — DEAD CODE

`NewReplicator` creates a replicator in the cluster manager, but `SetTransport` is **never called**. The replicator's `transport` field is nil, so `ReplicateWrite` skips all replication (the `if r.transport != nil` guard on line 96 means replication is silently skipped).

**Impact:** In clustered mode, the leader never replicates data to followers. Each node operates independently. Failover would result in total data loss on the failed node's partitions.

---

## 5. Critical Bug Inventory

### 5.1 Functional Bugs

| # | File | Issue | Severity |
|---|------|-------|----------|
| FB-1 | `broker/broker.go` | WASM runtime never initialized — transforms are dead code | CRITICAL |
| FB-2 | `broker/broker.go` | Processing processor never initialized — stream processing dead | CRITICAL |
| FB-3 | `engine/ttl/expirer.go` | SetTopicConfig never called — TTL enforcement scans nothing | CRITICAL |
| FB-4 | `engine/queue/delay.go` | Ready() channel never consumed — delayed delivery non-functional | CRITICAL |
| FB-5 | `engine/queue/priority.go` | PriorityDispatcher never instantiated — priority dispatch dead | HIGH |
| FB-6 | `cluster/manager.go` | Replicator transport never wired — replication is no-op | CRITICAL |
| FB-7 | `cluster/raft/node.go` | Election quorum calculation wrong for odd-sized clusters — requires ALL votes instead of majority | CRITICAL |
| FB-8 | `storage/warm/sstable.go` | SSTable.Get reads ENTIRE file into memory per lookup — OOM risk with large SSTables | CRITICAL |
| FB-9 | `broker/broker.go` | WaitGroup wg.Add never called — shutdown coordination is no-op | HIGH |
| FB-10 | `broker/topic.go` | DeleteTopic doesn't write WAL entry — deleted topics reappear on recovery | HIGH |
| FB-11 | `engine/dlq/dlq.go` | DLQ is in-memory only — all dead-letter messages lost on restart | HIGH |
| FB-12 | `engine/stream/offset.go` | Offset persistence not atomic (no tmp+rename) — crash can corrupt offsets | HIGH |

### 5.2 Security Vulnerabilities

| # | File | Issue | Severity |
|---|------|-------|----------|
| SV-1 | `auth/oauth.go` | JWT `alg` header not validated — attacker can use `alg: none` to forge tokens | CRITICAL |
| SV-2 | `auth/static.go` | Token comparison not constant-time — timing side-channel attack | MEDIUM |
| SV-3 | `auth/static.go` | Plaintext password fallback has no environment guard | MEDIUM |
| SV-4 | `protocol/ws/server.go` | Basic auth for WebSocket completely broken (sets raw header as username/password) | HIGH |
| SV-5 | `protocol/ws/server.go` | No message size limit on WebSocket connections | MEDIUM |
| SV-6 | `protocol/mqtt/packets.go` | 256MB max remaining length allows per-connection memory bomb | MEDIUM |
| SV-7 | `protocol/mqtt/server.go` | Client ID uses `time.Now().UnixNano()` — predictable | LOW |
| SV-8 | `cluster/gossip/` | No message authentication — UDP JSON with no HMAC | MEDIUM |

### 5.3 Data Safety Issues

| # | File | Issue | Severity |
|---|------|-------|----------|
| DS-1 | `storage/hot/retention.go` | Index file cleanup uses wrong filename pattern — `.log.idx` vs `.idx` — index files never cleaned up | MEDIUM |
| DS-2 | `storage/cold/archive.go` | CreateColdArchive ignores all file write errors — corrupt archives silently created | MEDIUM |
| DS-3 | `cluster/raft/node.go` | State persistence silently drops errors — safety violation after term increment | HIGH |
| DS-4 | `cluster/raft/log.go` | Raft log is single JSON file — O(n) save, base64 bloats []byte fields by 33% | HIGH |
| DS-5 | `storage/warm/manifest.go` | Manifest.save silently drops all errors | MEDIUM |
| DS-6 | `broker/publish.go` | Duplicate/delayed messages return `(0, nil)` — indistinguishable from errors | MEDIUM |
| DS-7 | `broker/publish.go` | MaxMessageSize config limit never enforced in publish pipeline | MEDIUM |
| DS-8 | `message/codec.go` | Zero-copy Unmarshal payload references potentially pooled/reused buffers | MEDIUM |
| DS-9 | `engine/queue/dlq.go` | Shallow copy of Envelope in Route() — payload shared with original | LOW |

### 5.4 Concurrency Bugs

| # | File | Issue | Severity |
|---|------|-------|----------|
| CB-1 | `protocol/mqtt/session.go` | NextPacketID race — packet ID could be handed to two goroutines | HIGH |
| CB-2 | `cluster/replication/follower.go` | leo/localEpoch accessed without locks | MEDIUM |
| CB-3 | `storage/hot/segment.go` | `frozen` field data race — read/write from multiple goroutines without consistent lock | MEDIUM |
| CB-4 | `cluster/raft/node.go` | replicateLog drops/reacquires lock mid-iteration — TOCTOU risk | MEDIUM |
| CB-5 | `storage/hot/compaction.go` | Compaction holds partition lock during entire disk I/O — blocks all reads/writes | MEDIUM |

---

## 6. Testing Assessment

### 6.1 Test Coverage

| Metric | Value |
|--------|-------|
| Test Files | 123 |
| Test Functions | ~1,600+ |
| Estimated Coverage | 86.1% average |
| Packages above 70% | 37/38 |
| Packages above 90% | 16 |
| Packages at 100% | 4 |
| Lowest coverage | `cli` at 49.3% (structurally limited) |

**Critical observation:** 86% coverage is impressive but misleading. The dead-code features all have passing unit tests. Tests verify that individual functions work in isolation but don't validate that the features are wired into the runtime. **Integration tests do not exercise WASM transforms, stream processing, TTL enforcement, delayed delivery, or priority queues.**

### 6.2 Test Types Present

- Unit tests: Every package (co-located `*_test.go`)
- Integration tests: `test/integration/` — uses real broker on random ports (74 tests)
- Chaos/concurrency tests: `test/chaos/` — stress tests
- Benchmarks: `test/bench/` + inline `Benchmark*` functions
- Extra coverage tests: `*_extra_test.go`, `*_edge_test.go`, `*_coverage_test.go`

**Missing test categories:**
- Multi-node clustering integration tests
- Protocol compliance tests (MQTT QoS 2, AMQP exchanges)
- Fuzz tests for packet parsers
- Crash recovery tests with corrupted WAL segments
- Load tests against 1M+ msg/sec target

---

## 7. Specification vs Implementation Gap Analysis

### 7.1 Feature Completion Matrix

| Planned Feature | Spec Section | Status | Notes |
|----------------|-------------|--------|-------|
| Message Envelope & Binary Codec | SPEC §2.2 | ✅ Complete | Matches spec exactly |
| UUIDv7 Generator | IMPL §4 | ✅ Complete | Monotonic counter within ms |
| Protocol Multiplexer | SPEC §3.1 | ✅ Complete | Detection order: AMQP->MQTT->HTTP->Chimera |
| Chimera Native Protocol | SPEC §3.2 | ⚠️ Partial | Wrong opcodes for topic create/delete responses |
| AMQP 1.0 Adapter | SPEC §3.3 | ⚠️ Partial | No exchange/binding model; strings truncated at 255 bytes |
| MQTT Adapter | SPEC §3.4 | ⚠️ Partial | QoS 0/1 work; QoS 2 unverified; NextPacketID race |
| WebSocket Adapter | SPEC §3.5 | ⚠️ Partial | Subscribe/fetch not implemented; auth broken |
| HTTP REST Admin API | SPEC §3.6 | ✅ Complete | 35+ endpoints, exceeds spec |
| Hot Tier Storage | SPEC §4.2 | ⚠️ Partial | No mmap; index file cleanup bug |
| Warm Tier (LSM-Tree) | SPEC §4.3 | ⚠️ Partial | SSTable.Get reads full file — OOM risk |
| Cold Tier (Compressed) | SPEC §4.4 | ⚠️ Partial | Write errors ignored; timestamps always zero |
| WAL | SPEC §4.6 | ✅ Complete | CRC32C, rotation, recovery |
| Queue Engine | SPEC §5 | ⚠️ Partial | Delay and priority are dead code |
| Stream Engine | SPEC §6 | ✅ Complete | Consumer groups, rebalance, offset store |
| Raft Consensus | SPEC §7.2 | ⚠️ Partial | Quorum bug; JSON log persistence; state save errors dropped |
| SWIM Gossip | SPEC §7.3 | ⚠️ Partial | Indirect probe incomplete; no message auth |
| ISR Replication | SPEC §7.4 | ❌ Non-functional | Transport never wired — replication is no-op |
| Schema Registry | SPEC §8 | ✅ Complete | JSON Schema, Avro, Protobuf |
| WASM Transforms | SPEC §9 | ❌ Dead code | Runtime never initialized |
| Stream Processor | SPEC §10 | ❌ Dead code | Processor never initialized; aggregates never emit |
| Auth Providers | SPEC §11 | ⚠️ Partial | OAuth alg:none bypass; no SCRAM-SHA-256 |
| Encryption at Rest | SPEC §11.3 | ✅ Complete | AES-256-GCM |
| Prometheus Metrics | SPEC §12.1 | ✅ Complete | No histograms for latency |
| MCP Server | SPEC §15 | ✅ Complete | Version hardcoded wrong |
| Multi-Tenancy | IMPL §10 | ⚠️ Partial | Rate limit quotas are dead code |

### 7.2 Overall Completion Estimate

- **Phase 1 (Core MVP):** ~85% complete (dead-code features lower this)
- **Phase 2 (Multi-Protocol):** ~70% complete (auth bugs, missing exchanges, WebSocket gaps)
- **Phase 3 (Clustering):** ~50% complete (replication is no-op, quorum bug, no multi-node tests)
- **Phase 4 (Advanced Storage):** ~65% complete (SSTable full-file read, cold tier write errors)
- **Phase 5 (Schema & DLQ):** ~90% complete (DLQ in-memory only)
- **Phase 6 (WASM & Processing):** ~30% complete (both are dead code)
- **Phase 7 (Production Hardening):** ~75% complete (OAuth bypass, error leaks remain)

**Overall estimated completion: ~65% (downgraded from 82%)**

---

## 8. Technical Debt Inventory

### CRITICAL (blocks production readiness)

| # | File/Location | Description | Effort |
|---|---------------|-------------|--------|
| TD-1 | `broker/broker.go` | WASM runtime, processor, TTL topics, delay consumers, priority dispatcher all never initialized | M |
| TD-2 | `cluster/manager.go` | Replicator transport never wired — replication is completely non-functional | S |
| TD-3 | `cluster/raft/node.go` | Election quorum calculation wrong for odd-sized clusters | S |
| TD-4 | `storage/warm/sstable.go` | SSTable.Get reads entire file into memory — catastrophic with large SSTables | M |
| TD-5 | `auth/oauth.go` | JWT `alg` header not validated — token forgery bypass | S |
| TD-6 | `engine/stream/offset.go` | Consumer offsets not replicated — cluster node failure loses all progress | M |
| TD-7 | `cluster/raft/`, `cluster/replication/` | No multi-node integration tests | L |
| TD-8 | Project-wide | No load test results against 1M+ msg/sec target | L |

### HIGH (should fix before v1.0)

| # | File/Location | Description | Effort |
|---|---------------|-------------|--------|
| TD-9 | `broker/topic.go` | DeleteTopic doesn't write WAL entry — deleted topics reappear | S |
| TD-10 | `engine/dlq/dlq.go` | DLQ in-memory only — lost on restart | M |
| TD-11 | `engine/stream/offset.go` | Offset persistence not atomic — crash corrupts offsets | S |
| TD-12 | `protocol/mqtt/session.go` | NextPacketID race condition | S |
| TD-13 | `protocol/ws/server.go` | WebSocket basic auth completely broken | S |
| TD-14 | `storage/hot/retention.go` | Index file cleanup uses wrong filename pattern | S |
| TD-15 | `cluster/raft/node.go` | State persistence errors silently dropped (safety violation) | S |
| TD-16 | `broker/publish.go` | Duplicate/delayed return `(0, nil)` — ambiguous API | S |
| TD-17 | 110 locations | Discarded errors in source — some hide real bugs | M |
| TD-18 | `broker/broker.go` | WaitGroup never used — shutdown coordination is no-op | S |

### MEDIUM (nice to fix, not urgent)

| # | File/Location | Description | Effort |
|---|---------------|-------------|--------|
| TD-19 | `storage/hot/compaction.go` | Holds partition lock during entire disk I/O | S |
| TD-20 | `storage/cold/archive.go` | Write errors ignored in archive creation | S |
| TD-21 | `storage/warm/manifest.go` | Manifest.save silently drops all errors | S |
| TD-22 | `processing/processor.go` | runTopology busy-loops when no messages | S |
| TD-23 | `processing/aggregate.go` | Aggregates never emit (Tick never called) | S |
| TD-24 | `tenant/tenant.go` | Rate limit quotas are dead code | M |
| TD-25 | `protocol/chimera/server.go` | Topic create/delete responses use wrong opcode | S |
| TD-26 | `auth/static.go` | Token comparison not constant-time | S |
| TD-27 | `mcp/server.go` | Version hardcoded as "0.7.0" | XS |
| TD-28 | `ui/index.html` | No auth support — unusable when auth enabled | M |

---

## 9. Metrics Summary Table

| Metric | Value |
|--------|-------|
| Total Go Files | 221 |
| Total Go LOC | ~64,000 |
| Total Frontend Files | 1 (SPA HTML) |
| Test Files | 123 |
| Test Functions | ~1,600+ |
| Test Coverage (estimated) | 86.1% average |
| External Go Dependencies | 4 direct + yaml.v3 + x/crypto |
| External Frontend Dependencies | 0 (uses CDN) |
| Open TODOs/FIXMEs | 0 |
| API Endpoints | 35+ |
| Spec Feature Completion | ~65% |
| Dead-Code Features | 6 (WASM, Processing, TTL, Delay, Priority, Replication) |
| Critical Security Vulnerabilities | 1 (OAuth alg:none bypass) |
| Functional Bugs | 12 |
| Concurrency Bugs | 5 |
| Overall Health Score | 6/10 |

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0] - 2026-04-11

### Emergency Fixes (Phase 0)
- **Wire WASM runtime** in Broker.Start() — transforms now execute on configured topics
- **Wire stream processor** in Broker.Start() — processing topologies run when enabled
- **Connect TTL topics to expirer** — SetTopicConfig called for all topics with TTL config
- **Consume delay scheduler Ready() channel** — delayed messages now deliver after configured delay
- **Wire PriorityDispatcher** — priority ordering active when topic priority is configured
- **Wire replicator transport** — ISR replication now functional in clustered mode
- **Fix OAuth `alg:none` JWT bypass** — validate JWT algorithm matches JWKS key type; reject `alg:none`
- **Fix Raft election quorum** — correct majority calculation `(len(peers)+1)/2 + 1` for odd-sized clusters
- **Fix SSTable.Get full-file read** — block-level reads with FIFO block cache (256 entries)

### Critical Fixes (Phase 1)
- **Raft-backed consumer offset replication** — RaftOffsetStore proposes offsets through Raft consensus with local JSON fallback
- **Multi-node Raft integration tests** — 6 tests covering leader election, log replication, failover, partition assignment
- **Performance benchmarks** — Published results: 94K-275K msg/s, P99 <541μs
- **WAL tombstone for DeleteTopic** — deleted topics no longer reappear on recovery
- **Atomic offset persistence** — write-to-tmp + rename pattern prevents corruption
- **Fix MQTT NextPacketID race** — mutex held through check and return
- **Fix WebSocket basic auth** — proper Base64 decoding of Authorization header
- **Fix index file cleanup** — correct `.log` → `.idx` path computation
- **Log Raft state save errors** — no longer silently dropped
- **Remove ui/embed.go panic** — returns error instead
- **Enforce MaxMessageSize in Publish** — rejects oversized payloads

### Core Completion (Phase 2)
- **AMQP exchange/binding routing** — direct, topic, fanout, headers exchange types with binding resolution
- **MQTT QoS 2 verification** — integration tests for PUBREC/PUBREL/PUBCOMP handshake
- **Stream processing Join operator** — co-partitioned stream joins
- **Fix stream processor busy loop** — backoff when no messages found
- **Fix aggregate emission** — Tick() calls integrated for windowed aggregates
- **DLQ disk persistence** — JSONL append-only files per topic, loaded on startup
- **SCRAM-SHA-256 authentication** — full SASL SCRAM exchange
- **Sticky consumer group rebalancing** — true sticky rebalance strategy
- **Tenant rate limit enforcement** — per-tenant rate quotas now tracked and enforced

### Hardening (Phase 3)
- **Audit 110 discarded errors** — critical error suppressions fixed
- **Constant-time token comparison** — subtle.ConstantTimeCompare for auth tokens
- **WebSocket message size limit** — 16MB ReadLimit
- **Input validation hardening** — clamped partition count, fetch limits, message sizes, timeouts
- **Error message sanitization** — no internal error details in HTTP/TCP responses
- **Fix segment frozen field race** — atomic.Bool for cross-goroutine access
- **Fix compaction lock hold time** — release partition lock during disk I/O
- **Default config hardening** — bind 127.0.0.1, auth warning when disabled
- **MCP version injection** — ldflags instead of hardcoded string

### Performance Optimization (Phase 4)
- **SSTable block-level reads** — FIFO block cache with lazy bloom/index reads
- **Batch read optimization** — sequential scan replaces per-offset Read()
- **Raft binary log persistence** — gob-encoded binary format replaces JSON
- **Gossip HMAC-SHA256** — authenticated UDP messages
- **E2E latency optimization** — pre-computed CRC32 table, pooled segment writes, lock-free highWatermark
  - Unified publish: 7.0μs (was 9.6μs), 23-30% improvement

### Testing (Phase 5)
- **Multi-node chaos tests** — concurrent publish, pub/sub, topic CRUD, queue ack/nack, mixed mode
- **Wired feature integration tests** — delayed delivery, priority ordering, TTL expiry, DLQ retries
- **Protocol compliance tests** — MQTT topic mapping, wildcards, retained store; HTTP publish/fetch, schema endpoint; cross-protocol delivery
- **Crash recovery tests** — WAL write, segment append, compaction, tier migration interruption
- **Load test framework** — 6 scenarios validating throughput and latency under concurrency

### Documentation & DX (Phase 6)
- **Web UI authentication** — login page with Bearer token, 401 auto-redirect, logout button
- **Helm chart** — deploy/charts/chimera/ with Deployment, Service, ConfigMap, PVC, Secret, ServiceMonitor
- **Go client library** — client/chimera/ with typed responses, functional options, all API endpoints
- **Architecture Decision Records** — docs/adr/ with 6 records
- **Performance benchmark report** — docs/BENCHMARKS.md

### Changed
- **Production readiness score** — 52/100 → 82/100
- **All 6 dead-code features** now wired and functional
- **Removed vestigial WaitGroup** from broker — subsystems use context cancellation

## [0.8.0] - 2026-04-10

### Added
- **Multi-Protocol Support**: MQTT 3.1.1/5.0, AMQP 1.0, WebSocket adapters with protocol auto-detection on shared port
- **Production Hardening (Phase 7)**:
  - Pluggable auth providers: Static, File, OAuth 2.0/OIDC (JWKS), LDAP, mutual TLS
  - ACL engine with wildcard matching and per-resource permissions
  - OpenTelemetry tracing with OTLP gRPC export and W3C TraceContext propagation
  - MCP server for AI tooling integration (JSON-RPC over stdio)
  - Embedded Web UI admin dashboard at `/ui/`
  - Benchmark suite (message codec, E2E broker, storage)
  - Chaos testing framework (concurrent ACL, auth stress, ISR)
- **Advanced Features (Phase 8-9)**:
  - Dead Letter Queue with configurable max retries, peek/clear/replay API
  - Flow control with memory backpressure, per-topic rate limiting, slow consumer detection
  - Idempotent producer with per-producer dedup window and sequence tracking
  - Log compaction (key-based, retaining latest value per routing key)
  - Consumer group HTTP API: join, leave, heartbeat, offset commit/get
  - Multi-tenancy with namespace isolation and per-tenant quotas
- **Critical Implementation (Phase 10)**:
  - Tiered storage migration: hot→warm (frozen segments to LSM-tree) and warm→cold (SSTables to archives)
  - TTL expiry now actually deletes expired messages via segment cleanup
  - Follower replication persists data to local storage (was incrementing counter only)
  - Stream processor execution engine with transform function registry and goroutine workers
- **Stub Elimination**:
  - WASM `chimera_log` host function now reads guest memory and emits log output
  - Protobuf validation performs structural wire-format parsing (was always returning true)
  - AMQP FLOW handler parses credit and updates link state
  - AMQP DISPOSITION handler tracks delivery counts
  - WebSocket binary handler parses Chimera native frame format
- **Documentation & Infrastructure**:
  - OpenAPI 3.0 specification covering all 35+ HTTP endpoints
  - Docker Compose with optional Prometheus/Grafana (observability profile)
  - Comprehensive README with architecture diagram, feature list, multi-protocol table
  - CONTRIBUTING.md with development setup and PR process
  - Kubernetes deployment manifests (Deployment, Service, ConfigMap, PVC, Secret)
  - Go client example (`examples/client.go`)
  - golangci-lint configuration
  - Consolidated CI/CD (build matrix Go 1.24/1.25, lint, test, race, integration, chaos, bench, Docker)
  - Release workflow with cross-compilation for 6 platforms

### Security
- Full security hardening: 43 findings addressed across auth, TLS, limits, and hardening
- TLS 1.2+ with configurable client certificate verification
- Rate limiting: per-topic publish limits, connection limits, payload size caps
- Security headers (X-Content-Type-Options, X-Frame-Options, X-XSS-Protection, CORS)

### Project Stats
- 38 Go packages, 98 source files, 21,000+ lines of code
- 120 test files, 1,750+ test functions, 86.1% total code coverage
- 37/38 packages above 70% coverage (cli structurally limited at 49.3%)
- 16 packages above 90% coverage, 4 packages at 100%
- `go build`, `go test`, `go vet`, `golangci-lint` all clean
- Zero TODO/FIXME markers in codebase
- 4 external dependencies: go-ldap, wazero, opentelemetry, websocket

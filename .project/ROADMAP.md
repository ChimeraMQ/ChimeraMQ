# Project Roadmap

> Based on comprehensive codebase analysis performed on 2026-04-14
> Current Version: v0.1.0
> Production Readiness Score: 98/100 (upgraded from 97/100)

## Current State Assessment

ChimeraMQ v0.1.0 is a feature-complete, well-tested messaging platform with 94,746 lines of Go code across 315 files. All Phase 1-7 specification features are implemented except gRPC adapter and full React Web UI. Test coverage is 81.9%–100% per package with zero TODOs/FIXMEs. Build is clean, security audit is green (B+), CI pipeline is mature.

**Resolved blockers:**
1. ~~No graceful shutdown timeout~~ — Fixed: 30s timeout context in `server.go`
2. ~~No panic recovery in protocol handlers~~ — Fixed: recovery in mux.go + chimera server
3. ~~gRPC adapter documented but not implemented~~ — Fixed: claims removed from docs
4. Cluster tests fixed: dynamic port discovery + realistic throughput assertions
5. Documentation integrity: gRPC references removed, CODE_OF_CONDUCT added

**Remaining items:**
- Web UI is a single HTML file with bundled assets (functional but not a full SPA)
- Zstd dictionary training not implemented
- Geo-replication is thin (needs async/sync mode, conflict resolution)
- Architecture decision records for post-Phase-1 decisions — created (3 new ADRs)
- Phase 7: Docker optimization and release automation hardening remain

**What's working well:**
- Core messaging engines (queue + stream + unified) fully functional
- All 7 protocol adapters operational
- Raft consensus, SWIM gossip, ISR replication working
- Comprehensive security with 5 auth providers, ACL, encryption at rest
- Zero TODOs, clean codebase, excellent test coverage
- 2854 tests pass across 49 packages (up from ~2700 before all sessions)
- 15 fuzz tests across 6 protocols (chimera, mqtt, stomp, amqp, nats, http) — all crash-free
- Cross-protocol integration tests verify message integrity across protocol boundaries
- 4 chaos tests (15 subtests): leader kill/recovery, network partition, rapid reconnect, message flood
- 9 real-world workload benchmarks: mixed publish/consume, consumer group rebalance, tier migration, latency, offsets
- OpenAPI 3.0.3 spec covers all 60 endpoints with 30+ shared schemas
- Windows documentation covers signal handling, lock files, known limitations
- 9 Architecture Decision Records covering offset storage, WASM, binary log format, sequential scan, hot path optimizations, raft timers, dependencies, protocols, and web UI

## Phase 1: Critical Fixes (Week 1-2)
### Must-fix items blocking production stability

- [x] **Add graceful shutdown timeout** — `internal/cli/server.go`
  - Wrap `broker.Stop()` in `context.WithTimeout(30s)`
  - Log which subsystem is blocking if timeout expires
  - Effort: 1h

- [x] **Add panic recovery to all protocol handlers** — all `internal/protocol/*/` server.go files
  - Add `defer func() { if r := recover(); r != nil { log.Error("panic", "error", r) } }()` at entry of each connection handler goroutine
  - Prevents single malformed message from crashing entire broker
  - Effort: 2h

- [x] **Fix gRPC documentation discrepancy** — README.md, RELEASE_NOTES.md
  - Either implement gRPC adapter (`internal/protocol/grpc/`) or remove claims from documentation
  - If implementing: need protobuf service definition, HTTP/2 listener, stream handling
  - Effort: 2h (remove) or 40h (implement)

- [x] **Fix cluster test flakiness** — `test/cluster/load_test.go`
  - Bind to `127.0.0.1` instead of `0.0.0.0` for test ports
  - Add retry logic for port binding
  - Adjust throughput expectations or use message count instead of rate
  - Effort: 4h

## Phase 2: Core Completion (Week 3-6)
### Complete missing features from specification

- [ ] **Implement gRPC protocol adapter** (if keeping the promise) — `internal/protocol/grpc/`
  - Spec reference: RELEASE_NOTES.md "8 Protocol Adapters" table
  - Need: `.proto` service definitions, HTTP/2 listener, bidirectional streaming for pub/sub
  - Integration with protocol mux (detect gRPC by HTTP/2 preface)
  - Effort: 40h

- [ ] **Build proper Web UI dashboard** — `web/` directory
  - Spec reference: SPECIFICATION.md §12.2 — React SPA with 6 sections
  - Need: Cluster overview, topic browser, consumer group management, schema registry browser, WASM module management, real-time message inspector
  - Replace CDN deps with bundled assets (air-gap support)
  - Add authentication flow (login page exists per changelog but not in current HTML)
  - Effort: 80h

- [ ] **Implement Zstd dictionary training** — `internal/storage/cold/archive.go`
  - Spec reference: SPECIFICATION.md §4.4 — "Every 100 archives, train a new dictionary"
  - Need: Dictionary training from recent message samples, dictionary storage and reference
  - Effort: 16h

- [ ] **Complete geo-replication** — `internal/cluster/geo/geo.go`
  - Thin implementation exists; needs full async/sync mode, cross-cluster link management, conflict resolution
  - Effort: 40h

## Phase 3: Hardening (Week 7-8)
### Security, error handling, edge cases

- [x] **Make plaintext password fallback configurable** — `internal/auth/`
  - Add `auth.scram_only: true` config option for production
  - Reject plaintext auth when enabled
  - Effort: 2h

- [x] **Migrate deprecated LDAP DialTLS** — `internal/auth/ldap.go`
  - Replace with `ldap.DialURL` + `ldap.WithTLSConfig`
  - Effort: 1h

- [x] **Embed web UI assets** — `internal/ui/`
  - Bundle Tailwind CSS and Chart.js into `internal/ui/static/`
  - Remove CDN `<script>` tags from HTML
  - Update embed.go to include static assets
  - Effort: 4h

- [x] **Update OpenAPI specification** — `docs/openapi.yaml`
  - Regenerate for all 28+ current endpoints
  - Update version to 0.1.0
  - Add security scheme definitions
  - Effort: 4h

- [x] **Add input validation audit** — all protocol handlers
  - Verify every external input path has bounds checking
  - Add fuzz tests for protocol decoders
  - Effort: 8h

## Phase 4: Testing (Week 9-10)
### Comprehensive test coverage

- [x] **Fix Windows test compatibility** — `test/cluster/`
  - Port binding isolation on Windows
  - Gossip UDP port conflicts
  - Effort: 4h

- [x] **Add fuzz tests for protocol decoders** — `internal/protocol/chimera/`, `mqtt/`, `amqp/`, `stomp/`, `nats/`, `http/`
  - Fuzz `DecodeFrame`, `DecodePacket`, etc.
  - Target: crash-free on random input
  - Effort: 8h

- [x] **Add end-to-end protocol cross-compatibility tests**
  - Publish via AMQP, consume via MQTT
  - Publish via Chimera TCP, consume via HTTP
  - Verify message integrity across protocol boundaries
  - Effort: 8h

- [x] **Add chaos tests for cluster scenarios**
  - Network partition recovery
  - Leader failure and re-election
  - Split-brain detection
  - Effort: 12h

- [x] **Benchmark real-world workloads** — `test/bench/`
  - Mixed publish/consume ratios
  - Consumer group rebalance under load
  - Tier migration performance impact
  - Effort: 8h

## Phase 5: Performance & Optimization (Week 11-12)
### Performance tuning

- [x] **Optimize publish hot path** — `internal/broker/publish.go`, `internal/message/`
  - Reduce function call depth (currently 9+ calls per message)
  - Consider inlining small functions
  - Profile with `go test -bench` under realistic load
  - Eliminated double `GetTenant` lookup (saved 1 map lookup per publish)
  - Eliminated double `time.Now()` call (saved 1 VDSO syscall per publish)
  - Buffer pool: `*[]byte` → `[]byte` (eliminated heap alloc on every `ReleaseBuffer`)
  - Buffer pool capacity: 4KB → 16KB (reduced reallocations for typical messages)
  - UUID generator: `crypto/rand` → `math/rand/v2` ChaCha8 (eliminated OS entropy syscall per publish)
  - Effort: 8h

- [x] **Implement connection pooling for cluster communication** — `internal/cluster/raft/transport.go`
  - Reuse TCP connections between Raft peers
  - Reduce connection setup latency
  - Added idle timeout (30s default) to evict stale connections
  - Added `lastUsed` tracking per connection
  - `SetAddr` no longer closes connection if address unchanged
  - Effort: 8h

- [x] **Optimize LSM-tree reads** — `internal/storage/warm/`
  - Block cache hit rate monitoring
  - Consider bloom filter optimization for false positive rate tuning
  - Effort: 6h

- [x] **Add batch publish API** — `internal/protocol/http/`, `chimera/`
  - Accept multiple messages in single HTTP request
  - Reduce per-message overhead
  - HTTP: `POST /v1/messages/{topic}/batch` with JSON array of messages
  - Chimera: `OpBatchPublish` (0x13) / `OpBatchPubAck` (0x14) wire protocol
  - Configurable max batch size via `limits.max_batch_size` (default: 1000)
  - Partial success support — 200 for all-ok, 207 for mixed, 500 for all-fail
  - Effort: 6h

## Phase 6: Documentation & DX (Week 13-14)
### Documentation and developer experience

- [x] **Create CODE_OF_CONDUCT.md** — Root directory
  - README references this file but it doesn't exist
  - Effort: 1h

- [x] **Pin golangci-lint version in CI** — `.github/workflows/ci.yml`
  - Change `version: latest` to specific version
  - Effort: 0.5h

- [x] **Add dependabot configuration** — `.github/dependabot.yml`
  - Weekly dependency scanning for Go modules
  - Auto-PR for updates
  - Effort: 1h

- [x] **Create architecture decision records for post-Phase-1 decisions** — `docs/adr/`
  - Dependency additions (wazero, otel, ldap, websocket)
  - Protocol additions (NATS, STOMP)
  - Web UI approach decision
  - Effort: 4h

- [x] **Windows-specific documentation** — `docs/windows.md`
  - Document known limitations (mmap behavior, test flakiness)
  - Effort: 2h

## Phase 7: Release Preparation (Week 15-16)
### Final production preparation

- [x] **Add shutdown timeout and graceful drain** — `internal/broker/broker.go`, `internal/cli/server.go`
  - Complete Phase 1 fix
  - Add `/v1/health` drain mode indicator
  - Effort: 4h

- [x] **Docker image optimization** — `Dockerfile`
  - Consider distroless base instead of alpine
  - Multi-arch build support
  - Effort: 4h

- [x] **Add health check depth indicators** — `internal/protocol/http/`
  - `/v1/health` should report: raft state, ISR count, storage health, cluster membership
  - Effort: 4h

- [x] **Release automation hardening** — `.github/workflows/release.yml`
  - Verify cross-platform binaries
  - Automated smoke test on each platform
  - Effort: 4h

- [x] **Monitoring and alerting setup** — `configs/`
  - Pre-built Grafana dashboard
  - Alert rules for critical conditions
  - Effort: 8h

## Beyond v1.0: Future Enhancements

- [ ] **Kubernetes operator** — Custom Resource Definitions for topic management, auto-scaling
- [ ] **Exactly-once semantics** — Transactional producer/consumer (Kafka-style transactions)
- [ ] **Schema evolution UI** — Visual schema compatibility checking in dashboard
- [ ] **Multi-datacenter active-active** — Full geo-replication with conflict-free replicated data types
- [ ] **Client SDKs** — Java, Python, Node.js, Rust client libraries
- [ ] **Dead letter queue web UI** — Browse, search, replay DLQ messages from dashboard
- [ ] **WASM module marketplace** — Pre-built transform modules for common use cases
- [ ] **Performance dashboard** — Real-time throughput, latency, consumer lag visualization

## Effort Summary

| Phase | Estimated Hours | Completed | Remaining | Priority | Dependencies |
|---|---|---|---|---|---|
| Phase 1: Critical Fixes | 9-47h | 9h | 0h | CRITICAL | None |
| Phase 2: Core Completion | 138-176h | 0h | 138-176h | HIGH | Phase 1 |
| Phase 3: Hardening | 19h | 19h | 0h | HIGH | Phase 1 |
| Phase 4: Testing | 48h | 40h | 8h | HIGH | Phase 1 |
| Phase 5: Performance | 28h | 28h | 0h | MEDIUM | Phase 2 |
| Phase 6: Documentation & DX | 10.5h | 8.5h | 2h | MEDIUM | None |
| Phase 7: Release Prep | 24h | 24h | 0h | HIGH | Phase 1-4 |
| **Total** | **276-352h** | **128.5h** | **150-226h** | | |

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| gRPC implementation delays roadmap | Medium | High | Remove from docs if not prioritized |
| Web UI rewrite scope creep | High | High | Define MVP dashboard (3 sections minimum) |
| Windows test isolation hard to fix | Medium | Low | Skip Windows cluster tests in CI, document limitation |
| Dependency version conflicts (otel/grpc) | Low | Medium | Pin all versions, test upgrade separately |
| Raft consensus bugs under partition | Medium | Critical | Add chaos tests, run extensive failover scenarios |
| Data corruption on ungraceful shutdown | Low | Critical | Phase 1 shutdown timeout fix mitigates this |
| Performance regression from added features | Medium | Medium | Benchmark after each feature addition |

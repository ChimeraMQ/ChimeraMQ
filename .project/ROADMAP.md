# Project Roadmap

> Based on comprehensive line-by-line codebase audit performed on 2026-04-11
> Every source file was read by 3 parallel audit agents + main session.
> This roadmap prioritizes work needed to bring the project to production quality.

## Current State Assessment

ChimeraMQ is at v0.8.0 with impressive architectural breadth: 38 packages, 86%+ test coverage, 5 protocol adapters, tiered storage, clustering, and enterprise security features. The code compiles cleanly, all tests pass, and the project is well-documented.

**However**, a deep line-by-line audit revealed that **6 major features are dead code** — they compile, they have tests, but they are never wired into the runtime:

1. **WASM Transforms** — `b.wasmRT` never initialized in `Broker.Start()`
2. **Stream Processor** — `b.processor` never initialized
3. **TTL Enforcement** — `SetTopicConfig` never called; scanner runs on empty set
4. **Delayed Delivery** — `Ready()` channel never consumed; messages never delivered
5. **Priority Queue** — `PriorityDispatcher` never instantiated
6. **ISR Replication** — `SetTransport` never called; replication is no-op

Additionally:
- **OAuth provider has a critical `alg: none` JWT bypass vulnerability**
- **Raft election quorum is miscalculated** — odd-sized clusters can't elect leaders
- **SSTable.Get reads entire file into memory** — OOM risk with large warm tier
- **WebSocket auth is completely broken**
- **MQTT has a packet ID race condition**

**What's actually working:**
- Single-node broker core (publish/subscribe via all 5 protocols)
- Segment-based hot storage with WAL durability
- Queue engine (round-robin dispatch, ack/nack, basic DLQ)
- Stream engine (consumer groups, rebalance, offset store)
- Schema registry and enforcement
- Auth (static, file, LDAP, mTLS — OAuth has bypass bug)
- TLS, encryption at rest, ACL engine
- Tier migration (hot->warm->cold)
- Test infrastructure (unit, integration, chaos, benchmarks)

---

## Phase 0: Emergency Fixes (Days 1-3)

### Must-fix items that are currently broken

- [x] **EF-1: Wire WASM runtime in Broker.Start()** — `broker/broker.go` line ~260: add WASM runtime initialization when `b.config.WASM.Enabled`. The `b.wasmRT` field is declared but never set. ~50 lines of init code. Effort: 2 hours.

- [x] **EF-2: Wire stream processor in Broker.Start()** — `broker/broker.go` line ~260: add processor initialization with `processing.BrokerAPI` adapter. ~30 lines. Effort: 2 hours.

- [x] **EF-3: Connect TTL topics to expirer** — `broker/broker.go` in `Start()` after topic manager init, call `b.ttlExpirer.SetTopicConfig()` for each topic with TTL config. In `CreateTopic()`, call `SetTopicConfig` for new topics. Effort: 2 hours.

- [x] **EF-4: Consume delay scheduler Ready() channel** — `engine/queue/engine.go` or `broker/`: start a goroutine that reads from `ds.Ready()` and re-publishes promoted messages. Without this, delayed delivery is non-functional. Effort: 3 hours.

- [x] **EF-5: Wire PriorityDispatcher** — `engine/queue/engine.go`: when creating `QueueState`, check topic priority config and create `PriorityDispatcher` instead of `Dispatcher`. Effort: 2 hours.

- [x] **EF-6: Wire replicator transport** — `cluster/manager.go` line ~270: call `replicator.SetTransport()` with the Raft TCP transport (or a dedicated replication transport). Without this, data replication is silently skipped. Effort: 1 hour.

- [x] **EF-7: Fix OAuth alg:none bypass** — `auth/oauth.go` in `verifyJWT()`: validate that the JWT `alg` header matches the expected algorithm for the key type (RS256/ES256/EdDSA). Reject `alg: none` and mismatched algorithms. **This is a critical security vulnerability.** Effort: 1 hour.

- [x] **EF-8: Fix Raft election quorum** — `cluster/raft/node.go` line ~298: change quorum check from `votesReceived > len(n.peers)/2+1` to `votesReceived >= (len(n.peers)+1)/2 + 1` (majority of total nodes including self). Current formula requires ALL votes for 3-node cluster. Effort: 30 minutes.

- [x] **EF-9: Fix SSTable.Get full-file read** — `storage/warm/sstable.go` lines 242-244: replace full-file read with block-level lookup using the block index. Read only the relevant block, not the entire file. Effort: 4 hours.

---

## Phase 1: Critical Fixes (Week 1-2)

### Must-fix items blocking basic clustered functionality

- [ ] **TD-6: Migrate consumer offsets to replicated storage** — `engine/stream/offset.go` stores offsets as JSON files. Migrate to internal compacted topic `$chimera/offsets` or Raft-backed store. This is critical for cluster failover — current approach loses offsets on node failure. Effort: 3-5 days.

- [ ] **TD-7: Add multi-node integration tests** — Create `test/integration/cluster_test.go` that starts 3+ broker nodes, validates leader election, data replication, failover, and consumer group rebalancing across nodes. Without this, clustering claims are unverifiable. Effort: 5-7 days.

- [ ] **TD-8: Run performance benchmarks against targets** — Execute `make bench` and `make bench-e2e` with realistic workloads. Publish results. If targets aren't met, profile and optimize the hot paths (Publish, Append, Marshal). Effort: 3-5 days.

- [x] **TD-9: Add WAL entry for DeleteTopic** — `broker/topic.go`: write a tombstone WAL entry when deleting a topic so recovery doesn't resurrect deleted topics. Effort: 2 hours.

- [x] **TD-11: Make offset persistence atomic** — `engine/stream/offset.go`: use write-to-tmp + rename pattern (already used by TopicManager.saveMetadata). Effort: 1 hour.

- [x] **TD-12: Fix MQTT NextPacketID race** — `protocol/mqtt/session.go`: hold mutex through both the check and return of packet ID, or use atomic increment. Effort: 1 hour.

- [x] **TD-13: Fix WebSocket basic auth** — `protocol/ws/server.go` lines 71-73: properly parse `Authorization: Basic base64(user:pass)` header instead of using raw header as username. Effort: 1 hour.

- [x] **TD-14: Fix index file cleanup** — `storage/hot/retention.go` line 91: change `seg.path + ".idx"` to match the actual index path computed by `SaveIndex` (which strips `.log` and appends `idx`). Effort: 30 minutes.

- [x] **TD-15: Don't drop Raft state save errors** — `cluster/raft/node.go` line 637-638: log and handle errors from `json.Marshal`/`os.WriteFile`. Effort: 30 minutes.

- [x] **TD-18: Fix WaitGroup usage** — `broker/broker.go`: either use `wg.Add(1)` for background goroutines or remove the WaitGroup. Currently `wg.Wait()` in `Stop()` is a no-op. Effort: 2 hours.

- [x] **Remove panic from UI embed** — `internal/ui/embed.go:19` calls `panic()` on embed error. Change to return error from `Start()`. Effort: 30 minutes.

- [x] **Add MaxMessageSize enforcement in Publish** — `broker/publish.go`: check `len(env.Payload)` against `b.config.Limits.MaxMessageSize` before processing. Effort: 30 minutes.

---

## Phase 2: Core Completion (Week 3-4)

### Complete missing core features from specification

- [x] **AMQP Exchange/Binding routing** — Implement direct, topic, fanout, and headers exchange types with binding resolution. Reference: SPEC §3.3. Affected files: `internal/protocol/amqp/`. Effort: 5-7 days.

- [x] **MQTT QoS 2 verification** — Add integration tests for MQTT QoS 2 exactly-once delivery (4-step PUBREC/PUBREL/PUBCOMP handshake). Reference: SPEC §3.4. Affected files: `internal/protocol/mqtt/`. Effort: 2-3 days.

- [x] **Stream Processing Join operator** — Implement `JoinOp` for co-partitioned stream joins. Reference: SPEC §10.2. Affected files: `internal/processing/`. Effort: 5-7 days.

- [x] **Fix stream processor busy loop** — `processing/processor.go`: add sleep/backoff when no messages found in `runTopology`. Effort: 1 hour.

- [x] **Fix aggregate emission** — `processing/processor.go`: integrate `Tick()` calls for aggregate operators so windows actually emit results. Effort: 2-3 hours.

- [x] **DLQ persistence** — `engine/dlq/dlq.go`: persist DLQ entries to disk (write-ahead or append-only file) so they survive restart. Effort: 1-2 days.

- [x] **SCRAM-SHA-256 authentication** — Implement per spec §11.1. Affected files: `internal/auth/`. Effort: 2-3 days.

- [x] **Consumer group sticky rebalancing** — Implement true sticky rebalance (currently falls through to round-robin). Reference: SPEC §6.2. Effort: 2-3 days.

- [x] **Tenant rate limit enforcement** — `tenant/tenant.go`: actually track and enforce rate limits (currently dead code). Effort: 1-2 days.

---

## Phase 3: Hardening (Week 5-6)

### Security, error handling, edge cases

- [x] **Audit 110 discarded errors** — Review all `_ = err` instances in source files. Many are intentional (deferred close), but some may hide real bugs. Fix any that are genuine error suppression. Effort: 2-3 days.

- [x] **Fix constant-time token comparison** — `auth/static.go`: use `subtle.ConstantTimeCompare` for token auth. Effort: 30 minutes.

- [x] **Remove plaintext password fallback** — `auth/static.go`: only allow bcrypt in production (or require explicit `CHIMERA_ALLOW_PLAINTEXT=1` env var). Effort: 1 hour.

- [x] **Add WebSocket message size limit** — `protocol/ws/server.go`: set `ReadLimit` on WebSocket connection. Effort: 30 minutes.

- [x] **Input validation hardening** — Clamp all user-controlled values: partition count, fetch limits, message sizes, timeout durations, keepalive intervals. Effort: 2-3 days.

- [x] **Error message sanitization** — Replace `err.Error()` with generic messages in HTTP/TCP responses. Effort: 1-2 days.

- [x] **Fix segment `frozen` field data race** — `storage/hot/segment.go`: use atomic or lock consistently for `frozen` field. Effort: 1 hour.

- [x] **Fix compaction lock hold time** — `storage/hot/compaction.go`: release partition lock during disk I/O, reacquire only for segment swap. Effort: 2-3 hours.

- [x] **Default configuration hardening** — Change default bind from `0.0.0.0` to `127.0.0.1`. Enable auth by default with a generated token. Add startup warning when auth is disabled. Effort: 1 day.

- [x] **MCP version injection** — `mcp/server.go`: inject version via ldflags instead of hardcoding "0.7.0". Effort: 30 minutes.

---

## Phase 4: Performance & Optimization (Week 7-8)

### Performance tuning and optimization

- [ ] **SSTable block-level reads** — Expand on EF-9: implement proper block-level caching, mmap for read-only SSTables. Affected files: `internal/storage/warm/sstable.go`. Effort: 3-5 days.

- [ ] **Partition write lock optimization** — Reduce lock hold time in `Partition.Append()` during segment rollover. Affected files: `internal/storage/hot/partition.go`. Effort: 2-3 days.

- [ ] **Batch read optimization** — Replace offset-by-offset `ReadRange()` with sequential scan. Effort: 2-3 days.

- [ ] **Raft log binary persistence** — Replace JSON with binary format for Raft log entries. Current JSON approach is O(n) per save with base64 bloat. Effort: 3-5 days.

- [x] **Gossip message authentication** — Add HMAC to UDP gossip messages. Effort: 1-2 days.

- [ ] **End-to-end latency optimization** — Profile P99 publish-to-consume latency. Target: <5ms publish, <2ms consume (per spec §17). Effort: 3-5 days.

---

## Phase 5: Testing & Validation (Week 9-10)

### Comprehensive test coverage expansion

- [ ] **Multi-node chaos tests** — Extend `test/chaos/` to start 3+ nodes, kill leader during publish, verify failover and data integrity. Effort: 3-5 days.

- [ ] **Dead-code integration tests** — Add integration tests that verify WASM transforms execute, TTL messages expire, delayed messages deliver, priority ordering works, and replication copies data. These features have unit tests but no integration validation. Effort: 3-5 days.

- [ ] **Protocol compliance tests** — Test MQTT compliance (QoS 0/1/2, retained, will). Test AMQP link flow control. Test WebSocket subscribe/fetch. Effort: 5-7 days.

- [ ] **Crash recovery tests** — Kill during WAL write, during segment append, during compaction, during tier migration. Verify no data loss. Effort: 3-5 days.

- [ ] **Load test framework** — Create `test/load/` with configurable producers/consumers. Target: validate 1M+ msg/sec single-node. Effort: 5-7 days.

---

## Phase 6: Documentation & DX (Week 11-12)

### Documentation and developer experience

- [ ] **Update TASKS.md** — Mark completed tasks. Add Phase 2-7 tasks. Effort: 2-3 hours.

- [ ] **Architecture decision records** — Create `docs/adr/` for key decisions: why JSON files for offsets, why no mmap, why WASM/Processor were unwired. Effort: 1-2 days.

- [ ] **Performance benchmark report** — Publish benchmark results to `docs/BENCHMARKS.md`. Effort: 1-2 days.

- [ ] **Web UI auth support** — Add login form / token input to dashboard so it works when auth is enabled. Effort: 1-2 days.

- [ ] **Helm chart** — Create Helm chart for Kubernetes deployment. Effort: 3-5 days.

- [ ] **Go client library** — Formalize `examples/client.go` into a proper Go client package. Effort: 5-7 days.

---

## Effort Summary

| Phase | Estimated Days | Priority | Dependencies |
|-------|---------------|----------|-------------|
| Phase 0: Emergency Fixes | 2-3 | CRITICAL | None — do this NOW |
| Phase 1: Critical Fixes | 15-20 | CRITICAL | Phase 0 |
| Phase 2: Core Completion | 20-28 | HIGH | Phase 0 |
| Phase 3: Hardening | 10-14 | HIGH | Phase 0 |
| Phase 4: Performance | 14-20 | MEDIUM | Phase 1 |
| Phase 5: Testing | 20-30 | MEDIUM | Phase 1, Phase 2 |
| Phase 6: Documentation | 14-20 | LOW | Phase 5 |
| **Total** | **95-135 days** | | |

Note: Phases 2 and 3 can run in parallel after Phase 0. Phases 4 and 5 can overlap.

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Dead-code features have implementation bugs found when wired | High | High | Add integration tests in Phase 0 alongside wiring |
| SSTable full-file read causes OOM in production | High | Critical | Fix EF-9 before any warm tier deployment |
| OAuth bypass exploited before fix deployed | Medium | Critical | Deploy EF-7 immediately; rotate any issued tokens |
| Raft quorum bug causes split-brain in 3-node cluster | High | Critical | Fix EF-8 before any cluster deployment |
| Performance targets unachievable | Medium | High | Profile early after Phase 0 wiring |
| Single contributor bus factor | High | Critical | Document everything; add ADRs |

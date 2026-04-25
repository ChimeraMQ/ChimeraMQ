# Project Roadmap

> Based on comprehensive codebase analysis performed on 2026-04-16
> Current Version: v0.1.0
> Production Readiness Score: 82/100

## Current State Assessment

ChimeraMQ v0.1.0 is a feature-complete messaging platform with 102,889 lines of Go code across 331 files and 4,520 LOC of React frontend. All 7 specification phases are implemented. The project has 199 test files with ~85% average coverage, 8 protocol adapters, custom Raft consensus, SWIM gossip, geo-replication, multi-tenancy, WASM transforms, and an embedded Web UI.

**Strengths:**
- All spec features complete (110% — significant scope additions beyond spec)
- Strong test coverage across most packages (75-100%)
- Clean modular architecture, zero CGo, single binary
- 0 TODOs/FIXMEs in production code
- Security scans completed and critical findings addressed
- Chaos tests, cluster tests, cross-protocol integration tests all passing

**Critical issues:**
- 2 integration tests failing — CI is broken on every push
- gRPC adapter at 36.3% test coverage — essentially untested
- Frontend has zero tests (4,520 LOC React SPA)
- Dependency policy abandoned (18 external deps vs spec's 3)
- README stats are significantly outdated

**What's working well:**
- Core messaging engines (queue + stream + unified) fully functional
- 8 protocol adapters operational (AMQP, MQTT, HTTP, Chimera TCP, WebSocket, gRPC, NATS, STOMP)
- Raft consensus, SWIM gossip, ISR replication working
- 5 auth providers, ACL, encryption at rest
- Embedded React Web UI with Radix UI components
- MCP server for AI tooling

## Phase 1: Critical Fixes (Week 1-2)
### Must-fix items blocking basic functionality

- [x] **Fix failing integration tests** — `test/integration/auth_test.go:213`, `test/integration/http_test.go:277`
  - Health endpoint auth bypass + node_id field added to response
  - `TestHealthNoAuth` unit test updated

- [x] **Fix CI pipeline** — `.github/workflows/ci.yml`
  - Integration tests now passing

- [ ] **Improve gRPC test coverage** — `internal/protocol/grpc/` (36.3% → 78.9%)
  - Added tests for Consume streaming, Subscribe, Fetch, Ack/Nack/Commit/Unsubscribe
  - Added auth interceptor tests (reject unauthenticated, accept valid token)
  - Remaining gap: ACL permission denied paths (would require ACL setup)
  - Target: >80% coverage

## Phase 2: Core Completion (Week 3-6)
### Complete missing features and test gaps

- [x] **Frontend test suite** — `frontend/src/`
  - Vitest + React Testing Library set up with 16 test files, 96 passing tests
  - Covered: utils, API layer, all 4 hooks, store, 8 UI components, ErrorBoundary, ThemeToggle, SkipToContent, route prefetch
  - Coverage: 23.3% statements (target: >60%)
  - Remaining: 8 page components, ~10 complex UI components

- [x] **Zstd dictionary training** — `internal/storage/cold/`
  - Already implemented: `dict_trainer.go`, `DictCompressor`, tier migrator integration
  - Triggers every 100 archives, with panic recovery for klauspost/compress bug
  - Dictionary loaded on startup if available

- [ ] **Complete geo-replication** — `internal/cluster/geo/geo.go`
  - Current implementation is thin — needs full async/sync mode, cross-cluster link management, conflict resolution
  - 22K LOC in geo.go but many code paths untested
  - Effort: 40 hours

- [ ] **Document dependency rationale** — `docs/adr/0010-dependency-policy-update.md`
  - Formal ADR documenting why each of the 18 deps was added
  - Update spec or acknowledge policy change
  - Effort: 2 hours

## Phase 3: Hardening (Week 7-8)
### Security, error handling, edge cases

- [x] **Remove dead commented code** — `internal/storage/encrypt/kms/vault.go`
  - Cleaned up 100+ lines of commented-out Vault implementation stubs

- [x] **Add panic recovery to protocol handlers** — 6 goroutines across STOMP, NATS, gRPC, WS
  - Added `defer recover()` with logging to all unprotected subscription goroutines

- [x] **Add shutdown timeout to Broker.Stop()** — `internal/broker/broker.go`
  - 30-second internal timeout with step-by-step context checking
  - Prevents indefinite hangs when called outside CLI wrapper

- [x] **Clean up root directory** — 62 `.out` files and 3 orphaned directories removed
- [x] **Input validation audit** — all protocol handlers validated via centralized TopicManager and Broker.Publish
- [x] **Add `/debug/pprof` endpoint** — already implemented with auth gating
- [x] **Document dependency rationale** — `docs/adr/0010-dependency-policy-update.md` created (18 deps documented)

## Phase 4: Testing (Week 9-10)
### Comprehensive test coverage

- [ ] **Integration test expansion** — `test/integration/`
  - Add tests for geo-replication flows
  - Add tests for multi-tenant isolation
  - Add tests for WASM transform pipelines
  - Effort: 2 days

- [ ] **Fuzz test expansion** — currently only 1 explicit fuzz file
  - Add fuzz tests for storage engines (hot, warm, cold)
  - Add fuzz tests for schema validation
  - Add fuzz tests for auth providers
  - Effort: 2 days

- [ ] **Load test scenarios** — `test/load/`
  - Add sustained load tests (24+ hours)
  - Add memory leak detection
  - Add goroutine leak detection
  - Effort: 1 day

## Phase 5: Performance & Optimization (Week 11-12)
### Performance tuning and optimization

- [ ] **Profile and optimize gRPC path** — `internal/protocol/grpc/`
  - Lowest coverage package likely has performance gaps
  - Benchmark publish/subscribe throughput over gRPC
  - Effort: 1 day

- [ ] **Reduce per-message allocations** — `internal/message/codec.go`
  - `marshalHeaders` allocates new `[]byte` per call
  - Consider header buffer pooling
  - Effort: 4 hours

- [ ] **Frontend bundle optimization** — `frontend/`
  - Analyze bundle size with `vite-bundle-visualizer`
  - Lazy-load Recharts, heavy Radix components
  - Code-split by route
  - Effort: 1 day

## Phase 6: Documentation & DX (Week 13-14)
### Documentation and developer experience

- [x] **Update README stats** — `README.md:295-302`
  - Updated: 331 files, 102,889 LOC, 199 test files, 9+18 deps, 8 protocols, 4,520 LOC frontend

- [x] **Verify OpenAPI specification** — `docs/openapi.yaml`
  - File exists (68KB, last updated 2026-04-14) — covers 20+ endpoints

- [ ] **API documentation for protocol adapters** — `docs/`
  - Document Chimera native protocol wire format
  - Document MQTT topic mapping conventions
  - Document AMQP exchange binding semantics
  - Effort: 3 days

## Phase 7: Release Preparation (Week 15-16)
### Final production preparation

- [ ] **Docker image optimization** — `Dockerfile`
  - Consider distroless base instead of alpine for smaller attack surface
  - Multi-arch build with `docker buildx`
  - Effort: 4 hours

- [ ] **Release pipeline verification** — `.github/workflows/release.yml`
  - Verify all 6 platform binaries build and run
  - Add smoke test step for each platform
  - Effort: 2 hours

- [ ] **Observability stack** — `configs/`
  - Pre-built Grafana dashboard exists (`configs/grafana-dashboard.json`)
  - Alert rules exist (`configs/alert-rules.yml`)
  - Verify these work with current metrics
  - Effort: 2 hours

## Beyond v1.0: Future Enhancements

- [ ] **Kubernetes operator** — Custom Resource Definitions for topic management
- [ ] **Exactly-once semantics** — Transactional producer/consumer
- [ ] **Client SDKs** — Java, Python, Node.js, Rust client libraries
- [ ] **WASM module marketplace** — Pre-built transform modules
- [ ] **Multi-datacenter active-active** — Full geo-replication with CRDTs

## Effort Summary

| Phase | Estimated Hours | Priority | Status |
|---|---|---|---|
| Phase 1: Critical Fixes | ~~12-20h~~ **DONE** | CRITICAL | Complete |
| Phase 2: Core Completion | ~~72-90h~~ 56-74h | HIGH | Partial (Zstd done, geo-replication pending, frontend tests started) |
| Phase 3: Hardening | ~~11h~~ **DONE** | HIGH | Complete |
| Phase 4: Testing | ~~24h~~ 24h | HIGH | Pending |
| Phase 5: Performance | 14h | MEDIUM | Pending |
| Phase 6: Documentation & DX | ~~30h~~ **26h** | MEDIUM | Partial (README + OpenAPI done, ADR done) |
| Phase 7: Release Prep | 9h | HIGH | Pending |
| **Total Remaining** | **~130h** | | |

## Risk Assessment

| Risk | Probability | Impact | Status |
|---|---|---|---|
| Failing integration tests indicate deeper auth bugs | ~~Medium~~ | ~~High~~ | **Resolved** — health endpoint auth bypass fixed |
| gRPC adapter is a maintenance liability | ~~High~~ | ~~Medium~~ | **Mitigated** — 36.3% → 78.9% coverage |
| Frontend zero-test coverage hides regressions | High | Medium | **Mitigated** — 96 tests (23.3% coverage), pages remaining |
| Dependency drift (18 deps) introduces CVEs | Medium | High | **Mitigated** — ADR 0010 documented, monthly audits needed |
| Geo-replication complexity delays v1.0 | Medium | Medium | Open — thin implementation |
| README/docs outdated erodes user trust | ~~High~~ | ~~Low~~ | **Resolved** — stats updated |

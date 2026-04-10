# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

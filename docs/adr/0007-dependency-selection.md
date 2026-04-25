# ADR 0007: External Dependency Selection

## Status: Accepted

## Context

ChimeraMQ is written in pure Go (no CGo) and aims for a minimal dependency
footprint. When external libraries were needed for WASM runtime, observability,
LDAP authentication, and WebSocket support, we evaluated several options.

## Dependencies Added

### wazero (github.com/tetratelabs/wazero)

**Purpose:** WASM runtime for transform pipelines.

**Alternatives considered:**
- wasmtime-go (Bytecode Alliance) — requires CGo
- wasm3 — requires CGo, C runtime
- Custom WASM interpreter — reinventing the wheel

**Decision:** wazero — pure Go WASM runtime, no CGo required.

**Why:**
- Only production-ready WASM runtime in pure Go
- Supports WASI, enabling filesystem and network access in modules
- Zero external dependencies of its own
- Actively maintained by Tetrate

### OpenTelemetry (go.opentelemetry.io/otel)

**Purpose:** Distributed tracing for publish pipeline and cluster operations.

**Alternatives considered:**
- OpenCensus — deprecated, merged into OpenTelemetry
- Custom tracing — reinventing tracing infrastructure
- Jaeger client — vendor-specific, deprecated

**Decision:** OpenTelemetry SDK with OTLP gRPC exporter.

**Why:**
- Industry standard for observability
- Compatible with any OTLP-compatible backend (Jaeger, Tempo, Honeycomb, Datadog)
- Trace context propagation across protocol boundaries
- Metrics collector already exists internally; OTel traces complement it

### go-ldap/ldap/v3 (github.com/go-ldap/ldap/v3)

**Purpose:** LDAP/Active Directory authentication provider.

**Alternatives considered:**
- Custom LDAP client — protocol complexity (RFC 4511)
- mcncl/go-ldap — unmaintained fork
- OS-level PAM integration — requires CGo

**Decision:** go-ldap/v3 — standard Go LDAP library.

**Why:**
- Most actively maintained LDAP client in Go ecosystem
- Supports StartTLS, SASL, NTLM
- Well-tested against Active Directory and OpenLDAP
- Pure Go, no CGo required

### coder/websocket (github.com/coder/websocket)

**Purpose:** WebSocket protocol adapter for browser-based clients.

**Alternatives considered:**
- gorilla/websocket — project archived, no longer maintained
- gobwas/ws — complex API, more low-level than needed
- nhooyr.io/websocket — predecessor, coder/websocket is the maintained fork

**Decision:** coder/websocket — maintained fork of nhooyr.io/websocket.

**Why:**
- Clean, idiomatic API with context support
- No external dependencies
- Active maintenance by Coder team
- Supports per-message compression (RFC 7692)

## Consequences

- Total external dependencies: 4 direct (ldap, wazero, otel, websocket) plus yaml.v3 and x/crypto
- All are pure Go, maintaining the "no CGo" guarantee
- Dependency updates via dependabot (weekly scans configured)
- golangci-lint pinned to v2.11 for CI determinism

# ADR 0010: Dependency Policy Update

## Status: Accepted

## Context

ADR 0007 documented the original 4 external dependencies (wazero, otel, ldap, websocket). Since then, the project has grown to 18 external dependencies. This ADR documents the full dependency list, rationale for each addition, and the updated dependency policy.

## Current Dependencies (go.mod)

### Direct Dependencies

| Module | Version | Purpose | Rationale |
|---|---|---|---|
| `github.com/tetratelabs/wazero` | v1.11.0 | WASM runtime | Pure Go, no CGo — only viable option |
| `github.com/go-ldap/ldap/v3` | v3.4.13 | LDAP/AD auth | Standard Go LDAP client, pure Go |
| `github.com/coder/websocket` | v1.8.13 | WebSocket adapter | Maintained, clean API, no deps |
| `github.com/klauspost/compress` | v1.18.5 | Zstd compression | Fastest pure Go Zstd implementation |
| `google.golang.org/grpc` | v1.80.0 | gRPC protocol adapter | Required for gRPC protocol support |
| `google.golang.org/protobuf` | v1.36.11 | Protobuf serialization | Required by gRPC adapter |
| `gopkg.in/yaml.v3` | v3.0.1 | YAML config parsing | Standard Go YAML parser |
| `golang.org/x/crypto` | v0.50.0 | SCRAM, bcrypt, TLS | Go standard library extension |
| `go.opentelemetry.io/otel` | v1.43.0 | OpenTelemetry core | Distributed tracing |

### Indirect Dependencies (transitive)

| Module | Pulled In By |
|---|---|
| `github.com/Azure/go-ntlmssp` | go-ldap (NTLM SASL support) |
| `github.com/cenkalti/backoff/v5` | otel/exporters (retry logic) |
| `github.com/cespare/xxhash/v2` | otel (fast hashing) |
| `github.com/go-asn1-ber/asn1-ber` | go-ldap (ASN.1 encoding) |
| `github.com/go-logr/logr` | otel (logging interface) |
| `github.com/go-logr/stdr` | otel (standard logger bridge) |
| `github.com/google/uuid` | grpc (UUID generation) |
| `github.com/grpc-ecosystem/grpc-gateway/v2` | otel/exporters (OTLP gateway) |
| `go.opentelemetry.io/auto/sdk` | otel (auto-instrumentation) |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace` | otel (OTLP exporter) |
| `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` | otel (OTLP gRPC) |
| `go.opentelemetry.io/otel/sdk` | otel (SDK) |
| `go.opentelemetry.io/otel/trace` | otel (trace API) |
| `go.opentelemetry.io/otel/metric` | otel (metric API) |
| `go.opentelemetry.io/proto/otlp` | otel (OTLP protocol) |
| `golang.org/x/net` | crypto (networking) |
| `golang.org/x/sys` | crypto (syscalls) |
| `golang.org/x/text` | ldap (text encoding) |
| `google.golang.org/genproto/googleapis/api` | grpc (protobuf APIs) |
| `google.golang.org/genproto/googleapis/rpc` | grpc (RPC status) |

### Total: 9 direct + 18 indirect = 27 modules

## Decisions

### Why the growth from spec's "3 dependencies"?

The original spec projected ldap, wazero, and yaml.v3. The actual count grew due to:

1. **OpenTelemetry (5 packages)** — Spec requirement for distributed tracing. Not negotiable for production observability.
2. **gRPC + Protobuf (3 packages)** — Spec requirement for gRPC protocol adapter. Protobuf is mandatory for gRPC wire format.
3. **kompress** — Zstd compression for cold storage tier. Pure Go, no alternative in stdlib.
4. **Transitive dependencies** — Pulled in automatically by above. Most are single-purpose utilities (xxhash, backoff, uuid).

### Dependency acceptance criteria

All external dependencies must satisfy:
1. **Pure Go** — no CGo allowed, ever
2. **Actively maintained** — releases within last 6 months
3. **Minimal transitive deps** — prefers zero-dependency modules
4. **Well-known maintainers** — Google, Coder, Tetrate, klauspost, etc.
5. **No runtime code generation** — must be safe for production use

### Exceptions granted

- `google.golang.org/grpc` — has significant transitive deps but is the only viable gRPC implementation in Go
- `go.opentelemetry.io/otel` — 5 packages but industry standard for observability

## Consequences

- Dependency count: 9 direct, 18 indirect (vs. spec's 3)
- All 27 modules are pure Go — "no CGo" guarantee maintained
- Binary size: ~28MB (acceptable for feature set)
- Security: Regular scanning via `go list -m -json` + CVE checks
- Update policy: Monthly dependency audits, quarterly version bumps
- This ADR supersedes ADR 0007's dependency count (rationale remains valid)

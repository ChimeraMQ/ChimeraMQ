# ChimeraMQ

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache%202.0-C41E3A?style=flat)](LICENSE)
[![Release](https://img.shields.io/github/v/release/chimeramq/chimera?style=flat&color=D4A017)](https://github.com/chimeramq/chimera/releases)

**Three Heads. One Binary. All Messages.**

A unified message queue and event streaming platform built in pure Go. ChimeraMQ combines three engines in a single binary with production-grade security, observability, and multi-protocol support.

| Head | Engine | What It Does |
|------|--------|-------------|
| **Lion** | Queue | Competing consumers, ack/nack, DLQ, delayed delivery, visibility timeout |
| **Goat** | Stream | Offset-based consumption, consumer groups, long-poll, partitioned log |
| **Serpent** | Protocol | 5 protocol adapters — HTTP, Chimera TCP, MQTT, AMQP 1.0, WebSocket |

A single topic can be consumed as both a **stream** and a **queue** simultaneously (unified mode).

## Architecture

```
     ┌─────────────────────────────────────────────────────────────┐
     │                      Protocol Adapters                      │
     │            HTTP  |  Chimera TCP  |  MQTT  |  AMQP  |  WS    │
     └──────────────────────────┬──────────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────────┐
     │                    Auth Middleware (RBAC)                    │
     │   Static | File | OAuth 2.0/OIDC | LDAP | mTLS + ACL Engine│
     └──────────────────────────┬──────────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────────┐
     │                  OpenTelemetry Tracing                       │
     └──────────────────────────┬──────────────────────────────────┘
                                │
     ┌──────────────────────────▼──────────────────────────────────┐
     │                      Broker Core                             │
     │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌──────────────┐  │
     │  │  Queue   │  │ Stream  │  │ Schema  │  │  Stream      │  │
     │  │ Engine   │  │ Engine  │  │Registry │  │  Processor   │  │
     │  └────┬────┘  └────┬────┘  └────┬────┘  └──────┬───────┘  │
     │       └──────┬──────┘            │               │          │
     │  ┌──────────▼───────────────────▼───────────────▼───────┐  │
     │  │                Unified Topic Manager                  │  │
     │  └────────────────────────┬─────────────────────────────┘  │
     │  ┌────────────────────────▼─────────────────────────────┐  │
     │  │             Tiered Storage Engine                     │  │
     │  │  Hot (segments) → Warm (LSM-tree) → Cold (archives)  │  │
     │  └──────────────────────────────────────────────────────┘  │
     │  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌───────────┐   │
     │  │  WAL     │  │  Flow    │  │Metrics │  │  Config   │   │
     │  │ (CRC32C) │  │ Control  │  │(Prom.) │  │           │   │
     │  └──────────┘  └──────────┘  └────────┘  └───────────┘   │
     └─────────────────────────────────────────────────────────────┘

     ┌─────────────────────────────────────────────────────────────┐
     │            Clustering (Raft + SWIM Gossip + ISR)            │
     └─────────────────────────────────────────────────────────────┘

     ┌──────────────────────────┐  ┌──────────────────────────────┐
     │  MCP Server (AI tooling) │  │  Web UI Dashboard (/ui/)     │
     └──────────────────────────┘  └──────────────────────────────┘
```

## Features

### Core
- **Unified Topic Model** — One topic, three consumption modes: stream, queue, or both
- **5 Protocol Adapters** — HTTP REST, Chimera native binary TCP, MQTT 3.1.1/5.0, AMQP 1.0, WebSocket
- **Consumer Groups** — Range, RoundRobin, and Sticky rebalancing with offset management
- **Write-Ahead Log** — Durability with CRC32C verification and configurable sync modes
- **Tiered Storage** — Hot (segment files) → Warm (LSM-tree with bloom filters) → Cold (compressed archives)

### Security
- **Authentication** — Static tokens, file-based, OAuth 2.0/OIDC (JWKS), LDAP, mutual TLS
- **Authorization** — ACL engine with wildcard matching, per-resource permissions
- **TLS** — Configurable TLS 1.2+ with client certificate verification
- **Rate Limiting** — Per-topic publish rate limits, connection limits, payload size caps

### Reliability
- **Dead Letter Queue** — Configurable max retries, peek/clear/replay API
- **Flow Control** — Memory backpressure with high/low watermarks, slow consumer detection
- **Idempotent Producer** — Per-producer dedup window with sequence tracking
- **Log Compaction** — Key-based compaction retaining latest value per routing key
- **TTL Expiry** — Automatic message expiration with segment-level cleanup

### Processing
- **WASM Transforms** — Runtime via wazero (pure Go, no CGo), module pooling, transform pipelines
- **Stream Processor** — Topology-based processing with filter, map, flatMap, aggregate operators
- **Schema Registry** — JSON Schema, Avro, Protobuf with compatibility modes
- **Multi-Tenancy** — Namespace isolation via topic prefix, per-tenant quotas

### Observability
- **Prometheus Metrics** — Built-in `/v1/metrics` endpoint
- **OpenTelemetry Tracing** — W3C TraceContext propagation, OTLP gRPC export
- **Web UI Dashboard** — Embedded SPA at `/ui/` with overview, topics, consumers, schemas, DLQ, cluster
- **MCP Server** — AI tooling integration via JSON-RPC over stdio

### Clustering
- **Custom Raft Consensus** — Leader election, log replication, snapshots
- **SWIM Gossip** — Failure detection with phi accrual, member state dissemination
- **ISR Replication** — In-sync replica sets with Leader/Quorum/All ack policies

## Quick Start

### Docker Compose (Recommended)

```bash
git clone https://github.com/ChimeraMQ/ChimeraMQ.git
cd ChimeraMQ

# Start ChimeraMQ
docker compose up -d

# With observability stack (Prometheus + Grafana)
docker compose --profile observability up -d
```

### Install from Source

```bash
git clone https://github.com/ChimeraMQ/ChimeraMQ.git
cd ChimeraMQ
make build

# Start with default config
./bin/chimera server --config configs/chimera.yaml
```

### Basic Operations

```bash
# Create topic
curl -X POST http://localhost:9090/v1/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"orders","mode":"unified","partitions":8}'

# Publish message
curl -X POST http://localhost:9090/v1/messages/orders \
  -H "Content-Type: application/json" \
  -d '{"order":"123"}'

# Consume messages
curl "http://localhost:9090/v1/messages/orders?partition=0&offset=0&limit=10"

# Health check
curl http://localhost:9090/v1/health

# Open dashboard
open http://localhost:9090/ui/
```

## Multi-Protocol Support

| Protocol | Port | Description |
|----------|------|-------------|
| HTTP REST | 9090 | Admin API, publish/consume, metrics, dashboard |
| Chimera TCP | 5672 | Binary protocol with pipelining, CRC32C, compression |
| MQTT | 5672* | MQTT 3.1.1/5.0 via protocol detection, QoS 0/1, retained messages |
| AMQP 1.0 | 5672* | AMQP via protocol detection, SASL PLAIN, link-based messaging |
| WebSocket | 5672*/ws | JSON and binary sub-protocols |

*Shared port with protocol auto-detection.

## HTTP Admin API

Full OpenAPI 3.0 spec: [docs/openapi.yaml](docs/openapi.yaml)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/topics` | Create topic |
| `GET` | `/v1/topics` | List topics |
| `GET` | `/v1/topics/{name}` | Describe topic |
| `DELETE` | `/v1/topics/{name}` | Delete topic |
| `POST` | `/v1/messages/{topic}` | Publish message |
| `GET` | `/v1/messages/{topic}` | Fetch messages |
| `POST` | `/v1/messages/{topic}/ack` | Acknowledge messages |
| `POST` | `/v1/messages/{topic}/nack` | Negative acknowledge |
| `POST` | `/v1/consumers/{group}/join` | Join consumer group |
| `POST` | `/v1/consumers/{group}/leave` | Leave consumer group |
| `POST` | `/v1/consumers/{group}/heartbeat` | Consumer heartbeat |
| `GET` | `/v1/consumers/{group}/offsets` | Get committed offsets |
| `POST` | `/v1/consumers/{group}/offsets` | Commit offsets |
| `POST` | `/v1/schemas/{subject}` | Register schema |
| `GET` | `/v1/schemas/{subject}/latest` | Get latest schema |
| `GET` | `/v1/dlq/{topic}` | Peek DLQ entries |
| `POST` | `/v1/dlq/{topic}/replay` | Replay DLQ entries |
| `DELETE` | `/v1/dlq/{topic}` | Clear DLQ |
| `POST` | `/v1/wasm/modules` | Upload WASM module |
| `POST` | `/v1/processors` | Create stream processor |
| `GET` | `/v1/cluster/members` | List cluster members |
| `GET` | `/v1/health` | Health check |
| `GET` | `/v1/metrics` | Prometheus metrics |

## Configuration

See [configs/chimera.yaml.example](configs/chimera.yaml.example) for the full reference.

```yaml
node:
  id: 1
  name: chimera-01
  data_dir: /var/lib/chimera

listener:
  bind: 0.0.0.0
  port: 5672
  admin_port: 9090

storage:
  hot:
    segment_size: 268435456    # 256MB
    sync_mode: interval
  wal:
    max_size: 134217728        # 128MB

defaults:
  topic:
    partitions: 8
    mode: unified

# Enable features
auth:
  type: static
  tokens:
    admin: "your-api-key"
observability:
  tracing:
    enabled: true
    endpoint: "localhost:4317"
  dashboard:
    enabled: true
```

## Project Structure

```
cmd/chimera/              Entry point, CLI router
internal/
  auth/                   Authentication providers (static, file, OAuth, LDAP, mTLS) + ACL engine
  broker/                 Broker orchestrator, config, publish pipeline
  cluster/                Raft consensus, SWIM gossip, ISR replication
  engine/                 DLQ, queue engine, stream engine, TTL
  flow/                   Flow control and backpressure
  idempotent/             Producer deduplication
  mcp/                    MCP server for AI tooling
  message/                Envelope codec, UUIDv7
  metrics/                Prometheus collector
  processing/             Stream processor with WASM transforms
  protocol/
    http/                 HTTP admin API server
    chimera/              Chimera binary TCP protocol
    mqtt/                 MQTT 3.1.1/5.0 adapter
    amqp/                 AMQP 1.0 adapter
    ws/                   WebSocket adapter
  schema/                 Schema registry (JSON, Avro, Protobuf)
  storage/
    hot/                  Segment storage with sparse indexing
    warm/                 LSM-tree with bloom filters, SSTables
    cold/                 Compressed cold archives
    tier/                 Tier migration orchestrator
    wal/                  Write-ahead log with CRC32C
    encrypt/              Encryption at rest
  tenant/                 Multi-tenancy with quotas
  tracing/                OpenTelemetry integration
  ui/                     Embedded Web UI dashboard
  wasm/                   WASM runtime (wazero)
test/
  integration/            End-to-end integration tests
  bench/                  Performance benchmarks
  chaos/                  Chaos and concurrency tests
web/dist/                 Dashboard SPA source
configs/                  Example configs, Prometheus config
docs/                     OpenAPI spec
```

## Development

```bash
make build          # Build binary → bin/chimera
make test           # Run all tests
make test-race      # Run tests with race detector
make integration    # Run integration tests
make chaos          # Run chaos/concurrency tests
make bench          # Run micro benchmarks
make bench-e2e      # Run end-to-end benchmarks
make lint           # Run go vet + golangci-lint
make cover          # Generate test coverage report
make docker         # Build Docker image
make release        # Cross-compile for 6 platforms
```

## Stats

| Metric | Value |
|--------|-------|
| Go packages | 38 |
| Source files | 331 |
| Lines of code | 102,889 |
| Test files | 199 |
| Test functions | 1,079+ |
| External dependencies | 9 direct + 18 indirect |
| Protocol adapters | 8 (AMQP, MQTT, HTTP, Chimera TCP, WebSocket, gRPC, NATS, STOMP) |
| Test coverage (avg) | ~85% |
| Frontend | 4,520 LOC React 19 + TypeScript |

## License

Apache License 2.0 — Copyright Ersin Koc / [ECOSTACK TECHNOLOGY OU](https://ecostack.ee).

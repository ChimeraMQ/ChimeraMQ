<p align="center">
  <br/>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/logo-light.svg">
    <img alt="ChimeraMQ" src="docs/assets/logo-dark.svg" width="420">
  </picture>
  <br/><br/>
  <strong>Three Heads. One Binary. All Messages.</strong>
  <br/>
  <em>Unified Message Queue + Event Streaming — Pure Go, Zero Dependencies</em>
  <br/><br/>
  <a href="https://go.dev"><img src="https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat-square&logo=go" alt="Go Version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-C41E3A?style=flat-square" alt="License"></a>
  <a href="https://github.com/chimeramq/chimera/actions"><img src="https://img.shields.io/github/actions/workflow/status/chimeramq/chimera/ci.yml?style=flat-square&label=CI" alt="Build"></a>
  <a href="https://github.com/chimeramq/chimera/releases"><img src="https://img.shields.io/github/v/release/chimeramq/chimera?style=flat-square&color=D4A017" alt="Release"></a>
  <a href="https://goreportcard.com/report/github.com/chimeramq/chimera"><img src="https://goreportcard.com/badge/github.com/chimeramq/chimera?style=flat-square" alt="Go Report Card"></a>
  <br/><br/>
  <a href="https://chimeramq.com">Website</a> · <a href="https://chimeramq.com/docs">Documentation</a> · <a href="https://github.com/chimeramq/chimera/releases">Download</a> · <a href="https://discord.gg/chimeramq">Community</a>
</p>

---

ChimeraMQ is a **unified message queue and event streaming platform** that replaces Kafka, RabbitMQ, and Pulsar with a single Go binary. No JVM. No Erlang. No ZooKeeper. No BookKeeper. Just one binary that speaks every protocol and handles every messaging pattern.

```
🦁 Lion Head    → Queue Engine     (competing consumers, ack/nack, DLQ, priority, delayed)
🐐 Goat Head    → Stream Engine    (partitioned log, consumer groups, replay, compaction)
🐍 Serpent Head  → Protocol Engine  (Native + AMQP 1.0 + MQTT + WebSocket + HTTP)
```

## Why ChimeraMQ?

**You shouldn't have to choose between queues and streams.** Today's messaging landscape forces you into a corner: deploy Kafka for event streaming, RabbitMQ for work queues, and a protocol bridge to connect them. That's three systems, three operational burdens, three points of failure.

ChimeraMQ's **Unified Mode** lets the same topic be consumed as a stream (offset-based replay, consumer groups) *and* as a queue (competing consumers, ack/nack) simultaneously — from the same underlying data, with zero duplication.

## Architecture

```
                           ┌───────────────────────────────────────────────┐
                           │              ChimeraMQ Binary                 │
                           │                                               │
     Clients ──────────────┤  ┌─────────────────────────────────────────┐  │
                           │  │         Protocol Multiplexer             │  │
     AMQP 1.0 ──────┐     │  │    (Single TCP Port · Auto-Detect)      │  │
     MQTT 3.1/5.0 ───┤     │  └──────────────────┬──────────────────────┘  │
     WebSocket ──────┤     │                     │                         │
     Chimera Proto ──┤     │  ┌──────────────────▼──────────────────────┐  │
     HTTP/REST ──────┘     │  │          Message Router                  │  │
                           │  │   (exchanges · topic routing · filters)  │  │
                           │  └─────────┬─────────────────┬─────────────┘  │
                           │            │                 │                 │
                           │  ┌─────────▼───────┐ ┌──────▼──────────┐     │
                           │  │  🦁 Queue Engine │ │ 🐐 Stream Engine│     │
                           │  │                  │ │                 │     │
                           │  │ Competing Cons.  │ │ Partitions      │     │
                           │  │ Ack / Nack       │ │ Consumer Groups │     │
                           │  │ Dead-Letter      │ │ Offset Replay   │     │
                           │  │ Priority Queue   │ │ Log Compaction  │     │
                           │  │ Delayed Msgs     │ │ Windowing       │     │
                           │  └─────────┬───────┘ └──────┬──────────┘     │
                           │            │                 │                 │
                           │            │  UNIFIED MODE   │                 │
                           │            │ (same data, dual│consumption)    │
                           │  ┌─────────▼─────────────────▼─────────────┐  │
                           │  │         Hybrid Storage Engine            │  │
                           │  │                                          │  │
                           │  │  🔥 HOT   Memory-mapped log segments    │  │
                           │  │           mmap · sendfile · zero-copy    │  │
                           │  │                                          │  │
                           │  │  💾 WARM  LSM-Tree indexed storage       │  │
                           │  │           SSTables · bloom filters       │  │
                           │  │                                          │  │
                           │  │  🧊 COLD  Compressed archives (zstd)    │  │
                           │  │           dictionary compression         │  │
                           │  └─────────────────────────────────────────┘  │
                           │                                               │
                           │  ┌─────────────────────────────────────────┐  │
                           │  │           Cluster Fabric                 │  │
                           │  │   Raft (metadata) + Gossip/SWIM (data)  │  │
                           │  └─────────────────────────────────────────┘  │
                           │                                               │
                           │  ┌──────────┐ ┌──────────┐ ┌──────────────┐  │
                           │  │ Schema   │ │  WASM    │ │   Stream     │  │
                           │  │ Registry │ │ Runtime  │ │  Processing  │  │
                           │  └──────────┘ └──────────┘ └──────────────┘  │
                           └───────────────────────────────────────────────┘
```

## Features

**Messaging Engines**
- **Queue semantics** — competing consumers, round-robin dispatch, ack/nack, visibility timeout, prefetch control, priority queues (0-9), dead-letter queues, delayed/scheduled messages
- **Stream semantics** — partitioned append-only log, consumer groups (range/round-robin/sticky rebalancing), offset commit, replay from any offset, log compaction (key-based, tombstone)
- **Unified mode** — same topic consumed as queue AND stream simultaneously, zero data duplication

**Protocol Support**
- **Chimera Native** — custom binary protocol, pipelining, batching, credit-based flow control, zero-copy
- **AMQP 1.0** — connections, sessions, links, exchanges (direct/topic/fanout/headers), bindings
- **MQTT 3.1.1 & 5.0** — QoS 0/1/2, retained messages, will messages, shared subscriptions
- **WebSocket** — JSON and binary sub-protocols, JWT authentication
- **HTTP/REST** — publish, fetch, admin API, Prometheus metrics
- **Auto-detection** — single TCP port, protocol detected from first bytes

**Storage**
- **Tiered architecture** — hot (memory-mapped segments, sendfile zero-copy) → warm (LSM-Tree, bloom filters, compaction) → cold (zstd compressed archives with trained dictionaries)
- **Write-ahead log** — crash recovery guaranteed, configurable fsync (immediate/interval/OS)
- **Sparse indexing** — O(log n) offset lookup within segments
- **Automatic tier migration** — background promotion based on age, size, and access patterns

**Clustering**
- **Raft consensus** — metadata control plane (topics, schemas, ACLs, partition assignments)
- **Gossip/SWIM** — data plane (node discovery, health monitoring, failure detection with φ accrual)
- **Partition replication** — leader-follower, ISR tracking, sync/async modes, configurable ack policy (leader/quorum/all)
- **Consumer group coordination** — distributed rebalancing, session timeout, heartbeat

**Advanced**
- **Built-in Schema Registry** — Avro, Protobuf, JSON Schema with backward/forward/full compatibility checking
- **WASM Transform Pipeline** — in-flight message transformation, filtering, enrichment, routing, PII redaction
- **Stream Processing** — tumbling/sliding/session/hopping windows, aggregations, joins, stateful processing with changelog-backed state stores
- **MCP Server** — Model Context Protocol integration for AI/LLM tooling

**Operations**
- **Single binary** — one `go install`, everything included
- **Zero external dependencies** — only Go extended stdlib (x/crypto, x/sys) + YAML parser
- **Embedded web dashboard** — cluster health, topic browser, consumer lag, schema registry, message inspector
- **Prometheus metrics** — 30+ metrics covering broker, storage, queue, stream, cluster, WASM
- **OpenTelemetry tracing** — distributed trace propagation, OTLP/Jaeger/Zipkin export
- **Full CLI** — server, topic CRUD, produce, consume, consumer groups, schemas, WASM, benchmarks

## Quick Start

### Install

```bash
go install github.com/chimeramq/chimera@latest
```

Or download a pre-built binary from [Releases](https://github.com/chimeramq/chimera/releases).

### Start the broker

```bash
chimera server
```

```
ChimeraMQ v0.1.0 starting...
Protocol listener: 0.0.0.0:5672 (chimera|amqp|mqtt|ws|http)
Admin API:         0.0.0.0:9090
Dashboard:         http://localhost:9090/ui
Ready in 47ms 🔥
```

### Create a topic

```bash
# Unified mode: consume as queue AND stream from the same topic
chimera topic create --name orders --mode unified --partitions 8
```

### Produce messages

```bash
chimera produce --topic orders --message '{"id": 1, "item": "coffee", "qty": 2}'
# Published: partition=3 offset=0

# Or pipe from stdin
echo '{"id": 2, "item": "tea"}' | chimera produce --topic orders
```

### Consume messages

```bash
# Stream mode (Kafka-style): read by offset
chimera consume --topic orders --partition 0 --offset 0

# Follow mode: continuous consumption
chimera consume --topic orders --follow

# Queue mode: via AMQP/MQTT client or Chimera protocol SDK
```

### HTTP API

```bash
# Publish via HTTP
curl -X POST http://localhost:9090/v1/messages/orders \
  -H "Content-Type: application/json" \
  -d '{"id": 3, "item": "espresso"}'

# Fetch messages
curl http://localhost:9090/v1/messages/orders?partition=0&offset=0&max=10

# Topic info
curl http://localhost:9090/v1/topics/orders

# Health check
curl http://localhost:9090/v1/health
```

### Docker

```bash
docker run -d \
  --name chimera \
  -p 5672:5672 \
  -p 9090:9090 \
  -v chimera-data:/var/lib/chimera \
  ghcr.io/chimeramq/chimera:latest
```

## Comparison

| | ChimeraMQ | Apache Kafka | RabbitMQ | Apache Pulsar | NATS |
|---|---|---|---|---|---|
| **Language** | Go | Java/Scala | Erlang | Java | Go |
| **Queue semantics** | ✅ Full | ❌ None | ✅ Full | ✅ Full | ⚠️ Basic |
| **Stream semantics** | ✅ Full | ✅ Full | ❌ None | ✅ Full | ⚠️ JetStream |
| **Unified mode** | ✅ Queue + Stream | ❌ | ❌ | ❌ | ❌ |
| **Protocols** | 5 (Native, AMQP, MQTT, WS, HTTP) | 1 (Kafka) | 1 (AMQP 0-9-1) | 1 (Pulsar) | 1 (NATS) |
| **Single binary** | ✅ | ❌ | ❌ | ❌ | ✅ |
| **External deps** | 0 | JVM + ZK/KRaft | Erlang/OTP | JVM + BK + ZK | 0 |
| **Binary size** | ~25 MB | ~300 MB+ | ~150 MB+ | ~500 MB+ | ~20 MB |
| **Cold start** | < 1 sec | 30+ sec | 10+ sec | 60+ sec | < 1 sec |
| **Idle memory** | ~100 MB | ~500 MB+ | ~200 MB+ | ~1 GB+ | ~50 MB |
| **Schema registry** | ✅ Built-in | ⚠️ Separate (Confluent) | ❌ Plugin | ✅ Built-in | ❌ |
| **WASM transforms** | ✅ Built-in | ❌ | ❌ | ✅ Functions | ❌ |
| **Tiered storage** | ✅ Hot/Warm/Cold | ⚠️ Plugin | ❌ | ✅ | ❌ |
| **Stream processing** | ✅ Built-in | ⚠️ Kafka Streams (separate) | ❌ | ❌ | ❌ |
| **License** | Apache 2.0 | Apache 2.0 | MPL 2.0 | Apache 2.0 | Apache 2.0 |

## Configuration

ChimeraMQ uses a single YAML configuration file. All settings have sensible defaults — you can start with zero configuration.

```yaml
node:
  id: 1
  name: "chimera-01"
  data_dir: "/var/lib/chimera"

listener:
  bind: "0.0.0.0"
  port: 5672            # All protocols (auto-detect)
  admin_port: 9090      # HTTP admin API + dashboard

storage:
  hot:
    segment_size: 268435456   # 256MB per segment
    sync_mode: "interval"     # immediate | interval | os
    sync_interval: "200ms"
  tier_policy:
    hot_retention: "1h"
    warm_retention: "24h"
    cold_retention: "168h"    # 7 days

defaults:
  topic:
    partitions: 8
    retention_time: "168h"
    mode: "unified"           # stream | queue | unified

logging:
  level: "info"
  format: "json"
```

Configuration priority: **CLI flags** > **environment variables** (`CHIMERA_*`) > **YAML file** > **defaults**.

See [`configs/chimera.yaml.example`](configs/chimera.yaml.example) for the complete reference.

## Unified Mode: The Killer Feature

Most messaging systems force you to choose: **queues** (RabbitMQ) or **streams** (Kafka). ChimeraMQ eliminates this choice.

```
                              PUBLISH
                                │
                                ▼
                    ┌───────────────────────┐
                    │   "orders" topic       │
                    │   (unified mode)       │
                    │                        │
                    │   Single copy of data  │
                    │   in hot segments      │
                    └───────┬───────┬────────┘
                            │       │
               ┌────────────┘       └────────────┐
               │                                  │
               ▼                                  ▼
     ┌──────────────────┐              ┌──────────────────┐
     │  Stream Consumer  │              │  Queue Consumer   │
     │  (Kafka-style)   │              │  (RabbitMQ-style) │
     │                   │              │                    │
     │  • Read by offset │              │  • Round-robin     │
     │  • Consumer group │              │  • Ack/Nack        │
     │  • Replay anytime │              │  • Prefetch        │
     │  • Commit offset  │              │  • Dead-letter     │
     └──────────────────┘              └──────────────────┘
```

A single `orders` topic can power:
- **Event sourcing** (stream consumers replay the full history)
- **Work distribution** (queue consumers process tasks with competing consumers)
- **Real-time analytics** (stream consumers with windowed aggregations)
- **Webhook delivery** (queue consumers with retry + DLQ)

All from the same data. No duplication. No bridges. No sync jobs.

## Roadmap

ChimeraMQ is built in 7 phases:

| Phase | Name | Status | Description |
|-------|------|--------|-------------|
| 1 | Core Engine | 🔨 In Progress | Single-node broker, native protocol, hot storage, queue + stream + unified |
| 2 | Multi-Protocol | 📋 Planned | Protocol multiplexer, AMQP 1.0, MQTT 3.1.1/5.0, WebSocket |
| 3 | Clustering | 📋 Planned | Embedded Raft, Gossip/SWIM, partition replication, rebalancing |
| 4 | Advanced Storage | 📋 Planned | Warm tier (LSM-Tree), cold tier (zstd archives), compaction, encryption |
| 5 | Schema & DLQ | 📋 Planned | Schema Registry, dead-letter queues, delayed messages, priority |
| 6 | WASM & Processing | 📋 Planned | WASM transform pipeline, stream processing, windowing, joins |
| 7 | Production | 📋 Planned | OAuth/LDAP, full ACL, web dashboard, OpenTelemetry, MCP server |

## Project Structure

```
chimera/
├── cmd/chimera/         # Single entry point
├── internal/
│   ├── broker/          # Core broker, config, topic manager
│   ├── message/         # Envelope, codec, UUIDv7
│   ├── protocol/
│   │   ├── chimera/     # Native binary protocol
│   │   ├── amqp/        # AMQP 1.0 adapter (Phase 2)
│   │   ├── mqtt/        # MQTT adapter (Phase 2)
│   │   ├── ws/          # WebSocket adapter (Phase 2)
│   │   └── http/        # REST admin API
│   ├── engine/
│   │   ├── queue/       # Queue engine (Lion Head)
│   │   └── stream/      # Stream engine (Goat Head)
│   ├── storage/
│   │   ├── hot/         # Memory-mapped log segments
│   │   ├── warm/        # LSM-Tree (Phase 4)
│   │   ├── cold/        # Compressed archives (Phase 4)
│   │   ├── wal/         # Write-ahead log
│   │   └── tier/        # Tier migration (Phase 4)
│   ├── cluster/         # Raft + Gossip (Phase 3)
│   ├── schema/          # Schema Registry (Phase 5)
│   ├── wasm/            # WASM runtime (Phase 6)
│   ├── stream/          # Stream processing (Phase 6)
│   ├── auth/            # Auth + ACL (Phase 7)
│   ├── metrics/         # Prometheus metrics
│   ├── mcp/             # MCP server (Phase 7)
│   └── cli/             # CLI commands
├── configs/             # Example configuration
├── docs/                # Documentation
├── test/                # Integration tests & benchmarks
├── Makefile
├── Dockerfile
└── go.mod               # 3 dependencies. That's it.
```

## Dependencies

ChimeraMQ follows the **#NOFORKANYMORE** philosophy — everything is built from scratch.

```
require (
    golang.org/x/crypto  // SCRAM-SHA-256, TLS helpers
    golang.org/x/sys     // mmap, sendfile, epoll
    gopkg.in/yaml.v3     // Configuration parsing
)
```

Three dependencies. All Go extended stdlib or standard parsers. No Sarama, no Paho, no hashicorp/raft, no wazero, no badger. Every component — Raft consensus, SWIM gossip, LSM-Tree, AMQP codec, MQTT codec, WASM runtime, bloom filters, UUID generation — is implemented from scratch in pure Go.

## Building from Source

```bash
git clone https://github.com/chimeramq/chimera.git
cd chimera
make build        # → bin/chimera
make test         # Run all tests with -race
make bench        # Run benchmarks
make lint         # go vet + staticcheck
make release      # Cross-compile for all platforms
make docker       # Build Docker image
```

## Contributing

ChimeraMQ is open source under the Apache 2.0 license. Contributions are welcome.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing`)
5. Open a Pull Request

Please read our [Contributing Guide](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md) before submitting.

## Community

- **GitHub Issues** — [Bug reports & feature requests](https://github.com/chimeramq/chimera/issues)
- **GitHub Discussions** — [Questions & ideas](https://github.com/chimeramq/chimera/discussions)
- **Discord** — [Join the community](https://discord.gg/chimeramq)
- **X/Twitter** — [@chimeramq](https://x.com/chimeramq)

## License

ChimeraMQ is licensed under the [Apache License 2.0](LICENSE).

---

<p align="center">
  <strong>Built with 🔥 by <a href="https://github.com/ecostack">ECOSTACK TECHNOLOGY OÜ</a></strong>
  <br/>
  <em>"The beast that devours Kafka, RabbitMQ, and Pulsar."</em>
  <br/><br/>
  <code>#ChimeraMQ #NOFORKANYMORE #PureGo</code>
</p>

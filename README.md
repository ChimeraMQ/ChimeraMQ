# ChimeraMQ

[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat&logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache%202.0-C41E3A?style=flat)](LICENSE)
[![Release](https://img.shields.io/github/v/release/chimeramq/chimera?style=flat&color=D4A017)](https://github.com/chimeramq/chimera/releases)

**Three Heads. One Binary. All Messages.**

A unified message queue and event streaming platform built in pure Go — no external dependencies beyond the standard library and `yaml.v3`.

ChimeraMQ combines three engines in a single binary:

| Head | Engine | What It Does |
|------|--------|-------------|
| **Lion** | Queue | Competing consumers, ack/nack, DLQ, delayed delivery, visibility timeout |
| **Goat** | Stream | Offset-based consumption, consumer groups, long-poll, partitioned log |
| **Serpent** | Protocol | HTTP admin API, Chimera native binary TCP protocol |

A single topic can be consumed as both a **stream** and a **queue** simultaneously (unified mode).

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    ChimeraMQ Broker                   │
│                                                      │
│  ┌─────────┐  ┌─────────┐  ┌──────────────────────┐ │
│  │  Queue   │  │ Stream  │  │  Protocol Multiplexer│ │
│  │ Engine   │  │ Engine  │  │  HTTP / Chimera TCP  │ │
│  │ (Lion)   │  │ (Goat)  │  │  (Serpent)           │ │
│  └────┬─────┘  └────┬────┘  └──────────┬───────────┘ │
│       │              │                   │             │
│       └──────┬───────┘                   │             │
│              │                           │             │
│  ┌───────────▼───────────────────────────▼───────────┐ │
│  │              Unified Topic Manager                 │ │
│  └───────────────────────┬───────────────────────────┘ │
│                          │                             │
│  ┌───────────────────────▼───────────────────────────┐ │
│  │                   Broker Core                      │ │
│  │  ┌─────┐  ┌──────────┐  ┌──────┐  ┌───────────┐  │ │
│  │  │ WAL │  │ Hot Tier │  │Metrics│  │  Config   │  │ │
│  │  │     │  │ (mmap)   │  │      │  │           │  │ │
│  │  └─────┘  └──────────┘  └──────┘  └───────────┘  │ │
│  └───────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

## Features

- **Unified Topic Model** — One topic, three consumption modes: stream, queue, or both
- **Zero External Dependencies** — Pure Go with only stdlib + `yaml.v3`
- **Write-Ahead Log** — Durability with CRC32C verification and configurable sync modes
- **Hot Tier Storage** — mmap-backed segment storage with sparse indexing
- **Consumer Groups** — Range, RoundRobin, and Sticky rebalancing strategies
- **Queue Engine** — Round-robin dispatch, prefetch limits, visibility timeout, DLQ routing
- **HTTP Admin API** — Full REST API for topic/message management
- **Chimera Protocol** — Custom binary TCP protocol with pipelining support
- **Prometheus Metrics** — Built-in `/v1/metrics` endpoint
- **UUIDv7 Message IDs** — Time-sortable with monotonic counter
- **Graceful Shutdown** — Clean shutdown on SIGINT/SIGTERM

## Quick Start

### Install from Source

```bash
# Clone and build
git clone https://github.com/chimeramq/chimera.git
cd chimera
make build

# Binary is at ./bin/chimera
```

### Docker (GHCR)

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/chimeramq/chimera:latest

# Run with default config
docker run -d \
  -p 5672:5672 \
  -p 9090:9090 \
  -v chimera-data:/var/lib/chimera \
  ghcr.io/chimeramq/chimera:latest

# Run with custom config
docker run -d \
  -p 5672:5672 \
  -p 9090:9090 \
  -v chimera-data:/var/lib/chimera \
  -v ./chimera.yaml:/etc/chimera/chimera.yaml \
  ghcr.io/chimeramq/chimera:latest
```

### Start the Broker

```bash
# With config file
./bin/chimera server --config configs/chimera.yaml

# With CLI overrides
./bin/chimera server --data-dir /tmp/chimera --port 5672 --admin-port 9090
```

### Create a Topic

```bash
./bin/chimera topic create --name orders --mode unified --partitions 8
```

Modes: `stream`, `queue`, `unified`

### Publish Messages

```bash
# Via CLI
./bin/chimera produce --topic orders --message '{"order":"123"}'

# Multiple messages
./bin/chimera produce --topic orders --message '{"order":"456"}' --count 100

# From stdin
echo '{"order":"789"}' | ./bin/chimera produce --topic orders
```

### Consume Messages

```bash
# Read from a partition
./bin/chimera consume --topic orders --partition 0 --offset 0 --limit 10

# Follow mode (continuous)
./bin/chimera consume --topic orders --follow
```

### Via HTTP API

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
```

## CLI Reference

```
chimera <command> [options]

Commands:
  server    Start the ChimeraMQ broker
  topic     Manage topics (create, list, describe, delete)
  produce   Produce messages to a topic
  consume   Consume messages from a topic
  version   Print version information
```

| Command | Subcommand | Key Flags |
|---------|-----------|-----------|
| `server` | — | `--config`, `--data-dir`, `--bind`, `--port`, `--admin-port`, `--log-level` |
| `topic` | `create` | `--name`, `--mode`, `--partitions` |
| `topic` | `list` | — |
| `topic` | `describe` | `<name>` |
| `topic` | `delete` | `<name>` |
| `produce` | — | `--topic`, `--message`, `--count` |
| `consume` | — | `--topic`, `--partition`, `--offset`, `--limit`, `--follow` |

Environment: `CHIMERA_ADMIN_ADDR` (default: `http://localhost:9090`) for CLI-to-broker communication.

## HTTP Admin API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/topics` | Create topic |
| `GET` | `/v1/topics` | List topics |
| `GET` | `/v1/topics/{name}` | Describe topic |
| `DELETE` | `/v1/topics/{name}` | Delete topic |
| `POST` | `/v1/messages/{topic}` | Publish message |
| `GET` | `/v1/messages/{topic}` | Consume messages (`?partition=&offset=&limit=&timeout=`) |
| `GET` | `/v1/health` | Health check |
| `GET` | `/v1/metrics` | Prometheus metrics |

**Ports:** `5672` (Chimera TCP protocol), `9090` (HTTP admin API)

## Configuration

See [configs/chimera.yaml.example](configs/chimera.yaml.example) for the full reference.

```yaml
node:
  id: 1
  name: chimera-01
  data_dir: /var/lib/chimera

listener:
  bind: 0.0.0.0
  port: 5672              # Chimera TCP protocol
  admin_port: 9090        # HTTP admin API
  max_connections: 100000

storage:
  hot:
    segment_size: 268435456    # 256MB per log segment
    sync_mode: interval        # immediate | interval | os
    sync_interval: 200ms
    max_segments: 10
  wal:
    max_size: 134217728        # 128MB max WAL size
    sync_mode: interval
    sync_interval: 100ms

defaults:
  topic:
    partitions: 8
    retention_time: 168h       # 7 days
    mode: unified              # stream | queue | unified

logging:
  level: info                  # debug | info | warn | error
  format: json                 # json | text
  output: stdout               # stdout | file
```

### Environment Variable Overrides

| Variable | Description |
|----------|-------------|
| `CHIMERA_NODE_ID` | Node ID |
| `CHIMERA_NODE_NAME` | Node name |
| `CHIMERA_DATA_DIR` | Data directory |
| `CHIMERA_LISTEN_PORT` | Chimera TCP port |
| `CHIMERA_ADMIN_PORT` | HTTP admin port |
| `CHIMERA_LOG_LEVEL` | Log level |
| `CHIMERA_LOG_FORMAT` | Log format |

## Project Structure

```
cmd/chimera/              Entry point, CLI router
internal/
  broker/                 Broker orchestrator, config, publish pipeline
  message/                Envelope, codec (binary wire format), UUIDv7
  storage/
    hot/                  mmap segment storage, sparse index, partitions
    wal/                  Write-ahead log with CRC32C
  engine/
    queue/                Queue engine (Lion) — dispatch, ack, DLQ, delay
    stream/               Stream engine (Goat) — consumer groups, long-poll
  protocol/
    http/                 HTTP admin API server
    chimera/              Chimera binary TCP protocol, client library
  metrics/                Prometheus-compatible metrics collector
  cli/                    CLI command handlers
configs/                  Example configuration
test/
  integration/            End-to-end integration tests
  bench/                  Performance benchmarks
```

## Development

```bash
make build          # Build binary → bin/chimera
make test           # Run all tests
make test-race      # Run tests with race detector
make integration    # Run integration tests
make bench          # Run micro benchmarks
make bench-e2e      # Run end-to-end benchmarks
make lint           # Run go vet
make cover          # Generate test coverage report
make clean          # Clean build artifacts
make docker         # Build Docker image (ghcr.io/chimeramq/chimera)
make release        # Cross-compile for linux/darwin/windows (amd64+arm64)
```

## License

Apache License 2.0 — Copyright Ersin Koc / [ECOSTACK TECHNOLOGY OU](https://ecostack.ee).

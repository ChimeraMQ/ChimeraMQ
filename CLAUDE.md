# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ChimeraMQ is a unified message queue and event streaming platform in pure Go (no CGo). A single binary provides three engines: **Lion** (queue with competing consumers, ack/nack, DLQ), **Goat** (stream with offset-based consumption, consumer groups), and **Serpent** (five protocol adapters). A single topic can be consumed as both a stream and a queue simultaneously.

- Module: `github.com/chimeramq/chimera`
- Go: 1.25+
- License: Apache 2.0

## Build & Development Commands

```bash
make build              # Build binary to bin/chimera (with version ldflags)
make test               # Unit tests: go test -count=1 -timeout 120s ./...
make test-race          # Tests with race detector
make lint               # go vet + golangci-lint
make cover              # Coverage report
make integration        # Integration tests (test/integration/)
make chaos              # Concurrency/chaos tests (test/chaos/)
make bench              # Micro benchmarks
make docker             # Build Docker image
make release            # Cross-compile for 6 platforms (linux/mac/windows x amd64/arm64)
make clean              # Remove bin/, coverage files
```

**Run a single test:**
```bash
go test ./internal/broker/ -run TestAcquireLockFile -v
```

**Run integration tests individually:**
```bash
go test ./test/integration/ -v -count=1 -timeout 120s
```

## Architecture

**Startup flow:** `cmd/chimera/main.go` -> `internal/cli/server.go` -> `internal/broker/broker.go`

The `Broker` struct is the central orchestrator holding all subsystem references. Startup sequence in `Broker.Start()`: Logger -> Auth -> ACL -> Data dir -> Lock file -> WAL -> Hot Storage -> Topic Manager -> Queue Engine -> Stream Engine -> Encryption -> Warm/Cold Tier -> Schema Registry -> TTL Expirer -> Cluster Manager -> DLQ -> Flow Control -> Idempotent Producer. Shutdown reverses this order.

**Publish pipeline:** `Broker.Publish()` flows through: idempotent dedup -> flow control -> schema enforcement -> WASM transforms -> partition routing -> WAL append -> hot storage append -> stream notification -> queue dispatch -> metrics update.

**Protocol multiplexing** (`internal/protocol/mux.go`): All protocols share TCP port 5672. The mux peeks at first bytes and routes by detection order: AMQP -> MQTT -> HTTP -> Chimera native TCP. Admin HTTP runs on separate port 9090.

**Config hierarchy:** CLI flags > env vars (`CHIMERA_*`) > YAML file > compiled defaults.

## Key Packages

| Package | Purpose |
|---------|---------|
| `internal/broker/` | Central orchestrator, config, publish pipeline, topic manager |
| `internal/protocol/` | Protocol adapters (http/, chimera/, mqtt/, amqp/, ws/) + mux.go for auto-detection |
| `internal/engine/queue/` | Queue engine: competing consumers, priority, delay, ack |
| `internal/engine/stream/` | Stream engine: offsets, consumer groups, waiters |
| `internal/storage/hot/` | Segment-based storage with sparse indexing, compaction |
| `internal/storage/warm/` | LSM-tree (bloom filters, SSTables, memtables) |
| `internal/storage/tier/` | Tier migration orchestrator (hot->warm->cold) |
| `internal/cluster/raft/` | Custom Raft consensus (leader election, log replication, snapshots) |
| `internal/cluster/gossip/` | SWIM gossip failure detection |
| `internal/auth/` | Auth providers (static, file, OAuth, LDAP, mTLS) + ACL engine |
| `internal/wasm/` | WASM runtime via wazero for transform pipelines |
| `internal/mcp/` | MCP server for AI tooling (JSON-RPC over stdio) |
| `internal/processing/` | Stream processor (filter, map, flatMap, aggregate, windowed) |
| `internal/message/` | Envelope codec, UUIDv7, wire format |
| `internal/schema/` | Schema registry (JSON, Avro, Protobuf) + enforcement |
| `internal/tenant/` | Multi-tenancy with namespace isolation and quotas |
| `internal/flow/` | Flow control / backpressure controller |
| `internal/idempotent/` | Producer deduplication |
| `internal/cli/` | CLI subcommands (server, topic, produce, consume, cluster, wasm, mcp) |

## Conventions

- **Commits:** Conventional commits format: `type(scope): description` (e.g., `feat(http): add consumer group endpoints`)
- **Testing:** Table-driven tests with standard `testing` package. Tests co-located as `*_test.go`. Extra coverage in `*_extra_test.go`, `*_edge_test.go`, `*_coverage_test.go` files.
- **Linter:** golangci-lint with govet, errcheck, staticcheck, unused, gosimple, ineffassign, typecheck, misspell, gofmt (configured in `.golangci.yml`)
- **Version injection:** Build uses ldflags (`-X main.version=...`) for version/commit/date
- **Pure Go:** No CGo dependency anywhere, including WASM runtime (wazero)
- **External deps are minimal:** Only 4 direct dependencies (ldap, wazero, otel, websocket) plus yaml.v3 and x/crypto

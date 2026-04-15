# ChimeraMQ v0.1.0 Release Notes

**Release Date:** 2026-04-12  
**Tag:** v0.1.0  
**Commit:** 1e2c147

## Overview

ChimeraMQ v0.1.0 is the initial production-ready release of the unified message queue and event streaming platform. This release includes all core features, protocol adapters, and enterprise capabilities.

## Downloads

| Platform | Architecture | Binary | Size |
|----------|-------------|--------|------|
| Linux | AMD64 | `chimera-v0.1.0-linux-amd64` | ~28MB |
| Linux | ARM64 | `chimera-v0.1.0-linux-arm64` | ~27MB |
| macOS | AMD64 | `chimera-v0.1.0-darwin-amd64` | ~30MB |
| macOS | ARM64 | `chimera-v0.1.0-darwin-arm64` | ~29MB |
| Windows | AMD64 | `chimera-v0.1.0-windows-amd64.exe` | ~29MB |
| Windows | ARM64 | `chimera-v0.1.0-windows-arm64.exe` | ~27MB |

## Features

### Protocol Adapters (7)
- **HTTP/REST** - Admin API, publish/fetch endpoints
- **Native TCP** - Custom binary protocol with pipelining
- **MQTT 3.1.1/5.0** - QoS 0/1/2, retained messages, will messages
- **AMQP 1.0** - Exchanges, bindings, sessions, links
- **WebSocket** - JSON and binary sub-protocols
- **STOMP** - Simple Text Oriented Messaging Protocol 1.2
- **NATS** - Core NATS protocol support

### Storage Tiers (3)
- **Hot** - Memory-mapped log segments with zero-copy sendfile
- **Warm** - LSM-tree with SSTables, bloom filters
- **Cold** - Zstd compressed archives with dictionary compression

### Clustering
- **Raft Consensus** - Metadata control plane
- **SWIM Gossip** - Node discovery and failure detection
- **ISR Replication** - Leader-follower replication with configurable ack policy

### Security
- Authentication: Static, File, OAuth/OIDC, LDAP, mTLS
- ACL with wildcard matching
- TLS 1.2+ support
- FIPS 140-2 compliance mode
- Encryption at rest (AES-256-GCM)
- KMS integration (AWS, Vault, Azure, GCP)

### Enterprise Features
- Multi-tenancy with resource quotas
- Geo-replication (async/sync modes)
- Audit logging with rotation
- Dead Letter Queue with conditional replay
- Stream processing (windowing, joins, aggregations)
- WASM transform pipeline
- Schema Registry (JSON, Avro, Protobuf)

### Operations
- Backup/restore CLI commands
- Rolling upgrade support
- Embedded Web UI dashboard
- Prometheus metrics
- OpenTelemetry tracing
- MCP server for AI tooling

## Performance

| Metric | Value |
|--------|-------|
| Throughput | 94K-275K msg/s |
| P99 Latency | <541μs |
| Binary Size | ~25-30MB |
| Idle Memory | ~50-100MB |

## Testing

- **1800+ tests** across 47 packages
- **90%+ code coverage**
- Unit tests, integration tests, chaos tests, load tests
- Race detector clean

## Quick Start

```bash
# Download and run
./chimera-v0.1.0-linux-amd64 server

# Or install via go
go install github.com/chimeramq/chimera@v0.1.0
```

## Docker

```bash
docker run -d \
  --name chimera \
  -p 5672:5672 \
  -p 9090:9090 \
  -v chimera-data:/var/lib/chimera \
  ghcr.io/chimeramq/chimera:v0.1.0
```

## Documentation

- [README.md](README.md) - Quick start and overview
- [CHANGELOG.md](CHANGELOG.md) - Full change history
- [PRODUCTIONREADY.md](.project/PRODUCTIONREADY.md) - Production readiness assessment
- [ROADMAP.md](.project/ROADMAP.md) - Future plans

## Verification

```bash
# Verify version
chimera version
# Expected: ChimeraMQ v0.1.0

# Run tests
go test -short ./...

# Check health
curl http://localhost:9090/v1/health
```

## Support

- GitHub Issues: https://github.com/chimeramq/chimera/issues
- Documentation: https://chimeramq.com/docs
- Community: https://discord.gg/chimeramq

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.

---

**Built with 🔥 by ECOSTACK TECHNOLOGY OÜ**

"The beast that devours Kafka, RabbitMQ, and Pulsar."

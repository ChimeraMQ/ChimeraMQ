# Contributing to ChimeraMQ

Thank you for your interest in contributing to ChimeraMQ!

## Development Setup

### Prerequisites

- Go 1.25+
- Git
- Docker (for containerized testing)

### Getting Started

```bash
git clone https://github.com/ChimeraMQ/ChimeraMQ.git
cd ChimeraMQ
go build ./...
go test ./... -count=1
```

## Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Ensure all tests pass (`make test`)
5. Run the linter (`make lint`)
6. Commit with a descriptive message
7. Push and open a Pull Request

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Use meaningful variable and function names
- Keep functions focused and concise
- Add tests for new functionality

## Project Structure

```
cmd/chimera/          # CLI entry point
internal/
  auth/               # Authentication providers (static, OAuth, LDAP, mTLS)
  broker/             # Core broker engine
  cluster/            # Raft consensus, SWIM gossip, replication
  engine/             # DLQ, queue, stream, TTL
  flow/               # Flow control and backpressure
  idempotent/         # Producer deduplication
  mcp/                # MCP server for AI tooling
  message/            # Envelope codec
  metrics/            # Prometheus collector
  processing/         # Stream processor topology
  protocol/           # Protocol adapters (HTTP, Chimera, MQTT, AMQP, WS)
  schema/             # Schema registry and enforcement
  storage/            # Hot/Warm/Cold tiered storage
  tenant/             # Multi-tenancy
  tracing/            # OpenTelemetry tracing
  ui/                 # Embedded Web UI dashboard
  wasm/               # WASM runtime for transforms
test/
  bench/              # Benchmark suite
  chaos/              # Chaos and concurrency tests
  integration/        # End-to-end integration tests
```

## Testing

```bash
make test              # Unit tests
make test-race         # Race detector
make integration       # Integration tests
make bench             # Benchmarks
```

## Commit Messages

Use the conventional commit format:

```
type(scope): description

feat(http): add consumer group endpoints
fix(storage): correct tier migration offset tracking
test(mqtt): add retained store wildcard tests
```

## Pull Request Process

1. PRs require at least one review
2. All CI checks must pass (build, test, lint)
3. Keep PRs focused — one feature or fix per PR
4. Update documentation if you change behavior

## Reporting Issues

- Use GitHub Issues
- Include steps to reproduce
- Specify Go version, OS, and ChimeraMQ version
- Include relevant logs or error messages

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

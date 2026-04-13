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

<!-- rtk-instructions v2 -->
# RTK (Rust Token Killer) - Token-Optimized Commands

## Golden Rule

**Always prefix commands with `rtk`**. If RTK has a dedicated filter, it uses it. If not, it passes through unchanged. This means RTK is always safe to use.

**Important**: Even in command chains with `&&`, use `rtk`:
```bash
# ❌ Wrong
git add . && git commit -m "msg" && git push

# ✅ Correct
rtk git add . && rtk git commit -m "msg" && rtk git push
```

## RTK Commands by Workflow

### Build & Compile (80-90% savings)
```bash
rtk cargo build         # Cargo build output
rtk cargo check         # Cargo check output
rtk cargo clippy        # Clippy warnings grouped by file (80%)
rtk tsc                 # TypeScript errors grouped by file/code (83%)
rtk lint                # ESLint/Biome violations grouped (84%)
rtk prettier --check    # Files needing format only (70%)
rtk next build          # Next.js build with route metrics (87%)
```

### Test (90-99% savings)
```bash
rtk cargo test          # Cargo test failures only (90%)
rtk vitest run          # Vitest failures only (99.5%)
rtk playwright test     # Playwright failures only (94%)
rtk test <cmd>          # Generic test wrapper - failures only
```

### Git (59-80% savings)
```bash
rtk git status          # Compact status
rtk git log             # Compact log (works with all git flags)
rtk git diff            # Compact diff (80%)
rtk git show            # Compact show (80%)
rtk git add             # Ultra-compact confirmations (59%)
rtk git commit          # Ultra-compact confirmations (59%)
rtk git push            # Ultra-compact confirmations
rtk git pull            # Ultra-compact confirmations
rtk git branch          # Compact branch list
rtk git fetch           # Compact fetch
rtk git stash           # Compact stash
rtk git worktree        # Compact worktree
```

Note: Git passthrough works for ALL subcommands, even those not explicitly listed.

### GitHub (26-87% savings)
```bash
rtk gh pr view <num>    # Compact PR view (87%)
rtk gh pr checks        # Compact PR checks (79%)
rtk gh run list         # Compact workflow runs (82%)
rtk gh issue list       # Compact issue list (80%)
rtk gh api              # Compact API responses (26%)
```

### JavaScript/TypeScript Tooling (70-90% savings)
```bash
rtk pnpm list           # Compact dependency tree (70%)
rtk pnpm outdated       # Compact outdated packages (80%)
rtk pnpm install        # Compact install output (90%)
rtk npm run <script>    # Compact npm script output
rtk npx <cmd>           # Compact npx command output
rtk prisma              # Prisma without ASCII art (88%)
```

### Files & Search (60-75% savings)
```bash
rtk ls <path>           # Tree format, compact (65%)
rtk read <file>         # Code reading with filtering (60%)
rtk grep <pattern>      # Search grouped by file (75%)
rtk find <pattern>      # Find grouped by directory (70%)
```

### Analysis & Debug (70-90% savings)
```bash
rtk err <cmd>           # Filter errors only from any command
rtk log <file>          # Deduplicated logs with counts
rtk json <file>         # JSON structure without values
rtk deps                # Dependency overview
rtk env                 # Environment variables compact
rtk summary <cmd>       # Smart summary of command output
rtk diff                # Ultra-compact diffs
```

### Infrastructure (85% savings)
```bash
rtk docker ps           # Compact container list
rtk docker images       # Compact image list
rtk docker logs <c>     # Deduplicated logs
rtk kubectl get         # Compact resource list
rtk kubectl logs        # Deduplicated pod logs
```

### Network (65-70% savings)
```bash
rtk curl <url>          # Compact HTTP responses (70%)
rtk wget <url>          # Compact download output (65%)
```

### Meta Commands
```bash
rtk gain                # View token savings statistics
rtk gain --history      # View command history with savings
rtk discover            # Analyze Claude Code sessions for missed RTK usage
rtk proxy <cmd>         # Run command without filtering (for debugging)
rtk init                # Add RTK instructions to CLAUDE.md
rtk init --global       # Add RTK to ~/.claude/CLAUDE.md
```

## Token Savings Overview

| Category | Commands | Typical Savings |
|----------|----------|-----------------|
| Tests | vitest, playwright, cargo test | 90-99% |
| Build | next, tsc, lint, prettier | 70-87% |
| Git | status, log, diff, add, commit | 59-80% |
| GitHub | gh pr, gh run, gh issue | 26-87% |
| Package Managers | pnpm, npm, npx | 70-90% |
| Files | ls, read, grep, find | 60-75% |
| Infrastructure | docker, kubectl | 85% |
| Network | curl, wget | 65-70% |

Overall average: **60-90% token reduction** on common development operations.
<!-- /rtk-instructions -->
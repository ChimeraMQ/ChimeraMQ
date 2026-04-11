# ChimeraMQ Performance Benchmarks

> Hardware: AMD Ryzen 9 9950X3D (16-core), Windows 11, Go 1.25
> Date: 2026-04-11 (updated after hot path optimizations)

## Summary

ChimeraMQ delivers excellent single-node performance with sub-7us publish latency
and 140K+ msg/s throughput in end-to-end benchmarks.

## End-to-End Publish Latency

| Benchmark | ns/op | allocs | msgs/sec |
|-----------|-------|--------|----------|
| E2E Publish (unified) | 6,984 | 8 | ~143K |
| E2E Publish Stream | 7,615 | 9 | ~131K |
| E2E Publish Queue | 6,855 | 9 | ~146K |
| E2E Publish Parallel | 3,814 | 9 | ~262K |
| E2E Publish MultiTopic | 7,336 | 9 | ~136K |
| E2E Publish Concurrent MultiTopic | 3,740 | 9 | ~267K |

## Load Test Results

| Scenario | Throughput | P99 Latency |
|----------|-----------|-------------|
| Single producer (1K msgs, 256B) | 81K msg/s | 537us |
| Multi producer (4x500) | 133K msg/s | 535us |
| 8 producers (8x500) | 228K msg/s | 541us |
| Large payload (4KB) | 43K msg/s | 534us |
| Queue mode (2 consumers) | 58K msg/s | 538us |
| Unified mode (2 consumers) | 160K msg/s | 530us |

## Codec Performance

| Benchmark | ns/op | B/op | allocs |
|-----------|-------|------|--------|
| Encode (1KB payload) | 2,285 | 4,127 | 2 |
| Decode | 205 | 232 | 3 |
| Round trip | 2,049 | 4,352 | 5 |
| Small payload (64B) | 1,657 | 4,126 | 2 |
| Large payload (64KB) | 18,379 | 78,146 | 5 |
| Encode parallel | 2,188 | 4,125 | 2 |
| Decode parallel | 105 | 232 | 3 |

## WAL Performance

| Benchmark | ns/op | B/op | allocs |
|-----------|-------|------|--------|
| Append (sync immediate) | 291,115 | 25 | 1 |
| Recovery (1000 entries) | 7,059,854 | 2.9M | 20,044 |
| Checkpoint | 205,570 | 563 | 7 |

## Envelope Operations

| Benchmark | ns/op | B/op | allocs |
|-----------|-------|------|--------|
| Create | 1.95 | 0 | 0 |
| Create parallel | 0.14 | 0 | 0 |
| Estimate size | 26.3 | 0 | 0 |
| With headers | 4.42 | 0 | 0 |
| Large payload (1MB) | 0.21 | 0 | 0 |

## Key Observations

1. **Zero-allocation envelope creation** — Creating envelopes is essentially free (1.95ns)
2. **Parallel scaling** — Parallel publish achieves 262K msg/s, 2.4x single-threaded
3. **Queue overhead minimal** — Queue mode is fastest at 6.9us, 2% faster than unified
4. **WAL sync is the bottleneck** — Immediate sync adds ~291us per append
5. **Codec efficient** — Decode at 205ns for 1KB payloads
6. **Hot path optimizations** — Pre-computed CRC32 table, pooled segment writes,
   lock-free highWater, sequential ReadRange reduced publish latency 23-30%

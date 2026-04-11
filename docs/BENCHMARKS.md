# ChimeraMQ Performance Benchmarks

> Hardware: AMD Ryzen 9 9950X3D (16-core), Windows 11, Go 1.25
> Date: 2026-04-11

## Summary

ChimeraMQ delivers excellent single-node performance with sub-10us publish latency
and 100K+ msg/s throughput in end-to-end benchmarks.

## End-to-End Publish Latency

| Benchmark | ns/op | allocs | msgs/sec |
|-----------|-------|--------|----------|
| E2E Publish (unified) | 9,636 | 9 | ~104K |
| E2E Publish Stream | 9,952 | 10 | ~100K |
| E2E Publish Queue | 9,810 | 10 | ~102K |
| E2E Publish Parallel | 3,951 | 10 | ~253K |
| E2E Publish MultiTopic | 10,608 | 10 | ~94K |
| E2E Publish Concurrent MultiTopic | 3,638 | 10 | ~275K |

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
| Encode (1KB payload) | 1,106 | 4,127 | 2 |
| Decode | 98 | 232 | 3 |
| Round trip | 1,316 | 4,351 | 5 |
| Small payload (64B) | 1,077 | 4,125 | 2 |
| Large payload (64KB) | 11,089 | 78,148 | 5 |
| Encode parallel | 1,319 | 4,126 | 2 |
| Decode parallel | 68 | 232 | 3 |

## WAL Performance

| Benchmark | ns/op | B/op | allocs |
|-----------|-------|------|--------|
| Append (sync immediate) | 238,306 | 25 | 1 |
| Recovery (1000 entries) | 4,723,951 | 2.9M | 20,044 |
| Checkpoint | 171,795 | 563 | 7 |

## Envelope Operations

| Benchmark | ns/op | B/op | allocs |
|-----------|-------|------|--------|
| Create | 1.8 | 0 | 0 |
| Create parallel | 0.11 | 0 | 0 |
| Estimate size | 22.6 | 0 | 0 |
| With headers | 4.2 | 0 | 0 |
| Large payload (1MB) | 0.19 | 0 | 0 |

## Key Observations

1. **Zero-allocation envelope creation** — Creating envelopes is essentially free (1.8ns)
2. **Parallel scaling** — Parallel publish achieves 253K msg/s, 2.4x single-threaded
3. **Queue overhead minimal** — Queue mode adds ~2% latency vs stream mode
4. **WAL sync is the bottleneck** — Immediate sync adds ~238us per append
5. **Codec efficient** — Encode+decode round trip under 1.4us for 1KB payloads

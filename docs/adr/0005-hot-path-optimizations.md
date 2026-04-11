# ADR 0005: Hot Path Performance Optimizations

## Status: Accepted

## Context

End-to-end publish latency was ~9.6us (unified mode). Profiling identified several
sources of unnecessary allocation, lock contention, and syscall overhead in the
publish and consume critical paths.

## Decision

Apply five targeted optimizations to the publish and consume hot paths:

1. **Pre-compute CRC32 table** — `crc32.MakeTable(crc32.Castagnoli)` was called
   per WAL append, allocating 1KB per message. Moved to package-level `var`.

2. **Remove ReleaseBuffer zeroing** — `codec.ReleaseBuffer` zeroed every byte
   before returning to pool. Marshal overwrites all bytes anyway, so zeroing
   was pure waste (O(n) memset per message).

3. **Pooled single-write segment append** — Segment used two `WriteAt` syscalls
   (length prefix + data). Combined into single pooled `WriteAt` via sync.Pool.

4. **Lock-free highWater** — `Partition.HighWatermark()` acquired RLock to read
   one uint64. Changed to `atomic.Uint64` for lock-free access.

5. **Sequential ReadRange in fetch** — `stream.readMessages` called `part.Read()`
   per offset (N lock cycles + N binary searches). Switched to `part.ReadRange()`
   which uses single lock + sequential scan.

## Why

- Each optimization targets a specific measured bottleneck
- No behavioral changes — all optimizations are transparent
- Atomic highWater is safe: only Append writes, many readers

## Trade-offs

- Pooled segment write adds pool Get/Put overhead per append, but saves a syscall
- Atomic highWater means non-atomic read of partition segments (but segments are
  protected by their own RWMutex, so this is safe)
- ReadRange allocates [][]byte slice upfront, but avoids per-message lock overhead

## Results

| Mode | Before | After | Improvement |
|------|--------|-------|-------------|
| Unified | 9,636 ns | 6,984 ns | 27.5% |
| Queue | 9,810 ns | 6,855 ns | 30.1% |
| Stream | 9,952 ns | 7,615 ns | 23.5% |

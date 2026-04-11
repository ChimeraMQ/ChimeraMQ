# ADR 0004: Sequential Scan for Hot Storage Range Reads

## Status: Accepted

## Context

`Partition.ReadRange` originally performed per-offset index lookups (binary search
in sparse index → file seek → read). For large ranges, this resulted in many
random I/O operations.

## Decision

Replace with sequential scan: find the starting position via index, then read
consecutive records without re-looking up each offset.

## Why

- Reduces I/O from O(N * log(K)) to O(N) where N = messages, K = index entries
- Better cache utilization (sequential file access)
- Simpler code (single loop vs nested index+read)
- Adding `ReadAtSequential` returns `(data, nextPosition)` to avoid redundant
  length parsing

## Trade-offs

- Lost some parallelism potential (sequential scan can't be parallelized)
- Not beneficial for single-offset lookups (use `Read(offset)` instead)
- The `lastSeg` tracking prevents infinite retry loops on I/O errors

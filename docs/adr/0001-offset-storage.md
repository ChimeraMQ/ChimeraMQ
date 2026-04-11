# ADR 0001: JSON File-Based Offset Storage

## Status: Accepted (Temporary)

## Context

Consumer group offsets need to be persisted so consumers can resume after restart.
Options considered:
1. JSON files per consumer group
2. Internal compacted topic (`$chimera/offsets`)
3. Raft-backed key-value store

## Decision

We chose JSON files (`offset.json` per group) for initial implementation.

## Why

- Simplest to implement and debug
- No dependency on Raft consensus for single-node deployments
- Human-readable for debugging and manual inspection
- Atomic write via tmp+rename pattern prevents corruption

## Consequences

- Not suitable for multi-node failover (offsets are local to one node)
- Must migrate to replicated storage before cluster deployment (see TD-6 in roadmap)
- JSON overhead is minimal for offset data (small payload, infrequent writes)

## Migration Path

Migrate to internal compacted topic or Raft-backed store for cluster support.

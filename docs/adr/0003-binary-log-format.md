# ADR 0003: Binary Log Format for Raft Persistence

## Status: Accepted

## Context

Raft log entries were originally stored as JSON with base64-encoded binary data.
This caused ~2x size overhead due to base64 encoding and JSON structural characters.

## Decision

Switch to a custom binary format with magic number, version, and fixed-size headers.

## Format

```
[magic:4 "RALT"][version:2][firstIndex:8][count:4]
Per entry: [index:8][term:8][type:4][dataLen:4][data:N]
```

## Why

- ~50% size reduction vs JSON+base64
- Faster encode/decode (no JSON parsing, no base64)
- Fixed-size header enables seeking to specific entries
- Atomic write via tmp+rename preserves crash safety
- Version field supports future format changes

## Migration

- `Load()` tries binary format first, falls back to JSON for legacy files
- `Save()` always writes binary format
- Seamless migration on first save after upgrade

## Alternatives Considered

- **Protobuf**: Adds dependency, overkill for simple key-value entries
- **MessagePack**: Better than JSON but still larger than custom binary
- **mmap**: Complex to implement correctly, deferred to future work
